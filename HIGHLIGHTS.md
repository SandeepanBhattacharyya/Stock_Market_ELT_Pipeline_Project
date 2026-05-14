# Code Highlights

Two pieces of code from this project that demonstrate real engineering depth —
not boilerplate, not tutorial code. Both solved actual production problems.

---

## 1. Airflow — Dynamic XCom-Keyed S3 Upload + Parallel Dimension Load + Resilient Slack Summary

This section of the DAG shows three things working together:

- **XCom-driven dynamic filenames** — the trading date resolved by the Polygon
  lookback is pushed to XCom in task `t01`, then pulled via Jinja templating
  directly inside the `LocalFilesystemToS3Operator` fields at execution time.
  This means the correct file is always uploaded even when the lookback resolves
  to a date other than today (e.g. Monday's run picking up Friday's data).

- **Parallel dimension loading inside a TaskGroup** — `DIM_SECURITY` and
  `DIM_DATE` have no dependency on each other, both sourcing from the already-merged
  CORE layer. Expressing them as `[merge_dim_security, merge_dim_date]` in the
  dependency chain halves the wall-clock time for the dimension phase.

- **`trigger_rule="all_done"` on the Slack summary** — the default `"all_success"`
  would silence the alert if any upstream task failed or was skipped. `"all_done"`
  guarantees the summary always fires, giving the on-call engineer immediate
  visibility into what loaded and what didn't — without needing to open the
  Airflow UI.

```python
# ── Upload to S3 with XCom-templated filename and key ──────────────────────
upload_files_to_s3 = LocalFilesystemToS3Operator(
    task_id="t03_upload_files_to_s3",
    filename=(
        "/tmp/eod_"
        "{{ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date')}}"
        ".csv"
    ),
    dest_bucket=S3_BUCKET,
    dest_key=(
        "market/bronze/eod/"
        "eod_prices_{{ ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date') }}.csv"
    ),
    aws_conn_id="aws_default",
    replace=True,
)

# ── Snowflake TaskGroup: sequential within, parallel where safe ─────────────
with TaskGroup(group_id="t04_snowflake_load") as snowflake_load:

    copy_to_raw      = SQLExecuteQueryOperator(task_id="s01_copy_to_raw",          ...)
    check_loaded     = SnowflakeCheckOperator(task_id="s02_check_eod_prices_exist", ...)
    premerge_metrics = SQLExecuteQueryOperator(task_id="s03_compute_premerge_metrics", ...)
    merge_core       = SQLExecuteQueryOperator(task_id="s04_merge_core_eod",        ...)
    merge_dim_security = SQLExecuteQueryOperator(task_id="s05_merge_dim_security",  ...)
    merge_dim_date     = SQLExecuteQueryOperator(task_id="s06_merge_dim_date",      ...)
    merge_fact       = SQLExecuteQueryOperator(task_id="s07_merge_fact_daily_price", ...)
    postmerge        = SQLExecuteQueryOperator(task_id="s08_compute_postmerge_metrics", ...)

    copy_to_raw >> check_loaded >> premerge_metrics >> merge_core
    #                                                      │
    merge_core >> [merge_dim_security, merge_dim_date] >> merge_fact >> postmerge
    #              └── both dims load in parallel ──┘

# ── Main pipeline chain ─────────────────────────────────────────────────────
download >> verify_file >> upload_files_to_s3 >> snowflake_load

# ── Slack summary fires regardless of upstream success/failure ──────────────
slack_summary = PythonOperator(
    task_id="t05_notify_slack_summary",
    python_callable=notify_slack_summary,
    trigger_rule="all_done",   # <── fires even if an upstream task failed/skipped
)

snowflake_load >> slack_summary
```

**Why this matters in an interview:**
> "I didn't just chain tasks sequentially. I identified that the two dimension
> loads had no dependency on each other and parallelized them. I also made the
> Slack alert unconditional — because a silent failure at 9 PM is worse than a
> noisy one."

---

## 2. SQL — RAW → CORE MERGE with CTE-Wrapped Deduplication + Reject Routing

**File:** `snowflake/02_raw_to_core/raw_to_core_merge.sql`

This is the core ELT transformation. It solves three problems at once:

- **Idempotent upsert** — using `MERGE` instead of `INSERT` means re-running
  the pipeline on the same date (e.g. after a data fix or file re-drop) will
  update existing rows rather than duplicate them. The pipeline is safe to
  re-trigger without manual cleanup.

- **Deduplication via CTE-wrapped window function** — if multiple files land
  for the same trading day (or the same file is re-processed), `ROW_NUMBER()
  OVER (PARTITION BY TRADE_DATE, SYMBOL ORDER BY _INGEST_TS DESC)` keeps only
  the most recently ingested record per `(date, symbol)` key. Snowflake raises
  a compilation error when window functions appear directly inside a `MERGE
  USING` subquery — wrapping in a named CTE resolves this cleanly.

- **Reject routing** — records with `VOLUME < 0` (data quality violations,
  tested with intentionally injected dummy rows) are never silently dropped
  or allowed into the clean layer. They are captured in `CORE.EOD_PRICES_REJECT`
  with a `REJECT_REASON` column, `_SRC_FILE` provenance, and `REJECT_TS`
  timestamp — fully auditable and re-processable.

```sql
MERGE INTO CORE.EOD_PRICES AS TGT
USING (
    -- CTE layer 1: normalize symbols (trim whitespace, uppercase)
    WITH raw_src_data AS (
        SELECT
            rw.TRADE_DATE,
            UPPER(TRIM(rw.SYMBOL))  AS SYMBOL,
            rw.OPEN, rw.HIGH, rw.LOW, rw.CLOSE, rw.VOLUME,
            rw._SRC_FILE, rw._INGEST_TS
        FROM RAW.RAW_EOD_PRICES rw
    ),
    -- CTE layer 2: deduplicate — keep latest ingest per (date, symbol)
    -- NOTE: window function is inside a named CTE to avoid Snowflake
    --       compilation error when using ROW_NUMBER() directly in MERGE USING
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY TRADE_DATE, SYMBOL
                ORDER BY _INGEST_TS DESC          -- latest file wins on re-ingest
            ) AS row_number_val
        FROM raw_src_data
        WHERE VOLUME >= 0                         -- exclude invalid rows before ranking
    )
    SELECT TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, VOLUME
    FROM ranked
    WHERE row_number_val = 1                      -- one row per (date, symbol)

) AS SRC
ON  TGT.TRADE_DATE = SRC.TRADE_DATE
AND TGT.SYMBOL     = SRC.SYMBOL

WHEN MATCHED THEN
    UPDATE SET
        TGT.OPEN     = SRC.OPEN,
        TGT.HIGH     = SRC.HIGH,
        TGT.LOW      = SRC.LOW,
        TGT.CLOSE    = SRC.CLOSE,
        TGT.VOLUME   = SRC.VOLUME,
        TGT.LOAD_TS  = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, VOLUME, LOAD_TS)
    VALUES (SRC.TRADE_DATE, SRC.SYMBOL, SRC.OPEN, SRC.HIGH,
            SRC.LOW, SRC.CLOSE, SRC.VOLUME, CURRENT_TIMESTAMP());


-- ── Reject routing: invalid records quarantined with audit trail ─────────────
-- Runs after the MERGE. Captures rows excluded above (VOLUME < 0).
INSERT INTO CORE.EOD_PRICES_REJECT
    (TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, VOLUME,
     REJECT_REASON, _SRC_FILE, _INGEST_TS)
SELECT
    TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, VOLUME,
    'NEGATIVE_VOLUME'   AS REJECT_REASON,
    _SRC_FILE,
    _INGEST_TS
FROM RAW.RAW_EOD_PRICES
WHERE VOLUME < 0;
```

**Why this matters in an interview:**
> "The MERGE is idempotent — I can re-run it against the same date without
> creating duplicates. The CTE structure wasn't just style; Snowflake throws
> a compilation error if you try to use ROW_NUMBER() directly in the USING
> clause. And bad rows don't get silently dropped — they land in a reject table
> with a reason code, a source file reference, and a timestamp, so nothing is
> ever lost."

---

## Quick Reference — What Each Piece Demonstrates

| Skill | Evidence |
|---|---|
| Airflow DAG design | TaskGroup, XCom, Jinja templating, parallel tasks, trigger rules |
| Airflow operators | `PythonOperator`, `LocalFilesystemToS3Operator`, `SQLExecuteQueryOperator`, `SnowflakeCheckOperator` |
| Snowflake ELT | Multi-CTE MERGE, window functions, idempotent upsert pattern |
| Data quality | Reject table with audit trail, validation before transformation |
| Production thinking | Re-runnable pipelines, unconditional alerting, parallel load optimization |
