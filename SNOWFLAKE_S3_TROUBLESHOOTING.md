# Snowflake External Stage → S3 Troubleshooting

> **Resolved:** May 8, 2026  
> **Environment:** Snowflake on `AWS_AP_SOUTHEAST_7` (Thailand) → S3 bucket in `ap-southeast-1` (Singapore)

---

## The Error

```
Error assuming AWS_ROLE:
User: arn:aws:iam::565139241044:user/hr7q1000-s is not authorized
to perform: sts:AssumeRole on resource:
arn:aws:iam::224976804920:role/snowflake_access_control_S3
```

This fired when running `LIST @S3_EOD_STAGE` after setting up the Snowflake
Storage Integration. The trust policy and IAM role looked correct — but the
error persisted. It turned out to be **three separate issues stacked on top
of each other**, each one hiding behind the previous fix.

---

## What Made This Hard

The error message points directly at IAM (`not authorized to perform sts:AssumeRole`),
which leads you straight to the trust policy. The trust policy was fine.
The real causes were spread across S3, the AWS Billing Console, and IAM Account
Settings — none of which the error message hints at.

---

## Root Causes & Fixes (in sequence)

### Issue 1 — Missing S3 Bucket Policy

**Why it happens:**  
Snowflake's IAM user (`account 565139241044`) lives in a **different AWS account**
than the IAM role (`account 224976804920`). For cross-account S3 access, the
bucket itself must explicitly grant the role permission via a bucket policy.
The role's own permissions are not enough on their own — S3 evaluates both
sides independently.

**Fix:**  
Added a cross-account bucket policy to `rbf-stocks-daily-sb02`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SnowflakeStageAccess",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::224976804920:role/snowflake_access_control_S3"
      },
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:GetObjectAttributes"
      ],
      "Resource": "arn:aws:s3:::rbf-stocks-daily-sb02/*"
    },
    {
      "Sid": "SnowflakeListAccess",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::224976804920:role/snowflake_access_control_S3"
      },
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::rbf-stocks-daily-sb02"
    }
  ]
}
```

**Navigation:** AWS Console → S3 → `rbf-stocks-daily-sb02` → Permissions → Bucket Policy → Edit

---

### Issue 2 — ap-southeast-7 (Thailand) Region Not Opted In

**Why it happens:**  
`ap-southeast-7` is a newer AWS opt-in region. The AWS account had never
enabled it, so any STS `AssumeRole` request originating from Snowflake
in that region was being silently rejected at the account level — before
it even reached IAM evaluation.

**Fix:**  
Opted into the region via the AWS Billing Console:

```
https://console.aws.amazon.com/billing/home#/account
→ Scroll to "AWS Regions"
→ Find "Asia Pacific (Thailand) ap-southeast-7"
→ Click Enable → Confirm
```

---

### Issue 3 — STS Regional Endpoint Not Activated for ap-southeast-7

**Why it happens:**  
Opting into a new region does **not** automatically activate its STS endpoint.
For newer opt-in regions, AWS requires the STS endpoint to be explicitly
toggled on in IAM Account Settings. Without this, Snowflake's `AssumeRole`
call fails even with a correct trust policy and an opted-in region.

**Fix:**  
Activated the STS endpoint via IAM:

```
AWS Console → IAM → Account Settings
→ Scroll to "Security Token Service (STS)"
→ Find "Asia Pacific (Thailand) ap-southeast-7"
→ Toggle to Active → Save
```

---

## Resolution Summary

| # | Issue | Where Fixed | Status |
|---|---|---|---|
| 1 | Missing cross-account S3 bucket policy | S3 → Permissions → Bucket Policy | ✅ Resolved |
| 2 | ap-southeast-7 region not opted in | Billing Console → AWS Regions | ✅ Resolved |
| 3 | STS endpoint inactive for ap-southeast-7 | IAM → Account Settings → STS | ✅ Resolved |

---

## Key Integration Values

These values are retrieved via `DESC INTEGRATION MY_S3_INTEGRATION1` in Snowflake
and must be copied exactly into the AWS IAM trust policy:

| Field | Value | Where Used |
|---|---|---|
| `STORAGE_AWS_IAM_USER_ARN` | `arn:aws:iam::565139241044:user/hr7q1000-s` | IAM Trust Policy → Principal |
| `STORAGE_AWS_EXTERNAL_ID` | `WK13577_SFCRole=5_YU8QJLh...` | IAM Trust Policy → Condition → `sts:ExternalId` |
| `STORAGE_AWS_ROLE_ARN` | `arn:aws:iam::224976804920:role/snowflake_access_control_S3` | Snowflake Storage Integration |

> ⚠️ **If the storage integration is ever recreated**, `STORAGE_AWS_IAM_USER_ARN`
> and `STORAGE_AWS_EXTERNAL_ID` will change. The IAM trust policy must be updated
> to match, otherwise the stage will break silently.

---

## Important Notes

- **Cross-region access works fine** — Snowflake in Thailand reading an S3 bucket
  in Singapore is fully supported. The bucket does not need to be in the same
  region as the Snowflake account.

- **ap-southeast-7 is an opt-in region** — this entire class of issue only
  applies to newer AWS regions. Established regions (us-east-1, eu-west-1, etc.)
  have STS active by default and do not require opt-in.

- **The error message is misleading** — `not authorized to perform sts:AssumeRole`
  points at IAM, but the actual blockers were an S3 bucket policy and two
  account-level region/STS settings. Always check cross-account bucket policies
  and regional STS activation before diving into trust policy debugging.
