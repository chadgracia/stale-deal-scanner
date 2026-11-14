# stale-deal-scanner — project notes

- Deploys to the AWS Lambda `stale-deal-scanner` (us-east-1), handler `lambda_function.lambda_handler`.
  Push to `main` → `.github/workflows/deploy.yml` zips `lambda_function.py` and runs
  `aws lambda update-function-code` via the `github-actions-deploy` OIDC role.
- Single-file package: `lambda_function.py` only (stdlib + `boto3` from the Lambda runtime). No vendored deps.
- Secrets/config live only in Lambda env vars — `FORM_LAMBDA_URL`, `HMAC_SECRET` (no fallback; empty = no reset links
  minted or accepted), optional `HMAC_SECRET_OLD` (an old reset link only gets the "expired" text), `DRY_RUN`,
  `TEST_EMAIL_OVERRIDE`, `SCANNER_URL`, `ADMIN_KEY` — never in code. Deploys do not change env vars or the EventBridge schedule.
- Pipeline date fields come back as `YYYY/MM/DD` on read (e.g. `2024/11/26 06:19:44 -0500`).
- Every change must keep the safeguard: never send client emails unless state can be saved
  (`save_state` must succeed before notices go out; stop sending if a mid-run save fails).
- The daily scan (`run_scan`) is REPORT ONLY. Auto-obsolete and client removal notices have been removed:
  it must never move a deal to Obsolete, never PUT to Pipeline, and never email a client. Its only email is
  Chad's report ("Due for review" + "Heads-up"). Hold deals are skipped entirely.
- Client reminders will be a separate future feature (`compose_email` is kept, unused, for it).
- Tests: `python3 -m unittest discover tests` (fake S3/SES/Pipeline; asserts no PUTs and Chad-only email).
- Last Confirmed: deal date field `LAST_CONFIRMED_FIELD = "custom_label_4012491"` (write `YYYY-MM-DD`, reads back `YYYY/MM/DD`).
  One-time backfill: manual invoke `{"backfill_last_confirmed": "dry" | "execute"}` (never reachable from
  EventBridge or the Function URL). It only fills blanks, re-GETs each deal before writing, and PUTs that one field only.
- Shared Unconfirmed rule (for future work): a deal is Unconfirmed when `LAST_CONFIRMED_FIELD` is blank or older than
  its threshold. Threshold = Refresh field `custom_label_3994687` if set (0 = never Unconfirmed); otherwise 30 days
  if Seller Role `custom_label_3938748` = 7020357 (GP — syndicating new allocation), else 60 days.
  Hold deals are never Unconfirmed.

## Obsolete review page (Function URL)
- `?action=review&key=<ADMIN_KEY>` (GET, read-only): every Obsolete deal, live from Pipeline via
  `GET /deals.json?conditions[deal_stage]=2348038` — NO `[]` after deal_stage (the `[]` form is silently
  ignored and returns unfiltered deals); stage re-checked per row. Cached 15 min in
  `s3://full-pipeline-cache/obsolete-review/deals.json` ("Refresh list" bypasses). Duplicate flag uses the active
  `deals.json` snapshot (same company + side + primary contact). Filtering is client-side JS.
- `review_apply` (POST): only ticked deals, max 50 per request (1/sec), each re-read live and skipped if no longer
  Obsolete. PUT = `deal_stage_id` (Inquiry/Confirm; Anthropic Sell → Hold) + `custom_label_4012491`
  (LAST_CONFIRMED, pre-restore updated_at as YYYY-MM-DD). Nothing else, never a client email.
  Visible unticked rows are logged as "kept". More than 50 → "Continue" re-posts the rest.
- `review_undo` (POST): only for deals the log shows as "restored"; sets stage back to Obsolete only.
- Every decision is logged to `s3://full-pipeline-cache/obsolete-review/log.json` BEFORE the Pipeline write;
  if the log can't be saved, nothing is written. One summary email to Chad per batch.
- `ADMIN_KEY` blank → all three actions return "Nothing here." Wrong key → same.
- The daily report links the page (`SCANNER_URL?action=review&key=ADMIN_KEY`) when both env vars are set.
- Needs: Lambda timeout ≥ ~2 min for a 50-deal batch; role `s3:PutObject` on `full-pipeline-cache/obsolete-review/*`.

## SECURITY RULES — MANDATORY (read before any change)

This repo is PUBLIC, and so are its GitHub Actions logs. Secrets have leaked from Chad's repos 5-6 times. Never again.

1. No secret value ever goes in code, tests, docs, comments, commit messages, PR text or logs. That includes API keys, admin keys, HMAC/signing secrets, tokens, passwords, and any URL containing `key=`, `token=` or `sso=`. Secrets come from `os.environ[...]` (Lambda environment variables) or `s3://pipeline-token/` only. Tests use obvious fakes like `"test-key"`.
2. Every `aws lambda ...` command in a workflow must end with `> /dev/null`. Lambda API responses include every environment variable, and the logs are public. Never print the environment (`printenv`, `env`, `set -x`, or `print(os.environ)`).
3. Never log or print a secret, even partially, or a request URL that carries one.
4. `.github/scripts/secret_guard.py` runs automatically before every `git commit` and `git push` (via `.claude/settings.json`), and again in CI. If it blocks you, remove the secret and move the value to an environment variable. NEVER bypass, weaken, delete or skip the guard, and never use `--no-verify` or `secret-guard: allow` to get past it without Chad's explicit approval.
5. A new secret needs Chad to add a Lambda environment variable first. Tell him the exact variable name. Code that reads it ships after that.
6. Before finishing, run `python3 .github/scripts/secret_guard.py --tree` and report the result.
7. NEVER build anything that asks for, collects, uploads, stores, emails or processes client identity data — tax IDs, SSNs, passports, driver's licenses, other government IDs, or dates of birth. Chad does not want or need it; identity verification is handled by Rainmaker's own forms. If a task seems to need it, stop and ask Chad.
