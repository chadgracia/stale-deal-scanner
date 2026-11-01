# stale-deal-scanner — project notes

- Deploys to the AWS Lambda `stale-deal-scanner` (us-east-1), handler `lambda_function.lambda_handler`.
  Push to `main` → `.github/workflows/deploy.yml` zips `lambda_function.py` and runs
  `aws lambda update-function-code` via the `github-actions-deploy` OIDC role.
- Single-file package: `lambda_function.py` only (stdlib + `boto3` from the Lambda runtime). No vendored deps.
- Secrets/config live only in Lambda env vars — `FORM_LAMBDA_URL`, `HMAC_SECRET`, `DRY_RUN`,
  `TEST_EMAIL_OVERRIDE`, `SCANNER_URL` — never in code. Deploys do not change env vars or the EventBridge schedule.
- Pipeline date fields come back as `YYYY/MM/DD` on read (e.g. `2024/11/26 06:19:44 -0500`).
- Every change must keep the safeguard: never send client emails unless state can be saved
  (`save_state` must succeed before notices go out; stop sending if a mid-run save fails).
- Auto-obsolete is to be removed: the scanner must never move a deal to Obsolete going forward.
