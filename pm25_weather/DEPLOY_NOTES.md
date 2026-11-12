# PM2.5 idempotent deployment

Run `bash deploy.sh` from the project root. AWS CLI credentials and sufficient AWS permissions must already be configured. Requires Python 3 and AWS CLI.

- Lambda: uses existing function names; deterministic ZIP and CodeSha256 comparison skip unchanged code. Changed code is uploaded via `update-function-code` (including analysis_batch). Configuration is updated only if runtime, handler, timeout, role or environment differs. No new Lambda function name or published version is created on repeat runs.
- Glue Job: create once; update job configuration only when different. The Glue script is uploaded only when its SHA-256 differs.
- S3: missing placeholder keys are created once; metadata and Glue script are overwritten at the SAME keys only if content changed. With S3 Versioning enabled, updating a changed file creates a new object version; unchanged files do not create new versions.
- Iceberg: CREATE TABLE only if absent. Existing Iceberg tables, snapshots and data are not deleted or overwritten by deploy.sh.
- Step Functions: create once, update definition in place only when different.
- EventBridge: same rule and target IDs are reused. Trigger mode `both` supports manual execution and S3 events.

**Required metadata**: `metadata.csv` is not present in this ZIP. `transform_batch` requires it at `s3://pm25-weather/metadata/station_coordinates.csv` but it can create by fetch_weather so don't worry about it. Supply the real CSV in the local `metadata/` directory or upload it to S3 before deployment. The script refuses to report success if it is missing. Do not synthesize coordinates.

**IAM**: LabRole must trust lambda.amazonaws.com, glue.amazonaws.com, states.amazonaws.com and (for event triggering) events.amazonaws.com, and have corresponding permissions. Existing AWS Academy LabRole may not permit editing its trust policy. If insufficient, the script fails rather than claiming success.

**Caveat**: This makes *deployment* idempotent; it does not guarantee that two S3 events or two Step Functions executions never generate duplicate processing output. That requires the pipeline's run_id/receipt and idempotency checks to be correct. This package has not been deployed or integration-tested against your AWS account.
