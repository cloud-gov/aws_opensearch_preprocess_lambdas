## Overview
This repository contains tests for AWS Lambda functions that serve as preprocessors in the AWS OpenSearch pipeline. The Lambda functions have three primary purposes:

- **CloudWatch Metric Stream Transform**: Retrieve and attach relevant resource tags to metrics, filter sensitive data, and format metrics for downstream systems
- **CloudWatch Log Transform**: Add resource tags and filtering information to log data before processing
- **Log Group Subscription Manager**: Automatically assign subscription filters to newly created CloudWatch log groups for broker-created services

These Lambda functions run in AWS environments processing high-volume data streams and ensuring proper data enrichment and filtering before sending to S3 for ingestion into OpenSearch.

## Architecture
The preprocessor Lambda functions sit between CloudWatch services and the OpenSearch pipeline, acting as enrichment, filtering, partitioning, and routing layers:

- **Metric Stream** → **Metric Transform Lambda** → **S3 Bucket** (partitioned by org) → **OpenSearch**
- **CloudWatch Logs** → **Log Transform Lambda** → **S3 Bucket** (partitioned by org) → **OpenSearch**
- **New Log Groups** → **Subscription Manager Lambda** → **Configured Subscription Filters** → **S3 Bucket** → **OpenSearch** 

### Organization partitioning
Both transform Lambdas write their enriched output to S3 themselves, grouped by the
`Organization GUID` resource tag, so that each object key begins with the owning
Cloud Foundry org:

```
s3://<bucket>/orgs/<org-guid>/<YYYY>/<MM>/<DD>/<HH>/<prefix>-<epoch>-<request-id>.json.gz
```

Everything under `orgs/` is a real org GUID, so it cannot collide with any other
top-level prefix in the bucket. Records whose org GUID is missing or unsafe land at
the top level instead, keeping the reserved fallback out of `orgs/`:

```
s3://<bucket>/unknown-org/<YYYY>/<MM>/<DD>/<HH>/<prefix>-<epoch>-<request-id>.json.gz
```

Each Lambda is deployed as a standalone `.py` file loaded directly from source, so
the two cannot import a shared module at runtime. The partitioning logic is instead
duplicated verbatim in both files, fenced between these markers:

```python
# --- BEGIN shared org partitioning ---
# --- END shared org partitioning ---
```

**When you change anything inside those markers, apply the identical change to both
files.** `tests/test_org_partition_parity.py` asserts the two copies are
byte-identical and runs the same behavioral cases against both, so a one-sided edit
fails the suite rather than silently diverging.

Notable behaviors:

- Records whose org GUID is missing, or whose value is not safe to interpolate
  into an S3 key, are still delivered — under the top-level `unknown-org/` prefix —
  so a bad tag never silently discards data. Note that S3 and ES metrics do not
  require the tag (unlike RDS and ElastiCache), so untagged buckets and domains
  will accumulate here.
- Only the exact reserved value is special-cased. An org GUID that merely
  resembles it (`unknown-org-2`) is treated as a real org and stays under `orgs/`.
- The metric transform uses the `metrics-` object prefix and the log transform
  uses `batch-`, so the two cannot be confused if they share a bucket.
- Keys carry the Lambda request ID because a timestamp alone is not unique across
  concurrent invocations writing the same org.

Both transform Lambdas therefore require `S3_BUCKET_NAME` and `s3:PutObject` on
the destination bucket, and acknowledge their input records with empty data since
the payload is no longer returned inline to Firehose.

## What This Repository Tests

### Metric Stream Transform Lambda
- **Environment-Specific Processing**: Validates that the Lambda works correctly across different environments (dev, staging, prod)
- **Metric Processing**: Ensures expected metrics are processed and transformed correctly
- **Output Formatting**: Verifies that transformed data meets downstream OpenSearch requirements
- **Data Filtering**: Confirms sensitive information (account IDs, etc.) is properly removed before storage
- **Tag Enrichment**: Tests that appropriate resource tags are successfully attached to metrics
- **Org Partitioning**: Confirms metrics are written to S3 under `orgs/<org-guid>/`, that untagged metrics fall back to the top-level `unknown-org/` prefix rather than being dropped, and that a missing destination bucket fails closed

### CloudWatch Log Transform Lambda
- **Log Data Enrichment**: Validates that logs are properly enriched with resource tags and metadata
- **Log Filtering**: Ensures sensitive information is filtered from log entries before processing
- **Format Compatibility**: Verifies log data is formatted correctly for OpenSearch ingestion
- **Tag Enrichment**: Tests that appropriate resource tags are successfully attached to metrics
- **Org Partitioning**: Confirms logs from different orgs land in separate org-prefixed objects


### Log Group Subscription Manager Lambda
- **Corrent filtering**: Tests that new CloudWatch log groups are properly filtered
- **Broker Service Identification**: Validates correct identification of broker-created services
- **Subscription Filter Assignment**: Ensures appropriate subscription filters are automatically configured
- **Error Handling**: Tests proper handling of edge cases and failure scenarios

## Testing Approach
- **Unit Tests**: Individual function testing for each Lambda
- **Duplication Parity**: Asserts the duplicated org-partitioning block is byte-identical across both transform Lambdas, and runs the same behavioral cases against both copies
- **Integration Tests**: End-to-end pipeline testing across all three Lambda functions
- **Environment Validation**: Cross-environment testing (dev, staging, production)
- **Error Recovery**: Testing failure modes and recovery mechanisms
