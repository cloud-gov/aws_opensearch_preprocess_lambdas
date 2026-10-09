## Overview
This repository contains tests for AWS Lambda functions that serve as preprocessors in the AWS OpenSearch pipeline. The Lambda functions have three primary purposes:

- **CloudWatch Metric Stream Transform**: Retrieve and attach relevant resource tags to metrics, filter sensitive data, and format metrics for downstream systems
- **CloudWatch Log Transform**: Add resource tags and filtering information to log data before processing
- **Log Group Subscription Manager**: Automatically assign subscription filters to newly created CloudWatch log groups for broker-created services

These Lambda functions run in AWS environments processing high-volume data streams and ensuring proper data enrichment and filtering before sending to S3 for ingestion into OpenSearch.

## Architecture
The preprocessor Lambda functions sit between CloudWatch services and the OpenSearch pipeline, acting as enrichment, filtering, partitioning, and routing layers:

- **Metric Stream** → **Metric Transform Lambda** → **S3 Bucket** (partitioned by org/space) → **OpenSearch**
- **CloudWatch Logs** → **Log Transform Lambda** → **S3 Bucket** (partitioned by org/space) → **OpenSearch**
- **New Log Groups** → **Subscription Manager Lambda** → **Configured Subscription Filters** → **S3 Bucket** → **OpenSearch** 

### Organization partitioning
Both transform Lambdas write their enriched output to S3 themselves, grouped by the
`Organization GUID` and `Space GUID` resource tags, so that each object key begins
with the owning Cloud Foundry org and space:

```
s3://<bucket>/orgs/<org-guid>/<space-guid>/<YYYY>/<MM>/<DD>/<HH>/<prefix>-<epoch>-<request-id>.json.gz
```

Those tag names must match `cf-tags.conf`, which renames them to `@cf.org_id` and
`@cf.space_id`. Renaming either one here drops the field from the indexed document.

Everything under `orgs/` is a real org GUID, so it cannot collide with any other
top-level prefix in the bucket. Records whose org GUID is missing or unsafe land at
the top level instead, keeping the reserved fallback out of `orgs/`:

```
s3://<bucket>/unknown-org/<space-guid>/<YYYY>/<MM>/<DD>/<HH>/<prefix>-<epoch>-<request-id>.json.gz
```

Each Lambda is deployed as a flat set of `.py` files at the root of its zip, so
the partitioning logic lives in a single shared module, `org_partitioning.py`,
which both transform Lambdas import as a top-level module:

```python
from org_partitioning import ORG_GUID_TAG, partition_for, put_all_partitions
```

**The Terraform that builds each zip must package `org_partitioning.py` alongside
the handler.** It is listed as a second `source` block in the `archive_file` for
both the `metrics_s3_ingestor` and `cloudwatch_s3_ingestor` modules. A zip
missing it fails at import time, before the handler ever runs.

The tests import the handlers as `lambda_functions.<handler>`, which does not put
`lambda_functions/` on `sys.path`. The root `conftest.py` adds it, so the same
import spelling works both under pytest and in the deployed Lambda.

Notable behaviors:

- The org and space levels fall back independently, so a bad space does not cost a
  record its org prefix.
- Records whose org GUID is missing, or whose value is not safe to interpolate
  into an S3 key, are still delivered — under the top-level `unknown-org/` prefix —
  so a bad tag never silently discards data.
- Only the exact reserved value is special-cased. An org GUID that merely
  resembles it (`unknown-org-2`) is treated as a real org and stays under `orgs/`.
- The metric transform uses the `metrics-` object prefix and the log transform
  uses `batch-`, so the two cannot be confused if they share a bucket.
- Keys are content-addressed: the date path and epoch come from the newest entry
  timestamp and the suffix is a SHA-256 digest of the object body. A Firehose
  retry of the same batch therefore resolves to the same key and overwrites it
  rather than adding a duplicate — nothing downstream deduplicates, since
  `logstash_parser.opensearch.document_id` is unset for these ingestors.
- Every partition is attempted even if an earlier one fails, so one bad
  partition cannot strand the rest. The batch still fails afterwards so Firehose
  retries it, and re-writing the partitions that already succeeded is a no-op.

Both transform Lambdas therefore require `S3_BUCKET_NAME` and `s3:PutObject` on
the destination bucket, and acknowledge their input records with empty data since
the payload is no longer returned inline to Firehose.

### Per-record result statuses

Both transform Lambdas classify each input record independently, and Firehose
treats the three statuses differently:

| Status | When | What Firehose does |
|---|---|---|
| `Ok` | The record produced at least one enriched entry, now in S3 | Delivers (an empty body, since the payload is in S3 already) |
| `ProcessingFailed` | The record could not be decoded or parsed | Writes the original record to `error_output_prefix` |
| `Dropped` | The record parsed, but nothing in it was enrichable | Treats it as handled and **discards it** |

A failing record only costs itself: the rest of the batch is still classified and
delivered. Entries are staged for S3 only after the whole record parses, so a
record can never be both written to S3 and returned as `ProcessingFailed` — which
would otherwise put the same data in both the destination and
`error_output_prefix` with no way to tell the copies apart.

`Dropped` is the one lossy path, and it is deliberate — an unrecognized namespace
or a resource with no org tag has nothing useful to index. Note that nothing
records those discards.

## What This Repository Tests

### Metric Stream Transform Lambda
- **Environment-Specific Processing**: Validates that the Lambda works correctly across different environments (dev, staging, prod)
- **Metric Processing**: Ensures expected metrics are processed and transformed correctly
- **Output Formatting**: Verifies that transformed data meets downstream OpenSearch requirements
- **Data Filtering**: Confirms sensitive information (account IDs, etc.) is properly removed before storage
- **Tag Enrichment**: Tests that appropriate resource tags are successfully attached to metrics
- **Org/Space Partitioning**: Confirms metrics are written under `orgs/<org-guid>/<space-guid>/`, that distinct orgs and spaces split into separate objects, and that untagged metrics fall back to `unknown-org/` rather than being dropped
- **Fail-closed configuration**: Confirms a missing `S3_BUCKET_NAME` or `ENVIRONMENT` raises rather than writing to the wrong place
- **Retry idempotency**: Confirms a retry of the same batch resolves to the same key and bytes, while genuinely different batches get different keys
- **Partial-failure isolation**: Confirms one failing partition does not stop the others from being written, and still fails the batch so Firehose retries
- **Per-record failure handling**: Confirms a malformed record returns `ProcessingFailed` with its original data so Firehose can write it to `error_output_prefix`, that one bad record does not cost the good records in the same batch, and that a failed record contributes nothing to S3

### CloudWatch Log Transform Lambda
- **Log Data Enrichment**: Validates that logs are properly enriched with resource tags and metadata
- **Log Filtering**: Ensures sensitive information is filtered from log entries before processing
- **Format Compatibility**: Verifies log data is formatted correctly for OpenSearch ingestion
- **Tag Enrichment**: Tests that appropriate resource tags are successfully attached to metrics
- **Org/Space Partitioning**: Confirms the `Space GUID` tag reaches both the S3 key and the indexed document, that distinct orgs and spaces split into separate objects, and that a missing space tag still lands under its real org prefix
- **Per-record failure handling**: Confirms a malformed line returns `ProcessingFailed` with its original data, that a record with one good and one bad line is not half-delivered, and that one bad record does not cost the good records in the same batch


### Log Group Subscription Manager Lambda
- **Corrent filtering**: Tests that new CloudWatch log groups are properly filtered
- **Broker Service Identification**: Validates correct identification of broker-created services
- **Subscription Filter Assignment**: Ensures appropriate subscription filters are automatically configured
- **Error Handling**: Tests proper handling of edge cases and failure scenarios

## Testing Approach
- **Unit Tests**: Individual function testing for each Lambda
- **Integration Tests**: End-to-end pipeline testing across all three Lambda functions
- **Environment Validation**: Cross-environment testing (dev, staging, production)
- **Error Recovery**: Testing failure modes and recovery mechanisms
