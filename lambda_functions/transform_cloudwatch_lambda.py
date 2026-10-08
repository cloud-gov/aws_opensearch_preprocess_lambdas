import boto3
import gzip
import hashlib
import io
import json
import os
import logging
import re
from collections import defaultdict
from datetime import datetime
from functools import lru_cache
import base64

logger = logging.getLogger()
logger.setLevel(logging.INFO)

LOG_BATCH_PREFIX = "batch"


# --- BEGIN shared org partitioning ---
# Duplicated verbatim in the other transform lambda. Apply every edit to both.

# Tag names must match cf-tags.conf, which renames them to @cf.org_id and
# @cf.space_id downstream.
ORG_GUID_TAG = "Organization GUID"
SPACE_GUID_TAG = "Space GUID"

SAFE_KEY_SEGMENT = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")

UNKNOWN_ORG_PARTITION = "unknown-org"
UNKNOWN_SPACE_PARTITION = "unknown-space"
ORG_KEY_NAMESPACE = "orgs"


def partition_for(entry, source=None):
    """
    Returns the (org, space) S3 partition for one enriched record. `source` only
    names the record in warnings.
    """
    tags = entry.get("Tags", {})
    return (
        guid_segment(
            tags.get(ORG_GUID_TAG), ORG_GUID_TAG, UNKNOWN_ORG_PARTITION, source
        ),
        guid_segment(
            tags.get(SPACE_GUID_TAG), SPACE_GUID_TAG, UNKNOWN_SPACE_PARTITION, source
        ),
    )


def guid_segment(value, tag, fallback, source):
    """
    Returns a tag value safe to use as one S3 key segment, else `fallback`.
    """
    if not value:
        logger.warning(
            "Missing %s tag for %s; using %s partition", tag, source, fallback
        )
        return fallback
    if not isinstance(value, str) or not SAFE_KEY_SEGMENT.fullmatch(value):
        logger.warning(
            "Unsafe %s value for %s; using %s partition", tag, source, fallback
        )
        return fallback
    return value


def batch_time(entries):
    """
    Returns the datetime for a batch's date prefix, taken from the newest entry
    timestamp (epoch ms) so the key does not depend on when the Lambda ran.
    Falls back to now if no entry carries a usable timestamp.
    """
    stamps = [
        e.get("timestamp")
        for e in entries
        if isinstance(e.get("timestamp"), (int, float))
        and not isinstance(e.get("timestamp"), bool)
    ]
    if not stamps:
        logger.warning("No usable timestamp in batch; date prefix will not be stable")
        return datetime.now()
    return datetime.fromtimestamp(max(stamps) / 1000)


def body_digest(raw):
    """
    Returns the key suffix for a batch body. Content-addressed so that a retry
    of the same batch overwrites its object instead of adding a duplicate.
    """
    return hashlib.sha256(raw).hexdigest()[:32]


def build_key(partition, name_prefix, digest, written_at):
    """
    Builds the S3 key for one partition:

        orgs/<org>/<space>/<YYYY>/<MM>/<DD>/<HH>/<name_prefix>-<epoch>-<digest>.json.gz

    A bad org GUID goes to unknown-org/ instead, so everything under orgs/ is a
    real org. Every component derives from the batch content, so the key is
    stable across retries.
    """
    org_guid, space_guid = partition
    date_path = written_at.strftime("%Y/%m/%d/%H")
    epoch = int(written_at.timestamp())
    if org_guid == UNKNOWN_ORG_PARTITION:
        prefix = UNKNOWN_ORG_PARTITION
    else:
        prefix = f"{ORG_KEY_NAMESPACE}/{org_guid}"
    return (
        f"{prefix}/{space_guid}/{date_path}/" f"{name_prefix}-{epoch}-{digest}.json.gz"
    )


def put_partition(s3_client, bucket, partition, entries, name_prefix="batch"):
    """
    Writes one gzipped NDJSON object for a single partition.
    """
    raw = b"".join((json.dumps(entry) + "\n").encode("utf-8") for entry in entries)
    buffer = io.BytesIO()
    # mtime=0 keeps the compressed bytes identical for identical input.
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz_file:
        gz_file.write(raw)
    s3_key = build_key(partition, name_prefix, body_digest(raw), batch_time(entries))
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/gzip",
        ContentEncoding="gzip",
        ServerSideEncryption="AES256",
    )
    logger.info(f"Successfully pushed {len(entries)} records to S3: {s3_key}")
    return s3_key


def put_all_partitions(s3_client, bucket, groups, name_prefix="batch"):
    """
    Writes one object per partition.

    Every partition is attempted even if an earlier one fails, so a single bad
    partition cannot strand the others. If any failed, raises afterwards so the
    caller fails the batch and Firehose retries it; keys are content-addressed,
    so re-writing the partitions that already succeeded is a no-op.
    """
    failed = []
    for partition, entries in sorted(groups.items()):
        try:
            put_partition(s3_client, bucket, partition, entries, name_prefix)
        except Exception as e:
            logger.error("Failed to push partition %s to S3: %s", partition, str(e))
            failed.append(partition)
    if failed:
        raise RuntimeError(
            f"Failed to push {len(failed)} of {len(groups)} partitions to S3: {failed}"
        )


# --- END shared org partitioning ---


def lambda_handler(event, context):
    """
    This function processes CloudWatch Logs from Firehose, enriches them with RDS tags,
    and stores them in S3.
    """
    output_records = []
    # (org, space) -> log entries for that partition
    s3_output = defaultdict(list)

    try:
        region = boto3.Session().region_name or os.environ.get("AWS_REGION")
        if not region:
            raise ValueError(
                "AWS_REGION environment variable or session region is required"
            )
        bucket = os.environ.get("S3_BUCKET_NAME")
        if not bucket:
            logger.error("S3_BUCKET_NAME environment variable not set.")
            raise ValueError("S3_BUCKET_NAME environment variable must be set.")
        account_id = os.environ.get("ACCOUNT_ID")
        if not account_id:
            raise ValueError("ACCOUNT_ID environment variable is required")

        rds_prefix = make_prefixes()  # Fetch prefix based on environment

        # Initialize clients
        s3_client = boto3.client("s3", region_name=region)
        rds_client = boto3.client("rds", region_name=region)

    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        return {"records": []}  # Fail processing if initialization fails
    except Exception as e:
        logger.error(f"Initialization error: {str(e)}")
        return {"records": []}

    for record in event["records"]:
        try:
            # Decode and decompress the CloudWatch Logs data
            compressed_data = base64.b64decode(record["data"])
            pre_json_value = gzip.decompress(compressed_data)

            processed_logs = []
            for line in pre_json_value.strip().splitlines():
                logs = json.loads(line)
                log_results = process_logs(
                    logs, rds_client, region, account_id, rds_prefix
                )
                if log_results:
                    processed_logs.extend(log_results)
        except Exception as e:
            # Hand the record back for Firehose to write to error_output_prefix
            # rather than failing the whole batch.
            logger.error(
                "Error processing record %s: %s", record.get("recordId"), str(e)
            )
            output_records.append(
                {
                    "recordId": record["recordId"],
                    "result": "ProcessingFailed",
                    "data": record["data"],
                }
            )
            continue

        if processed_logs:
            for log in processed_logs:
                s3_output[partition_for_log(log)].append(log)

            # Mark the record as successfully processed (but data is now in S3)
            output_records.append(
                {
                    "recordId": record["recordId"],
                    "result": "Ok",
                    "data": base64.b64encode(b"").decode("utf-8"),  # Empty data
                }
            )
        else:
            # Mark the record as dropped if no logs were processed
            output_records.append(
                {
                    "recordId": record["recordId"],
                    "result": "Dropped",
                    "data": record["data"],
                }
            )

    # One object per org/space partition.
    put_all_partitions(
        s3_client,
        bucket,
        s3_output,
        name_prefix=LOG_BATCH_PREFIX,
    )
    return {"records": output_records}


def partition_for_log(log):
    """
    Returns the (org, space) S3 partition for an enriched log entry.
    """
    return partition_for(log, log.get("logGroup"))


def make_prefixes():
    """
    Determines the prefix based on the ENVIRONMENT variable.
    """
    environment = os.getenv("ENVIRONMENT")
    if not environment:
        raise RuntimeError("ENVIRONMENT is required")

    rds_prefix = "cg-aws-broker-"
    environment_suffixes = {
        "production": "prod",
        "staging": "stage",
        "development": "dev",
    }

    if environment not in environment_suffixes:
        raise RuntimeError(f"Invalid ENVIRONMENT: {environment}")

    rds_prefix += environment_suffixes[environment]
    return rds_prefix


def process_logs(logs, client, region, account_id, rds_prefix):
    """
    Enriches CloudWatch Logs with tags.
    """
    try:
        return_logs = []
        resource_name = logs["logGroup"].split("/")[4]
        tags = get_resource_tags_from_log(
            resource_name, client, region, account_id, rds_prefix
        )

        if len(tags.keys()) > 0:
            for event in logs["logEvents"]:
                entry = {
                    "logGroup": logs["logGroup"],
                    "logStream": logs["logStream"],
                    "message": event["message"],
                    "timestamp": event["timestamp"],
                    "Tags": tags,
                }
                return_logs.append(entry)
        else:
            return None

    except Exception as e:
        logger.error(f"Could not process logs: {e}")
        return None
    return return_logs


def get_resource_tags_from_log(
    resource_name, client, region, account_id, rds_prefix
) -> dict:
    """
    Retrieves tags from an instance based on its ARN.
    """
    tags = {}
    try:
        if resource_name is not None and resource_name.startswith(rds_prefix):
            arn = f"arn:aws-us-gov:rds:{region}:{account_id}:db:{resource_name}"
            tags = get_tags_from_arn(arn, client)
    except Exception as e:
        logger.error(f"Error getting tags for resource {resource_name}: {e}")
    return tags


@lru_cache(maxsize=256)
def get_tags_from_arn(arn, client) -> dict:
    """
    Retrieves tags from an instance using its ARN.  Uses lru_cache to minimize API calls.
    """
    tags = {}
    if ":db:" in arn:
        try:
            response = client.list_tags_for_resource(ResourceName=arn)
            tags = {tag["Key"]: tag["Value"] for tag in response.get("TagList", [])}
            if ORG_GUID_TAG not in tags:
                logger.warning(f"{ORG_GUID_TAG} tag missing for ARN: {arn}")
                return {}
        except Exception as e:
            logger.error(f"Could not fetch tags for ARN {arn}: {e}")
    return tags
