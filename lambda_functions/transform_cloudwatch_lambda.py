import boto3
import gzip
import io
import json
import os
import logging
import re
import uuid
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


def build_key(partition, name_prefix, request_id=None, now=None):
    """
    Builds the S3 key for one partition:

        orgs/<org>/<space>/<YYYY>/<MM>/<DD>/<HH>/<name_prefix>-<epoch>-<suffix>.json.gz

    A bad org GUID goes to unknown-org/ instead, so everything under orgs/ is a
    real org. The suffix keeps concurrent invocations from overwriting each other.
    """
    org_guid, space_guid = partition
    if request_id is not None and SAFE_KEY_SEGMENT.fullmatch(str(request_id)):
        suffix = str(request_id)
    else:
        suffix = uuid.uuid4().hex
    written_at = now or datetime.now()
    date_path = written_at.strftime("%Y/%m/%d/%H")
    epoch = int(written_at.timestamp())
    if org_guid == UNKNOWN_ORG_PARTITION:
        prefix = UNKNOWN_ORG_PARTITION
    else:
        prefix = f"{ORG_KEY_NAMESPACE}/{org_guid}"
    return (
        f"{prefix}/{space_guid}/{date_path}/" f"{name_prefix}-{epoch}-{suffix}.json.gz"
    )


def put_partition(
    s3_client, bucket, partition, entries, name_prefix="batch", request_id=None
):
    """
    Writes one gzipped NDJSON object for a single partition.
    """
    try:
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
            for entry in entries:
                gz_file.write((json.dumps(entry) + "\n").encode("utf-8"))
        compressed_data = buffer.getvalue()
        s3_key = build_key(partition, name_prefix, request_id)
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=compressed_data,
            ContentType="application/gzip",
            ContentEncoding="gzip",
            ServerSideEncryption="AES256",
        )
        logger.info(f"Successfully pushed {len(entries)} records to S3: {s3_key}")
    except Exception as e:
        logger.error(f"Unexpected error pushing to S3: {str(e)}")
        raise e


def put_all_partitions(s3_client, bucket, groups, name_prefix="batch", request_id=None):
    """
    Writes one object per partition, in a stable order.
    """
    for partition, entries in sorted(groups.items()):
        put_partition(
            s3_client,
            bucket,
            partition,
            entries,
            name_prefix=name_prefix,
            request_id=request_id,
        )


def request_id_from(context):
    """
    Returns the Lambda request ID, or None if it is unusable as a key segment.
    """
    request_id = getattr(context, "aws_request_id", None)
    if isinstance(request_id, str) and SAFE_KEY_SEGMENT.fullmatch(request_id):
        return request_id
    return None


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
                try:
                    logs = json.loads(line)
                    log_results = process_logs(
                        logs, rds_client, region, account_id, rds_prefix
                    )
                    if log_results:
                        processed_logs.extend(log_results)
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON: {e}. Line: {line}")
                    continue  # Skip to the next line if JSON decoding fails
            if processed_logs:
                for log in processed_logs:
                    s3_output[partition_for_log(log)].append(log)

                # Mark the record as successfully processed (but data is now in S3)
                output_record = {
                    "recordId": record["recordId"],
                    "result": "Ok",
                    "data": base64.b64encode(b"").decode("utf-8"),  # Empty data
                }
                output_records.append(output_record)
            else:
                # Mark the record as dropped if no logs were processed
                output_record = {
                    "recordId": record["recordId"],
                    "result": "Dropped",
                    "data": record["data"],
                }
                output_records.append(output_record)

        except Exception as e:
            logger.error(f"Error processing record {record['recordId']}: {str(e)}")
            # Consider marking the record as failed, or attempt to re-queue it.
            output_record = {
                "recordId": record["recordId"],
                "result": "ProcessingFailed",
                "data": record["data"],  # Keep original data for retry
            }
            output_records.append(output_record)

    # One object per org/space partition.
    put_all_partitions(
        s3_client,
        bucket,
        s3_output,
        name_prefix=LOG_BATCH_PREFIX,
        request_id=request_id_from(context),
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
