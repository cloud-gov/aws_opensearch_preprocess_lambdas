import json
import base64
import boto3
import gzip
import io
import logging
import os
import re
import uuid
from collections import defaultdict
from datetime import datetime
from functools import lru_cache

logger = logging.getLogger()
logger.setLevel(logging.INFO)
default_keys_to_remove = ["metric_stream_name", "account_id", "region"]
EXPECTED_NAMESPACES = ["AWS/S3", "AWS/ES", "AWS/RDS", "AWS/ElastiCache"]

# Object name prefix for metric batches, distinct from the log transform's so
# the two Lambdas cannot be confused if they share a bucket.
METRIC_BATCH_PREFIX = "metrics"


# --- BEGIN shared org partitioning ---
# Duplicated verbatim in transform_lambda.py and transform_cloudwatch_lambda.py:
# each Lambda is deployed as a standalone .py file, so there is no shared module
# to import at runtime. tests/test_org_partition_parity.py fails if the copies
# drift, so apply edits to BOTH files.

# Tags holding the org and space identifiers used as the S3 partition.
ORG_GUID_TAG = "Organization GUID"
SPACE_GUID_TAG = "Space_GUID"

# Tag values are external input interpolated into an S3 key, so they are
# validated against an allowlist rather than a denylist (AGENTS.md 5.1).
SAFE_KEY_SEGMENT = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")

# Fallbacks for a missing or unsafe GUID, so a bad tag never discards data.
# A bad org lands at the top level, keeping orgs/ free of anything that is not a
# real org GUID; a bad space only demotes the space level.
UNKNOWN_ORG_PARTITION = "unknown-org"
UNKNOWN_SPACE_PARTITION = "unknown-space"

# Top-level namespace for real org GUIDs, so per-org data cannot collide with
# another top-level prefix in the bucket.
ORG_KEY_NAMESPACE = "orgs"


def partition_for(entry, source=None):
    """
    Returns the (org, space) S3 partition for one enriched record.

    `source` only names the record in warnings; it never reaches the S3 key.
    Each level falls back independently, so a bad space does not cost the record
    its org prefix.
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
    Validates one tag value for use as a single S3 key segment, returning
    `fallback` (and warning) when it is missing or unsafe.
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
    Builds the S3 key for one (org, space) partition's object:

        orgs/<org-guid>/<space-guid>/<YYYY>/<MM>/<DD>/<HH>/<name_prefix>-<epoch>-<suffix>.json.gz

    The unknown-org fallback is deliberately NOT nested under orgs/, so that
    everything under orgs/ is a real org GUID:

        unknown-org/<space-guid>/<YYYY>/<MM>/<DD>/<HH>/<name_prefix>-<epoch>-<suffix>.json.gz

    The space level is always present, so the segment after the org is never
    mistaken for a date.

    The suffix exists because a timestamp alone is not unique: two concurrent
    invocations writing the same partition in the same second would otherwise
    resolve to the same key and one would overwrite the other. Uses the Lambda
    request ID when safe, else a random value.
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
    Writes one gzipped NDJSON object for a single (org, space) partition.
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
    Writes one object per (org, space) partition, in a stable order.
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
    Returns the Lambda request ID, or None when the context has no usable
    string ID, in which case build_key falls back to a random suffix.
    """
    request_id = getattr(context, "aws_request_id", None)
    if isinstance(request_id, str) and SAFE_KEY_SEGMENT.fullmatch(request_id):
        return request_id
    return None


# --- END shared org partitioning ---


@lru_cache(maxsize=1)
def get_clients(region):
    return {
        "s3": boto3.client("s3", region_name=region),
        "es": boto3.client("es", region_name=region),
        "rds": boto3.client("rds", region_name=region),
        "elasticache": boto3.client("elasticache", region_name=region),
    }


def partition_for_metric(metric):
    """
    Returns the (org, space) S3 partition for an enriched metric, naming the
    metric in any warning so a bad tag is actionable.
    """
    return partition_for(
        metric, f"{metric.get('namespace')}/{metric.get('metric_name')}"
    )


def lambda_handler(event, context):
    output_records = []
    # (org GUID, space GUID) -> enriched metrics for that partition, accumulated
    # across every input record so each org/space gets one object per invocation.
    s3_output = defaultdict(list)
    region = boto3.Session().region_name or os.environ.get("AWS_REGION")
    rds_prefix, s3_prefix, domain_prefix, redis_prefix = make_prefixes()
    account_id = os.environ.get("ACCOUNT_ID")
    bucket = os.environ.get("S3_BUCKET_NAME")
    if not bucket:
        # Fail closed: metrics are no longer returned inline, so without a
        # destination they would be silently discarded.
        logger.error("S3_BUCKET_NAME environment variable not set.")
        raise ValueError("S3_BUCKET_NAME environment variable must be set.")
    # Get cached clients
    clients = get_clients(region)
    s3_client = clients["s3"]
    es_client = clients["es"]
    rds_client = clients["rds"]
    redis_client = clients["elasticache"]
    try:
        for record in event["records"]:
            pre_json_value = base64.b64decode(record["data"])
            processed_metrics = []
            for line in pre_json_value.strip().splitlines():
                metric = json.loads(line)
                for key in default_keys_to_remove:
                    metric.pop(key, None)
                metric_results = process_metric(
                    metric,
                    region,
                    s3_client,
                    s3_prefix,
                    es_client,
                    domain_prefix,
                    rds_client,
                    rds_prefix,
                    redis_client,
                    redis_prefix,
                    account_id,
                )
                if metric_results is not None:
                    metric_results["dimensions"].pop("ClientId", None)
                    processed_metrics.append(metric_results)

            if processed_metrics:
                for metric in processed_metrics:
                    s3_output[partition_for_metric(metric)].append(metric)

                # Mark the record as successfully processed (but data is now in S3)
                output_record = {
                    "recordId": record["recordId"],
                    "result": "Ok",
                    "data": base64.b64encode(b"").decode("utf-8"),  # Empty data
                }
                output_records.append(output_record)
            else:
                output_record = {
                    "recordId": record["recordId"],
                    "result": "Dropped",
                    "data": record["data"],
                }
                output_records.append(output_record)
            logger.info(f"Processed record with {len(processed_metrics)} metrics")
    except Exception as e:
        logger.error(f"Error processing metrics: {str(e)}")
        raise e

    # Push the metrics to S3, one object per org/space partition.
    put_all_partitions(
        s3_client,
        bucket,
        s3_output,
        name_prefix=METRIC_BATCH_PREFIX,
        request_id=request_id_from(context),
    )
    return {"records": output_records}


def make_prefixes():
    environment = os.getenv("ENVIRONMENT")
    if not environment:
        RuntimeError("environment is required")
    # Prefix setup zone
    s3_prefix = (
        f"{environment}-cg-" if environment in ["development", "staging"] else "cg-"
    )
    domain_prefix = "cg-broker-"
    rds_prefix = "cg-aws-broker-"
    redis_prefix = ""
    if environment == "production":
        domain_prefix = domain_prefix + "prd-"
        rds_prefix = rds_prefix + "prod"
        redis_prefix = "prd-"
    if environment == "staging":
        domain_prefix = domain_prefix + "stg-"
        rds_prefix = rds_prefix + "stage"
        redis_prefix = "stg-"
    if environment == "development":
        domain_prefix = domain_prefix + "dev-"
        rds_prefix = rds_prefix + "dev"
        redis_prefix = "dev-"

    return rds_prefix, s3_prefix, domain_prefix, redis_prefix


def process_metric(
    metric,
    region,
    s3_client,
    s3_prefix,
    es_client,
    domain_prefix,
    rds_client,
    rds_prefix,
    redis_client,
    redis_prefix,
    account_id,
):
    try:
        namespace = metric.get("namespace")
        if namespace not in EXPECTED_NAMESPACES:
            logger.error(
                f"Hello developer, you need to add the following metric to the lambda function: {str(namespace)}"
            )
            return None

        tags = get_resource_tags_from_metric(
            metric,
            region,
            s3_client,
            s3_prefix,
            es_client,
            domain_prefix,
            rds_client,
            rds_prefix,
            redis_client,
            redis_prefix,
            account_id,
        )
        if len(tags.keys()) > 0:
            metric["Tags"] = tags
            return metric
        else:
            return None
    except Exception as e:
        logger.error(f"Could not process metric: {e}")
        return None


def get_resource_tags_from_metric(
    metric,
    region,
    s3_client,
    s3_prefix,
    es_client,
    domain_prefix,
    rds_client,
    rds_prefix,
    redis_client,
    redis_prefix,
    account_id,
) -> dict:
    tags = {}
    try:
        namespace = metric.get("namespace")
        dimensions = metric.get("dimensions", {})
        if namespace == "AWS/S3":
            bucket_name = dimensions.get("BucketName")
            if bucket_name.startswith(s3_prefix):
                tags = get_tags_from_name(bucket_name, "S3", s3_client)
        elif namespace == "AWS/ES":
            domain_name = dimensions.get("DomainName")
            if domain_name.startswith(domain_prefix):
                arn = f"arn:aws-us-gov:es:{region}:{account_id}:domain/{domain_name}"
                tags = get_tags_from_arn(arn, es_client)
        elif namespace == "AWS/RDS":
            db_name = dimensions.get("DBInstanceIdentifier")
            if db_name is not None and db_name.startswith(rds_prefix):
                arn = f"arn:aws-us-gov:rds:{region}:{account_id}:db:{db_name}"
                # copy avoids mutating the cached value returned by get_tags_from_arn
                result_tags = get_tags_from_arn(arn, rds_client).copy()
                if result_tags and metric.get("metric_name") == "FreeStorageSpace":
                    size = get_rds_description(rds_client, db_name)
                    # assign the reference between assigning db_size
                    tags = result_tags
                    tags.update({"db_size": size})
                else:
                    tags = result_tags
        elif namespace == "AWS/ElastiCache":
            cache_name = dimensions.get("CacheClusterId")
            if cache_name is not None and cache_name.startswith(redis_prefix):
                # Try cluster first
                cluster_arn = f"arn:aws-us-gov:elasticache:{region}:{account_id}:cluster:{cache_name}"
                tags = get_tags_from_arn(cluster_arn, redis_client)

                # If cluster tags are empty, try to get replication group
                if not tags:
                    try:
                        # Get cluster info to find replication group
                        cluster_info = redis_client.describe_cache_clusters(
                            CacheClusterId=cache_name, ShowCacheNodeInfo=False
                        )
                        replication_group_id = cluster_info["CacheClusters"][0].get(
                            "ReplicationGroupId"
                        )

                        if replication_group_id:
                            rg_arn = f"arn:aws-us-gov:elasticache:{region}:{account_id}:replicationgroup:{replication_group_id}"
                            tags = get_tags_from_arn(rg_arn, redis_client)

                        if tags == {}:
                            logger.info(
                                "RG ARN: %s, Cluster ARN: %s", rg_arn, cluster_arn
                            )
                    except Exception as e:
                        logger.error("Could not get replication group info: %s", str(e))
    except Exception as e:
        logger.error(f"Error with getting tags for resource: {e}")
    return tags


@lru_cache(maxsize=512)
def get_rds_description(rds_client, db_name):
    try:
        size = rds_client.describe_db_instances(DBInstanceIdentifier=db_name)
        return size["DBInstances"][0]["AllocatedStorage"]
    except Exception as e:
        logger.error(f"Error with getting rds_description: {e}")


@lru_cache(maxsize=256)
def get_tags_from_name(name, type, client) -> dict:
    tags = {}
    if type == "S3":
        try:
            response = client.get_bucket_tagging(Bucket=name)
            tags = {tag["Key"]: tag["Value"] for tag in response.get("TagSet", [])}
        except client.exceptions.NoSuchTagSet as e:
            logger.error(f"Could not fetch tags: {e}")
    return tags


@lru_cache(maxsize=1024)
def get_tags_from_arn(arn, client) -> dict:
    tags = {}
    if ":domain/" in arn:
        try:
            response = client.list_tags(ARN=arn)
            tags = {tag["Key"]: tag["Value"] for tag in response.get("TagList", [])}
        except Exception as e:
            logger.error(f"Could not fetch tags: {e}")
    if ":db:" in arn:
        try:
            response = client.list_tags_for_resource(ResourceName=arn)
            tags = {tag["Key"]: tag["Value"] for tag in response.get("TagList", [])}
            if ORG_GUID_TAG not in tags:
                return {}
        except Exception as e:
            logger.error(f"Could not fetch tags: {e}")
    if ":elasticache:" in arn:
        try:
            response = client.list_tags_for_resource(ResourceName=arn)
            tags = {tag["Key"]: tag["Value"] for tag in response.get("TagList", [])}

            if ORG_GUID_TAG not in tags:
                return {}
        except Exception as e:
            logger.error("Could not fetch tags for ARN %s: %s", arn, str(e))
            return {}
    return tags
