import json
import base64
import boto3
import gzip
import hashlib
import io
import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from functools import lru_cache

logger = logging.getLogger()
logger.setLevel(logging.INFO)
default_keys_to_remove = ["metric_stream_name", "account_id", "region"]
EXPECTED_NAMESPACES = ["AWS/S3", "AWS/ES", "AWS/RDS", "AWS/ElastiCache"]

METRIC_BATCH_PREFIX = "metrics"


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
    Returns the (org, space) S3 partition for an enriched metric.
    """
    return partition_for(
        metric, f"{metric.get('namespace')}/{metric.get('metric_name')}"
    )


def lambda_handler(event, context):
    output_records = []
    # (org, space) -> metrics for that partition, accumulated across all records
    s3_output = defaultdict(list)
    region = boto3.Session().region_name or os.environ.get("AWS_REGION")
    rds_prefix, s3_prefix, domain_prefix, redis_prefix = make_prefixes()
    account_id = os.environ.get("ACCOUNT_ID")
    bucket = os.environ.get("S3_BUCKET_NAME")
    if not bucket:
        logger.error("S3_BUCKET_NAME environment variable not set.")
        raise ValueError("S3_BUCKET_NAME environment variable must be set.")
    # Get cached clients
    clients = get_clients(region)
    s3_client = clients["s3"]
    es_client = clients["es"]
    rds_client = clients["rds"]
    redis_client = clients["elasticache"]
    for record in event["records"]:
        try:
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

        if processed_metrics:
            for metric in processed_metrics:
                s3_output[partition_for_metric(metric)].append(metric)

            # Mark the record as successfully processed (but data is now in S3)
            output_records.append(
                {
                    "recordId": record["recordId"],
                    "result": "Ok",
                    "data": base64.b64encode(b"").decode("utf-8"),  # Empty data
                }
            )
        else:
            output_records.append(
                {
                    "recordId": record["recordId"],
                    "result": "Dropped",
                    "data": record["data"],
                }
            )
        logger.info(f"Processed record with {len(processed_metrics)} metrics")

    # One object per org/space partition.
    put_all_partitions(
        s3_client,
        bucket,
        s3_output,
        name_prefix=METRIC_BATCH_PREFIX,
    )
    return {"records": output_records}


def make_prefixes():
    environment = os.getenv("ENVIRONMENT")
    if not environment:
        raise RuntimeError("ENVIRONMENT is required")
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
