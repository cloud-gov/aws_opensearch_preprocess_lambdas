import json
import base64
import boto3
import logging
import os
from functools import lru_cache

logger = logging.getLogger()
logger.setLevel(logging.INFO)
default_keys_to_remove = ["metric_stream_name", "account_id", "region"]
EXPECTED_NAMESPACES = ["AWS/S3", "AWS/ES", "AWS/RDS"]


def lambda_handler(event, context):
    output_records = []
    region = boto3.Session().region_name or os.environ.get("AWS_REGION")
    rds_prefix, s3_prefix, domain_prefix = make_prefixes()
    account_id = os.environ.get("ACCOUNT_ID")
    s3_client = boto3.client("s3", region_name=region)
    es_client = boto3.client("es", region_name=region)
    rds_client = boto3.client("rds", region_name=region)
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
                    account_id,
                )
                if metric_results is not None:
                    metric_results["dimensions"].pop("ClientId", None)
                    processed_metrics.append(metric_results)

            if processed_metrics:
                # Create newline-delimited JSON (no compression)
                output_data = (
                    "\n".join([json.dumps(metric) for metric in processed_metrics])
                    + "\n"
                )

                # Just base64 encode for Firehose transport (no gzip)
                encoded_output = base64.b64encode(output_data.encode("utf-8")).decode(
                    "utf-8"
                )

                output_record = {
                    "recordId": record["recordId"],
                    "result": "Ok",
                    "data": encoded_output,
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
    if environment == "production":
        domain_prefix = domain_prefix + "prd-"
    if environment == "staging":
        domain_prefix = domain_prefix + "stg-"
    if environment == "development":
        domain_prefix = domain_prefix + "dev-"

    rds_prefix = "cg-aws-broker-"
    if environment == "production":
        rds_prefix = rds_prefix + "prod"
    if environment == "staging":
        rds_prefix = rds_prefix + "stage"
    if environment == "development":
        rds_prefix = rds_prefix + "dev"

    return rds_prefix, s3_prefix, domain_prefix


def process_metric(
    metric,
    region,
    s3_client,
    s3_prefix,
    es_client,
    domain_prefix,
    rds_client,
    rds_prefix,
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
    except Exception as e:
        logger.error(f"Error with getting tags for resource: {e}")
    return tags


@lru_cache(maxsize=256)
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


@lru_cache(maxsize=256)
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
            if "Organization GUID" not in tags:
                return {}
        except Exception as e:
            logger.error(f"Could not fetch tags: {e}")
    return tags
