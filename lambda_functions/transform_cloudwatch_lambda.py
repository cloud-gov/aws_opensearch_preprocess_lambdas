import boto3
import gzip
import json
from datetime import datetime
import time
import io
import os
import logging
from functools import lru_cache
import base64

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    This function processes CloudWatch Logs from Firehose, enriches them with tags,
    and stores them in S3.
    """
    output_records = []
    s3_output = []

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

        rds_prefix, opensearch_prefix = make_prefixes()  # Fetch prefix based on environment

        # Initialize clients
        s3_client = boto3.client("s3", region_name=region)
        rds_client = boto3.client("rds", region_name=region)
        es_client = boto3.client("es", region_name=region)

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
                        logs, rds_client, es_client, region, account_id, rds_prefix, opensearch_prefix
                    )
                    if log_results:
                        processed_logs.extend(log_results)
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON: {e}. Line: {line}")
                    continue  # Skip to the next line if JSON decoding fails
            if processed_logs:
                s3_output.extend(processed_logs)  # Flatten the logs directly

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

    # After processing all records, push the combined logs to S3
    if s3_output:
        try:
            # Convert logs to newline-delimited JSON
            buffer = io.BytesIO()
            with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
                for log in s3_output:
                    gz_file.write((json.dumps(log) + "\n").encode("utf-8"))
            compressed_data = buffer.getvalue()
            s3_key = f"{datetime.now().strftime('%Y/%m/%d/%H')}/batch-{int(time.time())}.json.gz"
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=compressed_data,
                ContentType="application/gzip",
                ContentEncoding="gzip",
                ServerSideEncryption="AES256",
            )

            logger.info(f"Successfully pushed {len(s3_output)} logs to S3: {s3_key}")
        except Exception as e:
            logger.error(f"Unexpected error pushing to S3: {str(e)}")
            raise e
    return {"records": output_records}


def make_prefixes():
    """
    Determines the prefix based on the ENVIRONMENT variable.
    """
    environment = os.getenv("ENVIRONMENT")
    if not environment:
        raise RuntimeError("ENVIRONMENT is required")

    rds_prefix = "cg-aws-broker-"
    opensearch_prefix = "cg-broker-"
    environment_suffixes = {
        "production": ("prod", "prd"),
        "staging": ("stage", "stg"),
        "development": ("dev", "dev"),
    }

    if environment not in environment_suffixes:
        raise RuntimeError(f"Invalid ENVIRONMENT: {environment}")

    rds_suffix, opensearch_suffix = environment_suffixes[environment]
    rds_prefix += rds_suffix
    opensearch_prefix += opensearch_suffix
    return rds_prefix, opensearch_prefix


def process_logs(logs, rds_client, es_client, region, account_id, rds_prefix, opensearch_prefix):
    """
    Enriches CloudWatch Logs with tags.
    """
    try:
        return_logs = []
        resource_name = logs["logGroup"].split("/")[4]
        tags = get_resource_tags_from_log(
            resource_name, rds_client, es_client, region, account_id, rds_prefix, opensearch_prefix
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
    resource_name, rds_client, es_client, region, account_id, rds_prefix, opensearch_prefix
) -> dict:
    """
    Retrieves tags from an instance based on its ARN.
    """
    tags = {}
    try:
        if resource_name is None:
            return tags
        if resource_name.startswith(rds_prefix):
            arn = f"arn:aws-us-gov:rds:{region}:{account_id}:db:{resource_name}"
            tags = get_tags_from_arn(arn, rds_client)
        elif resource_name.startswith(opensearch_prefix):
            arn = f"arn:aws-us-gov:es:{region}:{account_id}:domain/{resource_name}"
            tags = get_tags_from_arn(arn, es_client)
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
            if "Organization GUID" not in tags:
                logger.warning(f"Organization GUID tag missing for ARN: {arn}")
                return {}
        except Exception as e:
            logger.error(f"Could not fetch tags for ARN {arn}: {e}")
    if ":domain/" in arn:
        try:
            response = client.list_tags(ARN=arn)
            tags = {tag["Key"]: tag["Value"] for tag in response.get("TagList", [])}
            if "Organization GUID" not in tags:
                logger.warning(f"Organization GUID tag missing for ARN: {arn}")
                return {}
        except Exception as e:
            logger.error(f"Could not fetch tags for ARN {arn}: {e}")
    return tags
