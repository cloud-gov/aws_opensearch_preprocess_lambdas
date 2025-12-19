import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logs = boto3.client("logs")
    FIREHOSE_ARN = os.environ["FIREHOSE_ARN"]
    ROLE_ARN = os.environ["ROLE_ARN"]
    rds_prefix = make_prefixes()
    detail = event.get("detail", {})
    params = detail.get("requestParameters", {})
    log_group_name = params.get("logGroupName")

    if log_group_name and log_group_name.startswith(f"/aws/rds/instance/{rds_prefix}"):
        filter_name = "firehose_for_opensearch"
        filter_pattern = ""
        try:
            logs.put_subscription_filter(
                logGroupName=log_group_name,
                filterName=filter_name,
                filterPattern=filter_pattern,
                destinationArn=FIREHOSE_ARN,
                roleArn=ROLE_ARN,
            )
            logger.info(f"subscription filter made for {log_group_name}")
        except logs.exceptions.ResourceAlreadyExistsException as e:
            logger.info(
                f"Subscription filter already exists for {log_group_name}, error: {e}"
            )
            raise RuntimeError(
                f"Subscription filter already exists for {log_group_name}"
            )
    else:
        logger.info(f"log group: {log_group_name} does not apply")


def make_prefixes():
    environment = os.getenv("ENVIRONMENT")
    if not environment:
        raise RuntimeError("environment is required")

    rds_prefix = "cg-aws-broker-"
    environment_suffixes = {
        "production": "prod",
        "staging": "stage",
        "development": "dev",
    }
    if environment not in environment_suffixes:
        raise RuntimeError(f"environment is invalid: {environment}")

    rds_prefix += environment_suffixes[environment]

    return rds_prefix
