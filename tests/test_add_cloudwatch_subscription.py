import json
import base64
from unittest.mock import patch, MagicMock
import gzip
from botocore.stub import Stubber
import boto3
import pytest

from lambda_functions.add_cloudwatch_subscrition import lambda_handler, make_prefixes

dummy_region = "us-gov-west-1"

def create_log_group_event(log_group_name):
    return {
        "version": "0",
        "id": "82bc3140-3fa6-4afa-8c0f-93e83a425393",
        "detail-type": "AWS API Call via CloudTrail",
        "source": "aws.logs",
        "account": "123456789",
        "time": "2025-10-22T15:59:06Z",
        "region": "us-west-1",
        "resources": [],
        "detail": {
            "eventVersion": "1.11",
            "userIdentity": {
                "type": "AssumedRole",
                "principalId": "Noseforatude:random-175919",
                "arn": "arn:aws:sts::1234556789:assumed-role/role/random-175919",
                "accountId": "1234556789",
                "accessKeyId": "WeAreWho1244",
                "sessionContext": {
                    "sessionIssuer": {
                        "type": "Role",
                        "principalId": "Noseforatude",
                        "arn": "arn:aws:iam::1234556789:role/aws-service-role/rds.amazonaws.com/role",
                        "accountId": "1234556789",
                        "userName": "AWSServiceRoleForRDS",
                    },
                    "attributes": {
                        "creationDate": "2025-10-22T15:58:58Z",
                        "mfaAuthenticated": "false",
                    },
                },
                "invokedBy": "rds.amazonaws.com",
            },
            "eventTime": "2025-10-22T15:59:06Z",
            "eventSource": "logs.amazonaws.com",
            "eventName": "CreateLogGroup",
            "awsRegion": "us-gov-west-1",
            "sourceIPAddress": "rds.amazonaws.com",
            "userAgent": "rds.amazonaws.com",
            "requestParameters": {
                "logGroupName": log_group_name
            },
            "responseElements": "null",
            "requestID": "5ac65be0-b8e0-42d5-91a6-a92b168d1729",
            "eventID": "ad11e4b2-e73b-48f3-8f59-227281cfa891",
            "readOnly": "false",
            "eventType": "AwsApiCall",
            "apiVersion": "20140328",
            "managementEvent": "true",
            "recipientAccountId": "1234567",
            "eventCategory": "Management",
        },
    }

def expected_filter_params(log_group_name):
    return {
        "logGroupName": log_group_name,
        "filterName": "firehose_for_opensearch",
        "filterPattern": "",
        "destinationArn": "fireexample",
        "roleArn": "roleexample",
    }

class TestCloudwatchLambdaHandler:

    def test_lambda_handler_rds_broker_logs(self, monkeypatch):
        """Test logs from broker"""
        # Sample cloudtrail data
        test_data = create_log_group_event("/aws/rds/instance/cg-aws-broker-test")

        context = MagicMock()

        monkeypatch.setenv("FIREHOSE_ARN", "fireexample")
        monkeypatch.setenv("ROLE_ARN", "roleexample")

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.add_cloudwatch_subscrition.make_prefixes",
            return_value=("cg-aws-broker-test", "cg-broker-test"),
        ):
            real_logs_client = boto3.client("logs", region_name=dummy_region)
            stubber = Stubber(real_logs_client)

            log_group = test_data["detail"]["requestParameters"]["logGroupName"]
            expected_param_for_stub = expected_filter_params(log_group)
            stubber.add_response("put_subscription_filter", {}, expected_param_for_stub)

            with patch("boto3.client", return_value=real_logs_client):
                with stubber:
                    lambda_handler(test_data, context)
    
    def test_lambda_handler_opensearch_broker_logs(self, monkeypatch):
        """Test logs from broker"""
        # Sample cloudtrail data
        test_data = create_log_group_event("/aws/OpenSearchService/domains/cg-broker-test/audit-logs")

        context = MagicMock()

        monkeypatch.setenv("FIREHOSE_ARN", "fireexample")
        monkeypatch.setenv("ROLE_ARN", "roleexample")

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.add_cloudwatch_subscrition.make_prefixes",
            return_value=("cg-aws-broker-test", "cg-broker-test"),
        ):
            real_logs_client = boto3.client("logs", region_name=dummy_region)
            stubber = Stubber(real_logs_client)

            log_group = test_data["detail"]["requestParameters"]["logGroupName"]
            expected_param_for_stub = expected_filter_params(log_group)
            stubber.add_response("put_subscription_filter", {}, expected_param_for_stub)

            with patch("boto3.client", return_value=real_logs_client):
                with stubber:
                    lambda_handler(test_data, context)

    def test_lambda_handler_broker_logs_already_exists(self, monkeypatch):
        """Test logs from broker that already exists"""
        # Sample cloudtrail data
        test_data = create_log_group_event("/aws/rds/instance/cg-aws-broker-test")

        context = MagicMock()

        monkeypatch.setenv("FIREHOSE_ARN", "fireexample")
        monkeypatch.setenv("ROLE_ARN", "roleexample")

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.add_cloudwatch_subscrition.make_prefixes",
            return_value=("cg-aws-broker-test", "cg-broker-test"),
        ):
            real_logs_client = boto3.client("logs", region_name=dummy_region)
            stubber = Stubber(real_logs_client)
            stubber.add_client_error(
                "put_subscription_filter",
                service_error_code="ResourceAlreadyExistsException",
            )
            log_group = test_data["detail"]["requestParameters"]["logGroupName"]
            with patch("boto3.client", return_value=real_logs_client):
                with pytest.raises(
                    RuntimeError,
                    match=f"Subscription filter already exists for {log_group}",
                ):
                    with stubber:
                        lambda_handler(test_data, context)

    def test_lambda_handler_not_broker_logs(self, monkeypatch):
        """Test logs from broker"""
        # Sample cloudtrail data
        test_data = create_log_group_event("/aws/rds/instance/apple")

        context = MagicMock()

        monkeypatch.setenv("FIREHOSE_ARN", "fireexample")
        monkeypatch.setenv("ROLE_ARN", "roleexample")

        with patch(
            "lambda_functions.add_cloudwatch_subscrition.make_prefixes",
            return_value=("cg-aws-broker-test", "cg-broker-test"),
        ), patch("boto3.client") as mock_logs_client:
            # Create a stubbed logs client
            logs_client = boto3.client("logs", region_name=dummy_region)
            stubber = Stubber(logs_client)
            mock_logs_client.return_value = logs_client
            with stubber:
                # execute lambda handler and expect no api calls
                lambda_handler(test_data, context)

    @pytest.mark.parametrize(
        "environment, expected_rds_prefix",
        [
            pytest.param("development", "cg-aws-broker-dev"),
            pytest.param("staging", "cg-aws-broker-stage"),
            pytest.param("production", "cg-aws-broker-prod"),
        ],
    )
    def test_environment_cloudwatch_rds_success(
        self,
        monkeypatch,
        environment,
        expected_rds_prefix,
    ):
        monkeypatch.setenv("FIREHOSE_ARN", "fireexample")
        monkeypatch.setenv("ROLE_ARN", "roleexample")

        monkeypatch.setenv("ENVIRONMENT", environment)

        rds_prefix, opensearch_prefix = make_prefixes()
        assert rds_prefix == expected_rds_prefix

        """Test that environment only accepts environment prefix that match environment"""
        test_data = create_log_group_event(f"/aws/rds/instance/{expected_rds_prefix}")

        context = MagicMock()
        with patch("lambda_functions.transform_lambda.logger"):
            real_logs_client = boto3.client("logs", region_name=dummy_region)
            stubber = Stubber(real_logs_client)

            log_group = test_data["detail"]["requestParameters"]["logGroupName"]
            expected_param_for_stub = expected_filter_params(log_group)
            stubber.add_response("put_subscription_filter", {}, expected_param_for_stub)

            with patch("boto3.client", return_value=real_logs_client):
                with stubber:
                    lambda_handler(test_data, context)

    @pytest.mark.parametrize(
        "environment, expected_rds_prefix",
        [
            pytest.param("development", "cg-aws-broker-prod"),
            pytest.param("staging", "cg-aws-broker-prod"),
            pytest.param("production", "cg-aws-broker-stage"),
        ],
    )
    def test_environment_cloudwatch_rds_failure(
        self,
        monkeypatch,
        environment,
        expected_rds_prefix,
    ):
        monkeypatch.setenv("FIREHOSE_ARN", "fireexample")
        monkeypatch.setenv("ROLE_ARN", "roleexample")

        monkeypatch.setenv("ENVIRONMENT", environment)

        rds_prefix, opensearch_prefix = make_prefixes()
        assert rds_prefix != expected_rds_prefix

        """Test that environment only accepts environment prefix that match environment"""
        test_data = create_log_group_event(f"/aws/rds/instance/{expected_rds_prefix}")

        context = MagicMock()
        with patch("lambda_functions.transform_lambda.logger"):
            real_logs_client = boto3.client("logs", region_name=dummy_region)
            stubber = Stubber(real_logs_client)

            with patch("boto3.client", return_value=real_logs_client):
                # stubber will fail if any calls are made
                with stubber:
                    lambda_handler(test_data, context)
