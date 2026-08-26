import json
import base64
from unittest.mock import patch, MagicMock, ANY
import gzip
from botocore.stub import Stubber
import boto3
import time
import pytest
from datetime import datetime

from lambda_functions.transform_cloudwatch_lambda import (
    lambda_handler,
    make_prefixes,
    get_resource_tags_from_log,
)

dummy_region = "us-gov-west-1"

def create_log_data(log_group, messages):
    base_timestamp = 1759774467000
    return {
        "messageType": "DATA_MESSAGE",
        "owner": "12345678910",
        "logGroup": log_group,
        "logStream": "cg-aws-broker-devtest.0",
        "subscriptionFilters": ["testing"],
        "logEvents": [
            {
                "id": "12345678912345678901234567890123456789123456789012345670",
                "timestamp": base_timestamp + i,
                "message": "This is a test",
            } for i, message in enumerate(messages)
        ],
    }

class TestLambdaHandler:

    def test_lambda_handler_single_log_line(self, monkeypatch):
        """Test processing a single log line"""
        # Sample log data as newline-delimited JSON
        log_data = create_log_data(
            "/aws/rds/instance/cg-aws-broker-devtest/postgresql",
            ["This is a test"],
        )
        mock_tags = {"Environment": "production", "Owner": "team-alpha"}
        # Create newline-delimited JSON
        ndjson_data = json.dumps(log_data) + "\n"
        compressed_data = gzip.compress(ndjson_data.encode("utf-8"))
        encoded_data = base64.b64encode(compressed_data).decode("utf-8")
        event = {"records": [{"recordId": "test-record-1", "data": encoded_data}]}
        context = MagicMock()
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        # Mock the S3 client completely - simpler approach
        mock_s3_client = MagicMock()
        mock_s3_client.put_object.return_value = {}

        with patch("lambda_functions.transform_cloudwatch_lambda.logger"), patch(
            "lambda_functions.transform_cloudwatch_lambda.get_resource_tags_from_log",
            return_value=mock_tags,
        ), patch("boto3.client", return_value=mock_s3_client):
            result = lambda_handler(event, context)

        # Verify S3 put_object was called with correct parameters
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args[1]
        assert call_args["Bucket"] == "test-bucket"
        assert call_args["ContentType"] == "application/gzip"
        assert call_args["ContentEncoding"] == "gzip"
        assert call_args["ServerSideEncryption"] == "AES256"
        assert call_args["Key"].endswith(".json.gz")  # Verify key format
        assert isinstance(call_args["Body"], bytes)  # Verify body is compressed bytes

        # Assertions
        assert "records" in result
        assert len(result["records"]) == 1
        assert result["records"][0]["recordId"] == "test-record-1"
        assert result["records"][0]["result"] == "Ok"

    def test_lambda_handler_multiple_log_lines(self, monkeypatch):
        """Test processing multiple log lines in one record, should seperate different events"""
        log_data = create_log_data(
            "/aws/rds/instance/cg-aws-broker-devtest/postgresql",
            ["This is a test", "do you like my test"],
        )
        mock_tags = {"Environment": "production", "Owner": "team-alpha"}

        # Create newline-delimited JSON
        ndjson_data = json.dumps(log_data) + "\n"
        compressed_data = gzip.compress(ndjson_data.encode("utf-8"))
        encoded_data = base64.b64encode(compressed_data).decode("utf-8")

        event = {"records": [{"recordId": "multi-log-record", "data": encoded_data}]}

        context = MagicMock()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        # Mock the S3 client completely
        mock_s3_client = MagicMock()
        mock_s3_client.put_object.return_value = {}

        with patch("lambda_functions.transform_cloudwatch_lambda.logger"), patch(
            "lambda_functions.transform_cloudwatch_lambda.get_resource_tags_from_log",
            return_value=mock_tags,
        ), patch("boto3.client", return_value=mock_s3_client):
            result = lambda_handler(event, context)

        # Verify put_object was called with correct bucket
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args[1]
        assert call_args["Bucket"] == "test-bucket"
        assert call_args["ContentType"] == "application/gzip"
        assert call_args["ServerSideEncryption"] == "AES256"

        assert len(result["records"]) == 1
        assert result["records"][0]["result"] == "Ok"

    @pytest.mark.parametrize(
        "environment, expected_rds_prefix",
        [
            pytest.param("development", "cg-aws-broker-dev"),
            pytest.param("staging", "cg-aws-broker-stage"),
            pytest.param("production", "cg-aws-broker-prod"),
        ],
    )
    def test_get_resource_tags_from_metric_rds_success(
        self,
        monkeypatch,
        environment,
        expected_rds_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)
        monkeypatch.setenv("CLIENT", "123456")

        rds_prefix, opensearch_prefix = make_prefixes()
        assert rds_prefix == expected_rds_prefix

        """Test that environment only accepts environment prefix that match environment"""
        log_data = create_log_data(
            f"/aws/rds/instance/{rds_prefix}-test/postgresql",
            ["This is a test", "do you like my test"],
        )

        # Create a stubbed rds client
        rds_client = boto3.client("rds", region_name=dummy_region)

        stubber = Stubber(rds_client)
        resource_name = log_data["logGroup"].split("/")[4]
        fake_arn = f"arn:aws-us-gov:rds:us-gov-west-1:123456:db:{resource_name}"

        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": environment},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }

        expected_param_for_stub = {"ResourceName": fake_arn}
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        stubber.activate()

        es_client = MagicMock()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=rds_client
        ):
            result = get_resource_tags_from_log(
                resource_name, rds_client, es_client, dummy_region, 123456, rds_prefix, opensearch_prefix
            )

        # if tags are returned environment is correct
        assert result["Environment"] == environment
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"

    @pytest.mark.parametrize(
        "environment, expected_rds_prefix",
        [
            pytest.param("development", "cg-aws-broker-prod"),
            pytest.param("staging", "cg-aws-broker-prod"),
            pytest.param("production", "cg-aws-broker-stage"),
        ],
    )
    def test_get_resource_tags_from_metric_rds_failure(
        self,
        monkeypatch,
        environment,
        expected_rds_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)
        monkeypatch.setenv("CLIENT", "123456")

        rds_prefix, opensearch_prefix = make_prefixes()
        assert rds_prefix != expected_rds_prefix

        """Test that environment only accepts environment prefix that match environment"""
        log_data = create_log_data(
            f"/aws/rds/instance/{rds_prefix}-test/postgresql",
            ["This is a test", "do you like my test"],
        )

        # Create a stubbed rds client
        rds_client = boto3.client("rds", region_name=dummy_region)

        stubber = Stubber(rds_client)
        resource_name = log_data["logGroup"].split("/")[4]
        fake_arn = f"arn:aws-us-gov:rds:us-gov-west-1:123456:db:{resource_name}"

        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": environment},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }

        expected_param_for_stub = {"ResourceName": fake_arn}
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        stubber.activate()

        es_client = MagicMock()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=rds_client
        ):
            result = get_resource_tags_from_log(
                resource_name, rds_client, es_client, dummy_region, 123456, expected_rds_prefix, opensearch_prefix
            )

        assert result == {}

    @pytest.mark.parametrize(
        "environment, expected_opensearch_prefix",
        [
            pytest.param("development", "cg-broker-dev"),
            pytest.param("staging", "cg-broker-stg"),
            pytest.param("production", "cg-broker-prd"),
        ],
    )
    def test_get_resource_tags_from_metric_opensearch_success(
        self,
        monkeypatch,
        environment,
        expected_opensearch_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)
        monkeypatch.setenv("CLIENT", "123456")

        rds_prefix, opensearch_prefix = make_prefixes()
        assert opensearch_prefix == expected_opensearch_prefix

        """Test that environment only accepts environment prefix that match environment"""
        log_data = create_log_data(
            f"/aws/OpenSearchService/domains/{opensearch_prefix}-abc123/audit-logs",
            ["This is a test"],
        )

        # Create a stubbed es client
        es_client = boto3.client("es", region_name=dummy_region)

        stubber = Stubber(es_client)
        resource_name = log_data["logGroup"].split("/")[4]
        fake_arn = f"arn:aws-us-gov:es:us-gov-west-1:123456:domain/{resource_name}"

        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": environment},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }

        expected_param_for_stub = {"ARN": fake_arn}
        stubber.add_response(
            "list_tags", fake_tags, expected_param_for_stub
        )
        stubber.activate()

        rds_client = MagicMock()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=es_client
        ):
            result = get_resource_tags_from_log(
                resource_name, rds_client, es_client, dummy_region, 123456, rds_prefix, opensearch_prefix
            )

        # if tags are returned environment is correct
        assert result["Environment"] == environment
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"
