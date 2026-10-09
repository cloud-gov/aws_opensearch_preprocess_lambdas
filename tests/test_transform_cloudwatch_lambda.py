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

ORG_TAGS = {"Organization GUID": "org-aaa", "Space GUID": "space-bbb"}


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
            }
            for i, message in enumerate(messages)
        ],
    }


def written_objects(s3_client):
    """Returns what was written to S3 as {key: [log entry, ...]}."""
    written = {}
    for call in s3_client.put_object.call_args_list:
        kwargs = call[1]
        body = gzip.decompress(kwargs["Body"]).decode("utf-8")
        written[kwargs["Key"]] = [
            json.loads(line) for line in body.strip().splitlines()
        ]
    return written


def only_object(s3_client):
    """Returns the (key, entries) of the single object written."""
    written = written_objects(s3_client)
    assert len(written) == 1, f"expected a single S3 object, got {list(written)}"
    return next(iter(written.items()))


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

    def test_lambda_handler_partitions_on_space_guid(self, monkeypatch):
        """
        The space tag must reach both the S3 key and the indexed document.

        The tag is named "Space GUID" with a space because cf-tags.conf renames
        [Tags][Space GUID] to [@cf][space_id]; an underscore would partition to
        unknown-space and leave the indexed document with no space.
        """
        log_data = create_log_data(
            "/aws/rds/instance/cg-aws-broker-devtest/postgresql",
            ["This is a test"],
        )
        ndjson_data = json.dumps(log_data) + "\n"
        encoded_data = base64.b64encode(
            gzip.compress(ndjson_data.encode("utf-8"))
        ).decode("utf-8")
        event = {"records": [{"recordId": "space-record", "data": encoded_data}]}

        context = MagicMock()
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        mock_s3_client = MagicMock()
        mock_s3_client.put_object.return_value = {}

        with patch("lambda_functions.transform_cloudwatch_lambda.logger"), patch(
            "lambda_functions.transform_cloudwatch_lambda.get_resource_tags_from_log",
            return_value=ORG_TAGS,
        ), patch("boto3.client", return_value=mock_s3_client):
            result = lambda_handler(event, context)

        assert result["records"][0]["result"] == "Ok"

        key, entries = only_object(mock_s3_client)
        # orgs/<org>/<space>/<YYYY>/<MM>/<DD>/<HH>/batch-<epoch>-<digest>.json.gz
        assert key.startswith("orgs/org-aaa/space-bbb/")
        assert "/batch-" in key

        assert len(entries) == 1
        assert entries[0]["Tags"]["Space GUID"] == "space-bbb"
        assert entries[0]["Tags"]["Organization GUID"] == "org-aaa"

    def test_lambda_handler_splits_objects_per_org_and_space(self, monkeypatch):
        """Logs from different orgs and spaces must land in separate objects"""
        log_data = create_log_data(
            "/aws/rds/instance/cg-aws-broker-devtest/postgresql",
            ["This is a test"],
        )
        encoded_data = base64.b64encode(
            gzip.compress((json.dumps(log_data) + "\n").encode("utf-8"))
        ).decode("utf-8")
        event = {
            "records": [
                {"recordId": "rec-0", "data": encoded_data},
                {"recordId": "rec-1", "data": encoded_data},
                {"recordId": "rec-2", "data": encoded_data},
            ]
        }
        # Same org for the first two but different spaces, then a different org.
        tag_sets = [
            {"Organization GUID": "org-aaa", "Space GUID": "space-one"},
            {"Organization GUID": "org-aaa", "Space GUID": "space-two"},
            {"Organization GUID": "org-zzz", "Space GUID": "space-one"},
        ]

        context = MagicMock()
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        mock_s3_client = MagicMock()
        mock_s3_client.put_object.return_value = {}

        with patch("lambda_functions.transform_cloudwatch_lambda.logger"), patch(
            "lambda_functions.transform_cloudwatch_lambda.get_resource_tags_from_log",
            side_effect=tag_sets,
        ), patch("boto3.client", return_value=mock_s3_client):
            lambda_handler(event, context)

        written = written_objects(mock_s3_client)
        assert len(written) == 3
        assert sorted(key.split("/20")[0] for key in written) == [
            "orgs/org-aaa/space-one",
            "orgs/org-aaa/space-two",
            "orgs/org-zzz/space-one",
        ]

    def test_lambda_handler_missing_space_keeps_real_org(self, monkeypatch):
        """
        A missing space tag must only demote the space level.

        The org and space fall back independently, so a resource tagged with an
        org but no space still lands under its real org prefix.
        """
        log_data = create_log_data(
            "/aws/rds/instance/cg-aws-broker-devtest/postgresql",
            ["This is a test"],
        )
        encoded_data = base64.b64encode(
            gzip.compress((json.dumps(log_data) + "\n").encode("utf-8"))
        ).decode("utf-8")
        event = {"records": [{"recordId": "no-space", "data": encoded_data}]}

        context = MagicMock()
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        mock_s3_client = MagicMock()
        mock_s3_client.put_object.return_value = {}

        with patch("lambda_functions.transform_cloudwatch_lambda.logger"), patch(
            "lambda_functions.transform_cloudwatch_lambda.get_resource_tags_from_log",
            return_value={"Organization GUID": "org-aaa"},
        ), patch("boto3.client", return_value=mock_s3_client):
            result = lambda_handler(event, context)

        assert result["records"][0]["result"] == "Ok"
        key, _ = only_object(mock_s3_client)
        assert key.startswith("orgs/org-aaa/unknown-space/")

    def test_lambda_handler_malformed_line(self, monkeypatch):
        """A malformed line fails its record rather than being skipped"""
        malformed = b'{"messageType": "DATA_MESSAGE"'  # Not valid JSON
        compressed_data = gzip.compress(malformed)
        encoded_data = base64.b64encode(compressed_data).decode("utf-8")
        event = {"records": [{"recordId": "malformed-record", "data": encoded_data}]}

        context = MagicMock()
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        mock_s3_client = MagicMock()
        mock_s3_client.put_object.return_value = {}

        with patch(
            "lambda_functions.transform_cloudwatch_lambda.logger"
        ) as mock_logger, patch("boto3.client", return_value=mock_s3_client):
            result = lambda_handler(event, context)

        # Firehose routes this to error_output_prefix, and the original data
        # has to come back for it to be written there.
        assert len(result["records"]) == 1
        assert result["records"][0]["recordId"] == "malformed-record"
        assert result["records"][0]["result"] == "ProcessingFailed"
        assert result["records"][0]["data"] == encoded_data
        mock_s3_client.put_object.assert_not_called()
        mock_logger.error.assert_called()

    def test_lambda_handler_does_not_half_deliver_a_record(self, monkeypatch):
        """
        A record with a good line and a bad line must not deliver the good one.

        Firehose statuses are per record, so a half-delivered record would be
        both written to S3 and written to error_output_prefix, with no way to
        tell the two copies apart.
        """
        log_data = create_log_data(
            "/aws/rds/instance/cg-aws-broker-devtest/postgresql",
            ["This is a test"],
        )
        mock_tags = {"Environment": "production", "Owner": "team-alpha"}

        body = json.dumps(log_data) + "\n" + '{"messageType": "DATA_MESSAGE"' + "\n"
        compressed_data = gzip.compress(body.encode("utf-8"))
        encoded_data = base64.b64encode(compressed_data).decode("utf-8")
        event = {"records": [{"recordId": "half-bad", "data": encoded_data}]}

        context = MagicMock()
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        mock_s3_client = MagicMock()
        mock_s3_client.put_object.return_value = {}

        with patch("lambda_functions.transform_cloudwatch_lambda.logger"), patch(
            "lambda_functions.transform_cloudwatch_lambda.get_resource_tags_from_log",
            return_value=mock_tags,
        ), patch("boto3.client", return_value=mock_s3_client):
            result = lambda_handler(event, context)

        assert result["records"][0]["result"] == "ProcessingFailed"
        mock_s3_client.put_object.assert_not_called()

    def test_one_bad_record_does_not_fail_the_good_ones(self, monkeypatch):
        """A single malformed record must not cost the rest of the batch"""
        log_data = create_log_data(
            "/aws/rds/instance/cg-aws-broker-devtest/postgresql",
            ["This is a test"],
        )
        mock_tags = {"Environment": "production", "Owner": "team-alpha"}

        good_data = base64.b64encode(
            gzip.compress((json.dumps(log_data) + "\n").encode("utf-8"))
        ).decode("utf-8")
        bad_data = base64.b64encode(
            gzip.compress(b'{"messageType": "DATA_MESSAGE"')
        ).decode("utf-8")

        event = {
            "records": [
                {"recordId": "good-1", "data": good_data},
                {"recordId": "bad-1", "data": bad_data},
                {"recordId": "good-2", "data": good_data},
            ]
        }

        context = MagicMock()
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        mock_s3_client = MagicMock()
        mock_s3_client.put_object.return_value = {}

        with patch("lambda_functions.transform_cloudwatch_lambda.logger"), patch(
            "lambda_functions.transform_cloudwatch_lambda.get_resource_tags_from_log",
            return_value=mock_tags,
        ), patch("boto3.client", return_value=mock_s3_client):
            result = lambda_handler(event, context)

        results = {r["recordId"]: r["result"] for r in result["records"]}
        assert results == {
            "good-1": "Ok",
            "bad-1": "ProcessingFailed",
            "good-2": "Ok",
        }

        # The two good records still reached S3
        mock_s3_client.put_object.assert_called_once()
        body = gzip.decompress(mock_s3_client.put_object.call_args[1]["Body"])
        assert len(body.decode("utf-8").strip().splitlines()) == 2

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
                {"Key": "Space GUID", "Value": "cloudgovtestspace"},
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
                resource_name,
                rds_client,
                es_client,
                dummy_region,
                123456,
                rds_prefix,
                opensearch_prefix,
            )

        # if tags are returned environment is correct
        assert result["Environment"] == environment
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"
        assert result["Space GUID"] == "cloudgovtestspace"

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
                {"Key": "Space GUID", "Value": "cloudgovtestspace"},
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
                resource_name,
                rds_client,
                es_client,
                dummy_region,
                123456,
                expected_rds_prefix,
                opensearch_prefix,
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
                {"Key": "Space GUID", "Value": "cloudgovtestspace"},
            ]
        }

        expected_param_for_stub = {"ARN": fake_arn}
        stubber.add_response("list_tags", fake_tags, expected_param_for_stub)
        stubber.activate()

        rds_client = MagicMock()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=es_client
        ):
            result = get_resource_tags_from_log(
                resource_name,
                rds_client,
                es_client,
                dummy_region,
                123456,
                rds_prefix,
                opensearch_prefix,
            )

        # if tags are returned environment is correct
        assert result["Environment"] == environment
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"
        assert result["Space GUID"] == "cloudgovtestspace"
