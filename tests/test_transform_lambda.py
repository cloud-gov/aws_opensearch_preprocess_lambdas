import gzip
import json
import base64
import re
from unittest.mock import patch, MagicMock
from botocore.stub import Stubber
import boto3
import pytest

from lambda_functions.transform_lambda import (
    lambda_handler,
    process_metric,
    default_keys_to_remove,
    get_resource_tags_from_metric,
    make_prefixes,
)

dummy_region = "us-gov-west-1"


def s3_capture():
    """Mock S3 client that records what the handler writes."""
    client = MagicMock()
    client.put_object.return_value = {}
    return client


def patched_clients(s3_client, tag_client=None):
    """
    Patches get_clients so writes go to `s3_client` and tag lookups to
    `tag_client`. Patches get_clients rather than boto3.client because
    get_clients is lru_cached and would leak between tests.
    """
    tag_client = tag_client if tag_client is not None else s3_client
    return patch(
        "lambda_functions.transform_lambda.get_clients",
        return_value={
            "s3": s3_client,
            "es": tag_client,
            "rds": tag_client,
            "elasticache": tag_client,
        },
    )


def written_objects(s3_client):
    """Returns what was written to S3 as {key: [metric, ...]}."""
    written = {}
    for call in s3_client.put_object.call_args_list:
        kwargs = call[1]
        body = gzip.decompress(kwargs["Body"]).decode("utf-8")
        written[kwargs["Key"]] = [
            json.loads(line) for line in body.strip().splitlines()
        ]
    return written


def only_object(s3_client):
    """Returns the (key, metrics) of the single object written."""
    written = written_objects(s3_client)
    assert len(written) == 1, f"expected a single S3 object, got {list(written)}"
    return next(iter(written.items()))


class TestLambdaHandler:

    def test_lambda_handler_single_metric_line(self, monkeypatch):
        """Test processing a single metric line"""
        # Sample metric data as newline-delimited JSON
        metric_data = {
            "timestamp": 1640995200000,
            "metric_stream_name": "test-stream",
            "account_id": "123456789012",
            "region": "us-east-1",
            "namespace": "AWS/ES",
            "metric_name": "CPUUtilization",
            "dimensions": {
                "InstanceId": "i-1234567890abcdef0",
                "ClientId": "client123",
            },
            "value": 85.5,
            "unit": "Percent",
        }
        mock_tags = {
            "Environment": "production",
            "Owner": "team-alpha",
            "Organization GUID": "org-aaa",
            "Space GUID": "space-bbb",
        }

        # Create newline-delimited JSON
        ndjson_data = json.dumps(metric_data) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")

        event = {"records": [{"recordId": "test-record-1", "data": encoded_data}]}

        context = MagicMock()
        context.aws_request_id = "req-1"

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        s3_client = s3_capture()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ), patched_clients(s3_client):
            # Set up the mock return value
            result = lambda_handler(event, context)
        # Assertions
        assert "records" in result
        assert len(result["records"]) == 1
        assert result["records"][0]["recordId"] == "test-record-1"
        assert result["records"][0]["result"] == "Ok"
        assert result["records"][0]["data"] == ""

        put_kwargs = s3_client.put_object.call_args[1]
        assert put_kwargs["Bucket"] == "test-bucket"
        assert put_kwargs["ContentType"] == "application/gzip"
        assert put_kwargs["ContentEncoding"] == "gzip"
        assert put_kwargs["ServerSideEncryption"] == "AES256"

        key, output_metrics = only_object(s3_client)
        # Date path and epoch come from the metric timestamp, not from now, and
        # the suffix is a content digest, so the whole key is deterministic.
        assert re.fullmatch(
            r"orgs/org-aaa/space-bbb/2022/01/01/00/metrics-1640995200-[0-9a-f]{32}\.json\.gz",
            key,
        ), key

        assert len(output_metrics) == 1
        metric = output_metrics[0]

        # Verify keys were removed
        assert "metric_stream_name" not in metric
        assert "account_id" not in metric
        assert "region" not in metric

        # Verify ClientId was removed from dimensions
        assert "ClientId" not in metric["dimensions"]

        # Verify core data is preserved
        assert metric["namespace"] == "AWS/ES"
        assert metric["metric_name"] == "CPUUtilization"
        assert metric["value"] == 85.5

        assert metric["Tags"]["Environment"] == "production"
        assert metric["Tags"]["Owner"] == "team-alpha"
        assert metric["Tags"]["Space GUID"] == "space-bbb"

    def test_lambda_handler_multiple_metric_lines(self, monkeypatch):
        """Test processing multiple metric lines in one record"""
        metrics = [
            {
                "timestamp": 1640995200000,
                "metric_stream_name": "test-stream",
                "namespace": "AWS/ES",
                "metric_name": "CPUUtilization",
                "dimensions": {"InstanceId": "i-123"},
                "value": 85.5,
                "unit": "Percent",
            },
            {
                "timestamp": 1640995260000,
                "metric_stream_name": "test-stream",
                "namespace": "AWS/S3",
                "metric_name": "BucketSizeBytes",
                "dimensions": {"BucketName": "TestingCheatsEnabled"},
                "value": 50,
                "unit": "Bytes",
            },
        ]
        mock_tags = {
            "Environment": "production",
            "Owner": "team-alpha",
            "Organization GUID": "org-aaa",
            "Space GUID": "space-bbb",
        }

        # Create newline-delimited JSON
        ndjson_data = "\n".join([json.dumps(metric) for metric in metrics]) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")

        event = {"records": [{"recordId": "multi-metric-record", "data": encoded_data}]}

        context = MagicMock()
        context.aws_request_id = "req-1"

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        s3_client = s3_capture()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ), patched_clients(s3_client):
            result = lambda_handler(event, context)

        assert len(result["records"]) == 1
        assert result["records"][0]["result"] == "Ok"

        # One shared partition means one object
        key, output_metrics = only_object(s3_client)
        assert key.startswith("orgs/org-aaa/space-bbb/")

        assert len(output_metrics) == 2
        assert output_metrics[0]["namespace"] == "AWS/ES"
        assert output_metrics[1]["namespace"] == "AWS/S3"
        assert output_metrics[0]["Tags"]["Environment"] == "production"
        assert output_metrics[0]["Tags"]["Owner"] == "team-alpha"
        assert output_metrics[1]["Tags"]["Environment"] == "production"
        assert output_metrics[1]["Tags"]["Owner"] == "team-alpha"

    def test_lambda_handler_splits_objects_per_org_and_space(self, monkeypatch):
        """Metrics from different orgs and spaces must land in separate objects"""
        metrics = [
            {
                "timestamp": 1640995200000,
                "namespace": "AWS/ES",
                "metric_name": "CPUUtilization",
                "dimensions": {"InstanceId": "i-123"},
                "value": 1,
            },
            {
                "timestamp": 1640995201000,
                "namespace": "AWS/ES",
                "metric_name": "CPUUtilization",
                "dimensions": {"InstanceId": "i-456"},
                "value": 2,
            },
            {
                "timestamp": 1640995202000,
                "namespace": "AWS/ES",
                "metric_name": "CPUUtilization",
                "dimensions": {"InstanceId": "i-789"},
                "value": 3,
            },
        ]
        tag_sets = [
            {"Organization GUID": "org-aaa", "Space GUID": "space-one"},
            {"Organization GUID": "org-aaa", "Space GUID": "space-two"},
            {"Organization GUID": "org-zzz", "Space GUID": "space-one"},
        ]

        ndjson_data = "\n".join([json.dumps(metric) for metric in metrics]) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")
        event = {"records": [{"recordId": "multi-org-record", "data": encoded_data}]}

        context = MagicMock()
        context.aws_request_id = "req-1"

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        s3_client = s3_capture()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            side_effect=tag_sets,
        ), patched_clients(s3_client):
            result = lambda_handler(event, context)

        assert result["records"][0]["result"] == "Ok"

        written = written_objects(s3_client)
        assert len(written) == 3
        prefixes = sorted(key.split("/20")[0] for key in written)
        assert prefixes == [
            "orgs/org-aaa/space-one",
            "orgs/org-aaa/space-two",
            "orgs/org-zzz/space-one",
        ]
        for entries in written.values():
            assert len(entries) == 1

    def test_lambda_handler_untagged_metric_falls_back_to_unknown_org(
        self, monkeypatch
    ):
        """A metric with no org/space tag is still delivered, not dropped"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/S3",
            "metric_name": "BucketSizeBytes",
            "dimensions": {"BucketName": "some-untagged-bucket"},
            "value": 7,
        }
        # S3 and ES metrics are not required to carry the org tag
        mock_tags = {"Environment": "production"}

        ndjson_data = json.dumps(metric_data) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")
        event = {"records": [{"recordId": "untagged-record", "data": encoded_data}]}

        context = MagicMock()
        context.aws_request_id = "req-1"

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        s3_client = s3_capture()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ), patched_clients(s3_client):
            result = lambda_handler(event, context)

        assert result["records"][0]["result"] == "Ok"

        key, output_metrics = only_object(s3_client)
        assert key.startswith("unknown-org/unknown-space/")
        assert len(output_metrics) == 1

    def test_lambda_handler_requires_bucket(self, monkeypatch):
        """Without a destination bucket the handler must fail closed"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/ES",
            "metric_name": "CPUUtilization",
            "dimensions": {"InstanceId": "i-123"},
            "value": 1,
        }
        ndjson_data = json.dumps(metric_data) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")
        event = {"records": [{"recordId": "no-bucket-record", "data": encoded_data}]}

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.delenv("S3_BUCKET_NAME", raising=False)

        with patch("lambda_functions.transform_lambda.logger"):
            with pytest.raises(ValueError, match="S3_BUCKET_NAME"):
                lambda_handler(event, MagicMock())

    def test_lambda_handler_requires_environment(self, monkeypatch):
        """Without ENVIRONMENT the handler must fail closed, not use a bare prefix"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/RDS",
            "metric_name": "CPUUtilization",
            "dimensions": {"DBInstanceIdentifier": "cg-aws-broker-prodthing"},
            "value": 1,
        }
        ndjson_data = json.dumps(metric_data) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")
        event = {"records": [{"recordId": "no-env-record", "data": encoded_data}]}

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")
        monkeypatch.delenv("ENVIRONMENT", raising=False)

        with patch("lambda_functions.transform_lambda.logger"):
            with pytest.raises(RuntimeError, match="ENVIRONMENT"):
                lambda_handler(event, MagicMock())

    def test_retry_of_same_batch_reuses_key(self, monkeypatch):
        """
        A Firehose retry must overwrite its object, not add a duplicate.

        Keys are content-addressed, so re-running the same batch under a new
        Lambda request ID has to resolve to the same key.
        """
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/ES",
            "metric_name": "CPUUtilization",
            "dimensions": {"InstanceId": "i-123"},
            "value": 1,
        }
        mock_tags = {"Organization GUID": "org-aaa", "Space GUID": "space-bbb"}

        ndjson_data = json.dumps(metric_data) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")
        event = {"records": [{"recordId": "retried-record", "data": encoded_data}]}

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        def invoke(request_id):
            context = MagicMock()
            context.aws_request_id = request_id
            s3_client = s3_capture()
            with patch("lambda_functions.transform_lambda.logger"), patch(
                "lambda_functions.transform_lambda.get_resource_tags_from_metric",
                return_value=mock_tags,
            ), patched_clients(s3_client):
                lambda_handler(event, context)
            key, _ = only_object(s3_client)
            return key, s3_client.put_object.call_args[1]["Body"]

        # AWS assigns a fresh request ID per invocation, so this is what a real
        # retry of the same batch looks like.
        first_key, first_body = invoke("aaaaaaaa-1111-2222-3333-444444444444")
        retry_key, retry_body = invoke("bbbbbbbb-5555-6666-7777-888888888888")

        assert retry_key == first_key
        # Identical bytes too, so the overwrite is a true no-op
        assert retry_body == first_body

    def test_differing_batches_get_different_keys(self, monkeypatch):
        """Content-addressed keys must still separate genuinely different batches"""
        mock_tags = {"Organization GUID": "org-aaa", "Space GUID": "space-bbb"}

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        def invoke(value):
            metric_data = {
                "timestamp": 1640995200000,
                "namespace": "AWS/ES",
                "metric_name": "CPUUtilization",
                "dimensions": {"InstanceId": "i-123"},
                "value": value,
            }
            ndjson_data = json.dumps(metric_data) + "\n"
            encoded = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")
            event = {"records": [{"recordId": "rec", "data": encoded}]}
            s3_client = s3_capture()
            with patch("lambda_functions.transform_lambda.logger"), patch(
                "lambda_functions.transform_lambda.get_resource_tags_from_metric",
                return_value=mock_tags,
            ), patched_clients(s3_client):
                lambda_handler(event, MagicMock())
            key, _ = only_object(s3_client)
            return key

        assert invoke(1) != invoke(2)

    def test_one_failing_partition_does_not_strand_the_others(self, monkeypatch):
        """
        A failed partition must not stop the remaining partitions from being
        written, and must still fail the batch so Firehose retries.
        """
        metrics = [
            {
                "timestamp": 1640995200000,
                "namespace": "AWS/ES",
                "metric_name": "CPUUtilization",
                "dimensions": {"InstanceId": f"i-{i}"},
                "value": i,
            }
            for i in range(3)
        ]
        tag_sets = [
            {"Organization GUID": "org-aaa", "Space GUID": "space-1"},
            {"Organization GUID": "org-bbb", "Space GUID": "space-1"},
            {"Organization GUID": "org-ccc", "Space GUID": "space-1"},
        ]

        ndjson_data = "\n".join(json.dumps(m) for m in metrics) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")
        event = {"records": [{"recordId": "partial-record", "data": encoded_data}]}

        context = MagicMock()
        context.aws_request_id = "req-1"

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        s3_client = s3_capture()
        attempted = []

        def put_object(**kwargs):
            attempted.append(kwargs["Key"])
            if "org-bbb" in kwargs["Key"]:
                raise RuntimeError("AccessDenied")
            return {}

        s3_client.put_object.side_effect = put_object

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            side_effect=tag_sets,
        ), patched_clients(s3_client):
            with pytest.raises(RuntimeError, match="1 of 3 partitions"):
                lambda_handler(event, context)

        # All three attempted, so org-ccc was not skipped by the org-bbb failure
        assert len(attempted) == 3
        assert sorted(key.split("/")[1] for key in attempted) == [
            "org-aaa",
            "org-bbb",
            "org-ccc",
        ]

    def test_lambda_handler_many_rds_metric_lines(self, monkeypatch):
        """Test processing multiple metric lines in one record"""

        # Clear the LRU cache before test
        from lambda_functions.transform_lambda import (
            get_clients,
            get_tags_from_arn,
            get_rds_description,
        )

        get_clients.cache_clear()
        get_tags_from_arn.cache_clear()
        get_rds_description.cache_clear()

        metrics = [
            {
                "timestamp": 1640995200000,
                "metric_stream_name": "test-stream",
                "namespace": "AWS/RDS",
                "metric_name": "CPUUtilization",
                "dimensions": {"DBInstanceIdentifier": "cg-aws-broker-prodjasontest"},
                "value": 100,
                "unit": "Percent",
            },
            {
                "timestamp": 1640995260000,
                "metric_stream_name": "test-stream",
                "namespace": "AWS/RDS",
                "metric_name": "FreeStorageSpace",
                "dimensions": {"DBInstanceIdentifier": "cg-aws-broker-prodjasontest"},
                "value": 100,
                "unit": "Bytes",
            },
            {
                "timestamp": 1640995200000,
                "metric_stream_name": "test-stream",
                "namespace": "AWS/RDS",
                "metric_name": "AppleJacks",
                "dimensions": {"DBInstanceIdentifier": "cg-aws-broker-prodjasontest"},
                "value": 100,
                "unit": "Percent",
            },
            {
                "timestamp": 1640995200000,
                "metric_stream_name": "test-stream",
                "namespace": "AWS/RDS",
                "metric_name": "WeloveLambda",
                "dimensions": {"DBInstanceIdentifier": "cg-aws-broker-prodjasontest"},
                "value": 100,
                "unit": "Percent",
            },
        ]

        # Create newline-delimited JSON
        ndjson_data = "\n".join([json.dumps(metric) for metric in metrics]) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")
        event = {"records": [{"recordId": "multi-metric-record", "data": encoded_data}]}
        context = MagicMock()
        context.aws_request_id = "req-1"

        # Create a stubbed rds client
        rds_client = boto3.client("rds", region_name=dummy_region)
        stubber = Stubber(rds_client)
        fake_arn = (
            "arn:aws-us-gov:rds:us-gov-west-1:123456:db:cg-aws-broker-prodjasontest"
        )
        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": "staging"},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
                {"Key": "Space GUID", "Value": "cloudgovtestspace"},
            ]
        }
        expected_param_for_stub = {"ResourceName": fake_arn}
        expected_param_for_describe = {
            "DBInstanceIdentifier": "cg-aws-broker-prodjasontest"
        }
        fake_describe = {"DBInstances": [{"AllocatedStorage": 100}]}

        # Only stub once per unique call - cache handles the rest
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        stubber.add_response(
            "describe_db_instances", fake_describe, expected_param_for_describe
        )

        stubber.activate()
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        # The fixture DB is named cg-aws-broker-prod*, so only the production
        # rds_prefix matches it.
        monkeypatch.setenv("ENVIRONMENT", "production")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        # Tag lookups hit the stubbed rds client; writes go to the capture mock
        s3_client = s3_capture()
        with patch("lambda_functions.transform_lambda.logger"), patched_clients(
            s3_client, tag_client=rds_client
        ):
            result = lambda_handler(event, context)

        assert len(result["records"]) == 1
        assert result["records"][0]["result"] == "Ok"

        key, output_metrics = only_object(s3_client)
        assert key.startswith("orgs/cloudgovtests/cloudgovtestspace/")
        assert len(output_metrics) == 4
        assert "db_size" not in output_metrics[0]["Tags"]
        assert "db_size" in output_metrics[1]["Tags"]
        assert "db_size" not in output_metrics[2]["Tags"]
        assert "db_size" not in output_metrics[3]["Tags"]

    def test_lambda_handler_multiple_records(self, monkeypatch):
        """Test processing multiple records"""
        records = []
        for i in range(3):
            metric_data = {
                "timestamp": 1640995200000 + i,
                "namespace": "AWS/ES",
                "metric_name": f"TestMetric{i}",
                "dimensions": {"ResourceId": f"resource-{i}"},
                "value": 100 + i,
                "unit": "Count",
            }
            ndjson_data = json.dumps(metric_data) + "\n"
            encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")

            records.append({"recordId": f"record-{i}", "data": encoded_data})
        mock_tags = {
            "Environment": "production",
            "Owner": "team-alpha",
            "Organization GUID": "org-aaa",
            "Space GUID": "space-bbb",
        }
        event = {"records": records}
        context = MagicMock()
        context.aws_request_id = "req-1"

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        s3_client = s3_capture()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ), patched_clients(s3_client):
            result = lambda_handler(event, context)

        assert len(result["records"]) == 3
        for i, record in enumerate(result["records"]):
            assert record["recordId"] == f"record-{i}"
            assert record["result"] == "Ok"

        # Metrics accumulate across records, so all three batch into one object
        key, output_metrics = only_object(s3_client)
        assert key.startswith("orgs/org-aaa/space-bbb/")
        assert len(output_metrics) == 3

    def test_lambda_handler_empty_metrics_filtered(self, monkeypatch):
        """Test that records with emmpry metrics, no valid metrics are filtered out"""
        # Invalid metric (missing required fields)
        invalid_metric = {
            "timestamp": 1640995200000,
            "namespace": "AWS/Test",
            # Missing metric_name and value
        }

        ndjson_data = json.dumps(invalid_metric) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")

        event = {"records": [{"recordId": "invalid-record", "data": encoded_data}]}

        context = MagicMock()
        context.aws_request_id = "req-1"
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        s3_client = s3_capture()
        with patch("lambda_functions.transform_lambda.logger"), patched_clients(
            s3_client
        ):
            result = lambda_handler(event, context)

        # Should return empty records list since no valid metrics
        assert len(result["records"]) == 1
        assert result["records"][0]["result"] == "Dropped"
        s3_client.put_object.assert_not_called()

    def test_lambda_handler_malformed_json(self, monkeypatch):
        """A malformed record is handed back as ProcessingFailed, not raised"""
        malformed_data = '{"invalid": "json"'  # Not valid JSON
        encoded_data = base64.b64encode(malformed_data.encode("utf-8")).decode("utf-8")

        event = {"records": [{"recordId": "malformed-record", "data": encoded_data}]}

        context = MagicMock()
        context.aws_request_id = "req-1"

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        s3_client = s3_capture()
        with patch(
            "lambda_functions.transform_lambda.logger"
        ) as mock_logger, patched_clients(s3_client):
            result = lambda_handler(event, context)

        # Firehose routes this to error_output_prefix, and the original data
        # has to come back for it to be written there.
        assert len(result["records"]) == 1
        assert result["records"][0]["recordId"] == "malformed-record"
        assert result["records"][0]["result"] == "ProcessingFailed"
        assert result["records"][0]["data"] == encoded_data
        s3_client.put_object.assert_not_called()
        mock_logger.error.assert_called()

    def test_one_bad_record_does_not_fail_the_good_ones(self, monkeypatch):
        """A single malformed record must not cost the rest of the batch"""
        good = {
            "timestamp": 1640995200000,
            "namespace": "AWS/ES",
            "metric_name": "CPUUtilization",
            "dimensions": {"InstanceId": "i-123"},
            "value": 1,
        }
        good_data = base64.b64encode((json.dumps(good) + "\n").encode("utf-8")).decode(
            "utf-8"
        )
        bad_data = base64.b64encode(b'{"invalid": "json"').decode("utf-8")

        event = {
            "records": [
                {"recordId": "good-1", "data": good_data},
                {"recordId": "bad-1", "data": bad_data},
                {"recordId": "good-2", "data": good_data},
            ]
        }
        mock_tags = {"Organization GUID": "org-aaa", "Space GUID": "space-bbb"}

        context = MagicMock()
        context.aws_request_id = "req-1"

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        s3_client = s3_capture()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ), patched_clients(s3_client):
            result = lambda_handler(event, context)

        results = {r["recordId"]: r["result"] for r in result["records"]}
        assert results == {
            "good-1": "Ok",
            "bad-1": "ProcessingFailed",
            "good-2": "Ok",
        }

        # The two good records still reached S3; the bad one contributed nothing
        _, output_metrics = only_object(s3_client)
        assert len(output_metrics) == 2

    def test_failed_record_contributes_nothing_to_s3(self, monkeypatch):
        """
        A record that fails partway through must not half-deliver.

        Metrics are only staged after the whole record parses, so a record
        cannot be both written to S3 and handed back as ProcessingFailed.
        """
        metrics = [
            {
                "timestamp": 1640995200000,
                "namespace": "AWS/ES",
                "metric_name": "CPUUtilization",
                "dimensions": {"InstanceId": "i-123"},
                "value": 1,
            },
            {
                "timestamp": 1640995201000,
                "namespace": "AWS/ES",
                "metric_name": "CPUUtilization",
                "dimensions": {"InstanceId": "i-456"},
                "value": 2,
            },
        ]
        # Valid first line, malformed second line, in one record.
        body = json.dumps(metrics[0]) + "\n" + '{"invalid": "json"' + "\n"
        encoded_data = base64.b64encode(body.encode("utf-8")).decode("utf-8")
        event = {"records": [{"recordId": "half-bad", "data": encoded_data}]}
        mock_tags = {"Organization GUID": "org-aaa", "Space GUID": "space-bbb"}

        context = MagicMock()
        context.aws_request_id = "req-1"

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        s3_client = s3_capture()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ), patched_clients(s3_client):
            result = lambda_handler(event, context)

        assert result["records"][0]["result"] == "ProcessingFailed"
        # The first line parsed fine, but nothing is written for a failed record
        s3_client.put_object.assert_not_called()

    def test_process_metric_valid(self, monkeypatch):
        """Test process_metric function with valid data"""
        input_metric = {
            "timestamp": 1640995200000,
            "namespace": "AWS/S3",
            "metric_name": "Duration",
            "dimensions": {"FunctionName": "my-function"},
            "value": 150.5,
            "unit": "Milliseconds",
        }
        mock_tags = {"Environment": "production", "Owner": "team-alpha"}

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        with patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ):
            result = process_metric(
                input_metric, dummy_region, "", "", "", "", "", "", "", "", 123456
            )

        assert result is not None
        assert result["namespace"] == "AWS/S3"
        assert result["metric_name"] == "Duration"
        assert result["value"] == 150.5

        assert result["Tags"]["Environment"] == "production"
        assert result["Tags"]["Owner"] == "team-alpha"

    def test_process_metric_missing_required_fields(self):
        """Test process_metric with missing required fields"""
        # Missing metric_name
        invalid_metric = {
            "timestamp": 1640995200000,
            "namespace": "AWS/ES",
            "value": 100,
        }

        result = process_metric(
            invalid_metric, dummy_region, "", "", "", "", "", "", "", "", 123456
        )
        assert result is None

        # Missing value
        invalid_metric2 = {
            "timestamp": 1640995200000,
            "namespace": "AWS/Test",
            "metric_name": "ES",
        }

        result2 = process_metric(
            invalid_metric2, dummy_region, "", "", "", "", "", "", "", "", 123456
        )
        assert result2 is None

    def test_process_metric_missing_namespace(self):
        """Test process_metric with missing namespace"""
        # Missing metric_name
        invalid_namespace = {
            "timestamp": 1640995200000,
            "namespace": "AWS/Test",
            "value": 100,
        }

        result = process_metric(
            invalid_namespace, dummy_region, "", "", "", "", "", "", "", "", 123456
        )
        assert result is None

        # Missing value
        invalid_metric2 = {
            "timestamp": 1640995200000,
            "namespace": "AWS/Test",
            "metric_name": "TestMetric",
        }

        result2 = process_metric(
            invalid_metric2, dummy_region, "", "", "", "", "", "", "", "", 123456
        )
        assert result2 is None

    def test_key_removal_configuration(self):
        """Test that default keys are properly configured"""
        expected_keys = ["metric_stream_name", "account_id", "region"]
        assert default_keys_to_remove == expected_keys

    def test_clientid_dimension_removal(self, monkeypatch):
        """Test that ClientId is removed from dimensions"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/ES",
            "metric_name": "TestMetric",
            "dimensions": {
                "InstanceId": "i-123",
                "ClientId": "should-be-removed",
                "OtherDim": "should-stay",
            },
            "value": 100,
        }
        mock_tags = {
            "Environment": "production",
            "Owner": "team-alpha",
            "Organization GUID": "org-aaa",
            "Space GUID": "space-bbb",
        }

        ndjson_data = json.dumps(metric_data) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")

        event = {"records": [{"recordId": "test-record", "data": encoded_data}]}
        context = MagicMock()
        context.aws_request_id = "req-1"

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", "development")
        monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")

        s3_client = s3_capture()
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ), patched_clients(s3_client):
            lambda_handler(event, context)

        _, output_metrics = only_object(s3_client)
        output_metric = output_metrics[0]

        assert "ClientId" not in output_metric["dimensions"]
        assert "InstanceId" in output_metric["dimensions"]
        assert "OtherDim" in output_metric["dimensions"]

    @pytest.mark.parametrize(
        "environment, expected_s3_prefix, expected_domain_prefix, expected_rds_prefix, expected_redis_prefix",
        [
            pytest.param(
                "development",
                "development-cg-",
                "cg-broker-dev-",
                "cg-aws-broker-dev",
                "dev-",
            ),
            pytest.param(
                "staging",
                "staging-cg-",
                "cg-broker-stg-",
                "cg-aws-broker-stage",
                "stg-",
            ),
            pytest.param(
                "production", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd-"
            ),
        ],
    )
    def test_get_resource_tags_from_metric_es_success(
        self,
        monkeypatch,
        environment,
        expected_s3_prefix,
        expected_domain_prefix,
        expected_rds_prefix,
        expected_redis_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)

        rds_prefix, s3_prefix, domain_prefix, redis_prefix = make_prefixes()
        assert s3_prefix == expected_s3_prefix
        assert domain_prefix == expected_domain_prefix
        assert rds_prefix == expected_rds_prefix
        assert redis_prefix == expected_redis_prefix

        """Test that environment only accepts environment prefix when correct environment"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/ES",
            "metric_name": "TestMetric",
            "dimensions": {
                "InstanceId": "i-123",
                "DomainName": f"{domain_prefix}-jason-test",
                "ClientId": 123456,
            },
            "value": 100,
        }

        # Create a stubbed es client
        es_client = boto3.client("es", region_name=dummy_region)

        stubber = Stubber(es_client)
        fake_arn = f"arn:aws-us-gov:es:us-gov-west-1:{metric_data['dimensions']['ClientId']}:domain/{metric_data['dimensions']['DomainName']}"
        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": environment},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }
        expected_param_for_stub = {"ARN": fake_arn}
        stubber.add_response("list_tags", fake_tags, expected_param_for_stub)
        stubber.activate()

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=es_client
        ):

            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "",
                "",
                es_client,
                expected_domain_prefix,
                "",
                "",
                "",
                "",
                123456,
            )

        # if tags are returned environment is correct
        assert result["Environment"] == environment
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"

    @pytest.mark.parametrize(
        "environment, expected_s3_prefix, expected_domain_prefix, expected_rds_prefix,expected_redis_prefix",
        [
            pytest.param(
                "development", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd-"
            ),
            pytest.param(
                "staging", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd-"
            ),
            pytest.param(
                "production",
                "development-cg-",
                "cg-broker-dev-",
                "cg-aws-broker-dev",
                "dev-",
            ),
        ],
    )
    def test_get_resource_tags_from_metric_es_failure(
        self,
        monkeypatch,
        environment,
        expected_s3_prefix,
        expected_domain_prefix,
        expected_rds_prefix,
        expected_redis_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)

        rds_prefix, s3_prefix, domain_prefix, redis_prefix = make_prefixes()
        assert s3_prefix != expected_s3_prefix
        assert domain_prefix != expected_domain_prefix
        assert rds_prefix != expected_rds_prefix
        assert redis_prefix != expected_redis_prefix

        """Test that environment will not accept the wrong prefix if wrong environment"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/ES",
            "metric_name": "TestMetric",
            "dimensions": {
                "InstanceId": "i-123",
                "DomainName": f"{domain_prefix}-jason-test",
                "ClientId": 123456,
            },
            "value": 100,
        }

        # Create a stubbed es client
        es_client = boto3.client("es", region_name=dummy_region)

        stubber = Stubber(es_client)
        fake_arn = f"arn:aws-us-gov:es:us-gov-west-1:{metric_data['dimensions']['ClientId']}:domain/{metric_data['dimensions']['DomainName']}"
        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": environment},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }
        expected_param_for_stub = {"ARN": fake_arn}
        stubber.add_response("list_tags", fake_tags, expected_param_for_stub)
        stubber.activate()

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=es_client
        ):

            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "",
                "",
                es_client,
                expected_domain_prefix,
                "",
                "",
                "",
                "",
                123456,
            )

        # if tags are returned empty environment mismatch does not return tags
        assert result == {}

    @pytest.mark.parametrize(
        "environment, expected_s3_prefix, expected_domain_prefix, expected_rds_prefix,expected_redis_prefix",
        [
            pytest.param(
                "development",
                "development-cg-",
                "cg-broker-dev-",
                "cg-aws-broker-dev",
                "dev-",
            ),
            pytest.param(
                "staging",
                "staging-cg-",
                "cg-broker-stg-",
                "cg-aws-broker-stage",
                "stg-",
            ),
            pytest.param(
                "production", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd-"
            ),
        ],
    )
    def test_get_resource_tags_from_metric_redis_success(
        self,
        monkeypatch,
        environment,
        expected_s3_prefix,
        expected_domain_prefix,
        expected_rds_prefix,
        expected_redis_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)
        rds_prefix, s3_prefix, domain_prefix, redis_prefix = make_prefixes()
        assert s3_prefix == expected_s3_prefix
        assert domain_prefix == expected_domain_prefix
        assert rds_prefix == expected_rds_prefix
        assert redis_prefix == expected_redis_prefix
        """Test that environment only accepts environment prefix when correct environment"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/ElastiCache",
            "metric_name": "TestMetric",
            "dimensions": {
                "CacheClusterId": f"{redis_prefix}jason-test",
                "ClientId": 123456,
            },
            "value": 100,
        }
        # Create a stubbed elasticache client
        elasticache_client = boto3.client("elasticache", region_name=dummy_region)
        stubber = Stubber(elasticache_client)
        fake_arn = f"arn:aws-us-gov:elasticache:us-gov-west-1:{metric_data['dimensions']['ClientId']}:cluster:{metric_data['dimensions']['CacheClusterId']}"
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
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=elasticache_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "",
                "",
                "",
                "",
                "",
                "",
                elasticache_client,
                redis_prefix,
                123456,
            )
        # if tags are returned environment is correct
        assert result["Environment"] == environment
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"

    @pytest.mark.parametrize(
        "environment, expected_s3_prefix, expected_domain_prefix, expected_rds_prefix,expected_redis_prefix",
        [
            pytest.param(
                "development", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd-"
            ),
            pytest.param(
                "staging", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd-"
            ),
            pytest.param(
                "production",
                "development-cg-",
                "cg-broker-dev-",
                "cg-aws-broker-dev",
                "dev-",
            ),
        ],
    )
    def test_get_resource_tags_from_metric_redis_failure(
        self,
        monkeypatch,
        environment,
        expected_s3_prefix,
        expected_domain_prefix,
        expected_rds_prefix,
        expected_redis_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)

        rds_prefix, s3_prefix, domain_prefix, redis_prefix = make_prefixes()
        assert s3_prefix != expected_s3_prefix
        assert domain_prefix != expected_domain_prefix
        assert rds_prefix != expected_rds_prefix
        assert redis_prefix != expected_redis_prefix

        """Test that environment only accepts environment prefix when correct environment"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/ElastiCache",
            "metric_name": "TestMetric",
            "dimensions": {
                "CacheClusterId": f"{expected_redis_prefix}jason-test",
                "ClientId": 123456,
            },
            "value": 100,
        }
        # Create a stubbed elasticache client
        elasticache_client = boto3.client("elasticache", region_name=dummy_region)
        stubber = Stubber(elasticache_client)
        fake_arn = f"arn:aws-us-gov:elasticache:us-gov-west-1:{metric_data['dimensions']['ClientId']}:cluster:{metric_data['dimensions']['CacheClusterId']}"
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

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=elasticache_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "",
                "",
                "",
                "",
                "",
                "",
                elasticache_client,
                redis_prefix,
                123456,
            )

        # if tags are returned empty environment mismatch does not return tags
        assert result == {}

    @pytest.mark.parametrize(
        "environment, expected_s3_prefix, expected_domain_prefix, expected_rds_prefix,expected_redis_prefix",
        [
            pytest.param(
                "development",
                "development-cg-",
                "cg-broker-dev-",
                "cg-aws-broker-dev",
                "dev-",
            ),
            pytest.param(
                "staging",
                "staging-cg-",
                "cg-broker-stg-",
                "cg-aws-broker-stage",
                "stg-",
            ),
            pytest.param(
                "production", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd-"
            ),
        ],
    )
    def test_get_resource_tags_from_metric_s3_success(
        self,
        monkeypatch,
        environment,
        expected_s3_prefix,
        expected_domain_prefix,
        expected_rds_prefix,
        expected_redis_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)

        rds_prefix, s3_prefix, domain_prefix, redis_prefix = make_prefixes()
        assert s3_prefix == expected_s3_prefix
        assert domain_prefix == expected_domain_prefix
        assert rds_prefix == expected_rds_prefix
        assert redis_prefix == expected_redis_prefix

        """Test that environment only accepts environment prefix that match environment"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/S3",
            "metric_name": "TestMetric",
            "dimensions": {
                "InstanceId": "i-123",
                "BucketName": f"{s3_prefix}testing-cheats-enabled",
            },
            "value": 100,
        }

        # Create a stubbed s3 client
        s3_client = boto3.client("s3", region_name=dummy_region)

        stubber = Stubber(s3_client)
        fake_bucket = f"{expected_s3_prefix}testing-cheats-enabled"

        fake_tags = {
            "TagSet": [
                {"Key": "Environment", "Value": environment},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }
        expected_param_for_stub = {"Bucket": fake_bucket}
        stubber.add_response("get_bucket_tagging", fake_tags, expected_param_for_stub)
        stubber.activate()

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=s3_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                s3_client,
                s3_prefix,
                "",
                "",
                "",
                "",
                "",
                "",
                123456,
            )

        # if tags are returned environment is correct
        assert result["Environment"] == environment
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"

    @pytest.mark.parametrize(
        "environment, expected_s3_prefix, expected_domain_prefix, expected_rds_prefix,expected_redis_prefix",
        [
            pytest.param(
                "development", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd-"
            ),
            pytest.param(
                "staging", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd-"
            ),
            pytest.param(
                "production",
                "development-cg-",
                "cg-broker-dev-",
                "cg-aws-broker-dev",
                "dev-",
            ),
        ],
    )
    def test_get_resource_tags_from_metric_s3_failure(
        self,
        monkeypatch,
        environment,
        expected_s3_prefix,
        expected_domain_prefix,
        expected_rds_prefix,
        expected_redis_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)

        rds_prefix, s3_prefix, domain_prefix, redis_prefix = make_prefixes()
        assert s3_prefix != expected_s3_prefix
        assert domain_prefix != expected_domain_prefix
        assert rds_prefix != expected_rds_prefix
        assert redis_prefix != expected_redis_prefix

        """Test that environment only accepts environment prefix that match environment"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/S3",
            "metric_name": "TestMetric",
            "dimensions": {
                "InstanceId": "i-123",
                "BucketName": f"{s3_prefix}testing-cheats-enabled",
            },
            "value": 100,
        }

        # Create a stubbed s3 client
        s3_client = boto3.client("s3", region_name=dummy_region)

        stubber = Stubber(s3_client)
        fake_bucket = f"{expected_s3_prefix}testing-cheats-enabled"

        fake_tags = {
            "TagSet": [
                {"Key": "Environment", "Value": environment},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }
        expected_param_for_stub = {"Bucket": fake_bucket}
        stubber.add_response("get_bucket_tagging", fake_tags, expected_param_for_stub)
        stubber.activate()

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=s3_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                s3_client,
                s3_prefix,
                "",
                "",
                "",
                "",
                "",
                "",
                123456,
            )

        # if tags are returned empty environment mismatch does not return tags
        assert result == {}

    @pytest.mark.parametrize(
        "environment, expected_s3_prefix, expected_domain_prefix, expected_rds_prefix,expected_redis_prefix",
        [
            pytest.param(
                "development",
                "development-cg-",
                "cg-broker-dev-",
                "cg-aws-broker-dev",
                "dev-",
            ),
            pytest.param(
                "staging",
                "staging-cg-",
                "cg-broker-stg-",
                "cg-aws-broker-stage",
                "stg-",
            ),
            pytest.param(
                "production", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd-"
            ),
        ],
    )
    def test_get_resource_tags_from_metric_rds_success(
        self,
        monkeypatch,
        environment,
        expected_s3_prefix,
        expected_domain_prefix,
        expected_rds_prefix,
        expected_redis_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)
        monkeypatch.setenv("CLIENT", "123456")

        rds_prefix, s3_prefix, domain_prefix, redis_prefix = make_prefixes()
        assert s3_prefix == expected_s3_prefix
        assert domain_prefix == expected_domain_prefix
        assert rds_prefix == expected_rds_prefix
        assert redis_prefix == expected_redis_prefix

        """Test that environment only accepts environment prefix that match environment"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/RDS",
            "metric_name": "TestMetric",
            "dimensions": {
                "DBInstanceIdentifier": f"{rds_prefix}testing-cheats-enabled",
                "ClientId": 123456,
            },
            "value": 100,
        }

        # Create a stubbed rds client
        rds_client = boto3.client("rds", region_name=dummy_region)

        stubber = Stubber(rds_client)
        fake_arn = f"arn:aws-us-gov:rds:us-gov-west-1:{metric_data['dimensions']['ClientId']}:db:{metric_data['dimensions']['DBInstanceIdentifier']}"

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

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=rds_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "",
                "",
                "",
                "",
                rds_client,
                expected_rds_prefix,
                "",
                "",
                123456,
            )

        # if tags are returned environment is correct
        assert result["Environment"] == environment
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"

    @pytest.mark.parametrize(
        "environment, expected_s3_prefix, expected_domain_prefix, expected_rds_prefix,expected_redis_prefix",
        [
            pytest.param(
                "development", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd"
            ),
            pytest.param(
                "staging", "cg-", "cg-broker-prd-", "cg-aws-broker-prod", "prd"
            ),
            pytest.param(
                "production",
                "development-cg-",
                "cg-broker-dev-",
                "cg-aws-broker-dev",
                "dev",
            ),
        ],
    )
    def test_get_resource_tags_from_metric_rds_failure(
        self,
        monkeypatch,
        environment,
        expected_s3_prefix,
        expected_domain_prefix,
        expected_rds_prefix,
        expected_redis_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)

        rds_prefix, s3_prefix, domain_prefix, redis_prefix = make_prefixes()
        assert s3_prefix != expected_s3_prefix
        assert domain_prefix != expected_domain_prefix
        assert rds_prefix != expected_rds_prefix
        assert redis_prefix != expected_redis_prefix

        """Test that environment only accepts environment prefix that match environment"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/RDS",
            "metric_name": "TestMetric",
            "dimensions": {
                "DBInstanceIdentifier": f"{rds_prefix}testing-cheats-enabled",
                "ClientId": 123456,
            },
            "value": 100,
        }

        # Create a stubbed rds client
        rds_client = boto3.client("rds", region_name=dummy_region)

        stubber = Stubber(rds_client)
        fake_arn = f"arn:aws-us-gov:rds:us-gov-west-1:{metric_data['dimensions']['ClientId']}:db:{metric_data['dimensions']['DBInstanceIdentifier']}"

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

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=rds_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "",
                "",
                "",
                "",
                rds_client,
                expected_rds_prefix,
                "redis_client",
                "",
                123456,
            )

        ## if tags are returned empty environment mismatch does not return tags
        assert result == {}

    def test_s3_tag_retrieval(self, monkeypatch):
        """Test that s3 tags are returned"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/S3",
            "metric_name": "TestMetric",
            "dimensions": {
                "InstanceId": "i-123",
                "BucketName": "cg-testing-cheats-enabled",
            },
            "value": 100,
        }

        # Create a stubbed s3 client
        s3_client = boto3.client("s3", region_name=dummy_region)

        stubber = Stubber(s3_client)
        fake_bucket = "cg-testing-cheats-enabled"

        fake_tags = {
            "TagSet": [
                {"Key": "Environment", "Value": "staging"},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }
        expected_param_for_stub = {"Bucket": fake_bucket}
        stubber.add_response("get_bucket_tagging", fake_tags, expected_param_for_stub)
        stubber.activate()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=s3_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                s3_client,
                "cg-",
                "es_client",
                "cg-broker-dev",
                "rds_client",
                "cg-broker_aws_dev",
                "redis_client",
                "",
                123456,
            )

        assert result["Environment"] == "staging"
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"

    def test_s3_tags_none(self, monkeypatch):
        """Test that none is returned when tags are none"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/S3",
            "metric_name": "TestMetric",
            "dimensions": {
                "InstanceId": "i-123",
                "BucketName": "cg-testing-cheats-enabled",
            },
            "value": 100,
        }

        # Create a stubbed s3 client
        s3_client = boto3.client("s3", region_name=dummy_region)

        stubber = Stubber(s3_client)
        fake_bucket = "cg-testing-cheats-enabled"

        fake_tags = {"TagSet": []}
        expected_param_for_stub = {"Bucket": fake_bucket}
        stubber.add_response("get_bucket_tagging", fake_tags, expected_param_for_stub)
        stubber.activate()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=s3_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                s3_client,
                "cg-",
                "es_client",
                "cg-broker-dev",
                "rds_client",
                "cg-broker_aws_dev",
                "redis_client",
                "",
                123456,
            )

        assert result == {}

    def test_es_tag_retrieval(self, monkeypatch):
        """Test that es tags are returned"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/ES",
            "metric_name": "TestMetric",
            "dimensions": {
                "InstanceId": "i-123",
                "DomainName": "cg-broker-jason-test",
                "ClientId": 123456,
            },
            "value": 100,
        }

        # Create a stubbed es client
        es_client = boto3.client("es", region_name=dummy_region)

        stubber = Stubber(es_client)
        fake_arn = f"arn:aws-us-gov:es:us-gov-west-1:{metric_data['dimensions']['ClientId']}:domain/{metric_data['dimensions']['DomainName']}"
        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": "staging"},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }
        expected_param_for_stub = {"ARN": fake_arn}
        stubber.add_response("list_tags", fake_tags, expected_param_for_stub)
        stubber.activate()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=es_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "s3_client",
                "cg-",
                es_client,
                "cg-broker",
                "rds_client",
                "cg-broker_aws_dev",
                "redis_client",
                "",
                123456,
            )

        assert result["Environment"] == "staging"
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"

    def test_es_tags_none(self, monkeypatch):
        """Test that none is returned when tags are none"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/ES",
            "metric_name": "TestMetric",
            "dimensions": {
                "InstanceId": "i-123",
                "DomainName": "cg-broker-jason-test",
                "ClientId": 123456,
            },
            "value": 100,
        }

        # Create a stubbed es client
        es_client = boto3.client("es", region_name=dummy_region)

        stubber = Stubber(es_client)
        fake_arn = f"arn:aws-us-gov:es:us-gov-west-1:{metric_data['dimensions']['ClientId']}:domain/{metric_data['dimensions']['DomainName']}"

        fake_tags = {"TagList": []}
        expected_param_for_stub = {"ARN": fake_arn}
        stubber.add_response("list_tags", fake_tags, expected_param_for_stub)
        stubber.activate()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=es_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "s3_client",
                "cg-",
                es_client,
                "cg-broker",
                "rds_client",
                "cg-broker_aws_dev",
                "redis_client",
                "",
                123456,
            )

        assert result == {}

    def test_rds_tag_retrieval(self, monkeypatch):
        """Test that rds tags are returned"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/RDS",
            "metric_name": "TestMetric",
            "dimensions": {
                "DBInstanceIdentifier": "cg-aws-broker-prodjasontest",
                "ClientId": 123456,
            },
            "value": 100,
        }

        # Create a stubbed rds client
        rds_client = boto3.client("rds", region_name=dummy_region)

        stubber = Stubber(rds_client)
        fake_arn = f"arn:aws-us-gov:rds:us-gov-west-1:{metric_data['dimensions']['ClientId']}:db:{metric_data['dimensions']['DBInstanceIdentifier']}"
        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": "staging"},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }
        expected_param_for_stub = {"ResourceName": fake_arn}
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        stubber.activate()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=rds_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "s3_client",
                "cg-",
                "es_client",
                "cg-broker",
                rds_client,
                "cg-aws-broker-prod",
                "redis_client",
                "",
                123456,
            )

        assert result["Environment"] == "staging"
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"

    def test_rds_tag_retrieval_with_size(self, monkeypatch):
        """Test that rds tags are returned"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/RDS",
            "metric_name": "FreeStorageSpace",
            "dimensions": {
                "DBInstanceIdentifier": "cg-aws-broker-prodjasontest",
                "ClientId": 123456,
            },
            "value": 100,
        }

        # Create a stubbed rds client
        rds_client = boto3.client("rds", region_name=dummy_region)

        stubber = Stubber(rds_client)
        fake_arn = f"arn:aws-us-gov:rds:us-gov-west-1:{metric_data['dimensions']['ClientId']}:db:{metric_data['dimensions']['DBInstanceIdentifier']}"
        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": "staging"},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }
        expected_param_for_stub = {"ResourceName": fake_arn}
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        expected_param_for_describe = {
            "DBInstanceIdentifier": metric_data["dimensions"]["DBInstanceIdentifier"]
        }
        fake_describe = {"DBInstances": [{"AllocatedStorage": 100}]}
        stubber.add_response(
            "describe_db_instances", fake_describe, expected_param_for_describe
        )
        stubber.activate()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=rds_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "s3_client",
                "cg-",
                "es_client",
                "cg-broker",
                rds_client,
                "cg-aws-broker-prod",
                "redis_client",
                "",
                123456,
            )

        assert result["db_size"] == 100
        assert result["Environment"] == "staging"
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"

    def test_rds_tags_none(self, monkeypatch):
        """Test that none is returned when tags are none"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/RDS",
            "metric_name": "TestMetric",
            "dimensions": {
                "DBInstanceIdentifier": "cg-aws-broker-prodjasontest",
                "ClientId": 123456,
            },
            "value": 100,
        }

        # Create a stubbed rds client
        rds_client = boto3.client("rds", region_name=dummy_region)

        stubber = Stubber(rds_client)
        fake_arn = f"arn:aws-us-gov:rds:us-gov-west-1:{metric_data['dimensions']['ClientId']}:db:{metric_data['dimensions']['DBInstanceIdentifier']}"
        fake_tags = {"TagList": []}
        expected_param_for_stub = {"ResourceName": fake_arn}
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        stubber.activate()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=rds_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "s3_client",
                "cg-",
                "es_client",
                "cg-broker",
                rds_client,
                "cg-aws-broker-prod",
                "redis_client",
                "",
                123456,
            )

        assert result == {}

    def test_rds_bad_tags_freestorage(self, monkeypatch):
        """Test that none is returned when tags are none"""
        metric_data = {
            "timestamp": 1640995200000,
            "namespace": "AWS/RDS",
            "metric_name": "FreeStorageSpace",
            "dimensions": {
                "DBInstanceIdentifier": "cg-aws-broker-prodjasontest",
                "ClientId": 123456,
            },
            "value": 100,
        }

        # Create a stubbed rds client
        rds_client = boto3.client("rds", region_name=dummy_region)

        stubber = Stubber(rds_client)
        fake_arn = (
            "arn:aws-us-gov:rds:us-gov-west-1:123456:db:cg-aws-broker-prodjasontest"
        )
        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": "staging"},
                {"Key": "Testing", "Value": "enabled"},
            ]
        }
        expected_param_for_stub = {"ResourceName": fake_arn}
        expected_param_for_describe = {
            "DBInstanceIdentifier": "cg-aws-broker-prodjasontest"
        }
        fake_describe = {"DBInstances": [{"AllocatedStorage": 100}]}
        stubber.add_response(
            "describe_db_instances", fake_describe, expected_param_for_describe
        )
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        stubber.activate()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=rds_client
        ):
            result = get_resource_tags_from_metric(
                metric_data,
                dummy_region,
                "s3_client",
                "cg-",
                "es_client",
                "cg-broker",
                rds_client,
                "cg-aws-broker-prod",
                "redis_client",
                "",
                123456,
            )

        assert result == {}
