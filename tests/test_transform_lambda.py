import json
import base64
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
        mock_tags = {"Environment": "production", "Owner": "team-alpha"}

        # Create newline-delimited JSON
        ndjson_data = json.dumps(metric_data) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")

        event = {"records": [{"recordId": "test-record-1", "data": encoded_data}]}

        context = MagicMock()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ):
            # Set up the mock return value
            result = lambda_handler(event, context)
        # Assertions
        assert "records" in result
        assert len(result["records"]) == 1
        assert result["records"][0]["recordId"] == "test-record-1"
        assert result["records"][0]["result"] == "Ok"

        # Decode and verify output
        output_data = base64.b64decode(result["records"][0]["data"]).decode("utf-8")
        output_metrics = [json.loads(line) for line in output_data.strip().split("\n")]

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
        mock_tags = {"Environment": "production", "Owner": "team-alpha"}

        # Create newline-delimited JSON
        ndjson_data = "\n".join([json.dumps(metric) for metric in metrics]) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")

        event = {"records": [{"recordId": "multi-metric-record", "data": encoded_data}]}

        context = MagicMock()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ):
            result = lambda_handler(event, context)

        assert len(result["records"]) == 1
        assert result["records"][0]["result"] == "Ok"

        # Decode and verify multiple metrics
        output_data = base64.b64decode(result["records"][0]["data"]).decode("utf-8")
        output_metrics = [json.loads(line) for line in output_data.strip().split("\n")]

        assert len(output_metrics) == 2
        assert output_metrics[0]["namespace"] == "AWS/ES"
        assert output_metrics[1]["namespace"] == "AWS/S3"
        assert output_metrics[0]["Tags"]["Environment"] == "production"
        assert output_metrics[0]["Tags"]["Owner"] == "team-alpha"
        assert output_metrics[1]["Tags"]["Environment"] == "production"
        assert output_metrics[1]["Tags"]["Owner"] == "team-alpha"

    def test_lambda_handler_many_rds_metric_lines(self, monkeypatch):
        """Test processing multiple metric lines in one record"""
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
            ]
        }
        expected_param_for_stub = {"ResourceName": fake_arn}

        # 1
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        expected_param_for_describe = {
            "DBInstanceIdentifier": "cg-aws-broker-prodjasontest"
        }
        fake_describe = {"DBInstances": [{"AllocatedStorage": 100}]}
        # 2
        stubber.add_response(
            "describe_db_instances", fake_describe, expected_param_for_describe
        )
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        # 3
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        # 4
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        stubber.activate()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=rds_client
        ):
            result = lambda_handler(event, context)

        assert len(result["records"]) == 1
        assert result["records"][0]["result"] == "Ok"

        # Decode and verify multiple metrics
        output_data = base64.b64decode(result["records"][0]["data"]).decode("utf-8")
        output_metrics = [json.loads(line) for line in output_data.strip().split("\n")]

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
        mock_tags = {"Environment": "production", "Owner": "team-alpha"}
        event = {"records": records}
        context = MagicMock()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ):
            result = lambda_handler(event, context)

        assert len(result["records"]) == 3
        for i, record in enumerate(result["records"]):
            assert record["recordId"] == f"record-{i}"
            assert record["result"] == "Ok"

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
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        with patch("lambda_functions.transform_lambda.logger"):
            result = lambda_handler(event, context)

        # Should return empty records list since no valid metrics
        assert len(result["records"]) == 1
        assert result["records"][0]["result"] == "Dropped"

    def test_lambda_handler_malformed_json(self, monkeypatch):
        """Test handling of malformed JSON"""
        malformed_data = '{"invalid": "json"'  # Not valid JSON
        encoded_data = base64.b64encode(malformed_data.encode("utf-8")).decode("utf-8")

        event = {"records": [{"recordId": "malformed-record", "data": encoded_data}]}

        context = MagicMock()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        with patch("lambda_functions.transform_lambda.logger") as mock_logger:
            with pytest.raises(json.JSONDecodeError):
                lambda_handler(event, context)

        mock_logger.error.assert_called()

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
        mock_tags = {"Environment": "production", "Owner": "team-alpha"}

        ndjson_data = json.dumps(metric_data) + "\n"
        encoded_data = base64.b64encode(ndjson_data.encode("utf-8")).decode("utf-8")

        event = {"records": [{"recordId": "test-record", "data": encoded_data}]}
        context = MagicMock()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        with patch("lambda_functions.transform_lambda.logger"), patch(
            "lambda_functions.transform_lambda.get_resource_tags_from_metric",
            return_value=mock_tags,
        ):
            result = lambda_handler(event, context)

        output_data = base64.b64decode(result["records"][0]["data"]).decode("utf-8")
        output_metric = json.loads(output_data.strip())

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
