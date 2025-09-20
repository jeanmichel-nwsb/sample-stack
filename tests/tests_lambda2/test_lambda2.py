import pytest
from aws_lambda_powertools.utilities.typing import LambdaContext

from lambda2.lambda2 import handler, _get_s3_client, logger


@pytest.fixture
def lambda_context() -> LambdaContext:
    context = LambdaContext()
    context._function_name = "lambda2"
    context._memory_limit_in_mb = 8192
    context._invoked_function_arn = "aws::"
    context._aws_request_id = "123456"
    return context


def test_handler(mocker, lambda_context):
    mocker.patch("lambda2.lambda2.Logger.info", return_value=None)
    mocker.patch("lambda2.lambda2.Logger.warning", return_value=None)
    mocker.patch("lambda2.lambda2.Logger.error", return_value=None)
    mocker.patch("lambda2.lambda2.boto3.client")
    s3_client = mocker.MagicMock()
    s3_client.put_object.return_value = None
    mocker.patch("lambda2.lambda2._get_s3_client", return_value=s3_client)

    event = {
        "Records": [
            {
                "body": '{"value": 10}'
            },
            {
                "body": '{"value": 11}'
            },
            {
                "body": '{"value": 12}'
            },
            {
                "body": 'value 13'
            },
            {
                "body": '{"not_value": 14}'
            }
        ]
    }

    handler(event, lambda_context)

    assert logger.info.call_count == 1
    assert logger.warning.call_count == 1
    assert logger.error.call_count == 1
    assert s3_client.put_object.call_count == 1
    s3_client.put_object.assert_called_with(Bucket="my-bucket",
                                            Key="object-10.txt",
                                            Body="Value is 10")
