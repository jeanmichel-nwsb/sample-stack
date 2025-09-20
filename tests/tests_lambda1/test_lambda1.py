import pytest
from aws_lambda_powertools.utilities.typing import LambdaContext

from lambda1.lambda1 import handler, logger


@pytest.fixture
def lambda_context() -> LambdaContext:
    context = LambdaContext()
    context._function_name = "lambda1"
    context._memory_limit_in_mb = 8192
    context._invoked_function_arn = "aws::"
    context._aws_request_id = "123456"
    return context


def test_handler(mocker, lambda_context):
    mocker.patch("lambda1.lambda1.Logger.info", return_value=None)
    mocker.patch("lambda1.lambda1.Logger.warning", return_value=None)
    mocker.patch("lambda1.lambda1.Logger.error", return_value=None)

    event = {
        "Records": [
            {
                "body": '{"value": 10}'
            },
            {
                "body": '{"value": 11}'
            },
            {
                "body": '{"value": 13}'
            },
            {
                "body": 'value 14'
            },
            {
                "body": '{"not_value": 15}'
            }
        ]
    }

    handler(event, lambda_context)

    assert logger.info.call_count == 2
    assert logger.warning.call_count == 2
    assert logger.error.call_count == 1
