import json
import boto3

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.logging.formatters.datadog import DatadogLogFormatter

from sample_module import Sample


# -------------------------------------------------------------------------------------------------------
# Logging utils

logger = Logger(service="sample-lambda1", logger_formatter=DatadogLogFormatter())

# -------------------------------------------------------------------------------------------------------

def _get_s3_client():
    return boto3.client("s3")


@logger.inject_lambda_context
def handler(event: dict, context: LambdaContext):

    s3_client = _get_s3_client()

    sample = Sample(10)

    records = event.get('Records', [])

    for record in records:
        try:
            if (body := json.loads(record.get("body"))) and (value := body.get("value")):
                if (value + sample.get_value()) % 5 == 0:
                    logger.info(f"body.value = {body.get("value")}, sample.value = {sample.get_value()}")
                    s3_client.put_object(Bucket="my-bucket",
                                         Key=f"object-{value}.txt",
                                         Body=f"Value is {value}")
                    sample.set_value(sample.get_value() + 1)
            else:
                logger.warning(f"no 'body' or invalid in record = {record}")
        except Exception as ex:
            logger.error(f"error processing record = {record} ({ex})")
