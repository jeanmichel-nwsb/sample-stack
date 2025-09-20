import json

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.logging.formatters.datadog import DatadogLogFormatter

from sample_module import Sample


# -------------------------------------------------------------------------------------------------------
# Logging utils

logger = Logger(service="sample-lambda1", logger_formatter=DatadogLogFormatter())

# -------------------------------------------------------------------------------------------------------


@logger.inject_lambda_context
def handler(event: dict, context: LambdaContext):

    sample = Sample(10)

    records = event.get('Records', [])

    for record in records:
        try:
            if body := json.loads(record.get("body")):
                if body.get("value") == sample.get_value():
                    logger.info(f"body.value = {body.get("value")}")
                    sample.set_value(sample.get_value() + 1)
                else:
                    logger.warning(f"no 'value' in record = {record}")
        except Exception as ex:
            logger.error(f"error procesing record = {record} ({ex})")
