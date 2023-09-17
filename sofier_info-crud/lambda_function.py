import json
import lambda_DEV
import lambda_PROD


def lambda_handler(event, context):
    stage = event['stageVariables']['lambda']
    if stage == "PROD":
        data = lambda_PROD.main(event)
    elif stage == "DEV":
        data = lambda_DEV.main(event)

    return data