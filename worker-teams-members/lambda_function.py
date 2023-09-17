import json
import lambda_DEV
import lambda_PROD


def lambda_handler(event, context):
    print(event)
    stage = event['stageVariables']['lambda']
    if stage == "DEV":
        data = lambda_DEV.lambda_handler(event)
        
    elif stage == "PROD":
        data = lambda_PROD.lambda_handler(event)

    return data