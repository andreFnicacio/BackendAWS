import json
import lambda_DEV
import lambda_PROD


def lambda_handler(event, context):
    print("CONTEXT")
    print(context)
    print("event")
    print(event)
    stage = event['stageVariables']['lambda']
    if stage == "DEV":
        data = lambda_DEV.main(event)
        
    elif stage == "PROD":
        data = lambda_PROD.main(event)

    return data