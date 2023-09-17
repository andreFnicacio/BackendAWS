import json
import lambda_DEV
import lambda_PROD


def lambda_handler(event, context):
    stage = event['stageVariables']['lambda']
    if stage == "DEV":
        data = lambda_DEV.main(event, context)
        
    elif stage == "PROD":
        data = lambda_PROD.main(event, context)

    return {
        'statusCode': 200,
        'body': json.dumps('SUCCESS!')
    }