from provider import *
import json
import os

provider = Dev_Provider()
dynamodb = provider.get_dynamodb()

def get_all_trainings():
    table = dynamodb.Table(os.environ['DEV_VIDEO_TABLE'])

    scan_kwargs = dict()

    items = list()
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        for item in (response.get('Items')):
            items.append((item))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    return items

def main(event, context):
    data = {}
    code = 401
    provider = Dev_Provider()
    
    # SEU CODIGO AQUI :)

    trainings = get_all_trainings()
    data = {'trainings': trainings}
    code = 201
    
    return {
            'statusCode': code,
            'body': json.dumps(data, default=json_serial)
        }