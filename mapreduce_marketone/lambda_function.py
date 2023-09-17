import json
from provider import *

provider = Dev_Provider()

def lambda_handler(event, context):
    # TODO implement
    mongodb_client = provider.get_mongodb()
    db = mongodb_client['marketone']
    collection = db.warehouse
    event['_id'] = event['execution_id']
    cursor = collection.insert_one(event)
    print(cursor)
    print(event)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
