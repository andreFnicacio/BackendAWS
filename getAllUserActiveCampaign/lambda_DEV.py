from provider import Dev_Provider, JSONEncoder
import json

provider = Dev_Provider()

def main(event, context):
    data = {}
    code = 401

    try:
        queryStringParameters = event['queryStringParameters']
        sofier = queryStringParameters['sofier']
        data = get_user_goals(sofier)
        code = 201
    except Exception as e:
        code = 500
        data = {'error': e}
    
    return {
            'statusCode': code,
            'body': JSONEncoder().encode(data)
        }

def get_user_goals(sofier):
    mongodb = provider.get_mongodb()
    db = mongodb['sofie']
    collection = db['__dev_goals_campaign']
    cursor = collection.find({'sofier': sofier})
    items = list()
    for item in cursor:
        new_item = {'campaign': item['campaign'], 'tasks': item['tasks']}
        items.append(new_item)
    return {'goals': items}
