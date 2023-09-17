# %%
from provider import *

provider = Dev_Provider()
dynamodb = provider.get_dynamodb()

def fetch_items_marketOne():
    mongodb = provider.get_mongodb()
    db = mongodb['marketone']
    collection = db.warehouse
    cursor = collection.find()
    items = list()
    for item in cursor:
        items.append(item)
    return items

def get_all_items_marketOne():
    data = fetch_items_marketOne()
    return data

def main(event, context):
    data = {}
    code = 401

    # SEU CODIGO AQUI :)
    try:
        items_marketOne = get_all_items_marketOne()
        data = items_marketOne
        code = 201
    except Exception as e:
        code = 500
        data = {'error': e}

    return {
            'statusCode': code,
            'body': JSONEncoder().encode(data)
        }