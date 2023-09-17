# %%
from provider import *

provider = Dev_Provider()
dynamodb = provider.get_dynamodb()

def fetch_trainings():
    mongodb = provider.get_mongodb()
    db = mongodb['sofie']
    collection = db.training_video
    cursor = collection.find()
    items = list()
    for item in cursor:
        items.append(item)
    return items

def get_all_trainings():
    data = fetch_trainings()
    return data

def main(event, context):
    data = {}
    code = 401

    # SEU CODIGO AQUI :)
    try:
        trainings = get_all_trainings()
        data = {'trainings': trainings}
        code = 201
    except Exception as e:
        code = 500
        data = {'error': e}

    return {
            'statusCode': code,
            'body': JSONEncoder().encode(data)
        }