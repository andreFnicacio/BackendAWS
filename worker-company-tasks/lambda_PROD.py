import json

from provider import *
from bson.json_util import dumps, loads

provider = Prod_Provider()

dynamodb = provider.get_dynamodb()

 

def buscar_tabela_banners(query):
        mongodb = provider.get_mongodb()
        db = mongodb['sofie']
        banners = db.banners
        banners = banners.find(query)
        list_final = list()

        # CAPTURA DAS QUESTIONS EXECUTION
        for data in banners:
            print(data)
            id = str(data['_id'])
            data['_id'] = id
            list_final.append(data)

        return list_final


def main(event):
    query_filter = dict()
    query_filter['active'] = True
    cursor = buscar_tabela_banners(query_filter) 

    return {
        'statusCode': 200,
        'body': json.dumps(cursor)    
    }        

