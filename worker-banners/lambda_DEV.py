import json

from provider import *
from bson.json_util import dumps, loads

provider = Dev_Provider()
    
def buscar_tabela_banners():
    query = {'active': True}
    mongodb = provider.get_mongodb()
    db = mongodb['sofie']
    banners = db.banners
    banners = banners.find(query)
    list_final = list()

    # CAPTURA DAS QUESTIONS EXECUTION
    for data in banners:
        list_final.append(data)

    return list_final


def main(event):
    status_code = 400
    data = None
    
    try:
        response = buscar_tabela_banners()
        data = {'banners': response}
        status_code = 200
    except Exception as e:
        print(e)
        data = {"error": e}
    return {
        'statusCode': status_code,
        'body': JSONEncoder().encode(data)
    }        

