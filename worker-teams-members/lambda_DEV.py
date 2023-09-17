import json

from provider import *

provider = Dev_Provider()

dynamodb = provider.get_dynamodb()

 

def buscar_tabela_sofier(query):
        mongodb = provider.get_mongodb()
        db = mongodb['sofie']
        sofier_info = db.sofier_info
        sofier_info = sofier_info.find(query)

        # CAPTURA DAS QUESTIONS EXECUTION

        finishedTask = list()

        for data in sofier_info:

            finishedTask.append(data['sofier'])

        return finishedTask


def lambda_handler(event):
    verify = event.get('pathParameters', None)   
    query_filter = dict()      
    if verify != None :
        leader = event['pathParameters'].get("leader", None)
        if leader != None:
            query_filter['team'] = leader
            body = buscar_tabela_sofier(query_filter)      
        return {
            'statusCode': 200,
            'body': json.dumps(body)    
        }        

