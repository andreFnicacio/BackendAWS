import json

from provider import *
from bson.json_util import dumps, loads

provider = Dev_Provider()

dynamodb = provider.get_dynamodb()

def buscar_tabela_goals(query):
        mongodb = provider.get_mongodb()
        db = mongodb['sofie']
        sofier_info = db['__dev_goals_campaign']
        sofier_info = sofier_info.find(query)

        # CAPTURA DAS QUESTIONS EXECUTION

        finishedTask = list()

        for data in sofier_info:
            finishedTask.append(data['sofier'])
        print(finishedTask)
        return finishedTask
 

def created_dispatch(item): 
    mongodb = provider.get_mongodb()
    db = mongodb['sofie']
    sofier_info = db['__dev_goals_campaign']
    item['tasks'] =  0 
    dispatch_result = sofier_info.insert_many([item])       
    return dispatch_result.inserted_ids 


def main(event):
    body = event['body']
    return_data = 'Success'
    status_code = 201
    print(body)
    body_count = json.loads(body) 
    create_campaign_goals_data = None

    verify_sofier = buscar_tabela_goals(body_count)

    if len(verify_sofier) <= 0:
        create_campaign_goals_data = created_dispatch(body_count)
        print(create_campaign_goals_data)
    else:
        status_code = 401         
        return_data = 'user already registered'



    return {
        'statusCode': status_code,
        'body': json.dumps(return_data)    
    }        

