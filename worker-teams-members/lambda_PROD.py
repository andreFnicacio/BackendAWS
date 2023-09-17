import json

from provider import *

provider = Dev_Provider()

dynamodb = provider.get_dynamodb()

def buscar_tabela_execution(leader):
        table = dynamodb.Table('table_sofier_info')
        params = dict(
            FilterExpression='#team = :leader',
            ExpressionAttributeNames={"#team": "team"},
            ExpressionAttributeValues={
                ':leader': leader
            }
        )

        tasks = list()
        finishedTask = list()

        while True:
            response = table.scan(**params)
            items = response.get("Items")
            if items:
                tasks.extend(items)

            last_key = response.get('LastEvaluatedKey')

            if not last_key:
                break

            params['ExclusiveStartKey'] = last_key

        # CAPTURA DAS QUESTIONS EXECUTION

        for data in tasks:

            finishedTask.append(data)

        return finishedTask


def lambda_handler(event):
    verify = event.get('pathParameters', None)         
    if verify != None :
        leader = event['pathParameters'].get("leader", None)
        if leader != None:
            body = buscar_tabela_execution(leader)
            print("BODY")
            print(body)        
        return {
            'statusCode': 200,
            'body': json.dumps(body)    
        }        

