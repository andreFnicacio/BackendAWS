import json

from provider import *

provider = Dev_Provider()

dynamodb = provider.get_dynamodb()

def buscar_tabela_execution(sofier):
        table = dynamodb.Table('table_sofier_ledger')
        params = dict(
            FilterExpression='#sofier = :sofier and #phase = :av',
            ExpressionAttributeNames={"#phase": "phase", "#sofier": "sofier"},
            ExpressionAttributeValues={
                ':sofier': sofier,
                ':av': 'AVAILABLE'
            }
        )

        tasks = list()

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
        print(tasks)
        count = 0
        balance = dict()
        for data in tasks:
            reward = int(data['reward'])
            print(reward)
            count += reward
    
        balance['balance'] = count

        return balance


def main(event):
    print(event)
    verify = event.get('pathParameters', None)         
    if verify != None :
        sofier = event['pathParameters'].get("sofier", None)
        if sofier != None:
            body = buscar_tabela_execution(sofier)
            print("BODY")
            print(body)        
        return {
            'statusCode': 200,
            'body': json.dumps(body)    
        }        

