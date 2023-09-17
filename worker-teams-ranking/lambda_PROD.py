from gettext import find
import json
from operator import le
from collections import Counter
from re import I
from datetime import datetime, timedelta

from provider import *

provider = Prod_Provider()

dynamodb = provider.get_dynamodb()

def remove_repetidos(lista):

  lista_numeros_unicos = []

  for numero in lista:
    if(numero in lista_numeros_unicos): #Verifica se o atual elemento existe na lista original
      pass #Se existir não faz nada
    else:
      lista_numeros_unicos.append(numero) #Se não existir, adiciona com o comando append() o numero na lista

  return lista_numeros_unicos


def formated_date(hoje, wd):
    data_inicial = hoje - timedelta(days=wd)
    data_final = data_inicial + timedelta(days=6)   

    data_inicial_formated = data_inicial.strftime('%Y-%m-%dT%H:%M:%S.%f')   

    data_final_formated = data_final.strftime('%Y-%m-%dT%H:%M:%S.%f')  

    return data_inicial_formated, data_final_formated  

def buscar_tabela_execution(date_initial_formated, date_finish_formated):
    table = dynamodb.Table('table_micro_task_execution')
    params = dict(
        FilterExpression='#when.#start BETWEEN :dtI AND :dtF AND #result = :f',
        ExpressionAttributeNames={"#when": "when", "#start": "start","#result": "result"},
        ExpressionAttributeValues={
            ':dtI': date_initial_formated,
            ':dtF': date_finish_formated,
            ':f': 'FINISH'
        }
    )
    # response = table.cam(**params)
    tasks = list()
    while True:
        response = table.scan(**params)
        items = response.get("Items")
        if items:
            tasks.extend(items)
        else:
            break
        last_key = response.get('LastEvaluatedKey', None)
        if last_key != None:
            break        
        params['ExclusiveStartKey'] = last_key    
    data_final = list()
    for data in tasks:
        data_final.append(data['who'])
    return data_final

def buscar_tabela_sofiers(leader):
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
            finishedTask.append(data['sofier'])
        return finishedTask

def operator_main(event):
    hoje = datetime.today()
    wd = hoje.weekday()    
    list_to_up = list()    
    ranking = list()     
    verify_path_parameters = event.get('pathParameters', None)  
    date_initial, date_finish = formated_date(hoje, wd)         
    if verify_path_parameters != None :
        leader = event['pathParameters'].get("leader", None)
        if leader != None:
            members = buscar_tabela_sofiers(leader)
            value_ranking =  buscar_tabela_execution(date_initial, date_finish)
            for sofier in value_ranking:
                if sofier in members:
                    list_to_up.append(sofier)   
            count = { it: freq for it, freq in Counter(list_to_up).items() }      
            res = {k: v for k, v in sorted(count.items(), key=lambda item: item[1], reverse=True)}
            new_value = list(res.values())   
            for value in new_value:
                print(value)
                if value > 1:
                    for i in range(3):
                        find_max = max(res, key=res.get)
                        ranking.append({f'position_{i}': find_max})
                        del res[find_max]
                    break
                elif len(new_value) > 2: 
                    index = 0                
                    for key in res:
                        index += 1
                        ranking.append({f'position_{index}': key})     
                else:
                    ranking = []     
      

    if ranking != []:
        ranking_final = remove_repetidos(ranking)
    else: 
        ranking_final = [{'position_1': '-'}, {'position_2': '-'}, {'position_3': '-'}]
    return ranking_final
     



def main(event):
    status_code = 400
    data = None
       
    try:
        response = operator_main(event)
        data = response
        status_code = 200
    except Exception as e:
        print(e)
        data = {"error": e}
    return {
        'statusCode': status_code,
        'body': JSONEncoder().encode(data)
    } 