#######################################################################
#                                                                     #
# >> micro_task-finish <<                                             #
#                                                                     #
# Têm por proósito registrar a finalização execução de uma tarefa     #
#                                                                     #
#######################################################################

import json
import boto3
import decimal
import math
import os
from provider import *

from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')

# Helper class to convert a DynamoDB item to JSON.


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return int(o) if o % 1 == 0 else float(o)

        return super(DecimalEncoder, self).default(o)


def main(event, context):
    
    task_id = event['pathParameters'].get('task_id')
    sofier = event['queryStringParameters'].get('sofier')
    execution_data = json.loads(event['body'], parse_float=decimal.Decimal)

    conf = {'aws': {'stage': event['stageVariables']['lambda']}}

    status, data = set_finish_execution(
        task_id, sofier, execution_data, **conf)
    print(status)
    print(data)

    return {
        'statusCode': status,
        'body': json.dumps(data, cls=DecimalEncoder),
        'headers': {
            'Access-Control-Allow-Origin': '*'
        }
    }


def distanceInMeters(origin, destination):
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6372800  # Earth radius in meters

    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi/2)**2 + math.cos(phi1) * \
        math.cos(phi2)*math.sin(dlambda/2)**2

    return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def set_finish_execution(task_id: str, sofier: str, execution_data: dict, **kwargs) -> tuple:
    """
    --

    [X] - Inserção na tabela `table_micro_task_execution`
    [X] - Edição na tabela `table_micro_task_in_person`
    """
    task_info_category = execution_data['task_info'].get('category') or 'in_person'
    task_info_name = execution_data['task_info']['name']

    #: Verificando se a execução da tarefa foi ADIADA, CANCELADA ou FINALIZADA
    postpone_or_cancel = 'FINISH'

    for each in execution_data['execution']:
        if each.get('context') == 'SYSTEM:TASK:POSTPONE_OR_CANCEL':
            postpone_or_cancel = each['response']
            break

    #: Determinando o RESULTADO da execução
    execution_location = execution_data.get('location', {})

    task_location = execution_data['task_info'].get('location', {})
    if task_location:
        distance = decimal.Decimal(str(distanceInMeters((execution_location.get('lat', 0),
                                                         execution_location.get('lng', 0)),
                                                        (task_location.get('lat', 0),
                                                         task_location.get('lng', 0)))))
        execution_data['distance'] = distance
    else:
        distance = None

    execution_data['result'] = postpone_or_cancel
    str_time_stamp = datetime.utcnow().isoformat()

    #: Editando a tabela `table_micro_task_in_person`
    state, status = ('FINISHED', 'SUCCESS') if task_info_category == 'challenger' else (
        'EXECUTED', 'EXECUTED')

    params_to_update = dict(
        Key={'task_id': task_id},
        UpdateExpression='SET #status.#distance = :d, #status.#state = :ste, #status.#status = :stus, last_movement = :lm, #status.last_sofier = :ls',
        ExpressionAttributeNames={'#distance': 'distance',
                                  '#status': 'status', '#state': 'state'},
        ExpressionAttributeValues={
            ':d': distance,
            ':ste': state,
            ':stus': status,
            ':lm': str_time_stamp,
            ':ls': sofier
        },
        ReturnValues='ALL_NEW'
    )

    #: Inserção na tabela `table_micro_task_execution`
    execution_data['timestamp'] = str_time_stamp
    if task_info_category == 'challenger':
        execution_data['audit'] = {
            "approved": True,
            "reason": "-",
            "when": str_time_stamp
        }
        dynamodb.Table(os.environ[f'{kwargs["aws"]["stage"]}_table_sofier_challenge']).put_item(
            Item=execution_data,
        )

    dynamodb.Table(os.environ[f'{kwargs["aws"]["stage"]}_table_micro_task_execution']).put_item(
        Item=execution_data,
    )

    # Chamada para finalizando a tarefa
    status_code, response = task_execution_finish(
        task_id, sofier, postpone_or_cancel, task_info_name, task_info_category, **kwargs)

    if status_code != 200:
        return status_code, response

    #: Efetuando o crédito na conta do *sofier* - VALIDATION
    if postpone_or_cancel == 'FINISH':

        #: Verificando o time do sofier
        if not task_info_category == 'challenger':
            team = dynamodb.Table('table_sofier_info').get_item(
                Key={'sofier': sofier},
                ProjectionExpression='team'
            ).get('Item', {}).get('team')
            if team:
                team = dynamodb.Table('table_sofier_info').query(
                    IndexName='username-index',
                    KeyConditionExpression='username = :u',
                    ExpressionAttributeValues={
                        ':u': team
                    },
                    ProjectionExpression='sofier'
                ).get('Items', [])
                if len(team) == 1:
                    execution_data['team'] = team[0]['sofier']

        task_info = dynamodb.Table(os.environ[f'{kwargs["aws"]["stage"]}_table_micro_task_in_person']).update_item(
            **params_to_update
        )['Attributes']

        if task_info['task']['category'] == 'challenger':
            address = 'Sofie'
            task_info_category == 'challenger'
        else:
            address = task_info['address']['formatted_address']
        # address = task_info['address']['formatted_address'] if not task_info_category == 'challenger' else 'CHALLENGER'
        cycle = task_info.get('cycle', None)
        
        print(f'Essa é uma tarefa do sofier {sofier}')
        reward = int(task_info['task']['reward'])
        print(f'A premiação de {reward} foi verificada')
        try:
            # PREMIAÇÃO ESPECIAL PARA OS VIPS - TEMPORÁREO
            provider = Prod_Provider()
            conn = provider.get_redis(db=1, decode_responses=True)
            # conn = Redis(host='54.94.105.153', port=5071, db=1, decode_responses=True)
            LIST_VIP = ['fernandogaba33@gmail.com', 'van_gonzales@hotmail.com', 'dantas-bruno@outlook.com', 'marciojordao@gmail.com', 'kath.melchior@gmail.com']
            print(LIST_VIP)
            task_company = task_info['company']
            if  task_company == "nulo":
                reward += 5
                print('+$$$$ ')
               
        except:
            print('error')
            pass
        print(f'A premiação de {reward} é a atual com o bônus')
        
        item = {
            'sofier': sofier,
            'execution_id': execution_data['execution_id'],
            'task_id': task_id,
            'reward': reward,
            'phase': 'VALIDATION',
            'history': [{'time_stamp': str_time_stamp, 'phase': 'VALIDATION'}],
            'last_move': str_time_stamp,
            'first_move': str_time_stamp,
            'company': task_info['company'],
            'sofie_place': {
                'name': task_info['sofie_place']['name'],
                'address': address
            },
            'task_info': {
                'type': task_info['task']['type'],
                'name': task_info['task']['name'],
                'category': task_info['task']['category']
            }
        }

        if task_info_category == 'challenger':
            item['phase'] = 'BLOCKED'

        dynamodb.Table(
            os.environ[f'{kwargs["aws"]["stage"]}_table_sofier_ledger']).put_item(Item=item)

        system_notify(item['task_id'], item['execution_id'], **kwargs)

        # Insere na tabela de Ciclos REVER - REVER - REVER- REVER -REVER
        if cycle:
            try:
                if int(cycle.get('qtd', 0)) > 1:
                    exp = datetime.fromisoformat(str_time_stamp)
                    exp = (exp + timedelta(minutes=int(cycle.get('wait', 0)))).isoformat()
                    life = {
                        'task_id': item['task_id'],
                        'when': str_time_stamp,
                        'who': sofier,
                        'expire_at': exp
                    }
                    dynamodb.Table(
                        os.environ[f'{kwargs["aws"]["stage"]}_table_micro_task_lifecycle']).put_item(Item=life)
            except:
                pass
    #: Verificando se é para enviar um email para o estabelecimento
    # TODO: CHAMADA PARA ENVIAR EMAIL
    return 200, {'message': 'SUCCESS'}


def task_execution_finish(task_id, sofier, postpone_or_cancel, task_info_name, task_info_category, ** kwargs):
    print('FINALIZANDO TAREFA')
    print(f'TAREFA DE: {sofier}\n{task_id}\n{postpone_or_cancel}\n{task_info_name} --- {task_info_category}')
    """
    Sinaliza ao sistema que uma determinada tarefa foi executada
    :param task_id:
        ID da tarefa em questão
    :param sofier:
        Sofier que está executando a tarefa
    :param postpone_or_cancel:
        FINISH   - Indica que é para FINALIZAR a tarefa
        POSTPONE - Indica que é para POSTERGAR a tarefa
        CANCEL   - Indica que é para CANCELAR a tarefa
    :return:
        Constante com o resultado da ação
    """

    SCRIPT_LUA_FINISH = """
    --[[
    **SINALIZAÇÃO DE QUE A TAREFA FOI EXECUTADA**
    
    Fluxo:
    ======
    - Se AÇÃO == FINISHED  
        - Exclui a chave de reserva
        - Exclui a chave de tarefa em execução
        - Exclui a chave de dados da micro tarefa
        - Exclui a localidade do conjunto de tarefas
        
    - Se AÇÃO == CANCEL
        - Exclui a chave de reserva
        - Exclui a chave de tarefa em execução
        
    - Se AÇÃO == POSTPONE
        - Incrementa o TTL de reserva em 24 horas
        - Exclui a chave de tarefa em execução 
        
    --]]
    local sofier = KEYS[1]
    local task_id = KEYS[2]
    local action = KEYS[3]
    if action == 'FINISH' then
        redis.call('DEL', string.format('SOFIE:MICROTASK:%s:RESERVED:%s#', task_id, sofier))
        redis.call('DEL', string.format('SOFIE:MICROTASK:%s:IN_EXECUTION:%s#', task_id, sofier))
        redis.call('DEL', string.format('SOFIE:MICROTASK:%s:DATA#', task_id))
        redis.call('ZREM', 'SOFIE:MICROTASK:IN_PERSON#', task_id)
    
    elseif action == 'FINISH_CHALLENGE' then
        redis.call('DEL', string.format('SOFIE:MICROTASK:%s:RESERVED:%s#', task_id, sofier))
        redis.call('DEL', string.format('SOFIE:MICROTASK:%s:IN_EXECUTION:%s#', task_id, sofier))
        
    elseif action == 'CANCEL' then
        redis.call('DEL', string.format('SOFIE:MICROTASK:%s:RESERVED:%s#', task_id, sofier))
        redis.call('DEL', string.format('SOFIE:MICROTASK:%s:IN_EXECUTION:%s#', task_id, sofier))
    elseif action == 'POSTPONE' then
        local ONE_DAY = 86400
        local ttl = redis.call('TTL', string.format('SOFIE:MICROTASK:%s:RESERVED:%s#', task_id, sofier))
        redis.call('EXPIRE', string.format('SOFIE:MICROTASK:%s:RESERVED:%s#', task_id, sofier), ONE_DAY + ttl)
        redis.call('DEL', string.format('SOFIE:MICROTASK:%s:IN_EXECUTION:%s#', task_id, sofier))
    else
        return 'UNSUCCESS'
        
    end
    return 'SUCCESS'
    """

    if postpone_or_cancel != 'FINISH' and task_info_category == 'challenger':
        return 200, {'message': data}
    
    provider = Prod_Provider()
    conn = provider.get_redis(decode_responses=True)

    # conn = Redis(host=os.environ[f'{kwargs["aws"]["stage"]}_REDIS_HOST'], port=int(
    #     os.environ[f'{kwargs["aws"]["stage"]}_REDIS_PORT']), decode_responses=True)
    SHA_SCRIPT_LUA_FINISH = conn.script_load(SCRIPT_LUA_FINISH)

    postpone_or_cancel = f'{postpone_or_cancel}_CHALLENGE' if task_info_category == 'challenger' else postpone_or_cancel

    data = conn.evalsha(
        SHA_SCRIPT_LUA_FINISH,
        3,
        sofier,
        task_id,
        postpone_or_cancel or 'FINISH'
    )

    return 200, {'message': data}


def system_notify(task_id, execution_id, **kwargs):
    provider = Prod_Provider()
    conn = provider.get_redis(decode_responses=True)
    # conn = Redis(host=os.environ[f'{kwargs["aws"]["stage"]}_REDIS_HOST'], port=int(
    #     os.environ[f'{kwargs["aws"]["stage"]}_REDIS_PORT']))

    company = '__PRIME__'  # kwargs.get('company', '__PRIME__')
    key = f'SOFIE:MICROAPP:{company}:AUDIT:BADGE#'
    conn.lpush(key, ','.join((task_id, execution_id)))
