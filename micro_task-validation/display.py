import os
import pandas as pd
import boto3
from provider import *


dynamodb = boto3.resource('dynamodb')


def listing(company: str = None, limit=50, **kwargs):
    host = os.environ['MONGODB_HOST']
    port = os.environ['MONGODB_PORT']
    table = os.environ[f'{kwargs["stage"]}_analytics']
    table_wh = 'warehouse'
    provider = None   
    
    if kwargs["stage"] == "PROD":
        provider = Prod_Provider()
    else:
        provider = Dev_Provider()
    mongodb = provider.get_mongodb()
    # mongodb = MongoClient(host=host, port=int(port))

    # params = {'result': 'FINISH'}
    params = {}

    # if company:
    #     params['company'] = company.lower()

    if 'address.state' in kwargs:
        if kwargs['address.state'] != 'TODOS':
            params['state'] = kwargs['address.state']

    if 'address.city' in kwargs:
        if kwargs['address.city'] != 'null':
            params['city'] = kwargs['address.city']

    if 'task_info.cnpj' in kwargs:
        params['CNPJ'] = kwargs['task_info.cnpj']

    if 'interval.start' in kwargs and 'interval.end' in kwargs:
        if company == 'vr' or company == 'VR':
            params['data da tarefa'] = {
                '$gte': kwargs['interval.start'],
                '$lte': kwargs['interval.end']
            }
        else:
            params['start'] = {
                '$gte': kwargs['interval.start'],
                '$lte': kwargs['interval.end']
            }

    limit = limit if isinstance(limit, int) else int(limit)

    skip = int(kwargs.get('last_key', '1'))
    if skip <= 0:
        skip = 1
    if company == 'vr' or company == 'VR':
        collection = mongodb['VR'][table_wh]
    else:
        collection = mongodb[company.lower()][table]
        if company:
            params['company'] = company.lower()
        params['result'] = 'FINISH'
        
    print(params)
        
    
    count = collection.count(filter=params)
    tasks = list()
    
    if count > 0:
        if company == 'vr' or company == 'VR':
            cursor = collection.find(
                filter=params,
                projection={
                    '_id': 1,
                    'CNPJ': 1,
                    'sofie_name': 1,
                    'formatted_address': 1,
                    'start': 1,
                    'city': 1,
                    'state': 1,
                    'when_audit': 1,
                    'execution_id': 1,
                    'lat':1,
                    'lng':1
                },
                skip=(skip - 1) * limit,
                limit=limit
            )
        else:
            cursor = collection.find(
                filter=params,
                projection={
                    '_id': 0,
                    'CNPJ': 1,
                    'sofie_name': 1,
                    'formatted_address': 1,
                    'start': 1,
                    'city': 1,
                    'state': 1,
                    'when_audit': 1,
                    'result': 1,
                    'task_id': 1,
                    'execution_id': 1
                },
                skip=(skip - 1) * limit,
                limit=limit
            )

        cursor.sort('start', -1)
        
        
        if company == 'vr' or company == 'VR':
            tasks = [i for i in cursor]
            for item in tasks:
                item['task_id'] = item['_id']
        else:
            tasks = [i for i in cursor]
            
    return 200, {
        'data': tasks,
        'previous_key': skip,
        'params': params,
        'last_key': skip + 1 if count > limit else 1,
        'count': count
    }


def get_execution(task_id: str, execution_id: str, **kwargs) -> tuple:
    host = os.environ['MONGODB_HOST']
    port = os.environ['MONGODB_PORT']
    table = os.environ[f'{kwargs["stage"]}_analytics']
    provider = None
    if kwargs["stage"] == "PROD":
        provider = Prod_Provider()
    else:
        provider = Dev_Provider()
    mongodb = provider.get_mongodb()
    # mongodb = MongoClient(host=host, port=int(port))
    print(f"quando solicitado o company {kwargs['company']} é esse aqui")
    if kwargs['company'] == 'vr':
        collection = mongodb['VR']['warehouse']
        print('entrou aqui')
        print(task_id, execution_id)
        item = collection.find_one(
            {
                '_id': task_id,
                'execution_id': execution_id
            }, {
                "_id": 0
            })
    else:
        collection = mongodb[kwargs['company']][table]

        item = collection.find_one(
            {
                'task_id': task_id,
                'execution_id': execution_id
            }, {
                "_id": 0
            })

    dado = None
    if item:
        print(f'esse é o item {item}')
        dado = display_alelo(item, **kwargs)
        print(f'esse é o dado {dado}')

    return (200, dado) if dado else (400, None)


def display_alelo_dynamo(itemMongo: dict, **kwargs):
    table = dynamodb.Table(os.environ[f'{kwargs["stage"]}_table_micro_task_execution'])
    # table = dynamodb.Table('table_micro_task_execution')
    executionId = itemMongo.get('execution_id')
    print(executionId)
    
    params = dict(
    FilterExpression='execution_id = :e',
    ExpressionAttributeValues={
        ':e': executionId
        }
    )
    
    itemDynamo = None

    while True:
        response = table.scan(**params)
        items = response.get("Items")
        if items:
            itemDynamo = items[0]
    
        last_pass = response.get('LastEvaluatedKey')
        if not last_pass:
            break

        params.update({'ExclusiveStartKey': last_pass})
    print('esse é o item dynamodb')
    print(itemDynamo)
    
    # if itemDynamo == None:
        
    #     params_fossil = dict(
    #         FilterExpression='execution_id = :e',
    #         ExpressionAttributeValues={
    #             ':e': executionId
    #             }
    #         )
    #     table_fossil = dynamodb.Table('fossil_micro_task_execution')
        
    #     while True:
    #         response_fossil = table_fossil.scan(**params_fossil)
    #         items_fossil = response_fossil.get("Items")
    #         if items_fossil:
    #             print(response_fossil)
    #             itemDynamo = items_fossil[0]
        
    #         last_pass_fossil = response_fossil.get('LastEvaluatedKey')
    #         if not last_pass_fossil:
    #             break
    
    #         params_fossil.update({'ExclusiveStartKey': last_pass_fossil})

    # print(itemDynamo)
    print(itemDynamo['company'])
    if itemDynamo['company'] == 'VR':
        
        dado = {
                "RAZAO_SOCIAL": itemMongo.get("sofie_name"),
                "PROPRIETARIO": itemMongo.get('CNPJ'),
                "ENDERECO": itemMongo.get('formatted_address'),
                "CIDADE": itemMongo.get('city'),
                "ESTADO": itemMongo.get('state'),       
                "AUDITORIA": itemMongo.get('when_audit'),
                "EC": itemMongo.get('NU_SO_EC'),
                "CNPJ": itemMongo.get('CNPJ'),
                "executions": itemDynamo['execution'],
                'lat': itemMongo['lat'],
                'lng': itemMongo['lng']
        }
        executions = itemDynamo['execution']
        dado['executions'] = sorted(executions, key=lambda executions: executions['index'])
    else:
        dado = {
                "RAZAO_SOCIAL": itemMongo.get("sofie_name"),
                "PROPRIETARIO": itemMongo.get('PROPRIETARIO_01'),
                "ENDERECO": itemMongo.get('formatted_address'),
                "CIDADE": itemMongo.get('city'),
                "ESTADO": itemMongo.get('state'),       
                "AUDITORIA": itemMongo.get('when_audit'),
                "EC": itemMongo.get('NU_SO_EC'),
                "CNPJ": itemMongo.get('CNPJ'),
                "executions": itemDynamo['execution']
        }
        
            
    return dado
    

def display_alelo(item, **kwargs):
    
    davi = True
    
    if davi:
        return display_alelo_dynamo(item, **kwargs)
    else:
        best_day = f"""
        {item.get("MELHORES_DIAS_0") or ""}
        {item.get("MELHORES_DIAS_1") or ""}
        {item.get("MELHORES_DIAS_2") or ""}
        {item.get("MELHORES_DIAS_3") or ""}
        {item.get("MELHORES_DIAS_4") or ""}
        {item.get("MELHORES_DIAS_5") or ""}
        {item.get("MELHORES_DIAS_6") or ""}
        {item.get("MELHORES_HORARIOS_0") or ""}
        {item.get("MELHORES_HORARIOS_1") or ""}
        {item.get("MELHORES_HORARIOS_2") or ""}
        """
        
        cred_days = f"""
        {item.get("CRED_DIA_0") or ""}
        {item.get("CRED_DIA_1") or ""}
        {item.get("CRED_DIA_2") or ""}
        {item.get("CRED_DIA_3") or ""}
        {item.get("CRED_DIA_4") or ""}
        {item.get("CRED_DIA_5") or ""}
        {item.get("CRED_DIA_6") or ""}
        {item.get("CRED_HORARIO_0") or ""}
        {item.get("CRED_HORARIO_1") or ""}
        {item.get("CRED_HORARIO_2") or ""}
        """
        
        function_day = f"""
        {item.get("DIA_FUNCIONAMENTO_0") or ""}
        {item.get("DIA_FUNCIONAMENTO_1") or ""}
        {item.get("DIA_FUNCIONAMENTO_2") or ""}
        {item.get("DIA_FUNCIONAMENTO_3") or ""}
        {item.get("DIA_FUNCIONAMENTO_4") or ""}
        {item.get("DIA_FUNCIONAMENTO_5") or ""}
        {item.get("DIA_FUNCIONAMENTO_6") or ""}
        """
        
        function_hour = f"""
        {item.get("HORA_FUNCIONAMENTO_0") or ""}
        {item.get("HORA_FUNCIONAMENTO_1") or ""}
        {item.get("HORA_FUNCIONAMENTO_2") or ""}
        """
        
        little_machines = f"""
        {item.get("MAQUININHAS_0") or ""}
        {item.get("MAQUININHAS_1") or ""}
        {item.get("MAQUININHAS_2") or ""}
        {item.get("MAQUININHAS_3") or ""}
        {item.get("MAQUININHAS_4") or ""}
        {item.get("MAQUININHAS_5") or ""}
        {item.get("MAQUININHAS_6") or ""}
        """
        
        apps = f"""
        {item.get("APP_0") or ""}
        {item.get("APP_1") or ""}
        {item.get("APP_2") or ""}
        {item.get("APP_3") or ""}
        {item.get("APP_4") or ""}
        {item.get("APP_5") or ""}
        {item.get("APP_6") or ""}
        """
        
        dado = {
            "RAZAO_SOCIAL": item.get("sofie_name"),
            "PROPRIETARIO": item.get('PROPRIETARIO_01'),
            "ENDERECO": item.get('formatted_address'),
            "CIDADE": item.get('city'),
            "ESTADO": item.get('state'),       
            "AUDITORIA": item.get('when_audit'),
            "EC": item.get('NU_SO_EC'),
            "CNPJ": item.get('CNPJ'),
        "executions": [
            {'CONFIRMACAO': f'{item.get("EXISTE_1") or ""} {item.get("EXISTE_2")}'},
            {'RESPONSAVEL': f'{item.get("RESPONSAVEL") or ""}'},
            {'LEAD': f'{item.get("LEAD") or ""}'},
            {'TEL_EC_1': f'{item.get("TEL_EC_1_phone") or ""}'},
            {'TEL_EC_2': f'{item.get("TEL_EC_2_phone") or ""}'},
            {'TEL_RESP_1': f'{item.get("TEL_RESP_1_phone") or ""}'},
            {'TEL_RESP_2': f'{item.get("TEL_RESP_2_phone") or ""}'},
            {'CEL_EC_1': f'{item.get("CEL_EC_1_phone") or ""}'},
            {'CEL_EC_2': f'{item.get("CEL_EC_2_phone") or ""}'},
            {'CEL_RESP_1': f'{item.get("CEL_RESP_1_phone") or ""}'},
            {'CEL_RESP_2': f'{item.get("CEL_RESP_2_phone") or ""}'},
            {'EMAIL_EC': f'{item.get("EMAIL_EC_email") or ""}'},
            {'EMAIL_RESP': f'{item.get("EMAIL_RESP_email") or ""}'},
            {'MELHORES_DATAS': f'{best_day}'},
            {'ANTECIPACAO': f'{item.get("ANTECIPACAO_0") or ""}'},
            {'CONHECE_PAINEL': f'{item.get("CONHECE_PAINEL") or ""}'},
            {'SABER_PAINEL': f'{item.get("SABER_PAINEL") or ""}'},
            {'FUNCIONARIOS': f'{item.get("FUNCIONARIOS") or ""}'},
            {'ATIVIDADE': f'{item.get("ATIVIDADE") or ""}'},
            {'DIA_FUNCIONAMENTO': f'{function_day}'},
            {'HORA_FUNCIONAMENTO': f'{function_hour}'},
            {'ADESIVO': f'{item.get("ADESIVO") or ""}'},
            {'ERRADO_ALIMENTACAO':
                f'{item.get("ERRADO_ALIMENTACAO_1") or ""} {item.get("ERRADO_ALIMENTACAO_2") or ""}'},
            {'COMPRA_TICKET': f'{item.get("COMPRA_TICKET") or ""}'},
            {'CREDENCIADO': f'{item.get("CREDENCIADO") or ""}'},
            {'MOTIVO': f'{item.get("MOTIVO") or ""}'},
            {'CRED_NOME': f'{item.get("CRED_NOME") or ""}'},
            {'CRED_CNPJ': f'{item.get("CRED_CNPJ_cnpj") or ""}'},
            {'CREDENCIADO_DATA': f'{cred_days}'},
            {'CRED_RESP': f'{item.get("CRED_RESP") or ""}'},
            {'CRED_TEL_1': f'{item.get("CRED_TEL_1_phone") or ""}'},
            {'CRED_TEL_2': f'{item.get("CRED_TEL_2_phone") or ""}'},
            {'CRED_CEL_1': f'{item.get("CRED_CEL_1_phone") or ""}'},
            {'CRED_CEL_2': f'{item.get("CRED_CEL_2_phone") or ""}'},
            {'CRED_MAIL': f'{item.get("CRED_MAIL_email") or ""}'},
            {'DELIVERY': f'{item.get("DELIVERY") or ""}'},
            {'APP': f'{apps}'},
            {'FOTO_1': f'{item.get("FOTO_1_0") or ""} {item.get("FOTO_1_1") or ""}'},
            {'FOTO_2': f'{item.get("FOTO_2_0") or ""} {item.get("FOTO_2_1") or ""}'},
            {'FOTO_3': f'{item.get("FOTO_3_0") or ""} {item.get("FOTO_3_1") or ""}'},
            {'FOTO_4': f'{item.get("FOTO_4_0") or ""} {item.get("FOTO_4_1") or ""}'},
        ]}
        
        
        
        return dado
        
