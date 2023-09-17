from json import dumps, loads, JSONEncoder
import boto3
from datetime import datetime
import pandas as pd
from decimal import Decimal


dynamodb = boto3.resource('dynamodb')

class DecimalEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        return super(DecimalEncoder, self).default(o)


def send_mail(email, message):
    clambda = boto3.client('lambda')
    data_to_email = {
        'subject': f'Relatório Operacional',
        'to': email,
        'message': message
    }

    data = dumps(data_to_email, cls=DecimalEncoder)
    clambda.invoke(
        FunctionName='arn:aws:lambda:{}:{}:function:{}'.format(
            'sa-east-1', '971419184909', 'handler_sofie_mf_sender-email'),
        InvocationType='Event',
        Payload=data

    )
    
def get_audit_data(start, finish):
    table_db = dynamodb.Table('table_micro_task_execution')

    params = dict(
        FilterExpression='#when.#start BETWEEN :start and :finish and #result <> :d',
        ExpressionAttributeNames={
            '#when':'when',
            '#start': 'start',
            '#result': 'result',
        }, 
        ExpressionAttributeValues={
            ':start': start,
            ':finish': finish,
            ':d': 'CANCEL',
        }
    )

    items = list()

    while True:
        response = table_db.scan(**params)
        for item in response['Items']:
            if item.get('audit'):
                dicionario = {}
                dicionario['_id'] = item['task_id']
                dicionario['execution_id'] = item['execution_id']
                dicionario['company'] = item['company']
                dicionario['tarefa'] = item['task_info']['name']
                dicionario['data da tarefa'] = item['when']['finish']
                dicionario['sofier'] = item['who']
                dicionario['status'] = item.get('result')
                dicionario['Auditor'] = item['audit'].get('who')
                dicionario['data da auditoria'] = item['audit']['when']
                dicionario['aprovada'] = item['audit']['approved']
                
                items.append(dicionario)

        last_key = response.get('LastEvaluatedKey')

        if not last_key:
            break

        params['ExclusiveStartKey'] = last_key
    return items

def audit(start, finish):
    items = get_audit_data(start, finish)

    df = pd.DataFrame(items)
    data_solicitacao = datetime.now().date()
    df.to_excel(f"/tmp/{data_solicitacao}auditorias.xlsx", index=False, encoding='UTF-8')
    
    path = f"/tmp/{data_solicitacao}auditorias.xlsx"
    name = f"{data_solicitacao}auditorias.xlsx"
    s3name = "sofie-reports"
    print(name)
    s3 = boto3.client('s3')
    with open(path, 'rb') as f:
        s3.upload_fileobj(f, s3name, name)
    link = f'https://sofie-reports.s3.sa-east-1.amazonaws.com/{data_solicitacao}_auditorias.xlsx'

    return link


    
def executions (start, finish):
    table_db = dynamodb.Table('table_micro_task_in_person')

    params = dict(
        FilterExpression='#last_movement BETWEEN :start and :finish and #status.#status <> :FAILURE and #status.#status <> :WAITING ',
        ExpressionAttributeNames={
            '#last_movement': 'last_movement',
            '#status': 'status'
        }, 
        ExpressionAttributeValues={
            ':start': start,
            ':finish': finish,
            ':FAILURE': 'FAILURE',
            ':WAITING': 'WAITING'
        }
    )

    items = list()

    while True:
        response = table_db.scan(**params)
        for item in response['Items']:
            dicionario = dict()
            dicionario['_id'] = item['task_id']
            dicionario['company'] = item['company']
            dicionario['tarefa'] = item['task']['name']
            dicionario['data da tarefa'] = item['last_movement']
            dicionario['sofier'] = item['status']['last_sofier']
            if item['status']['status'] == 'EXECUTED':
                dicionario['status'] = 'Executado'
            elif item['status']['status'] == 'SUCCESS':
                dicionario['status'] = 'Concluído'
            else:
                dicionario['status'] = item['status']['status']
            for valor in item['address']:
                dicionario[valor] = item['address'][valor]
            
            items.append(dicionario)

        last_key = response.get('LastEvaluatedKey')

        if not last_key:
            break

        params['ExclusiveStartKey'] = last_key

    df = pd.DataFrame(items)
    data_solicitacao = datetime.now().date()
    df.to_excel(f"/tmp/{data_solicitacao}_executions.xlsx", index=False, encoding='UTF-8')
    
    path = f"/tmp/{data_solicitacao}_executions.xlsx"
    name = f"{data_solicitacao}_executions.xlsx"
    s3name = "sofie-reports"
    print(name)
    s3 = boto3.client('s3')
    with open(path, 'rb') as f:
        s3.upload_fileobj(f, s3name, name)
    link = f'https://sofie-reports.s3.sa-east-1.amazonaws.com/{data_solicitacao}_executions.xlsx'

    return link

def available():
    print('BUSCANDO TABLE')
    table = dynamodb.Table('table_micro_task_in_person')
    # #task.#name
    scan_kwargs = dict(
        ProjectionExpression='#company, #address, #task, execution_id, task_id, #original',
        FilterExpression='attribute_exists(#address) and #status.#status <> :SUCCESS and #status.#status <> :EXECUTED and #company <> :c',
        ExpressionAttributeNames={'#status': 'status', '#company': 'company', '#address':'address', '#task':'task', '#original': 'original' },
        ExpressionAttributeValues={':SUCCESS': 'SUCCESS', ':EXECUTED': 'EXECUTED', ':c': 'sofie'}
    )

    FINAL_LIST = list()
    done = False
    start_key = None
    print('COMECANDO PESQUISA')
    
    
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        
        for item in  response['Items']:
            dicionario = {}
            dicionario['task_id'] = item['task_id']
            dicionario['company'] = item['company']
            dicionario['tarefa'] = item['task']['name']
            dicionario['status'] = item['status']['status']
            for valor in item['address']:
                dicionario[valor] = item['address'][valor]
            dicionario['CNPJ'] = item['original'].get('CNPJ')
            FINAL_LIST.append(dicionario)
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    

    print('CRIANDO DF')
    df = pd.DataFrame(FINAL_LIST)
    data_solicitacao = datetime.now().date()
    print('CONVERTENDO PRA EXCEL')
    df.to_excel(f"/tmp/{data_solicitacao}_task_available.xlsx", index=False, encoding='UTF-8')
    
    path = f"/tmp/{data_solicitacao}_task_available.xlsx"
    name = f"{data_solicitacao}_task_available.xlsx"
    s3name = "sofie-reports"
    print(name)
    s3 = boto3.client('s3')
    print('ESCREVENDO NA S3')
    with open(path, 'rb') as f:
        s3.upload_fileobj(f, s3name, name)
    
    link = f'https://sofie-reports.s3.sa-east-1.amazonaws.com/{data_solicitacao}_task_available.xlsx'
    print(link)
    return link

def create_message(data):
    message = list()
    message.append(f'<p>Tudo pronto!</p>'
        f'<p>O seu relat&oacute;rio est&aacute; dispon&iacute;vel para ser baixado. Clique em download para obter seu relat&oacute;rio.</p>'
        f'<p>Baixe agora mesmo!</p>'
        f'<p><a href="{data}">DOWNLOAD</a></p>'
    )
    return ''.join(message)

def main(event, context):
    print(event['queryStringParameters'])
    queryStringParameters = event['queryStringParameters']
    report = queryStringParameters['report']
    sofier = queryStringParameters['sofier']
    data = ''
    if report == 'execution':
        start, finish = queryStringParameters['interval.start'], queryStringParameters['interval.end']
        data = executions(start.split('.')[0], finish.split('.')[0])
        message = create_message(data)
        print(message)
        send_mail(sofier, message)
    elif report == 'available':
        print('ENTRANDO EM AVAILABLE')
        data = available()
        message = create_message(data)
        print(message)
        send_mail(sofier, message)
            
    elif report == 'audit':
        start, finish = queryStringParameters['interval.start'], queryStringParameters['interval.end']
        data = audit(start.split('.')[0], finish.split('.')[0])
        message = create_message(data)
        print(message)
        send_mail(sofier, message)
        
         
    # TODO implement
    
    headers = {
        'Access-Control-Allow-Origin': '*',
        }
    return {
        'statusCode': 200,
        'headers':headers,
        'body': 'success',
    }
