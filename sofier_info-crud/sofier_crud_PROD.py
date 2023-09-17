import boto3
import json
from provider import *
from boto3.dynamodb.conditions import Key
from datetime import datetime

class SofierCRUD(object):
    """
    Operações básicas de CRUD
    """
    
    provider = Prod_Provider()
    con = provider.get_redis()

    def __init__(self):
        """
        Inicializa o objeto
        """
        super().__init__()

        self.__ddds = {'11': {'cidade': 'São Paulo', 'estado': 'SP'}, '12': {'cidade': 'São José dos Campos', 'estado': 'SP'}, '13': {'cidade': 'Santos', 'estado': 'SP'}, '14': {'cidade': 'Bauru', 'estado': 'SP'}, '15': {'cidade': 'Sorocaba', 'estado': 'SP'}, '16': {'cidade': 'Ribeirão Preto', 'estado': 'SP'}, '17': {'cidade': 'São José do Rio Preto', 'estado': 'SP'}, '18': {'cidade': 'Presidente Prudente', 'estado': 'SP'}, '19': {'cidade': 'Campinas', 'estado': 'SP'}, '21': {'cidade': 'Rio de Janeiro', 'estado': 'RJ'}, '22': {'cidade': 'Campos dos Goytacazes', 'estado': 'RJ'}, '24': {'cidade': 'Volta Redonda', 'estado': 'RJ'}, '27': {'cidade': 'Vila Velha/Vitória', 'estado': 'ES'}, '28': {'cidade': 'Cachoeiro de Itapemirim', 'estado': 'ES'}, '31': {'cidade': 'Belo Horizonte', 'estado': 'MG'}, '32': {'cidade': 'Juiz de Fora', 'estado': 'MG'}, '33': {'cidade': 'Governador Valadares', 'estado': 'MG'}, '34': {'cidade': 'Uberlândia', 'estado': 'MG'}, '35': {'cidade': 'Poços de Caldas', 'estado': 'MG'}, '37': {'cidade': 'Divinópolis', 'estado': 'MG'}, '38': {'cidade': 'Montes Claros', 'estado': 'MG'}, '41': {'cidade': 'Curitiba', 'estado': 'PR'}, '42': {'cidade': 'Ponta Grossa', 'estado': 'PR'}, '43': {'cidade': 'Londrina', 'estado': 'PR'}, '44': {'cidade': 'Maringá', 'estado': 'PR'}, '45': {'cidade': 'Foz do Iguaçú', 'estado': 'PR'}, '46': {'cidade': 'Francisco Beltrão/Pato Branco', 'estado': 'PR'}, '47': {'cidade': 'Joinville', 'estado': 'SC'}, '48': {'cidade': 'Florianópolis', 'estado': 'SC'}, '49': {'cidade': 'Chapecó', 'estado': 'SC'}, '51': {'cidade': 'Porto Alegre', 'estado': 'RS'}, '53': {'cidade': 'Pelotas', 'estado': 'RS'}, '54': {'cidade': 'Caxias do Sul', 'estado': 'RS'}, '55': {'cidade': 'Santa Maria', 'estado': 'RS'}, '61': {'cidade': 'Brasília', 'estado': 'DF'}, '62': {'cidade': 'Goiânia', 'estado': 'GO'}, '63': {'cidade': 'Palmas', 'estado': 'TO'}, '64': {'cidade': 'Rio Verde', 'estado': 'GO'}, '65': {'cidade': 'Cuiabá', 'estado': 'MT'}, '66': {'cidade': 'Rondonópolis', 'estado': 'MT'}, '67': {'cidade': 'Campo Grande', 'estado': 'MS'}, '68': {'cidade': 'Rio Branco', 'estado': 'AC'}, '69': {'cidade': 'Porto Velho', 'estado': 'RO'}, '71': {'cidade': 'Salvador', 'estado': 'BA'}, '73': {'cidade': 'Ilhéus', 'estado': 'BA'}, '74': {'cidade': 'Juazeiro', 'estado': 'BA'}, '75': {'cidade': 'Feira de Santana', 'estado': 'BA'}, '77': {'cidade': 'Barreiras', 'estado': 'BA'}, '79': {'cidade': 'Aracaju', 'estado': 'SE'}, '81': {'cidade': 'Recife', 'estado': 'PE'}, '82': {'cidade': 'Maceió', 'estado': 'AL'}, '83': {'cidade': 'João Pessoa', 'estado': 'PB'}, '84': {'cidade': 'Natal', 'estado': 'RN'}, '85': {'cidade': 'Fortaleza', 'estado': 'CE'}, '86': {'cidade': 'Teresina', 'estado': 'PI'}, '87': {'cidade': 'Petrolina', 'estado': 'PE'}, '88': {'cidade': 'Juazeiro do Norte', 'estado': 'CE'}, '89': {'cidade': 'Picos', 'estado': 'PI'}, '91': {'cidade': 'Belém', 'estado': 'PA'}, '92': {'cidade': 'Manaus', 'estado': 'AM'}, '93': {'cidade': 'Santarém', 'estado': 'PA'}, '94': {'cidade': 'Marabá', 'estado': 'PA'}, '95': {'cidade': 'Boa Vista', 'estado': 'RR'}, '96': {'cidade': 'Macapá', 'estado': 'AP'}, '97': {'cidade': 'Coari', 'estado': 'AM'}, '98': {'cidade': 'São Luís', 'estado': 'MA'}, '99': {'cidade': 'Imperatriz', 'estado': 'MA'}
                       }

        print('PEGANDO TABLE SOFIER INFO')
        self.__table = boto3.resource('dynamodb').Table('table_sofier_info')
        print('TABELA RESGATADA')
        print('PEGANDO TABELA EXECUTIONS')
        self.__table_executions = boto3.resource(
            'dynamodb').Table('table_micro_task_execution')
        print('TABELA RESGATADA')

        #self.__conn = Redis('54.232.237.28', 5071)
        # provider = Prod_Provider()
        # conn = provider.get_redis()
        print('PEGANDO REDIS')
        # self.__conn = Redis(host='172.16.220.40', port=5071)
        # self.__conn = Redis(host='redis.mysofie.com', port=5071)
        print('REDIS RESGATADO')

    def listing(self, parameters: dict) -> dict:
        """

        """

        filter_params = parameters.get('filter', dict())
        list_sofier = list()
        filter_string = ''
        filter_values = dict()

        params = dict(
            ProjectionExpression='short_name, sofier, #status, #when, main_phone, #location',
            ExpressionAttributeNames={'#status': 'status',
                                      '#when': 'when', '#location': 'location'},
            #ScanIndexForward = True if parameters.get('order', 'DESC') == 'ASC' else False,
            Limit=int(parameters['limit']),
        )

        last_sofier_id = parameters.get('last_sofier_id', None)
        if last_sofier_id:
            params['ExclusiveStartKey'] = {'sofier': last_sofier_id}

        if 'status' in parameters and parameters['status']:
            filter_string += '#status = :status'

            status = parameters['status']
            filter_values[':status'] = status

        if 'email' in filter_params:
            filter_string += ' and begins_with(sofier, :sofier)' if filter_string else 'begins_with(sofier, :sofier)'
            filter_values[':sofier'] = filter_params['email']

        if 'short_name' in filter_params:
            filter_string += ' and begins_with(short_name, :short_name)' if filter_string else 'begins_with(short_name, :short_name)'
            filter_values[':short_name'] = filter_params['short_name']

        if filter_string:
            params['FilterExpression'] = filter_string
            params['ExpressionAttributeValues'] = filter_values

        qtt = 0
        while True:
            response = self.__table.scan(**params)

            #buffer = response.get('Items')
            # if buffer:
            #    list_sofier.extend(buffer)

            for each in response['Items']:
                location = each.pop('location') if 'location' in each else None
                if location:
                    each['city'] = location.get('city', '')
                    each['state'] = location.get('state', '')

                list_sofier.append(each)

                qtt += 1
                if qtt == params['Limit']:
                    break

            if qtt == params['Limit']:
                last_key = each
                break

            last_key = response.get('LastEvaluatedKey')
            if not last_key:
                break

            params['ExclusiveStartKey'] = last_key

        #last_key = response.get('LastEvaluatedKey', dict()).get('sofier')
        last_key = last_key.get('sofier') if last_key else None

        # COUNTER
        scan_params2 = dict(
            ProjectionExpression='sofier',
            #ScanIndexForward = True if parameters.get('order', 'DESC') == 'ASC' else False
        )

        if filter_string:
            if '#status' in filter_string:
                scan_params2['ExpressionAttributeNames'] = {
                    '#status': 'status'}

            scan_params2['FilterExpression'] = filter_string
            scan_params2['ExpressionAttributeValues'] = filter_values

        count = 0

        while True:
            response2 = self.__table.scan(**scan_params2)
            count = count + response2['Count']
            last_key2 = response2.get('LastEvaluatedKey')
            if not last_key2:
                break
            scan_params2['ExclusiveStartKey'] = last_key2

        # COUNTER STATUS
        scan_params3 = dict(
            ProjectionExpression='#status',
            ExpressionAttributeNames={'#status': 'status'},
        )

        count_status = dict(PENDING=0, WAITING=0, APPROVED=0, REJECTED=0)

        while True:
            response3 = self.__table.scan(**scan_params3)

            for each in response3['Items']:
                count_status[each['status']] += 1

            last_key3 = response3.get('LastEvaluatedKey')
            if not last_key3:
                break
            scan_params3['ExclusiveStartKey'] = last_key3

        return {'data': sorted(list_sofier, key=lambda x: x['when'], reverse=True), 'last_key': last_key, 'previous_key': last_sofier_id, 'count': count, 'report': count_status}

    def put_info(self, data: dict) -> dict:
        """
        Atualiza os dados na tabela
        """
        
        full_name = data.get('full_name')
        short_name = data.get('short_name')

        if 'main_phone' in data and data['main_phone']:
            ddd = data['main_phone'][:2]
            location = self.__ddds.get(ddd)
            if location:
                if (data.get('zipcode', '') != ''):
                    print('zipcode HERE')
                else:
                    data.update({
                        'location': {
                            'city': location['cidade'],
                            'state': location['estado'],
                        }
                    })

        if full_name:
            data['full_name'] = ' '.join(
                [i.lower().capitalize() for i in full_name.split(' ')])
        if short_name:
            data['short_name'] = ' '.join(
                [i.lower().capitalize() for i in short_name.split(' ')])

        self.__table.put_item(Item=data)
        return data
    
    def send_push(self, status, sofier):
        
        clambda = boto3.client('lambda')
        sofier = sofier
        status = status
        message = ''
        title = ''
        print(status)
        print(sofier)
        if status == 'REJECTED':
            title = "Parece que tivemos um problema."
            message = "Sua documentação foi avaliada e infelizmente precisou ser rejeitada. Por favor, envie fotos de um documento válido em seu nome para começar já a fazer sua renda extra."
        elif status == 'APPROVED':
            title = "Boas notícias!"
            message = "Sua documentação foi avaliada e aprovada. Entre já no app e comece a fazer sua renda extra!"
        content = {
            'to':sofier,
            'Sofier': sofier,
            'subject': title,
            'message': message
        }
        print(content)
        function_name = 'handler_sender_push'
        clambda.invoke(
                    FunctionName=function_name,
                    InvocationType='Event',
                    Payload=json.dumps(content)
                )
                
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
    
    def update_info(self, data: dict, push: str) -> dict:
        """
        Atualiza os dados na tabela
        """
        print('update')
        provider = Prod_Provider()
        con = provider.get_redis()
        sofier = self.get_info(data['sofier'])
        print(sofier)
        
        if sofier['status'] == 'PENDING' or sofier['status'] == 'REJECTED' and 'attachs' in data and 'documents' in data['attachs']:
            data['status'] = 'WAITING'

        who = data.get('who')
        history = sofier.get('history', [])
        print('iniciando')
        now = datetime.utcnow().isoformat()
        print(f'now {now}')
        if who:
            history.append(
                {
                    'status': data['status'],
                    'when': now,
                    'who': who
                }
            )

            company = '__PRIME__'
            key = f'SOFIE:MICROAPP:{company}:SOFIER:BADGE#'
            if con.exists(key):
                con.lrem(key, 0, data['sofier'])

        else:
            history.append(
                {
                    'status': data['status'],
                    'when': now,
                    'who': data['sofier'],
                    'annotation': 'alteração e/ou atualizaçao dos dados do sofier'
                }
            )

        data.update({'history': history})
        print(data['status'], data['sofier'])
        try:
            if push:
                self.send_push(data['status'], data['sofier'])
        except Exception as e:
            print('ERRO')
            print(e)
        
        full_name = data.get('full_name')
        short_name = data.get('short_name')

        if full_name:
            data['full_name'] = ' '.join(
                [i.lower().capitalize() for i in full_name.split(' ')])
        if short_name:
            data['short_name'] = ' '.join(
                [i.lower().capitalize() for i in short_name.split(' ')])

        sofier.update(data)

        if sofier['status'] == 'WAITING':
            self.__system_notify(data['sofier'])

        self.put_info(sofier)

        return sofier

    def get_info(self, sofier: str) -> dict:
        """
        Retorna os dados cadastrais do _sofier
        """
        print('BUSCANDO DADOS SOFIER')
        buffer = self.__table.get_item(
            Key={'sofier': sofier}
        )
        print('GET ITEM')

        _sofier = buffer.get('Item')

        _tasks = self.tasks(sofier)

        _sofier['sofier_id'] = {
            'entrance_date': _sofier['when'],
            'sofier_id': _sofier['username'],
            'tasks': _tasks
        }
        print('RETORNANDO DADOS SOFIER')


        return _sofier

    def __system_notify(self, sofier):
        provider = Prod_Provider()
        con = provider.get_redis()
        company = '__PRIME__'  # kwargs.get('company', '__PRIME__')
        key = f'SOFIE:MICROAPP:{company}:SOFIER:BADGE#'
        con.lpush(key, sofier)

    def tasks(self, sofier):
        print('BUSCANDO TASKS')
        params = dict(
            ProjectionExpression='execution_id',
            FilterExpression='#result = :r and #who = :c',
            ExpressionAttributeValues={
                ':r': 'FINISH',
                ':c': sofier
            },
            ExpressionAttributeNames={
                '#result': 'result',
                '#who': 'who'
            }
        )

        tasks = list()

        while True:
            response = self.__table_executions.scan(**params)
            items = response.get("Items")

            if items:
                tasks.extend(items)

            last_pass = response.get('LastEvaluatedKey')
            if not last_pass:
                break

            params.update({'ExclusiveStartKey': last_pass})
        print('RETORNANDO TASKS')

        return len(tasks)
