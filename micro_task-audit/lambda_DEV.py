"""
Efetua a auditoria de uma execução de tarefa
"""
import os
import boto3
import json

from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4
from provider import *

sqs = boto3.resource('sqs')
dynamodb = boto3.resource('dynamodb')
cache_db = None

def main(event, context):

    pathParameters = event['pathParameters']

    task_id = pathParameters.get('task_id')
    execution_id = pathParameters.get('execution_id')

    body = json.loads(event['body'])
    ret = AuditTask(task_id, execution_id,
                          **body)(**{'aws': {'stage': event['stageVariables']['lambda']}})

    return {
        'statusCode': 200,
        'body': json.dumps({'SUCCESS': ret}),
        'headers': {
            'Access-Control-Allow-Origin': '*'
        }
    }

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        return super(DecimalEncoder, self).default(o)


class AuditTask(object):
    """
    Efetua o fluxo de auditoria de uma tarefa, a saber:

    [X] - Edita o registro da execução da tarefa, inserindo as informações referente à auditoria

    [X] - Se APROVADO, passa a tarefa de `state.state` de *NEW* para *FINISHED*
    [X] - Se APROVADO, passa a tarefa de `state.status` de *SUCCESS* para *FAILURE*

    [x] - Se REPROVADO, a tarefa é liberada para um próximo _sofier_

    [x] - Se APROVADO, passa o prêmio do _sofier_ de *VALIDATION* para *BLOCKED*, senão passa para *WRONG*
    """

    def __init__(self, task_id: str, execution_id: str, who: str, approved: bool, reason: str):
        """
        Inicializa o objeto
        """
        super().__init__()

        self.__task_id = task_id
        self.__execution_id = execution_id
        self.__who = who
        self.__approved = approved
        self.__reason = reason

        self.__task = None

        self.__sofier = None
        self.__cache_db = None

    def __call__(self, **kwargs):
        """
        Execução propriamente dita
        """
        provider = Dev_Provider()
        cache_db = provider.get_redis()

        self.__task = dynamodb.Table('__dev_table_micro_task_in_person').get_item(Key={'task_id': self.__task_id})['Item']

        execution = self.edit_execution(**kwargs)

        self.edit_task(**kwargs)

        self.return_task(cache_db, **kwargs)

        self.sofier_reward(**kwargs)

        self.sofier_leader_reward(**kwargs)
            
        if self.__reason != "":
            print('NOTIFICANDO')
            self.notification(**kwargs)
        
        # self.mapreduce(execution, **kwargs)
       
        # self.system_notify(cache_db, **kwargs)

        self.goals_notify(execution, **kwargs)

        return True

    def goals_notify(self, execution, **kwargs):
        body = {"sofier": execution["who"], "task": execution["task_info"]["name"]}
        queue = sqs.get_queue_by_name(QueueName='queueGoals')
        queue.send_message(
            MessageBody=json.dumps(body),
            MessageAttributes={
                'aws': {
                    'DataType': 'String',
                    'StringValue': "DEV"
                },
                'context': {
                    'DataType': 'String',
                    'StringValue': 'audit'
                }
            }
        )

    def edit_execution(self, **kwargs):
        """
        Edita a tabela de execução, inserindo as informações referente à auditoria
        """

        response = dynamodb.Table('__dev_table_micro_task_execution').update_item(
            Key={
                'task_id': self.__task_id,
                'execution_id': self.__execution_id
            },
            UpdateExpression='SET audit = :a',
            ExpressionAttributeValues={
                ':a': {
                    'when': datetime.utcnow().isoformat(),
                    'who': self.__who,
                    'approved': self.__approved,
                    'reason': self.__reason or '-'
                }
            },
            ReturnValues='ALL_NEW'
        )

        self.__sofier = response['Attributes']['who']
        return response['Attributes']

    def edit_task(self, **kwargs):
        """
        Edita a tabela de backlog, mudando o state/status da tarefa
        """

        dynamodb.Table('__dev_table_micro_task_in_person').update_item(
            Key={'task_id': self.__task_id},
            UpdateExpression='SET #status.#state = :state, #status.#status = :status',
            ExpressionAttributeNames={
                '#status': 'status', '#state': 'state'},
            ExpressionAttributeValues={
                ':state': 'FINISHED',
                ':status': 'SUCCESS' if self.__approved else 'FAILURE'
            }
        )

    def return_task(self, conn, **kwargs):
        """
        Caso a execução da tarefa seja reprovada pela auditoria, a mesma é liberada para o próximo sofier
        """
        print(f'TAREFA APROVADA?: {self.__approved}')
        if not self.__approved:
            print('inserindo novamente no redis')
            data = {
                'category': self.__task['task']['category'],
                'type': self.__task['task']['type'],
                'task_id': self.__task['task_id'],
                'reward': float(self.__task['task']['reward']),
                'name': self.__task['sofie_place']['name'],
                'task_name': self.__task['task']['name'],
                'address': self.__task['address']['formatted_address'],
                'lat': float(self.__task['google_maps']['results'][0]['geometry']['location']['lat']),
                'lng': float(self.__task['google_maps']['results'][0]['geometry']['location']['lng'])
            }

            pipe = conn.pipeline()
            pipe.hmset(f'SOFIE:MICROTASK:{data["task_id"]}:DATA#', data)
            pipe.geoadd('SOFIE:MICROTASK:IN_PERSON#',
                        data['lng'], data['lat'], data['task_id'])
            pipe.execute()

    def sofier_reward(self, **kwargs):
        """
        Movimenta o *prêmio* do _sofier_ de acordo com a aprovação, ou reprovação, da execução da tarefa
        """
        phase = 'BLOCKED' if self.__approved else 'WRONG'
        dt_when = datetime.utcnow().isoformat()

        update_expression = ['phase = :p', 'last_move = :m',
                             'history = list_append(history, :h)']

        expression_attribute_values = {':p': phase, ':m': dt_when, ':h': [
            {'phase': phase, 'time_stamp': dt_when}]}

        if phase == 'BLOCKED':
            update_expression.extend(['tax = :b', 'date_to_available = :d'])
            expression_attribute_values.update({':b': True, ':d': dt_when})

        dynamodb.Table('__dev_table_sofier_ledger').update_item(
            Key={
                'sofier': self.__sofier,
                'execution_id': self.__execution_id
            },
            UpdateExpression=f'SET {",".join(update_expression)}',
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="ALL_NEW"
        )

    def sofier_leader_reward(self, **kwargs):
        if self.__approved:
            sofier_db = dynamodb.Table(
                os.environ[f'{kwargs["aws"]["stage"]}_table_sofier_info'])

            sofier = sofier_db.get_item(Key={'sofier': self.__sofier})['Item']
            team = sofier.get('team')
            
            if team:
                leader = sofier_db.query(
                    IndexName='username-index',
                    KeyConditionExpression='username = :u',
                    ExpressionAttributeValues={
                        ':u': team
                    }
                ).get('Items', [])

                if leader:
                    str_time_stamp = datetime.utcnow().isoformat()
                    # rew = Decimal(float(self.__task['task']['reward']) * 0.1)
                    category = 'TV1'
                    rew = 1
                    if leader[0]['username'] == '81353AB0':
                        rew = 2

                    item = {
                        'sofier': leader[0]['sofier'],
                        'execution_id': self.__execution_id,
                        'task_id': self.__task_id,
                        'reward': rew,
                        'phase': 'BLOCKED',
                        'tax': True,
                        'date_to_available': str_time_stamp,
                        'history': [
                            {'time_stamp': str_time_stamp, 'phase': 'VALIDATION'},
                            {'time_stamp': str_time_stamp, 'phase': 'BLOCKED'}
                        ],
                        'last_move': str_time_stamp,
                        'first_move': str_time_stamp,
                        'company': self.__task['company'],
                        'sofie_place': {
                            'name': 'Prêmio - Tarefa do time',
                            'company': self.__task['sofie_place']['name'],
                            'address': self.__task['address']['formatted_address']
                        },
                        'task_info': {
                            'type': self.__task['task']['type'],
                            'name': self.__task['task']['name'],
                            'category': category,
                            'group': 'commission'
                        }
                    }

                    dynamodb.Table('__dev_table_sofier_ledger').put_item(Item=item)
                        
    def notification(self, **kwargs):
        tb = dynamodb.Table('__dev_table_sofier_notification')
        response = tb.get_item(Key={'sofier': self.__sofier}).get('Item')
        
        print(kwargs)
        
        time_stamp = datetime.utcnow().isoformat()
        title = ""
        content = ""
        if self.__approved:
            title = "Você teve um comentário em uma tarefa"
            content = f"Uma de suas tarefas do {self.__task['task']['name']} foi aprovada, mas temos algo pra te falar:\n\n\"{self.__reason}\"\n\nPara confirmar que você leu e entendeu, clique no botão abaixo."
        else:
            title = "Poxa, uma de suas tarefas foi recusada"
            content = f"Parece que uma de suas tarefas do {self.__task['task']['name']} foi recusada. Mas pra te ajudar, temos umas dicas pra você:\n\n\"{self.__reason}\"\n\nPara confirmar que você leu e entendeu, clique no botão abaixo."

        notification = {
            "notification_id": str(uuid4()),
            "title": title,
			"content": content,
			"deeplink": "reserved",
			"data": {
			    "who": self.__who,
			    "execution_id": self.__execution_id
			},
			"read": False,
			"when": time_stamp,
			"response":""
        }
        
        if response:
            response['notifications'].append(notification)
        else:
            response = {
                "sofier": self.__sofier,
                "notifications": [
                    notification    
                ]
            }
        
        tb.put_item(Item=response)

    # def mapreduce(self, execution, **kwargs):
    #     body = {
    #         'company': execution['company'],
    #         'task_id': execution['task_id'],
    #         'execution_id': execution['execution_id'],
    #         'stage': kwargs["aws"]["stage"]

    #     }

    #     print('INICIANDO SQS')
    #     queue = sqs.get_queue_by_name(QueueName='queue_mapreduce')
    #     print(json.dumps(body))
    #     queue.send_message(
    #         MessageBody=json.dumps(body),
    #         MessageAttributes={
    #             'aws': {
    #                 'DataType': 'String',
    #                 'StringValue': body['stage']
    #             },
    #             'context': {
    #                 'DataType': 'String',
    #                 'StringValue': 'audit'
    #             }
    #         }
    #     )
    #     print('INFILEIRADO')


    # def system_notify(self, conn, **kwargs):
    #     company = '__PRIME__'
    #     key = f'SOFIE:MICROAPP:{company}:AUDIT:BADGE#'
    #     conn.lrem(key, 0, f'{self.__task_id},{self.__execution_id}')

