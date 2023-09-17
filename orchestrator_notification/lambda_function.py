import json
from provider import *
from decimal import Decimal

provider = Dev_Provider()

class DecimalEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):

    def send_mail(event):
        print(event['Records'][0]['body'])
        clambda = provider.get_lambda()
        data_to_email = {
            'subject': f'Relatório Operacional',
            'to': 'nicaciodagga@gmail.com',
            'message': event['Records'][0]['body']
        }

        data = json.dumps(data_to_email, cls=DecimalEncoder)
        clambda.invoke(
            FunctionName='arn:aws:lambda:{}:{}:function:{}'.format(
                'sa-east-1', '971419184909', 'handler_sofie_mf_sender-email'),
            InvocationType='Event',
            Payload=data

        )

    send_mail(event)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
