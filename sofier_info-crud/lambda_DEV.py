import json
import decimal
from datetime import datetime

from sofier_crud_DEV import SofierCRUD
from uuid import uuid4

def main(event):
    """

    """
    try:
        status = 200
        sofier = None

        pathParameters = event['pathParameters']
        if pathParameters:
            sofier = pathParameters.get('sofier')

        verb = event['httpMethod']

        crud = SofierCRUD()

        if verb == 'POST':

            data = json.loads(event['body'], parse_float=decimal.Decimal)
            status = data.get(
                'status', 'WAITING' if 'attachs' in data else 'PENDING')
                
            data.update({
                'status': status,
                'history': [
                    {
                        'status': status,
                        'when': datetime.utcnow().isoformat(),
                        'who': sofier

                    }
                ]
            })

            if 'username' not in data:
                data.update({ 'username': uuid4().hex[:8].upper() })
                
            else:
                team = data.get('team')
                if team:
                    team = team.upper()
                    if data.get('username', '').upper() == team.upper():
                        del data['team']
                    else:
                        data['team'] = team

            b_ret = crud.put_info(data)
            data_resp = {'data': b_ret}
            status = 200 if b_ret else 500

        elif verb == 'GET':
            print("BUSCANDO DADOS DO USUARIO")
            if sofier:
                data_resp = crud.get_info(sofier)
                
                if data_resp['status'] == 'REJECTED':
                    data_resp['status'] = 'PENDING'
                
                team = data_resp.get('team')
                if team and team == '':
                    del data_resp['team']
                    
            else:
                queryStringParameters = event['queryStringParameters'] if event['queryStringParameters'] else {
                }

                params = {
                    'last_sofier_id': queryStringParameters.pop('last_sofier_id', None),
                    'status': queryStringParameters.pop('status', None),
                    'limit': queryStringParameters.pop('limit', 10) or 10,
                }

                params.update({'filter': queryStringParameters})

                data_resp = crud.listing(params)

            status = 200 if data_resp else 404

        elif verb == 'PUT':
            print(event)
            agent = event['headers']['User-Agent']
            print(agent)
            push = 'available' if 'Windows' in agent else None
            print(push)
            data = json.loads(event['body'], parse_float=decimal.Decimal)
            b_ret = crud.update_info(data, push)
            #data_resp = {'SUCCESS': b_ret}
            data_resp = {'data': b_ret}
            status = 200 if b_ret else 500

        else:
            status, data_resp = 400, {
                'SUCCESS': False, 'reason': f'Método HTTP não suportado: [{verb}]'}
    except Exception as err:
        status, data_resp = 500, {'SUCCESS': False, 'reason': str(err)}

    return {
        'statusCode': status,
        'body': json.dumps(data_resp),
        'headers': {
            'Access-Control-Allow-Origin': '*'
        }
    }
