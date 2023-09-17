from json import dumps, loads, JSONEncoder
from decimal import Decimal
from tempfile import SpooledTemporaryFile
import report


class DecimalEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    status_code, content_type, data = 500, 'application/json', None
    method = event['httpMethod']
    pathParameters = event.get('pathParameters')
    print(event)

    if method == 'GET':
        accept = event['headers'].get('accept', 'application/json')
        if accept == 'text/csv':
        

            if pathParameters and 'task_id' in pathParameters and 'execution_id' in pathParameters:
                pass
            else:
                params = {'stage': event['stageVariables']['lambda']}
                params.update(event['queryStringParameters'] or dict())
                status_code, content_type, data = report.listing(**params)
                
                headers = {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/excel',
                    'Content-Disposition': 'attachment; filename=relatorio.csv'
                    }
                    
                #file = SpooledTemporaryFile(mode='w+t', encoding='UTF-8')
                #file.write(u'\ufeff')
                #file.write(data)
                #file.seek(0)
                #data = file.read()
                
                
                return {
                    'statusCode': status_code,
                    'headers': headers,
                    'body': data
                }
               

        else:
            import display
            print('DISPLAY')
            params = {'stage': event['stageVariables']['lambda']}
            params.update(event['queryStringParameters'] or dict())

            if pathParameters and 'task_id' in pathParameters and 'execution_id' in pathParameters:
                task_id = pathParameters['task_id']
                execution_id = pathParameters['execution_id']
                print(f"esse é o task_id: {task_id} que vai para o get")
                status_code, data = display.get_execution(
                    task_id, execution_id, **params)
                print('foi para o get execution')
            else:
                status_code, data = display.listing(**params)
    headers = {'Access-Control-Allow-Origin': '*'}
    headers['Content-Type'] = 'application/json'
    print(f'essa é a data final {data}')
    return {
        'statusCode': status_code,
        'headers': headers,
        'body': data if isinstance(data, str) else dumps(data or {}, cls=DecimalEncoder)
    }
