import json
import audit


def lambda_handler(event, context):

    pathParameters = event['pathParameters']

    task_id = pathParameters.get('task_id')
    execution_id = pathParameters.get('execution_id')

    body = json.loads(event['body'])
    ret = audit.AuditTask(task_id, execution_id,
                          **body)(**{'aws': {'stage': event['stageVariables']['lambda']}})

    return {
        'statusCode': 200,
        'body': json.dumps({'SUCCESS': ret}),
        'headers': {
            'Access-Control-Allow-Origin': '*'
        }
    }
