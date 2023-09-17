import json
import boto3
import traceback


clambda = boto3.client('lambda')
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    status_code, data = 200, []

    for record in event['Records']:
        try:
            content = json.loads(record['body'])
            function_name = 'map_reduce_audit'
            if content.get('task_info').get('name') == 'marketone':
                function_name = 'mapreduce_marketone'

            clambda.invoke(
                FunctionName=function_name,
                InvocationType='Event',
                Payload=json.dumps(content)
            )

        except Exception as err:
            data.append(traceback.format_exception(
                None, err, err.__traceback__))

    return {
        'statusCode': status_code,
        'body': json.dumps(data if len(data) > 0 else dict(success=True))
    }
