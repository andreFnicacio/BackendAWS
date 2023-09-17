from provider import Dev_Provider
import json

provider = Dev_Provider()

def main(event, context):
    data = {}
    code = 401

    return {
            'statusCode': code,
            'body': json.dumps(data)
        }

