import json



def list_user_logistic():
    list_users = ['davi.aquila@mysofie.com', 'andre.nicacio@mysofie.com', 'nicaciodagga@gmail.com']
    return list_users

def lambda_handler(event, context):
    list_users = list_user_logistic()
    print(list_users)
    return {
        'statusCode': 200,
        'body':  json.dumps(list_users),
        'headers': {
            'Access-Control-Allow-Origin': '*'
        }
    }
