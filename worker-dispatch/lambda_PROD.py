from uuid import uuid4
from methods_http_PROD import *
#CHAMADO DO PROVIDER MONGO/GOOGLE
methods_http = Base_methods()


def main(event, context):
    method = event['httpMethod']
    if method == 'POST':      
        body = methods_http.get_POST_method(event['body'])
        return {
            
            'statusCode': 201,
            'body': body,
            'headers': {
                'Access-Control-Allow-Origin': '*'
            }
        }
        
    elif method == 'GET':
        body = methods_http.get_GET_method(event)

        return {
            
            'statusCode': 200,
            'body': body,
            'headers': {
                'Access-Control-Allow-Origin': '*'
            }
        }
        
    elif method == 'PUT':
        body = methods_http.get_PUT_method(event)   
        print(body)     

        return {
            
            'statusCode': 201,
            'body': body,
            'headers': {
                'Access-Control-Allow-Origin': '*'
            }
        }        

