import json
from uuid import uuid4
from business_ruller_DEV import *
from provider import JSONEncoder
from datetime import datetime


business_ruler = Base_ruller()

class Base_methods():    
    
    def get_POST_method(self, body):
        print('METODO POST')  
        date_time_now = datetime.now()
        date_time_text = date_time_now.strftime('%d/%m/%YT%H:%M')              
        body = json.loads(body)
        if body.get('category', None):      
            body['dispatch_id'] = str(uuid4())
            body['_id'] = body['dispatch_id']
            category = body.get('category', None)
            delivery = body.get('info_delivery', None)
            print(delivery)
            if delivery != None:
                formatted_address = delivery.get('formatted_address', None)
                if formatted_address == None:  
                    body['info_delivery'] = business_ruler.get_google_data(body['info_delivery'])

            if category == '3P':
                delivery = body.get('info_delivery', None)
                if delivery != None:
                    formatted_address = delivery.get('formatted_address', None)
                    if formatted_address == None:  
                        body['info_delivery'] = business_ruler.get_google_data(body['info_delivery'])

                body['info_collect'] = business_ruler.get_google_data(body['info_collect'])            
            else:
                body['info_delivery'] = business_ruler.get_google_data(body['info_delivery'])

            body['status'] = "PENDING"    
            body['timestamp'] = date_time_text   
            print("Sucesso")   

            business_ruler.set_to_queue_notification()            
            business_ruler.created_dispatch(body)            
            return JSONEncoder().encode({'id': body})                                
        else:

            return 400

    def get_GET_method(self, event):
        print('METODO GET')      
        query_filter = dict()  
        if event['queryStringParameters']:
            query_paramns = event['queryStringParameters'] 
            verify = query_paramns['company'] if query_paramns['company'] else None

            if verify != None:
                if query_paramns.get('filter',None):
                    string = query_paramns.get('filter',None)
                    string_to_json = json.loads(string)
                    query_filter = string_to_json
                elif query_paramns.get('sofier',None):
                    string = query_paramns.get('sofier',None)
                    query_filter['who'] = query_paramns['sofier']
                query_filter['company'] = query_paramns['company']        
        else:
            None


        task = None
        body = None
        admin = False

        verify = event.get('pathParameters', None)
        verify_query_strings = query_filter.get('company', '__PRIME__') 
        resource = event.get('resource', '')
        #VERIFICANDO ADMIN
        if verify != None :
            if verify['dispatch_id'] != 'admin':
                task = event['pathParameters'].get("dispatch_id", None)
                print(f'pegando task: {task}')
        
        if resource == '/dispatch/admin':
            admin = True

        #FLUXO NATURAL
        if task:
            body = business_ruler.fetch_one_item(task)
        elif admin:
            body = business_ruler.fetch_admin_items() 
        elif verify_query_strings:
            #if verify_query_strings == '__PRIME__':
            #    del query_filter['company']
            body = business_ruler.fetch_items_by_query(query_filter) 
        else:
            return 400                         
        return JSONEncoder().encode(body)     
        
    def get_PUT_method(self, event):
        print('METODO PUT')        
        print(event['body'])
        body = json.loads(event['body'])
        verify = body.get('dispatch_id', None)
        if verify != None:
            print(body)
            dispatch_id = body['dispatch_id'] 

            delivery = body.get('info_delivery', None)
            if delivery != None:
                formatted_address = delivery.get('formatted_address', None)
                if formatted_address == None:  
                    geo_data_delivery = business_ruler.get_google_data(body['info_delivery']) 
                    print(geo_data_delivery)        
                    body['info_delivery']['formatted_address'] = geo_data_delivery['formatted_address'] 
                    body['info_delivery']['location'] = geo_data_delivery['location']            
            print("Sucesso")         
            business_ruler.update_dispatch(dispatch_id, body)  

            return JSONEncoder().encode(body)
        else:
            return 400
