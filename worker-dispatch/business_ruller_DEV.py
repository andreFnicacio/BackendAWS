from operator import itemgetter
import json
from uuid import uuid4
from provider import *
import datetime


#CHAMADO DO PROVIDER MONGO/GOOGLE
provider = Dev_Provider()

sqs = provider.get_sqs() 

def get_geocode(address):
    postal_code = address.get('zipcode', None)
    if(postal_code != None):
         address_to_google_collect = (
             f"{address['address']} - "
             f"{address['address_number']} - "
             f"{address['city']} - "
             f"{address['state']}, "
             f"{postal_code}"
         )
         PARAMS.update({'address': address_to_google_collect})
         response = requests.get(URL, PARAMS)
         if response.status_code == 200:
             data = response.json()
         else:
             data = f"Google Error {response.status_code}"     
         return data            


def create_message():
    message = list()
    message.append( f'<p>No mundo ninja, aqueles que quebram as regras sao lixo, e verdade, mas aqueles que abandonam seus companheiros sao piores do que lixo. - Kakashi Hatake</p>')
    return ''.join(message)             

class Base_ruller():
    def created_dispatch(self, item): 

        client = provider.get_mongodb()
        db = client.sofie
        table_logistic_dispatch = db.table_logistic_dispatch   
        dispatch_result = table_logistic_dispatch.insert_many([item])       

        return dispatch_result.inserted_ids  
        
    def update_dispatch(self ,dispatch_id, body):
        client = provider.get_mongodb()
        db = client.sofie
        table_logistic_dispatch = db.table_logistic_dispatch   
        myquery_dispatch = { "dispatch_id": dispatch_id }
        newvalues = { "$set": body }
        dispatch_result = table_logistic_dispatch.update_one(myquery_dispatch, newvalues)       

        print(f"Inserted Dispatch: {dispatch_result}") 

    def fetch_all_items(self):
        mongodb = provider.get_mongodb()
        db = mongodb['sofie']
        table_logistic_dispatch = db.table_logistic_dispatch    
        table_logistic_dispatch = table_logistic_dispatch.find()

        items = list() 

        for item in table_logistic_dispatch:
            formated_items = dict()
            formated_items['status'] = item['status']
            verify = item.get('info_collect', None)
            if verify != None:
                formated_items['action_date'] = item['info_collect']['action_date']
            else:
                formated_items['action_date'] = item['info_delivery']['action_date']  
            formated_items['category'] = item['category']                          
            formated_items['product'] = item['product']                          
            formated_items['need_atention'] = True 
                                     
            print(formated_items)
            items.append(formated_items)
                        
        return items   

    def fetch_admin_items(self):
        mongodb = provider.get_mongodb()
        db = mongodb['sofie']
        table_micro_task_logistic = db.table_micro_task_logistic        
        table_logistic_dispatch = db.table_logistic_dispatch
        #filter = {'status': {'$ne': 'FINISHED'}}
        table_logistic_dispatch = table_logistic_dispatch.find()
        table_micro_task_logistic = table_micro_task_logistic.find()  



        items = list() 

        for item in table_micro_task_logistic:
            formated_items_logistic = dict()
            formated_items_logistic['status'] = item['status']
            verify = item.get('info_collect', None)
            if verify != None:
                formated_items_logistic['action_date'] = item['info_collect']['action_date']
            else:
                formated_items_logistic['action_date'] = item['info_delivery']['action_date']  
            formated_items_logistic['category'] = item['category']                          
            formated_items_logistic['product'] = item['product']                          
            formated_items_logistic['need_atention'] = True
            formated_items_logistic['dispatch_id'] = item['dispatch_id']
            
            items.append(formated_items_dispatch)

        for item in table_logistic_dispatch:
            formated_items_dispatch = dict()
            formated_items_dispatch['status'] = item['status']
            verify = item.get('info_collect', None)
            if verify != None:
                formated_items_dispatch['action_date'] = item['info_collect']['action_date']
            else:
                formated_items_dispatch['action_date'] = item['info_delivery']['action_date']  
            formated_items_dispatch['category'] = item['category']                          
            formated_items_dispatch['product'] = item['product']
            date = datetime.datetime.strptime(formated_items_dispatch['action_date'], '%Y-%m-%dT%H:%M')
            formated_items_dispatch['action_date_formatted'] = f'{date.day}/{date.month}/{date.year} {date.hour}:{date.minute}'
                                   
            if formated_items_dispatch['category'] == '3P':
                print("Get Work")
                if item.get('info_delivery', None):
                    formated_items_dispatch['need_atention'] = False
                else:
                    formated_items_dispatch['need_atention'] = True
            else:
                formated_items_dispatch['need_atention'] = False
            formated_items_dispatch['dispatch_id'] = item['dispatch_id'] 
                                     
            print(formated_items_dispatch)
            items.append(formated_items_dispatch)
                        
        newlist = sorted(items, key=itemgetter('action_date'), reverse=False) 
        return newlist    

    def fetch_items_by_query(self, query):
        mongodb = provider.get_mongodb()
        db = mongodb['sofie']
        table_logistic_dispatch = db.table_logistic_dispatch    
        table_logistic_dispatch = table_logistic_dispatch.find(query).sort("timestamp", 1)

        items = list() 

        for item in table_logistic_dispatch:
            formated_items = dict()
            formated_items['status'] = item['status']
            verify = item.get('info_collect', None)
            if verify != None:
                formated_items['action_date'] = item['info_collect']['action_date']
            else:
                formated_items['action_date'] = item['info_delivery']['action_date']                     
            formated_items['category'] = item['category']                          
            formated_items['product'] = item['product']
            date = datetime.datetime.strptime(formated_items['action_date'], '%Y-%m-%dT%H:%M')
            formated_items['action_date_formatted'] = f'{date.day}/{date.month}/{date.year} {date.hour}:{date.minute}'

            if formated_items['category'] == '3P':
                print("Get Work")
                if item.get('info_delivery', None):
                    formated_items['need_atention'] = False
                else:
                    formated_items['need_atention'] = True
            else:
                formated_items['need_atention'] = False   
            formated_items['dispatch_id'] = item['dispatch_id']              
                                     
            items.append(formated_items)
        newlist = sorted(items, key=itemgetter('action_date'), reverse=False) 
        return newlist       

    def fetch_one_item(self ,task):
        print(type(task))
        print(task)
        mongodb = provider.get_mongodb()
        db = mongodb['sofie']
        collection_dispatch = db.table_logistic_dispatch    

        logistic = collection_dispatch.find_one({"dispatch_id": task})
        print(logistic)
        if(logistic == None):
            collection_logistic = db.table_micro_task_logistic    

            logistic = collection_logistic.find_one({"dispatch_id": task})
            items_dispatch = logistic
        else:
            items_dispatch = logistic

        return items_dispatch    

   
    def get_google_data(self, info):
        res = get_geocode(info)      
        print(f'geodata result: {res}')
        info['formatted_address'] = res['results'][0]['formatted_address'] 
        info['location'] = res['results'][0]['geometry']['location'] 
        return info      
    
    def set_to_queue_notification(self):
        print('INICIANDO SQS')
        queue = sqs.get_queue_by_name(QueueName='queue_mapreduce')
        mensagem = create_message()
        queue.send_message(
            MessageBody=json.dumps(mensagem),
            MessageAttributes={
                'aws': {
                    'DataType': 'String',
                    'StringValue': 'DEV'
                },
                'context': {
                    'DataType': 'String',
                    'StringValue': 'audit'
                }
            }
        )
        print('INFILEIRADO')        
