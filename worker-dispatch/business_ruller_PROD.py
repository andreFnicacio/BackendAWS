from operator import itemgetter
from uuid import uuid4
from provider import *

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
             data = 400
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
        dispatch_result = table_logistic_dispatch.insert_one(item)       

        return dispatch_result
        
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

    def fetch_items_by_query(self, query):
        mongodb = provider.get_mongodb()
        db = mongodb['sofie']
        table_logistic_dispatch = db.table_logistic_dispatch    
        table_logistic_dispatch = table_logistic_dispatch.find(query)

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

            if formated_items['category'] == '3P':
                if item.get('info_delivery', None) != None:
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
        if res != 400:       
            info['formatted_address'] = res['results'][0]['formatted_address'] 
            info['location'] = res['results'][0]['geometry']['location'] 
            return info
        else:
            return res
      
    
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
