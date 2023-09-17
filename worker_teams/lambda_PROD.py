import json
from provider import *

provider = Dev_Provider()

def fetch_items_by_query(query):
    mongodb = provider.get_mongodb()
    db = mongodb['sofie']
    table_sofier_team = db.table_sofier_team    
    table_sofier_team = table_sofier_team.find_one(query)
    return table_sofier_team 

def main(event):
    query_filter = dict()
    verify = event.get('pathParameters', None)    
    if verify != None :
        task = event['pathParameters'].get("lider", None)
        if task != None:
            query_filter['lider'] = task
            body = fetch_items_by_query(query_filter)
            print("BODY")
            print(body)        
            return {
                'statusCode': 200,
                'body': JSONEncoder().encode(body)    
            }        
        else:
            return 400
