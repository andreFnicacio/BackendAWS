import json
from provider import *
from decimal import Decimal
from datetime import datetime



provider = Dev_Provider()

class DecimalEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):


    date = datetime.now()   
    date_time_text = date.strftime('%Y-%m-%dT%H:%M')  
    date_today = datetime.strptime(date_time_text,'%Y-%m-%dT%H:%M')

    def clean_campaigns():
        mongodb = provider.get_mongodb()
        db = mongodb['sofie']
        clean_campaigns = db.campaign.find()
        print(clean_campaigns)         

        for item in clean_campaigns:

            date = item['finish']
            date_final = datetime.strptime(date,'%Y-%m-%dT%H:%M')    


            quantidade_dias = abs((date_final - date_today).days)   
            print(quantidade_dias)               


            if quantidade_dias >= 9 :
                db.campaign.delete_one(item)
        
        

    clean_campaigns()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }