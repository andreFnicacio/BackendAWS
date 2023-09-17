import json
from tokenize import String
from provider import *
from datetime import datetime, timedelta, timezone
import pytz

provider = Dev_Provider()
mongodb = provider.get_mongodb()

sofier = ""

def main(event, context):
    status_code = 200
    for record in event['Records']:

        body = json.loads(record['body'])
        if body:
            start_goal(body)
        else:
            status_code = 401

        return {
            'statusCode': status_code
        }       

def start_goal(body):
    sofier = body['sofier']
    task = body['task']

    active_campaign = get_all_actives_campaign(task)
    user_goals = get_user_goals(sofier)

    final_result = get_common_goals(user_goals, active_campaign)

    compute_task(final_result)

def compute_task(goals):
    for act in goals:
        goal = act['goal']
        campaign = act['campaign']
        print(act)

        goal['tasks'] = goal['tasks'] + 1

        tasks = goal['tasks']

        if goal['tasks'] > campaign['goal']:
            tasks = goal['tasks'] % campaign['goal']
            if tasks == 0:
                tasks = campaign['goal']


        if tasks >= campaign['goal']:
            pay_user(campaign['reward'])
        
        update_goal(goal)

def pay_user(reward):
    print('usuario bateu a meta e precisa ser pago')
    # TODO enfileira pedido de pagamento para usuario

def get_common_goals(user_goals, active_campaign):
    final_result = list()

    for user_goal in user_goals:
        for campaign in active_campaign:
            if user_goal['campaing'] == campaign['name']:
                final_result.append({'goal': user_goal, 'campaign': campaign})
    return final_result

def get_all_actives_campaign(task):
    all_campaign = get_all_campaign()
    active = list()
    for campaign in all_campaign:
        if task in campaign['task']:
            now = datetime.utcnow().isoformat()
            start_cpg = campaign['start']
            finish_cpg = campaign['finish']
            if start_cpg <= now and now <= finish_cpg:
                active.append(campaign)
    return active

def update_goal(goal):
    db = mongodb['sofie']
    collection = db['__dev_goals_campaign']
    cursor = collection.update_one({'_id': goal['_id']}, {'$set': goal})

def get_user_goals(sofier):
    db = mongodb['sofie']
    collection = db['__dev_goals_campaign']
    cursor = collection.find({'sofier': sofier})
    items = list()
    for item in cursor:
        items.append(item)
    return items

def get_all_campaign():
    print('buscando campanhas')
    db = mongodb['sofie']
    collection = db['__dev_campaign']
    cursor = collection.find()
    items = list()
    for item in cursor:
        items.append(item)
    return items
