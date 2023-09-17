import boto3
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
import json


#ADICIONADO PARAMETROS GOOGLE
URL = 'https://maps.googleapis.com/maps/api/geocode/json'
GOOGLE_KEY = 'AIzaSyCSRZqt2y20HHhVuUZbZcludaIspJEeloY'
PARAMS = {
    'key': GOOGLE_KEY,
    'address': ''
}


class Base_Provider():
    __redis_address = 'ec2_redis'
    __mongodb_address = 'ec2_mongodb'

    def get_resource(self, resource_name):
        ssm = boto3.client('ssm', 'sa-east-1')
        response = ssm.get_parameters(
            Names=resource_name, WithDecryption=False
        )
        res = list()
        for parameter in response['Parameters']:
            print(parameter)
            res.append(parameter['Value'])
        return res
    def get_host_and_port(self, host_variable, redis_port_address):
        resources = [host_variable, redis_port_address]
        res = self.get_resource(resources)
        return res[0], res[1]



class Prod_Provider(Base_Provider):

    def get_mongodb(self):
        return MongoClient('mongodb.mysofie.com', 5060)


class Dev_Provider(Base_Provider):

    def get_mongodb(self):
        return MongoClient('mongodb.mysofie.com', 5060)

    def get_sqs(self):
        return boto3.resource('sqs')    

    def get_dynamodb(self):
        return boto3.resource('dynamodb')            

    def get_lambda(self):
        return boto3.client('lambda')            


def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%dT%H:%M:%S.%f')
    else:
        return obj


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)
