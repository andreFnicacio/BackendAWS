import boto3
from redis import Redis
from pymongo import MongoClient
from bson import ObjectId
import json
import datetime as dt

class Base_Provider():
    __redis_address = 'ec2_redis'
    __mongodb_address = 'ec2_mongodb'
    
    def get_resource(self, resource_name):
        ssm = boto3.client('ssm', 'sa-east-1')
        response = ssm.get_parameters(
            Names=resource_name,WithDecryption=False
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
        
    def get_dynamodb(self):
        return boto3.resource('dynamodb')
    
    def base_get_redis(self, redis_port_address, db, decode):
        # host, port = self.get_host_and_port(self.__redis_address, redis_port_address)
        host = '172.16.220.40'
        port = 0
        if redis_port_address == 'redis_port_prod':
            port = 5071
        else:
            port = 5091
        print(f'pegando novo endereco: porta {port}')
        if db:
            return Redis(host = host, port = port, db = db, decode_responses = decode)
        else:
            return Redis(host = host, port = port, decode_responses = decode)
        

class Prod_Provider(Base_Provider):
    
    __redis_port = 'redis_port_prod'
    __mongodb_port = 'mongodb_port_prod'
    
    def get_redis(self, *args, **kwargs):
        db = kwargs.get("db", None)
        decode = kwargs.get("decode_responses", False)
        if db:
            return Redis(host = 'redis.mysofie.com', port = 5071, db = db, decode_responses = decode)
        else:
            return Redis(host = 'redis.mysofie.com', port = 5071, decode_responses = decode)

    def get_mongodb(self):
        return MongoClient('mongodb.mysofie.com', 5060)
    
        
class Dev_Provider(Base_Provider):
    
    __redis_port = 'redis_port_dev'
    __mongodb_port = 'mongodb_port_dev'
    
    def get_redis(self, *args, **kwargs):
        db = kwargs.get("db", None)
        decode = kwargs.get("decode_responses", False)
        if db:
            return Redis(host = 'redis.mysofie.com', port = 5091, db = db, decode_responses = decode)
        else:
            return Redis(host = 'redis.mysofie.com', port = 5091, decode_responses = decode)
    
    def get_mongodb(self):
        return MongoClient('mongodb.mysofie.com', 5060)
    
    
def json_serial(obj):
        if isinstance(obj, dt):
            return obj.strftime('%Y-%m-%dT%H:%M:%S.%f')
        else:
            return obj

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)