import boto3
from redis import Redis
from datetime import datetime

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
        host, port = self.get_host_and_port(self.__redis_address, redis_port_address)
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
    
    
def json_serial(obj):
        """
        Função com o objetivo de transformar um objeto Python em um JSON
        Foi criado para suportar o tipo datetime sem o incoveniente do par `$date`
        Deve ser utilizado em conjunto com `json.dumps`
        :param obj:
            Fragmento de informação
        :return:
            Saída no formato adequado em JSON
        """
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%S.%f')
        else:
            return obj