import configparser
from sqlalchemy import create_engine
import boto3

def auth(config_file):
    config = configparser.ConfigParser() #creat config instance 

    # read the configuration file
    config.read(config_file) #read the config file

    # get all the sections
    config.sections() #get all sections from config file

    # get postgresql section
    database = config.get('postgresql', 'database1')
    user = config.get('postgresql', 'user')
    password = config.get('postgresql', 'password')
    host = config.get('postgresql', 'host')
    port = config.get('postgresql', 'port')
    #table = config.get('postgresql', 'source')
    alchemyEngine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{database}', pool_recycle=3600)

    # AWS credentials
    service_name = config.get('aws_s3', 'service_name')
    region_name = config.get('aws_s3', 'region_name')
    aws_access_key_id = config.get('aws_s3', 'aws_access_key_id')
    aws_secret_access_key = config.get('aws_s3', 'aws_secret_access_key')
    s3_bucket = config.get('aws_s3', 's3_bucket')

    s3_resource = boto3.resource(
    service_name=service_name,
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)

    s3_client = boto3.client(
    service_name=service_name,
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

    
    for credential in [database, user, password, host, port]:
        print(credential != None and credential != "")
    
    return s3_resource, s3_client, s3_bucket, alchemyEngine
