import pandas as pd
from sqlalchemy import create_engine
import io
import boto3

def load(alchemyEngine, final_df, table):
    # Connect to PostgreSQL server
    dbConnection = alchemyEngine.connect()
    try:
       final_df.to_sql(table, alchemyEngine, if_exists="append", index=False)
    except Exception as e:  
        print(e)
    else:
        print(f'PostgreSQL Table "{table}", has been loaded.')
    finally:
       dbConnection.close()


def read_s3(s3_bucket, s3_resource, key):
    try:
        s3 = s3_resource
        response = s3.Object(s3_bucket, key).get()
        data = response['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(data), low_memory=False)
        print('File: ',key, 'loaded from S3')
        return df
    except Exception as e:  
        print(e)
    
    

    