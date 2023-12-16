
import pandas as pd


def extract_db(table,alchemyEngine):
# Connect to PostgreSQL server
    dbConnection = alchemyEngine.connect()
    print(dbConnection)
# Read data from PostgreSQL database table and load into a DataFrame instance
    try:
        sql = f'select * from "{table}"'
        db_df = pd.read_sql(sql, dbConnection)
    except Exception as e:
        print(e)
    finally:
        dbConnection.close()
    return db_df

def create_db_table(alchemyEngine, final_df, table):
    # Connect to PostgreSQL server
    dbConnection = alchemyEngine.connect()
    try:
       final_df.to_sql(table, dbConnection, if_exists='fail')
    except Exception as e:  
        print(e)
    else:
        print(f'PostgreSQL Table "{table}", has been created successfully.')
    finally:
       dbConnection.close()
