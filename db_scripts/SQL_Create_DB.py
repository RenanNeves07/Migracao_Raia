import pandas as pd
import concurrent.futures
import sqlalchemy
import urllib.parse
from tqdm import tqdm
from sqlalchemy import MetaData
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
start = datetime.now()

def create_sqlalchemy_engine(connection_parameters):
    engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={connection_parameters}")
    return engine

# Function to insert data into the database
def inserir(chunk, engine, table, mode="append"):
    try:
        chunk.to_sql(table, engine, index=False, if_exists=mode, index_label='FuncInicio', schema="dbo")
        return True
    except Exception as e:
        print('\nErro:', e)
        return False

def drop_table(engine,table):
    metadata = MetaData()
    try:
        metadata.reflect(engine)
        if table in metadata.tables:
            metadata.tables[table].drop(engine)
        return True
    except Exception as e:
        return False


chunk_n = 9
df = pd.read_json(f'D:\\Bencorp Projex\\MIGRACAO_RAIA_DROGASIL\\raia_GED.json', orient='records')


USER = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASS')
DRIVER = os.getenv('DB_DRIVER')
SERVER = os.getenv('DB_SERVER')
DATABASE = os.getenv('DB_NAME')
table = f'RAIA_GED_PORTO'

# Database connection setup
params = f"DRIVER={DRIVER};SERVER={SERVER};PORT=1433;DATABASE={DATABASE};UID={USER};PWD={PASSWORD}"
db_params = urllib.parse.quote_plus(params)
engine_sql = create_sqlalchemy_engine(db_params)
retorno_db = drop_table(engine=engine_sql,table=table)

# Split DataFrame into chunks for parallel processing
chunksize = len(df) // 10
chunks = [df[i:i + chunksize] for i in range(0, len(df), chunksize)]

# Concurrent execution of database updates
with tqdm(total=len(chunks), leave=False, colour='green', desc="Atualizando banco de dados - TB_EMPRESAS") as bar:
    inserir(df.iloc[:0], engine_sql, table)
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for result in executor.map(lambda chunk: inserir(chunk, engine_sql, table), chunks):
            results.append(result)
            bar.update(len(chunks))