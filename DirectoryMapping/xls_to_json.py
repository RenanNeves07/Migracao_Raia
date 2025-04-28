import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
df = pd.read_excel('C:\\Users\\Lenovo\\Downloads\\Dados_importacao_ged.xlsx')
df.to_json('D:\\Bencorp Projex\\MIGRACAO_RAIA_DROGASIL\\py_upload_ged_scripts\\upload_ae.json', orient='records')
