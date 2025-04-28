import pandas as pd
from dotenv import load_dotenv
import os


filepath = 'D:\\Bencorp Projex\\MIGRACAO_RAIA_DROGASIL\\raia_GED.json'
df = pd.read_json(filepath,orient='records')


df.to_excel('D:\\Bencorp Projex\\MIGRACAO_RAIA_DROGASIL\\raia_GED.xlsx')

