from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import pandas as pd
import time
import sqlalchemy
from sqlalchemy import text
import urllib
import os
from dotenv import load_dotenv
import shutil

load_dotenv()
chunk_n = '0'
USER = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASS')
DRIVER = os.getenv('DB_DRIVER')
SERVER = os.getenv('DB_SERVER')
DATABASE = os.getenv('DB_NAME')
TABLE = f'AE_RAIA_{chunk_n}'

Cliente = 'RAIA_DROGASIL'
DiretorioCliente = f'D:\\RAIA_DROGASIL\\IMAGENS\\'
download_directory = f'D:\\RAIA_DROGASIL\\IMAGENS\\TEMP\\'
new_directory = f'D:\\RAIA_DROGASIL\\IMAGENS\\FINAL\\'

pastaPrincipal = os.getcwd()
pastaArquivos = os.path.join(pastaPrincipal,'Arquivos_Download')


def create_db_connection_parameters():
    params = f'''DRIVER={{{DRIVER}}};
                SERVER={SERVER};
                PORT=1433;
                DATABASE={DATABASE};
                UID={USER};
                PWD={PASSWORD}'''
    
    db_params = urllib.parse.quote_plus(params)
    return db_params

def create_sqlalchemy_engine(connection_parameters):
    engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={connection_parameters}")
    return engine

def criar_diretorio(caminho):
    if not os.path.exists(caminho):
        os.makedirs(caminho)
        print(f"Directory '{caminho}' created.")

def start_webDriver(driver):
    download_dir = f'{download_directory}'
    chrome_options = Options()
    chrome_options.add_argument('--disable-features=InsecureDownloadWarnings')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_experimental_option('prefs',{'plugins.always_open_pdf_externally': True,'download.default_directory': download_dir})
    driver = webdriver.Chrome(options=chrome_options)
    driver.get("about:blank")
    driver.set_window_size(100,200)
    driver.set_window_position(0,0)
    return driver

def is_file_downloaded(filename):
    filepath = os.path.join(download_directory, filename)
    return os.path.exists(filepath)

def verifica_arquivo(diretorio, nome_arquivo):
    for arquivo in os.listdir(diretorio):
        if arquivo == nome_arquivo:
            return True
    return False


def image_download(webdriver, df, engine):
    filename = df['NOME_IMAGEM'][0]
    row_index = df['INDEX_IMAGEM'][0]
    new_filename = f'{row_index}_{filename}'
    try:
        webdriver.get('about:blank')
        webdriver.get(df['LINK_FINAL'][0])
        iframe = webdriver.find_element(By.ID,'IWURLWINDOW')
        webdriver.switch_to.frame(iframe)
        webdriver.find_element(By.XPATH,'//*[@id="main-content"]/a').click()
        wait = WebDriverWait(webdriver, 60)
        wait.until(lambda driver: is_file_downloaded(filename))
        downloaded_filepath = os.path.join(download_directory, filename)
        new_filepath = os.path.join(new_directory, new_filename)
        shutil.move(downloaded_filepath, new_filepath)
        if verifica_arquivo(new_directory,new_filename) == True:
            print(f'File {new_filename} downloaded successfully.')
            downloadStatus = 1
    except Exception as e:
        downloadStatus = 2

    update_query = f'''
        UPDATE {TABLE} 
        SET BAIXADO = {downloadStatus}
        WHERE INDEX_IMAGEM = {df['INDEX_IMAGEM'][0]}
        '''
    max_attempts = 30
    attempt = 1
    success = False
    while attempt <= max_attempts and not success:
        try:
            with engine.connect() as conn:
                conn.execute(text(update_query))
                conn.commit()  
            success = True
            print("Operation successful!")
        except Exception as e:
            print(f"Attempt {attempt} failed:", e)
            attempt += 1
            if attempt <= max_attempts:
                print("Retrying in 5 seconds...")
                time.sleep(1)     
    if not success:
            print("Maximum attempts reached. Operation failed.")
                
        

sql_connection_parameters = create_db_connection_parameters()
sql_connection_engine = create_sqlalchemy_engine(sql_connection_parameters)
criar_diretorio(DiretorioCliente)
criar_diretorio(download_directory)
criar_diretorio(new_directory)
webdriver = start_webDriver(webdriver)

while True:
    query = f'''
    SELECT TOP 1 *
    FROM 
        {TABLE} 
    WHERE 
        BAIXADO = 0 
    ORDER BY 
        INDEX_IMAGEM ASC;'''
    df = pd.read_sql(query, sql_connection_engine)
    if df.empty:
        print("No more images to download!")
        break 
    
    image_download(webdriver=webdriver, df=df, engine=sql_connection_engine)
