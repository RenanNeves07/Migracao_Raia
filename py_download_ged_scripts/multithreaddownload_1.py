from concurrent.futures import ThreadPoolExecutor
from auth_service import AuthSOCWebService
from download_ged_service import SOAPRequest_downloadArquivosPorGed
from dotenv import load_dotenv
import requests
import os
import pandas as pd
import re

load_dotenv()
json_path = os.getenv('DATA_PATH_JSON')
username = os.getenv('USERNAME_ID')
password = os.getenv('PASSWORD_ID')
chaveAcesso = os.getenv('PASSWORD_ID')
codigoEmpresaPrincipal = os.getenv('PRINCIPAL_EMP')
codigoResponsavel = os.getenv('RESPONSIBLE_ID')
codigoUsuario = os.getenv('USER2') #'112448'  #

arquivos = os.listdir(os.getenv('DOWNLOAD_GED'))
numerals = [re.search(r'^(\d+)_', filename).group(1) for filename in arquivos]
df = pd.read_json(json_path)
df['INDEX_IMAGEM'] = df['INDEX_IMAGEM'].astype(str)
mask = df['INDEX_IMAGEM'].isin(numerals)
df = df[~mask]

def process_row(row):
    codigoEmpresa = row['CD_EMPRESA']
    codigoArquivo = row['CD_ARQUIVO_GED']
    codigoGed = row['CD_GED']

    client = "https://ws1.soc.com.br/WSSoc/DownloadArquivosWs?wsdl"
    action = 'http://services.soc.age.com/DownloadArquivosWs/downloadArquivosGedPorLote'
    auth_instance = AuthSOCWebService(username, password)
    created = auth_instance.create_timestamp()
    expires = auth_instance.create_expires()
    nonce = auth_instance.encode_nonce()
    encoded_digest = auth_instance.calculate_digest(nonce, created, password)
    username_token_id = auth_instance.calculate_username_token_id()
    timestamp_id = auth_instance.calculate_timestamp_id(created, expires)

    soap_request_instance = SOAPRequest_downloadArquivosPorGed(
        codigoEmpresa, chaveAcesso, codigoEmpresaPrincipal,
        codigoResponsavel, codigoUsuario, client,
        timestamp_id, created, expires,
        username, encoded_digest, nonce,
        codigoGed, username_token_id, codigoArquivo=codigoArquivo
    )

    soap_request_xml = soap_request_instance.generate_soap_request_arquivo_ged()
    soap_request_header = soap_request_instance.headers(action)

    try:
        response = requests.post(client, data=soap_request_xml, headers=soap_request_header)
        if response.status_code == 200:
            decoded_content = response.content.decode('iso-8859-1')
            response_xml = soap_request_instance.extract_xml_response(decoded_content)
            response_info = soap_request_instance.extract_response_info(response_xml)
            response_xml = soap_request_instance.extract_xml_response(decoded_content)

            file_downloaded_name = soap_request_instance.extract_and_save_files(response.content,os.getenv('DOWNLOAD_GED'))
            folder_path = os.getenv('DOWNLOAD_GED')
            actual_file_path = os.path.join(folder_path, file_downloaded_name)
            future_file_name = str(row['INDEX_IMAGEM']) + f'_{file_downloaded_name}'
            future_file_path = os.path.join(folder_path, future_file_name)
            if not os.path.exists(future_file_path):
                os.rename(actual_file_path, future_file_path)
            else:
                os.remove(actual_file_path)

            print("SOAP request was successful.")
            print(f"SOAP request messages:{response_info}")
            print(f'FILE DOWNLOADED: {file_downloaded_name} ITEM: of {len(df)}')

            row['BAIXADO'] = 'OK'
            
        else:
            print(f"SOAP request failed with status code: {response.status_code}")
            print(f"SOAP request failed with status code: {response.content}")

    except Exception as e:
        print(f"Error sending SOAP request: {str(e)}")

    return row

# Use ThreadPoolExecutor to parallelize the processing of rows
with ThreadPoolExecutor(max_workers=20) as executor:
    # Process the DataFrame in parallel
    results = list(executor.map(process_row, df.to_dict('records')))

# Update the DataFrame with the results
df = pd.DataFrame(results)
df.to_json(json_path, orient='records')