import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from datetime import datetime, timedelta, timezone
import secrets
import base64
import hashlib
import requests
import os
import io

url = 'https://ws1.soc.com.br/WebSoc/exportadados?parametro='

empresas = ['1379598'] #RAIA DROGASIL
tipos = ['2','3']
empresa = 'RAIA_DROGASIL'
dataframes=[]

for tipo in tipos:
    for item in empresas:
        body = f'''{{'empresa':'{item}',
                    'codigo':'11672',
                    'chave':'8abb9e50b4201b1cbfd5',
                    'tipoSaida':'json',
                    'tipoBusca':'0',
                    'sequencialFicha':'',
                    'cpfFuncionario':'',
                    'filtraPorTipoSocged':'1',
                    'codigoTipoSocged':'{tipo}',
                    'dataInicio':'',
                    'dataFim':'',
                    'dataEmissaoInicio':'11/03/2024',
                    'dataEmissaoFim':'13/03/2024'}}'''
    response = requests.post(f'{url}{body}')
    if response.status_code == 200:
        try:
            data = io.StringIO(response.text)
            df = pd.read_json(data)
            dataframes.append(df)
        except ValueError as e:
            print(f'erro {e}')
    else:
        print('Erro Chamada')

final_df = pd.concat(dataframes, ignore_index=True)
#final_df.to_excel(f'vidas_{empresa}.xlsx', index=False)
df = pd.read_json('D:\Bencorp Projex\MIGRACAO_RAIA_DROGASIL\py_upload_ged_scripts\chunk_3.json',orient='records')
df = df.astype(str)
#df['UPLOAD'] = df['UPLOAD'].apply(lambda x: '')
df['COD_GED_BEN'] = df['COD_GED_BEN'].apply(lambda x:'')

filtered_df = final_df[final_df['OBSERVACAO'].str.contains('MIGRACAO PORTO SEGURO>BENCORP: 11/03/2024', na=False)]
filtered_df['INDEX_CREATED'] = filtered_df['NM_GED'].str.extract(r'(\d+)_')
filtered_df['INDEX_CREATED'] = filtered_df['INDEX_CREATED'].astype(str)
df['INDEX_IMAGEM'] = df['INDEX_IMAGEM'].astype(str)
final_df = df[~df['INDEX_IMAGEM'].isin(filtered_df['INDEX_CREATED'])]


#df['FILE_EXTENSION'] = df['NOME_ARQUIVO_FIM'].apply(lambda x: x.split('.')[-1].upper())
#remove_extension = lambda row: os.path.splitext(row['FILE_NAME'])[0]
#df['NOME_ARQUIVO_FIM'] = df.apply(remove_extension, axis=1)

def criar_ged(row):

    if row['FILE_NAME'] == 'NAO':
        return 'SemArquivoParaUpload'

    # Valor do password
    username = "U2763307"
    password = "af07993cfdf3858fdef7327bda6cd9fe89187637"
    bpassword = bytes(password, "utf-8")

    # CABEÇALHO AUTENTICAÇÃO
    codigoEmpresa = '1379598' #row['CD_EMPRESA_BEN']#"266260"
    chaveAcesso = "af07993cfdf3858fdef7327bda6cd9fe89187637"
    codigoEmpresaPrincipal = '257308'
    codigoResponsavel = '83642'
    codigoUsuario = '2763307'
    homologacao = ''
    nome = ''
    razaoSocial = '' 

    """
    # Valor do password
    username = "257308"
    password = "c4be4a31d9"
    bpassword = bytes(password, "utf-8")

    codigoEmpresa = '1379598' #row['CD_EMPRESA_BEN']#"266260"
    chaveAcesso = "c4be4a31d9"
    codigoEmpresaPrincipal = '257308'
    codigoResponsavel = '83642'
    codigoUsuario = '2313240'
    homologacao = ''
    nome = ''
    razaoSocial = '' 
    """

    # PARAMETROS DO WEBSERVICE
    classificacao = 'GED'

    if row['COD_FUN_BEN'] != 'NAO':
        codigoFuncionario = row['COD_FUN_BEN']
        codigoFuncionario = str(codigoFuncionario)
        if codigoFuncionario.endswith('.0'):
            codigoFuncionario = codigoFuncionario[:-2]
        print(codigoFuncionario)
    else:
        codigoFuncionario = ''

    codigoGed = ''
    codigoSequencialFicha = ''
    codigoTipoGed = row['COD_TIPO_GED_BEN']
    extensaoArquivo = row['EXTENSAO']#'PDF'
    nomeArquivo = row['FILE_NAME']
    nomeGed = str(row['INDEX_IMAGEM']) + '_' + row['TIPO_GED'] + '_' + row['NOME_FUNCIONARIO']
    nomeTipoGed = ''
    sobreescreveArquivo = 'true'
    codigoUnidadeGed = ''
    dataValidadeGed = ''
    revisaoGed = ''
    observacao = 'MIGRACAO PORTO SEGURO>BENCORP: 11/03/2024'

    created = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    bcreated = bytes(created, "utf-8")
    expires_timestamp = datetime.now(timezone.utc) + timedelta(minutes=1)
    expires = expires_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    nonce1 = secrets.token_bytes(16)
    nonce = base64.b64encode(nonce1).decode('utf-8')
    nonce2 = base64.b64decode(nonce)
    nonce_base64 = base64.b64encode(nonce2).decode('utf-8')
    concat_bytes = nonce2 + bcreated + bpassword
    sha1_obj = hashlib.sha1()
    sha1_obj.update(concat_bytes)
    digest_bytes = sha1_obj.digest()
    encoded_digest = base64.b64encode(digest_bytes).decode("utf-8")
    credentials = username + password
    hashed_credentials = hashlib.sha1(credentials.encode()).hexdigest()
    username_token_id = "UsernameToken-" + hashed_credentials
    timestamp_str = created + expires
    timestamp_hash = hashlib.sha1(timestamp_str.encode()).hexdigest()
    timestamp_id = "TS-" + timestamp_hash


    caminhoArquivo = f'E:\\RAIA_DROGASIL\\IMAGENS\\\FINAL\\{nomeArquivo}'
    with open(caminhoArquivo, 'rb') as f:
        arquivo_bytes = f.read()
        arquivo_base64 = base64.b64encode(arquivo_bytes).decode()

    client = 'https://ws1.soc.com.br/WSSoc/services/UploadArquivosWs?wsdl'
    headers = {'Content-Type': 'application/xop+xml', 'Content-Transfer-Encoding': 'binary',
                'Content-ID': '<soap_message>', 'Accept': 'application/soap+xml',
                'SOAPAction': 'http://services.soc.age.com/UploadArquivosWs/uploadArquivo'}
    body = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:ser="http://services.soc.age.com/">
        <soapenv:Header>
        <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
        <wsu:Timestamp wsu:Id="{timestamp_id}">
        <wsu:Created>{created}</wsu:Created>
        <wsu:Expires>{expires}</wsu:Expires>
        </wsu:Timestamp>
        <wsse:UsernameToken wsu:Id="{username_token_id}">
        <wsse:Username>{username}</wsse:Username>
        <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">{encoded_digest}</wsse:Password>
        <wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">{nonce_base64}</wsse:Nonce>
        <wsu:Created>{created}</wsu:Created>
        </wsse:UsernameToken>
        </wsse:Security>
        </soapenv:Header>
        <soapenv:Body>
        <ser:uploadArquivo>
        <arg0>
        <arquivo>{arquivo_base64}</arquivo>
        <classificacao>{classificacao}</classificacao>
        <codigoEmpresa>{codigoEmpresa}</codigoEmpresa>
        <codigoFuncionario>{codigoFuncionario}</codigoFuncionario>
        <codigoGed>{codigoGed}</codigoGed>
        <codigoSequencialFicha>{codigoSequencialFicha}</codigoSequencialFicha>
        <codigoTipoGed>{codigoTipoGed}</codigoTipoGed>
        <extensaoArquivo>{extensaoArquivo}</extensaoArquivo>
        <identificacaoVo>
        <chaveAcesso>{chaveAcesso}</chaveAcesso>
        <codigoEmpresaPrincipal>{codigoEmpresaPrincipal}</codigoEmpresaPrincipal>
        <codigoResponsavel>{codigoResponsavel}</codigoResponsavel>
        <homologacao>{homologacao}</homologacao>
        <codigoUsuario>{codigoUsuario}</codigoUsuario>
        </identificacaoVo>
        <nomeArquivo>{nomeArquivo}</nomeArquivo>
        <nomeGed>{nomeGed}</nomeGed>
        <nomeTipoGed>{nomeTipoGed}</nomeTipoGed>
        <sobreescreveArquivo>{sobreescreveArquivo}</sobreescreveArquivo>
        <codigoUnidadeGed>{codigoUnidadeGed}</codigoUnidadeGed>
        <dataValidadeGed>{dataValidadeGed}</dataValidadeGed>
        <revisaoGed>{revisaoGed}</revisaoGed>
        <observacao>{observacao}</observacao>
        </arg0>
        </ser:uploadArquivo>
        </soapenv:Body>
    </soapenv:Envelope>
    '''

    response = requests.post(client, headers=headers, data=body)
    response_str = response.text
    print(response_str)
    return response_str


def process_row(row):
    try:
        criarGed = criar_ged(row)
        final_df.at[row.name, 'UPLOAD'] = str(criarGed)
    except Exception as e:
        final_df.at[row.name, 'UPLOAD'] = f'Erro: {str(e)} :: {criar_ged}'

with ThreadPoolExecutor(max_workers=2) as executor:  # Adjust max_workers as needed
    futures = [executor.submit(process_row, row) for _, row in final_df.iterrows()]

    # Wait for all threads to finish
    concurrent.futures.wait(futures)

# Display the updated DataFrame
print(df)