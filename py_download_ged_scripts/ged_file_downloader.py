import requests
import os
from auth_service import AuthSOCWebService
from download_ged_service import SOAPRequest_downloadArquivosPorGed

class GEDFileDownloader:
    def __init__(self, codigo_empresa, ged_code):
        path2 = os.getcwd()
        path3 = os.path.join(path2,'Arquivos')
        self.codigo_empresa = codigo_empresa
        self.path = path3
        self.ged_code = ged_code

    def download_and_extract_files(self):
        username = "257308"
        password = "c4be4a31d9"
        chave_acesso = 'c4be4a31d9'
        codigo_empresa_principal = '257308'
        codigo_responsavel = '83642'
        codigo_usuario = '2434240'
        codigo_empresa = self.codigo_empresa
        codigo_ged = self.ged_code

        client = 'https://ws1.soc.com.br/WSSoc/DownloadArquivosWs?wsdl'
        action = 'http://services.soc.age.com/DownloadArquivosWs/downloadArquivosPorGed'

        auth_instance = AuthSOCWebService(username, password)
        created = auth_instance.create_timestamp()
        expires = auth_instance.create_expires()
        nonce = auth_instance.encode_nonce()
        encoded_digest = auth_instance.calculate_digest(nonce, created, password)
        username_token_id = auth_instance.calculate_username_token_id()
        timestamp_id = auth_instance.calculate_timestamp_id(created, expires)

        soap_request_instance = SOAPRequest_downloadArquivosPorGed(codigo_empresa, chave_acesso, codigo_empresa_principal,
                                        codigo_responsavel, codigo_usuario, client,
                                        timestamp_id, created, expires,
                                        username, encoded_digest, nonce,
                                        codigo_ged, username_token_id)

        soap_request_xml = soap_request_instance.generate_soap_request()
        soap_request_header = soap_request_instance.headers(action)

        try:
            response = requests.post(client, data=soap_request_xml, headers=soap_request_header)
            if response.status_code == 200:
                decoded_content = response.content.decode('iso-8859-1')
                response_xml = soap_request_instance.extract_xml_response(decoded_content)
                response_info = soap_request_instance.extract_response_info(response_xml)
                response_xml = soap_request_instance.extract_xml_response(decoded_content)
                extract = soap_request_instance.extract_and_save_files(response.content, self.path)

                exit_message = f'''SOAP request was successful.\nSOAP request messages:{response_info}\nExtraction Message: {extract}\n'''
            else:
                exit_message= f"SOAP request failed with status code: {response.status_code}"
        except Exception as e:
            exit_message = f"Error sending SOAP request: {str(e)}"
        
        return exit_message