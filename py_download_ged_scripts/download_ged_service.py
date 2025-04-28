import xml.etree.ElementTree as ET
import re
import zipfile
import tempfile


class SOAPRequest_downloadArquivosPorGed:
    def __init__(self, codigoEmpresa, chaveAcesso, codigoEmpresaPrincipal, codigoResponsavel, codigoUsuario, client,
                 timestamp_id, created, expires, username, encoded_digest, nonce, codigoGed, username_token_id, codigoArquivo=None):
        self.codigoEmpresa = codigoEmpresa
        self.chaveAcesso = chaveAcesso
        self.codigoEmpresaPrincipal = codigoEmpresaPrincipal
        self.codigoResponsavel = codigoResponsavel
        self.codigoUsuario = codigoUsuario
        self.client = client
        self.timestamp_id = timestamp_id
        self.created = created
        self.expires = expires
        self.username = username
        self.encoded_digest = encoded_digest
        self.nonce_base64 = nonce
        self.codigoGed = codigoGed
        self.username_token_id = username_token_id
        self.codigoArquivo = codigoArquivo
        
    def generate_soap_request(self):
        soap_request = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:ser="http://services.soc.age.com/">
             <soapenv:Header>
              <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
               <wsu:Timestamp wsu:Id="{self.timestamp_id}">
                <wsu:Created>{self.created}</wsu:Created>
                <wsu:Expires>{self.expires}</wsu:Expires>
               </wsu:Timestamp>
               <wsse:UsernameToken wsu:Id="{self.username_token_id}">
                <wsse:Username>{self.username}</wsse:Username>
                <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">{self.encoded_digest}</wsse:Password>
                <wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">{self.nonce_base64}</wsse:Nonce>
                <wsu:Created>{self.created}</wsu:Created>
               </wsse:UsernameToken>
              </wsse:Security>
             </soapenv:Header>
             <soapenv:Body>
             <ser:downloadArquivosPorGed>
             <downloadPorGed>
             <identificacaoWsVo>
             <codigoEmpresaPrincipal>{self.codigoEmpresaPrincipal}</codigoEmpresaPrincipal>
             <codigoResponsavel>{self.codigoResponsavel}</codigoResponsavel>
             <codigoUsuario>{self.codigoUsuario}</codigoUsuario>
             </identificacaoWsVo>
             <codigoEmpresa>{self.codigoEmpresa}</codigoEmpresa>
             <codigoGed>{self.codigoGed}</codigoGed>
             </downloadPorGed>
             </ser:downloadArquivosPorGed>
             </soapenv:Body>
            </soapenv:Envelope>'''

        return soap_request

    def generate_soap_request_arquivo_ged(self):
        soap_request = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://services.soc.age.com/">
             <soapenv:Header>
              <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
               <wsu:Timestamp wsu:Id="{self.timestamp_id}">
                <wsu:Created>{self.created}</wsu:Created>
                <wsu:Expires>{self.expires}</wsu:Expires>
               </wsu:Timestamp>
               <wsse:UsernameToken wsu:Id="{self.username_token_id}">
                <wsse:Username>{self.username}</wsse:Username>
                <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">{self.encoded_digest}</wsse:Password>
                <wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">{self.nonce_base64}</wsse:Nonce>
                <wsu:Created>{self.created}</wsu:Created>
               </wsse:UsernameToken>
              </wsse:Security>
             </soapenv:Header>
             <soapenv:Body>
             <ser:downloadArquivosGedPorLote>
             <downloadPorLote>
             <identificacaoWsVo>
             <codigoEmpresaPrincipal>{self.codigoEmpresaPrincipal}</codigoEmpresaPrincipal>
             <codigoResponsavel>{self.codigoResponsavel}</codigoResponsavel>
             <codigoUsuario>{self.codigoUsuario}</codigoUsuario>
             </identificacaoWsVo>
             <codigoEmpresa>{self.codigoEmpresa}</codigoEmpresa>
             <codigosArquivosGed>{self.codigoArquivo}</codigosArquivosGed>
             <codigoGed>{self.codigoGed}</codigoGed>
             </downloadPorLote>
             </ser:downloadArquivosGedPorLote>
             </soapenv:Body>
            </soapenv:Envelope>      
            '''

        return soap_request

    def headers(self, SOAPAction):
        self.SOAPAction = SOAPAction
        return {
            'Content-Type': 'application/xop+xml',
            'charset': 'UTF-8',
            'Content-Transfer-Encoding': 'binary',
            'Content-ID': '<soap_message>',
            'Accept': 'application/soap+xml',
            'SOAPAction': SOAPAction
        }

    def extract_xml_response(self, response):
        xml_pattern = r'<soap:Envelope.*?</soap:Envelope>'
        match = re.search(xml_pattern, response, re.DOTALL)
        
        if match:
            xml_content = match.group(0)
            return xml_content
        else:
            return None



    def extract_response_info(self, response_content):

        response_info = {}
        

        try:
            root = ET.fromstring(response_content)

            codigo_mensagem = root.find(".//codigoMensagem")
            mensagem = root.find(".//mensagem")
            numero_erros = root.find(".//numeroErros")

            response_info["codigoMensagem"] = codigo_mensagem.text if codigo_mensagem is not None else None
            response_info["mensagem"] = mensagem.text if mensagem is not None else None
            response_info["numeroErros"] = int(numero_erros.text) if numero_erros is not None else None

        except ET.ParseError as e:
            print(f"Error parsing SOAP response XML: {str(e)}")

        return response_info
    
    def extract_and_save_files(self, response_content, save_path):
        try:
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            temp_file.write(response_content)
            temp_file.close()

            with zipfile.ZipFile(temp_file.name) as zip_file:
                zip_count = len(zip_file.namelist())
                zip_file.extractall(save_path)
                extracted_files = zip_file.namelist()
            
            #print(f'{extracted_files[0]}')
            return_print = f'{extracted_files[0]}'
            #return_print = f"{zip_count} File(s) extracted and saved successfully in {save_path}."
        except ET.ParseError as e:
            return_print = f"Error parsing SOAP response XML: {str(e)}"
        except Exception as e:
            return_print = f"Error extracting and saving files: {str(e)}"
        
        return return_print
