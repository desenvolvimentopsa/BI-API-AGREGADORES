import requests
import json
import time
import datetime

class Agregador:
    def __init__(self, rd, api, url, user, password, dominio, timeout=10):
        self.rd = rd
        self.url = url
        self.api = api
        self.user = user
        self.password = password
        self.dominio = dominio
        self.timeout = timeout
        
    def getData(self):
        try:
            print(f'Iniciando a busca da API {self.api}:', flush=True)
            tempoExecucao = time.time()
            self.urlBase = self.url
            offset = 0
            retorno = []
            resultMetaData = {}
            while True:
                # Atualizando a URL
                # Independente da URL passada, a URL final será a URLBase + limit=200&offset=0, proporcionando com que qualquer API possa ser passada para esta classe.
                # Ex: https://api.sienge.com.br/{env['dominio']}/public/api/v1/sales-contracts?customerId=1&cpf=12345678901
                # Será transformada em: https://api.sienge.com.br/{env['dominio']}/public/api/v1/sales-contracts?customerId=1&cpf=12345678901&limit=200&offset=0
                self.url = f'{self.urlBase}&limit=200&offset={offset}'
                print(self.url, flush=True)
                status, response = self.getApi()
                
                if status:
                    # Retirando do JSON os campos offset e limit
                    resultMetaData = response['resultSetMetadata']
                    if 'offset' in resultMetaData:
                        del resultMetaData['offset']
                    if 'limit' in resultMetaData:
                        del resultMetaData['limit']
                    offset += len(response['results'])
                    retorno.extend(response['results'])
                    if len(response['results']) == 0:
                        break
                else:
                    raise Exception(response)
                
            keyName = f'{self.dominio}-{self.api}'
            
            # Calculando tempo de execução
            tempoExecucao = time.time() - tempoExecucao
            resultMetaData['date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            resultMetaData['executionTime'] = f'{tempoExecucao:.2f}'
            resultMetaData['keyName'] = keyName
            info = {'resultSetMetadata': resultMetaData, 'results': retorno}
            
            # Salvando no Redis o JSON
            self.rd.set(keyName, json.dumps(info))
            return True, resultMetaData
            
        except Exception as err:
            print("Error on 'getData' service: ", err, flush=True)
            error = {'error': str(err)}
            return False, error
        
    def getApi(self):
        try:
            while True:
                response = requests.get(self.url, auth=(self.user, self.password))
                if response.status_code == 200:
                    return True, response.json()
                
                elif response.status_code == 429:
                    print('Atingido o limite de requisições. Aguardando 10 segundos...', flush=True)
                    time.sleep(self.timeout)
                else:
                    raise Exception(f'Erro ao buscar a API: {response.json()}')
                
        except Exception as e:
            return False, str(e)