import requests
import json
import time
import datetime

class Agregador:
    def __init__(self, rd, api, user, password, dominio, timeout=10):
        self.rd = rd
        self.api = api
        self.user = user
        self.password = password
        self.dominio = dominio
        self.timeout = timeout
        
    def getData(self):
        try:
            print('Iniciando a busca dos contratos de vendas...', flush=True)
            tempoExecucao = time.time()
            offset = 0
            retorno = []
            resultMetaData = {}
            while True:
                self.url = f"https://api.sienge.com.br/{self.dominio}/public/api/v1/{self.api}?limit=200&offset={offset}"
                print(self.url, flush=True)
                response = self.getApi()
                resultMetaData = response['resultSetMetadata']
                if 'offset' in resultMetaData:
                    del resultMetaData['offset']
                if 'limit' in resultMetaData:
                    del resultMetaData['limit']
                offset += len(response['results'])
                retorno.extend(response['results'])
                if len(response['results']) == 0:
                    break
                
            # Calculando tempo de execução
            tempoExecucao = time.time() - tempoExecucao
            resultMetaData['data'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            resultMetaData['executionTime'] = f'{tempoExecucao:.2f}'
            salesContracts = {'resultSetMetadata': resultMetaData, 'results': retorno}
            
            # Salvando no Redis o JSON
            self.rd.set('salesContracts', json.dumps(salesContracts))
            return True
            
        except Exception as err:
            print("Error on 'getData' service: ", err, flush=True)
            return False
        
    def getApi(self):
        try:
            while True:
                response = requests.get(self.url, auth=(self.user, self.password))
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    print('Atingido o limite de requisições. Aguardando 10 segundos...', flush=True)
                    time.sleep(self.timeout)
                else:
                    raise Exception(f'Erro ao buscar a API: {response.json()}')
                
        except Exception as e:
            print(e, flush=True)
            return False