import requests
import redis
import json
from requests.auth import HTTPBasicAuth
import time
import sys
import datetime

# CONFIGURA O REDIS
# DADOS CONFIGURAÇÃO REDIS
rd = None
env = {}
infoConfig = {}
try:
    rd = redis.Redis("localhost", 6445)
    envjs = rd.get("system_config")
    if envjs is None:
        print("CHAVE system_config NAO ENCONTRADA", flush=True)
        time.sleep(10)
        sys.exit(1)
    env = json.loads(envjs)
except Exception as e:
    print("SERVICE REDIS DOWN: ", e, flush=True)
    time.sleep(10)
    sys.exit(1)

def getSalesContracts(user, password, dominio):
    try:
        offset = 0
        retorno = []
        resultMetaData = {}
        while True:
            url = f"https://api.sienge.com.br/{dominio}/public/api/v1/sales-contracts?limit=200&offset={offset}"
            response = getApi(url, user, password)
            resultMetaData = response['resultSetMetadata']
            if 'offset' in resultMetaData:
                del resultMetaData['offset']
            if 'limit' in resultMetaData:
                del resultMetaData['limit']
            offset += len(response['results'])
            retorno.extend(response['results'])
            if len(response['results']) == 0:
                break
        return retorno, resultMetaData
    except Exception as err:
        print("Error on 'getSalesContracts' service: ", err, flush=True)
        return None
    
def getApi(url, user, password):
    try:
        while True:
            response = requests.get(url, auth=(user, password))
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print('Atingido o limite de requisições. Aguardando 10 segundos...')
                time.sleep(10)
            else:
                print(f'Erro ao buscar a API: {response.status_code}')
                print(response.json())
                return None

    except Exception as e:
        print(e)
        return None
    
def saveSalesContracts(salesContracts):
    try:
        print('Salvando contratos de vendas...')
        rd.set('salesContracts', json.dumps(salesContracts))
    except Exception as err:
        print("Error on 'saveSalesContracts' service: ", err, flush=True)
        return None
    
def main():
    try:
        user = env['sienge_user']
        password = env['sienge_pwd']
        dominio = env['dominio']
        tempoExecucao = time.time()
        salesContracts, resultMetaData = getSalesContracts(user, password, dominio)
        tempoExecucao = time.time() - tempoExecucao
        if salesContracts and resultMetaData is not None:
            resultMetaData['data'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            resultMetaData['executionTime'] = f'{tempoExecucao:.2f}'
            salesContracts = {'resultSetMetadata': resultMetaData, 'results': salesContracts}
            saveSalesContracts(salesContracts)
        else:
            raise Exception('Erro ao buscar contratos de vendas')
    except Exception as err:
        print("Error on 'main' service: ", err, flush=True)
        return None
    
main()