import redis
import json
import time
import sys

from Agregador import *

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
    
def main():
    try:
        salesContract = Agregador(rd, 'sales-contracts', env['sienge_user'], env['sienge_pwd'], env['dominio'])
        dados = salesContract.getData()
        if dados:
            print('Contratos de vendas salvos com sucesso!', flush=True)
        else:
            raise Exception('Erro ao salvar contratos de vendas!')
        
    except Exception as err:
        print("Error on 'main' service: ", err, flush=True)
        return None
    
main()