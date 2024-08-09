import redis
import json
import time
import sys
import datetime

from Agregador import *
from Exchange import *

# VARIÁVEIS GLOBAIS
dataInicio = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
incremento = 0
result = {}
api = 'sales-contracts'
amqps= "amqps://sg_biapi:lvxJUm3GCHjhUJAJb6fdSxVUfcqzN@tall-cyan-dog.rmq4.cloudamqp.com/sg_biapi_host"

# CONFIGURA O REDIS
# DADOS CONFIGURAÇÃO REDIS
rd = None
try:
    rd = redis.Redis("localhost", 6445)
except Exception as e:
    print("SERVICE REDIS DOWN: ", e, flush=True)
    time.sleep(10)
    sys.exit(1)

# Exemplo de payload:
# data:
# {
# "salesContractId": int,
# "client": "bambui",
# "service": "SALES_CONTRACT_CREATED",
# "api": "sales-contracts",
# "url": "bambui.sienge.com.br",
# "user": "psa",
# "pass": "secret"
# }

def callback(ch, method, properties, body):
    try:
        global incremento, result, api, dataInicio
        # Independente, libera a mensagem da fila
        ch.basic_ack(delivery_tag = method.delivery_tag)
        
        # Incrementa o contador de acessos, quantas vezes esse agregador rodou.
        incremento = incremento + 1
        
        # JSON de resultado da execução
        result = {
            "dataInicio": dataInicio,
            "dataUltimoAcesso": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "api": api,
            "acessos": incremento,
            "status": "DONE",
        }
        
        # Fazer o tratamento do body
        msg = body.decode("utf-8")
        message = json.loads(msg)
        payload = message.get('payload')
        hmac = message.get('hmac')
        
        # Verifica se o payload é válido
        if exchange.getPayload(hmac, payload):
            print(f"[x] Payload recebido [{payload}]\n", flush=True)
            print(exchange.payload, flush=True)
            msg = json.loads(exchange.payload)
        
            if api != msg['api'].lower():
                print(f"API {api} não é a mesma da mensagem recebida", flush=True)
                raise Exception(f"API {api} não é a mesma da mensagem recebida")
        
        # Pegando a primeira posição do split da URL, que será o domínio
        # bambui.sienge.com.br -> bambui
        dominio = msg['url'].split(".")[0]
        
        # URL da API
        url = f"https://api.sienge.com.br/{dominio}/public/api/v1/{api}?"
        
        # Instanciando a classe do Agregador
        extraction = Agregador(rd, api, url, msg['user'], msg['pass'], dominio)
        
        status, retorno = extraction.getData()
        
        # Verifica se o status é True
        if status:
            retorno['payload'] = msg
            payloadRetorno = json.dumps(retorno)
            # Momento em que eu criptgrafo o retorno e ele fica em exchange.payload
            # Porém ele ainda não foi enviado, apenas criptografado e aguardando um sendMsg.
            exchange.setPayload(payloadRetorno)
            # Enviando a mensagem, além de criptografada, envia o 'result' que contém informações para métricas.
            print(f'Enviando a mensagem: {json.dumps(result)}', flush=True)
            exchange.sendMsg(json.dumps(result))
            
        else:
            # Se o status for False, retorna o JSON que a função getData retornou
            raise Exception(retorno)
        
    except Exception as err:
        # Se ocorrer algum erro, retorna o erro
        result['body'] = body.decode("utf-8")
        result['status'] = "ERROR"
        result['error'] = str(err)
        print(f"Erro ao extrair: {result}", flush=True)
        exchange.sendMsg(json.dumps(result))
        
# INICIA O CONSUMO DA FILA + INSTANCIAMENTO DO EXCHANGE
serviceid  = '01234567890123456789012345678901'
servicekey = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ123456'
host       = 'vini'
version    = '1.0 08/08/2024 Vini - Exemplo de uso empreendimentos.'
service    = 'agregador_sales-contracts'
system     = 'sg_biapi'
try:
    exchange = QueueExchange(
        amqps   = amqps,
        host    = host,
        system  = system, 
        service = service,
        version = version,
        callback= callback)
except Exception as ex:
    print("[!] rabbitmq service offline", amqps, ex, flush=True)
    sys.exit(0)
    
exchange.setCrypto(serviceid, servicekey)

exchange.start_consuming()