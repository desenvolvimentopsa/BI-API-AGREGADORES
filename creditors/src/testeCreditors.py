import time, json, sys

import pprint

from Exchange import *

serviceid  = '01234567890123456789012345678901'
servicekey = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ123456'
host       = 'vini'
version    = '1.0 08/08/2024 Vini - Exemplo de uso empresas'
service    = 'agregador_creditors'
system     = 'sg_biapi'

amqps= "amqps://sg_biapi:lvxJUm3GCHjhUJAJb6fdSxVUfcqzN@tall-cyan-dog.rmq4.cloudamqp.com/sg_biapi_host"
try:
    exchange = QueueExchange(
        amqps    = amqps,
        host     = host,
        system   = system, 
        service  = service, 
        version  = version)
except Exception as ex:
    print("[!] Exchange offline", ex)
    sys.exit(0)

payload = {
    "companyId": 1,
    "client": "bambui",
    "api": "companies",
    "url": "bambui.sienge.com.br",
    "user": "bambui-psad",
    "pass": "STPOi4bCpz0UuZfaCNB7dEjQQKjqrbeM"
}

exchange.setCrypto(serviceid, servicekey)
exchange.setPayload(json.dumps(payload))
exchange.sendMsg('ESTOU ENVIANDO UMA MSG SECRETA', service)
