import time, json, sys

from Exchange import *

amqps= 'amqps://sg_etl:43uG2QOEn0SsuycQCH7a0hug7@tall-cyan-dog.rmq4.cloudamqp.com/sg_etl_host' 
serviceid  = '01234567890123456789012345678901'
servicekey = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ123456'
host       = 'giannis'
version    = '1.0 31/07/2024 MJ - Exemplo de uso cliente'
service    = 'gateway'          # DEVE SER O MESMO NOME DO WORKER
system     = 'sg_etl'
client     = 'softgold'

print("SEND TASKS", flush=True)
try:
    exchange = TaskExchange(
        amqps   = amqps,
        host    = host,
        system  = system, 
        service = service,
        version = version, 
        client  = client)
except Exception as ex:
    print("\n[!] Task Exchange offline", ex)
    sys.exit(0)

print("CONNECTED", flush=True)
payload = exchange.setCrypto(serviceid, servicekey)

contract = 138
while contract:
    contract -= 1
    dados = {
    "salesContractId": contract,
    "client":       "bambui",
    "service":    "SALES_CONTRACT_CREATED"
    }

    Payload = json.dumps(dados)
    print("Payload: ", Payload)
    exchange.setPayload(Payload)
    print(exchange.hmac)
    print(exchange.payload)
    try:
        exchange.sendTask(Payload)
    except Exception as ex:
        print(ex, flush=True)
    

