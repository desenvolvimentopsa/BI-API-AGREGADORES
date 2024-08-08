import time, json, sys

from Exchange import *

def callback(ch, method, properties, body):
    print("Chegou uma tarefa")
    print(body)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    time.sleep(5)
    exchange.cancel()

amqps= 'amqps://sg_etl:43uG2QOEn0SsuycQCH7a0hug7@tall-cyan-dog.rmq4.cloudamqp.com/sg_etl_host' 
serviceid  = '01234567890123456789012345678901'
servicekey = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ123456'
host       = 'giannis'
version    = '1.0 31/07/2024 MJ - Exemplo de uso cliente'
service    = 'scripter'
system     = 'sg_etl'
client     = 'softgold'
try:
    exchange = QueueExchange(
        amqps   = amqps,
        host    = host,
        system  = system, 
        service = service,
        version = version, 
        client  ='myclient',
        callback= callback)
except Exception as ex:
    print("[!] rabbitmq service offline", amqps, ex)
    sys.exit(0)

payload = exchange.setCrypto(serviceid, servicekey)

dados = {
"salesContractId": 677838,
"client":       "bambui",
"service":    "SALES_CONTRACT_CREATED"
}

Payload = json.dumps(dados)

exchange.setPayload(Payload)
print(exchange.hmac)
print(exchange.payload)

msg = 'ESTOU ENVIANDO UMA MSG SECRETA'
exchange.sendMsg(msg)

exchange.start_consuming()

print("CONSUMI UMA MENSGEM E JA ESTOU DE VOLTA")