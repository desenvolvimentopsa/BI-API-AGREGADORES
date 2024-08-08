import time, json, sys

import pprint

from Exchange import *

version    = '1.0 31/07/2024 MJ - Exemplo de uso controller'
service    = 'controller'
system     = 'sg_etl'
client     = 'softgold'
host       = 'giannis'

def callback(ch, method, properties, body):
    print("Chegou uma tarefa")
    serviceid  = '01234567890123456789012345678901'
    servicekey = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ123456'

    msg = body.decode("utf-8")
    message = json.loads(msg)

    hmac = message.get('hmac')
    payload = message.get('payload')
    print(f"Payload recebido [{payload}]")
    exchange.setCrypto(serviceid, servicekey)
    if exchange.getPayload(hmac, payload):
        pprint.pprint(message, width=10)
        print(exchange.payload)
    else:
        print("INFORMAÇÃO INVALIDA")

    ch.basic_ack(delivery_tag = method.delivery_tag)

    time.sleep(1)

amqps= 'amqps://sg_etl:43uG2QOEn0SsuycQCH7a0hug7@tall-cyan-dog.rmq4.cloudamqp.com/sg_etl_host' 
try:
    exchange = QueueExchange(
        amqps    = amqps,
        host     = host,
        system   = system, 
        service  = service, 
        version  = version,
        client   = 'myclient',
        callback = callback)
except Exception as ex:
    print("[!] Exchange offline", ex)
    sys.exit(0)

exchange.start_consuming()
