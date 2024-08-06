import json, time, sys
import pika

versao = "v1.0 14/12/2023 - Baltazar - Prefetch count"
versao = "v1.1 06/02/2024 - MJ. - Detecta Serviço rabbitmq ausente no inicio"
versao = "v1.2 20/02/2024 - MJ. - Possibilita uma taksqueue por serviço"
versao = "v1.3 06/03/2024 - MJ. - Verboso apenas nos erros"
versao = "v1.5 07/03/2024 - MJ. - External Rabbitmq Connection"
versao = "v1.6 26/06/2024 - MJ. - Conversao dos parametros host, service e client para lowercase"

# CONFIGURACAO DO EXCHANGE
# EXCHANGE TIPO DIRECT
# Nesta versao, exchange nao mantem conexao com rabbitmq para envio de msg
class QueueExchange:
    def __init__(self, host:str, client:str, service:str, rabbit_callback: object = None):
        host             = host.lower()
        client           = client.lower()
        service          = service.lower()
        self.host        = host
        self.client      = client
        self.service     = f"{service}_service_{client}"
        self.credentials = pika.PlainCredentials(username='usramqp', password='Qc6ZxJSBrhRVsrY')
        self.callback    = rabbit_callback
        if rabbit_callback is not None:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host= self.host, credentials= self.credentials ))
            self.channelIn  = self.connection.channel() # start main channel
            self.channelIn.queue_declare(queue= self.service)
            self.channelIn.basic_consume(queue= self.service, 
                                        auto_ack= True,
                                        on_message_callback= self.callback)
            self.channelIn.basic_qos(prefetch_count=1)
            print(f"CONSUMINDO DA FILA {self.service}", flush=True)
        else: pika.BlockingConnection(pika.ConnectionParameters(host= self.host, credentials= self.credentials ))
        
    def sendMsg(self, message: dict, rkey: str = 'queue_controller'):
        trials      = 5
        waitTime    = 60
        messageSent = False
        while not messageSent and trials:
            try:
                messagejs = json.dumps(message)
                connection = pika.BlockingConnection(pika.ConnectionParameters(host= self.host, credentials= self.credentials ))
                channelOut = connection.channel() # start a channel
                channelOut.queue_declare(queue = rkey)
                channelOut.basic_publish(
                    exchange   = '', 
                    routing_key= rkey, 
                    body       = messagejs, 
                    properties = pika.BasicProperties(
                                    content_type = 'text/plain',
                                    delivery_mode= pika.DeliveryMode.Transient))
                messageSent = True
                connection.close()
            except Exception as ex:
                print(f"Exchange: TROUBLE CNX RABBITMQ REMAIN ATTEMPT: {trials}")
                print(ex, flush=True)
                sys.stdout.flush()
                time.sleep(waitTime)
                trials -= 1

    def start_consuming(self):
        if self.callback is not None:
            self.channelIn.start_consuming()


class TaskExchange:
    def __init__(self, host:str, client:str, service: str = 'service', callback: object = None):
        self.client      = client
        self.host        = host
        self.service     = f'{service}_task_queue_{client}'
        self.credentials = pika.PlainCredentials(username='usramqp', password='Qc6ZxJSBrhRVsrY')
        self.callback    = callback
        if callback is not None:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host= self.host, credentials= self.credentials ))
            self.channelIn  = self.connection.channel() # start main channel
            self.channelIn.queue_declare(queue= self.service, durable=True)
            self.channelIn.basic_consume(queue= self.service, 
                                        auto_ack= False,
                                        on_message_callback= self.callback)
            self.channelIn.basic_qos(prefetch_count=1)
        else: pika.BlockingConnection(pika.ConnectionParameters(host= self.host, credentials= self.credentials ))
 
    def sendTask(self, message: dict):
        queue       = self.service
        trials      = 5
        waitTime    = 60
        messageSent = False
        
        while not messageSent and trials:
            try:
       
                messagejs = json.dumps(message)
                connection = pika.BlockingConnection(pika.ConnectionParameters(host= self.host, credentials= self.credentials ))
                channelOut = connection.channel() # start a channel
                channelOut.queue_declare(queue = queue, durable= True)
                channelOut.basic_publish(
                    exchange   = '', 
                    routing_key= queue, 
                    body       = messagejs, 
                    properties = pika.BasicProperties(
                                    content_type = 'text/plain',
                                    delivery_mode= pika.DeliveryMode.Persistent))
                messageSent = True
                connection.close()
            except Exception as ex:
                print(f"Exchange: TROUBLE CNX RABBITMQ REMAIN ATTEMPT: {trials}",  flush=True)
                print(ex, flush=True)
                time.sleep(waitTime)
                trials -= 1

    def start_consuming(self):
        if self.callback is not None:
            self.channelIn.start_consuming()
