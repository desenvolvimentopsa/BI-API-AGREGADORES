import json, time, sys, inspect, os
import hmac, hashlib

import pika
from datetime import datetime
from pytz import timezone
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend

versao = "v1.0 14/12/2023 - Baltazar - Prefetch count"
versao = "v1.1 06/02/2024 - MJ. - Detecta Serviço rabbitmq ausente no inicio"
versao = "v1.2 20/02/2024 - MJ. - Possibilita uma taksqueue por serviço"
versao = "v1.3 06/03/2024 - MJ. - Verboso apenas nos erros"
versao = "v1.5 07/03/2024 - MJ. - External Rabbitmq Connection"
versao = "v1.6 26/06/2024 - MJ. - Conversao dos parametros host, service e client para lowercase"
versao = "v1.7 26/07/2024 - MJ. - Prepacao para utilizar Cloudamqp ou Rabbitmq local amqps/system"

# CONFIGURACAO DO EXCHANGE - Apoia no comunicação inter-serviços de um sistema
# EXCHANGE TIPO DIRECT 
# 
# user sg_etl:43uG2QOEn0SsuycQCH7a0hug7
class QueueExchange:
    def __init__(self, amqps:str, host:str, system:str, service:str, 
                     version:str, client:str = '#', callback: object = None, delivery_mode = 1):
        paramOK = len(service) and len(system) and len(amqps) and len(host) and len(version)            
        if not (paramOK):
            msg = f"\n\nVerifique os parametros:\namqps[{amqps}]\nhost[{host}]\nsystem[{system}]\nservice[{service}]\nversion[{version}]"
            raise Exception(msg)

        self.delivery  = delivery_mode
        self.payload   = ""
        self.hmac      = ""
        self.version   = ""
        self.serviceid = ""
        self.type      = "msg"
        self.host      = host.lower()
        self.client    = client.lower()
        self.service   = service.lower()
        self.system    = system.lower()
        self.client    = client
        self.params    = pika.URLParameters(amqps)
        self.callback  = callback
        self.queue     = f"{self.system}_{service}_{self.client}" if client != '#' else f"{self.system}_{service}"
        if callback is not None:
            self.connection = pika.BlockingConnection(self.params)
            self.channelIn  = self.connection.channel()          # start main channel
            self.channelIn.exchange_declare(exchange = f"{self.system}.direct", exchange_type = 'direct')
            self.channelIn.queue_declare   (queue= self.queue, durable=True)
            self.channelIn.queue_bind      (queue= self.queue, exchange= f"{self.system}.direct", routing_key= self.queue)
            self.consumerTag = self.channelIn.basic_consume(queue= self.queue, on_message_callback= self.callback)
            self.channelIn.basic_qos       (prefetch_count=1)
        else: 
            self.connection = pika.BlockingConnection(self.params) # testa conexão
            self.connection.close()                  

    def cancel(self):
        self.channelIn.basic_cancel(self.consumerTag)

    # METODOS INTERNOS _
    # MODELO MSG QUE TRAFEGA ENTRE SERVIÇOS E CONTROLLER
    """
    MSG JSON CONTROLADOR
    data:
    {
    "salesContractId": <int>,
    "client":       "bambui",
    "service":    "SALES_CONTRACT_CREATED"
    }

    message:
    { ...,
    "serviceid": <registred uuid within controller>,
    "payload":   aes-256 <data>,
    "hmac":      hmac <payload>,
    "type":      "work",
    ...
    }
    """
    def _setMsg(self, msg:str, caller:str):
        now = datetime.now(timezone('America/Sao_Paulo'))
        message =msg.strip('"')

        self.Msg = {
            "system":    self.system,
            "target":    self.target, 
            "service":   self.service,
            "serviceid": self.serviceid,
            "payload":   self.payload,
            "hmac":      self.hmac,
            "type":      self.type,
            "version":   self.version,
            "host":      self.host,
            "client":    self.client,      
            "caller":    caller,            
            "logDate":   f"{now}",          
            "msg":       f"{message}"                      
        }

    # Envia uma mensagem para um serviço associado ao sistema
    # controller é o nome padrão do serviço, caso nao seja informado
    def sendMsg(self, message: str, service:str = 'controller'):
        trials      = 5
        waitTime    = 12
        messageSent = False
        caller      = inspect.stack()[1].function
        if caller == '<module>': caller = 'main'

        self.target = service
        self._setMsg(message, caller)
        targetQueue = f"{self.system}_{self.target}_{self.client}" if self.client != '#' else f"{self.system}_{self.target}"
        while not messageSent and trials:
            try:
                connection = pika.BlockingConnection(self.params)
                channelOut = connection.channel()                  # start a channel
                channelOut.exchange_declare(exchange = f"{self.system}.direct", exchange_type = 'direct')
                channelOut.queue_declare   (queue= targetQueue, durable= True)
                channelOut.queue_bind      (queue= targetQueue, exchange= f"{self.system}.direct", routing_key= targetQueue)
                channelOut.basic_publish(
                    exchange    = f"{self.system}.direct", 
                    routing_key = targetQueue, 
                    body        = json.dumps(self.Msg), 
                    properties  = pika.BasicProperties(content_type = 'text/plain', delivery_mode= self.delivery))
                messageSent = True
                connection.close()
            except Exception as ex:
                print(f"Exchange: TROUBLE CNX RABBITMQ REMAIN ATTEMPT: {trials}")
                print(ex, flush=True)
                sys.stdout.flush()
                time.sleep(waitTime)
                trials -= 1
        if messageSent:
            print(f"MSG SENT TO [{targetQueue}]", flush=True)
            self.payload = ""
            self.hmac    = ""
        else:
            print(f"MSG NOT SENT TO [{targetQueue}]", flush=True)

    def start_consuming(self):
        if self.callback is not None:
            print(f"{self.service.upper()} QUEUE: [{self.queue}]", flush=True)
            self.channelIn.start_consuming()

    def setCrypto(self, serviceid:str, servicekey:str):
        self.serviceid  = serviceid
        self.servicekey = servicekey
        self.Crypto     = Payload(serviceid, servicekey) 

    def setPayload(self, data:str):
        try:
            self.hmac    = ""
            self.payload = ""
            self.hmac, self.payload = self.Crypto.setPayload(data)
        except Exception as ex:
            print(ex)

    def getPayload(self, hmac:str, safeData:str):
        try:
            self.hmac    = ""
            self.payload = ""
            self.hmac, self.payload = self.Crypto.getPayload(safeData)
        except Exception as ex:
            print(ex)
            return False

        return hmac == self.hmac

class Payload:
    def __init__(self, serviceid:str, servicekey:str):
        self.serviceid  = serviceid
        self.servicekey = servicekey
        
    
    def setPayload(self, data:str):
        data     = data.encode('utf-8')
        tamanho  = len(data)
        if tamanho > 65536 or tamanho <= 0:
            raise Exception("Invalid Payload size")

        hmacData= hmac.new(self.serviceid.encode('utf-8'), data, 'sha256').hexdigest()
        iv       = os.urandom(16)
        padder   = padding.PKCS7(128).padder()
        datarw   = padder.update(data) + padder.finalize()
        cipher   = Cipher(algorithms.AES(self.servicekey.encode('utf-8')), modes.CBC(iv))
        encrypt  = cipher.encryptor()
        safeData = encrypt.update(datarw) + encrypt.finalize()
        payload  = (iv + safeData)

        return (hmacData, payload.hex())

    def getPayload(self, safeData:str):
        safeData = bytes.fromhex(safeData)
        iv       = safeData[:16]
        cipher   = Cipher(algorithms.AES(self.servicekey.encode('utf-8')), modes.CBC(iv))
        decrypt  = cipher.decryptor()
        rawdata  = decrypt.update(safeData[16:]) + decrypt.finalize()
        if len(rawdata)%16 != 0:
             raise Exception("Invalid Payload size block {0}".format(len(rawdata)))

        unpadder = padding.PKCS7(128).unpadder()
        data     = unpadder.update(rawdata) + unpadder.finalize()
        hmacData = hmac.new(self.serviceid.encode('utf-8'), data, 'sha256').hexdigest()
   
        return (hmacData, data.decode('utf-8'))

# PERMITE CRIAR UM DISTRIBUIDOR DE TAREFAS
# O DISTRIBUIDOR DEVE RECEBER UMA FUNCAO DE CALLBACK
class TaskExchange:
    def __init__(self, amqps:str, host:str, system:str, 
                 service:str, version:str, client:str = '#', 
                 callback: object = None, delivery_mode:int = 2):
        paramOK = len(service) and len(system) and len(amqps) and len(host) and len(version)            
        if not (paramOK):
            msg = f"\n\nVerifique os parametros:\namqps[{amqps}]\nhost[{host}]\nsystem[{system}]\nservice[{service}]\nversion[{version}]"
            raise Exception(msg)

        self.delivery  = delivery_mode
        self.payload   = ""
        self.hmac      = ""
        self.version   = ""
        self.serviceid = ""
        self.type      = "task"
        self.host      = host.lower()
        self.service   = service.lower()
        self.system    = system.lower()
        self.client    = client.lower()
        self.params    = pika.URLParameters(amqps)
        self.callback  = callback
        self.queue     = f'{self.system}_{self.service}_tasks_{client}' if client != '#' else f'{self.system}_{self.service}_tasks'
        if callback is not None:
            self.connection = pika.BlockingConnection(self.params)
            self.channelIn  = self.connection.channel() # start main channel
            self.channelIn.exchange_declare (exchange   = f"{self.system}.direct", exchange_type = 'direct')
            self.channelIn.queue_declare    (queue      = self.queue,  durable       = True)
            self.channelIn.queue_bind       (queue      = self.queue,  exchange      = f"{self.system}.direct", routing_key= self.queue)
            self.consumerTag = self.channelIn.basic_consume (queue = self.queue, 
                                                             auto_ack = False,
                                                             on_message_callback = self.callback)
            self.channelIn.basic_qos(prefetch_count=1)
        else: 
            conexao = pika.BlockingConnection(self.params) #testa conexao
            conexao.close()
    def _setTask(self, msg:str, caller:str):
        now = datetime.now(timezone('America/Sao_Paulo'))
        message =msg.strip('"')

        self.Msg = {
            "system":    self.system,
            "target":    self.target, 
            "service":   self.service,
            "serviceid": self.serviceid,
            "payload":   self.payload,
            "hmac":      self.hmac,
            "type":      self.type,
            "version":   self.version,
            "host":      self.host,
            "client":    self.client,      
            "caller":    caller,            
            "logDate":   f"{now}",          
            "msg":       f"{message}"                      
        }

    def cancel(self):
        self.channelIn.basic_cancel(self.consumerTag)

    def sendTask(self, message:str):
        trials      = 5
        waitTime    = 12
        messageSent = False
        caller      = 'TaskExchange'
        self.target = 'workers'
        self._setTask(message, caller)

        while not messageSent and trials:
            try:
                connection = pika.BlockingConnection(self.params)
                channelOut = connection.channel()                      # start a channel
                channelOut.exchange_declare(exchange   = f"{self.system}.direct", exchange_type = 'direct')
                channelOut.queue_declare   (queue      = self.queue,  durable  = True)
                channelOut.queue_bind      (queue      = self.queue,  exchange = f"{self.system}.direct", routing_key= self.queue)

                channelOut.basic_publish   (exchange    = f"{self.system}.direct", 
                                            routing_key = self.queue, 
                                            body        = json.dumps(self.Msg), 
                                            properties  = pika.BasicProperties(content_type = 'text/plain', 
                                                                               delivery_mode= self.delivery))
                messageSent = True
                connection.close()
            except Exception as ex:
                print(f"TaskExchange: TROUBLE CNX RABBITMQ REMAIN ATTEMPT: {trials}")
                print(ex, flush=True)
                time.sleep(waitTime)
                trials -= 1
        if not messageSent:
            print(f"TASK SENT TO: {self.queue}", flush=True)

    def start_consuming(self):
        if self.callback is not None:
            print(f"{self.service.upper()} QUEUE: [{self.queue}]", flush=True)
            self.channelIn.start_consuming()

    def setCrypto(self, serviceid:str, servicekey:str):
        self.serviceid  = serviceid
        self.servicekey = servicekey
        self.Crypto     = Payload(serviceid, servicekey) 

    def setPayload(self, data:str):
        try:
            self.hmac    = ""
            self.payload = ""
            self.hmac, self.payload = self.Crypto.setPayload(data)
        except Exception as ex:
            print(ex)

    def getPayload(self, hmac:str, safeData:str):
        try:
            self.hmac    = ""
            self.payload = ""
            self.hmac, self.payload = self.Crypto.getPayload(safeData)
        except Exception as ex:
            print(ex)
            return False

        return hmac == self.hmac

# RPC - QUANDO FOR UTILIZAR COMO ENDPOINT
# FORNECER FUNCAO DE TRABALHO NA INICIALIZACAO
class RPCExchange:
    def __init__(self, amqps:str, host:str, system:str, 
                 service:str, version:str, client:str = '#', workfunction=None, delivery_mode=2):
        paramOK = len(service) and len(system) and len(amqps) and len(host) and len(version)            
        if not (paramOK):
            msg = f"\n\nVerifique os parametros:\namqps[{amqps}]\nhost[{host}]\nsystem[{system}]\nservice[{service}]\nversion[{version}]"
            raise Exception(msg)

        self.callback  = workfunction
        self.delivery  = delivery_mode
        self.payload   = ""
        self.hmac      = ""
        self.version   = ""
        self.serviceid = ""
        self.type      = "rpc"
        self.host      = host.lower()
        self.service   = service.lower()
        self.system    = system.lower()
        self.client    = client.lower()
        self.params    = pika.URLParameters(amqps)

        if (callback is not None):
            self.connection = pika.BlockingConnection(self.params)
            self.channelIn  = self.connection.channel() # start main channel
            self.channelIn.exchange_declare (exchange = f"{self.system}.direct", exchange_type = 'direct')
            self.queue      = f'{self.system}_{self.service}_rpc_{client}' if client != '#' else f'{self.system}_{self.service}_rpc'
            self.channelIn.queue_declare(queue          = self.queue) # not durable to permit more than one server to the server 
            self.channelIn.queue_bind   (exchange       = f"{self.system}.direct", queue = self.queue, routing_key = self.queue)
            self.channelIn.basic_qos    (prefetch_count = 1)
            self.channelIn.basic_consume(queue               = self.queue, 
                                         auto_ack            = False,
                                         on_message_callback = self.RPCResp)
        else: 
            conexao = pika.BlockingConnection(self.params) #testa conexao
            conexao.close()
  
    def RPCResp(self, ch, method, properties, body):
        print("Chegou Trabalho")
        resposta = "{}"
        workOK   = False 
        msg      = body.decode("utf-8")
        try:
            if self.callback is not None:
                resposta = self.callback(msg)
            workOK = True
        except Exception as ex:
            print(ex)
            resposta = f"ERRO: {ex}"

        if workOK and 'replay_to' in properties.headers:
            print(f"Resposta: {resposta}", flush=True)
            self._setJSON(resposta, 'RPCResp')

            ch.basic_publish(exchange    = f"{self.system}.direct", 
                             routing_key = properties.reply_to, 
                             body        = json.dumps(resposta), 
                             properties  = pika.BasicProperties(content_type  = 'application/json', 
                                                                delivery_mode = self.delivery))
        else:
            print(f"Resposta: {resposta}", properties.headers, flush=True)

        ch.basic_ack(delivery_tag = method.delivery_tag)

    def _setJSON(self, msg:str, caller:str):
        now = datetime.now(timezone('America/Sao_Paulo'))
        message =msg.strip('"')

        self.Msg = {
            "system":    self.system,
            "target":    self.target, 
            "service":   self.service,
            "serviceid": self.serviceid,
            "payload":   self.payload,
            "hmac":      self.hmac,
            "type":      self.type,
            "version":   self.version,
            "host":      self.host,
            "client":    self.client,      
            "caller":    caller,            
            "logDate":   f"{now}",          
            "msg":       f"{message}"                      
        }

    def cancel(self):
        self.channelIn.basic_cancel(self.consumerTag)

    def callRPC(self, message:str):
        trials      = 5
        waitTime    = 12
        messageSent = False
        caller      = 'RPCExchange'
        self.target = 'rpc'
        self._setJSON(message, caller)
        self.correlation_id = str(uuid.uuid4())

        while not messageSent and trials:
            try:
                connection = pika.BlockingConnection(self.params)
                channelOut = connection.channel()                      # start a channel
                channelOut.exchange_declare(exchange   = f"{self.system}.direct", exchange_type = 'direct')
                channelOut.queue_declare   (queue      = self.queue,  durable  = True)
                channelOut.queue_bind      (queue      = self.queue,  exchange = f"{self.system}.direct", routing_key= self.queue)

                channelOut.basic_publish   (exchange    = f"{self.system}.direct", 
                                            routing_key = self.queue, 
                                            body        = json.dumps(self.Msg), 
                                            properties  = pika.BasicProperties(content_type  = 'application/json',
                                                                               delivery_mode = self.delivery,
                                                                               reply_to      = self.response,
                                                                               correlation_id= self.correlation_id))
                messageSent = True
                connection.close()
            except Exception as ex:
                print(f"TaskExchange: TROUBLE CNX RABBITMQ REMAIN ATTEMPT: {trials}")
                print(ex, flush=True)
                time.sleep(waitTime)
                trials -= 1
        if not messageSent:
            print(f"TASK SENT TO: {self.queue}", flush=True)

    def start_consuming(self):
        if self.callback is not None:
            print(f"{self.service.upper()} QUEUE: [{self.queue}]", flush=True)
            self.channelIn.start_consuming()

    def setCrypto(self, serviceid:str, servicekey:str):
        self.serviceid  = serviceid
        self.servicekey = servicekey
        self.Crypto     = Payload(serviceid, servicekey) 

    def setPayload(self, data:str):
        try:
            self.hmac    = ""
            self.payload = ""
            self.hmac, self.payload = self.Crypto.setPayload(data)
        except Exception as ex:
            print(ex)

    def getPayload(self, hmac:str, safeData:str):
        try:
            self.hmac    = ""
            self.payload = ""
            self.hmac, self.payload = self.Crypto.getPayload(safeData)
        except Exception as ex:
            print(ex)
            return False

        return hmac == self.hmac
