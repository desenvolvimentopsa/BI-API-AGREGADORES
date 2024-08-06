import json, sys, uuid

import pika, redis

from Logger import LogService
from messageModel import MessageModel

# AGUARDA CONFIGURAÇÕES DA GESTAO ENVIADOS PELA CALSE SENDCONFIG
# OUVE NA FILA CLIENTE.SISTEMA.KEYSETTER E FILTRA AS MENSAGENS NA KEY SETUP.SISTEMA.KEYSETTER

versao = "v1.0 20/01/2024 MJ. Envia / Recebe / Apaga Configuração REDIS"
versao = "v1.1 21/01/2024 MJ. SETJS bug chave inexistente"
versao = "v1.2 22/01/2024 MJ. Supressao de erro quando mensagem de outro cliente e menos verboso"
versao = "v1.3 23/01/2024 MJ. Nova Logger e system_config"
versao = "v1.4 09/02/2024 MJ. Servico Logger vem instanciado na inicialização"
versao = "v1.5 11/02/2024 MJ. URL amqps retirada de Logger"
versao = "v1.6 19/02/2024 MJ. Nome do cliente pode ter ao menos 2 caracteres"

host  = 'redis'
# CONFIGURACAO DO EXCHANGE
# EXCHANGE TIPO DIRECT
# SET   -> grava configuracao
# SETJS -> grava configuracao em json
# DEL   -> apaga configuracao
class RedisConfig:
    def callBack(self, ch, method, properties, body):
        bodyMsg = json.loads(body)['msg']
        print(f"RECEIVED: {bodyMsg}", flush=True)
        # CHECK IF MESSAGE EXTRUCTURE IS VALID
        messageIsValid = (True, 'Inicio')
        message = MessageModel.model_validate_json(bodyMsg)
        
        #if not any(tipo in message.type for tipo in ['SET','GET','DEL','SETJS']): messageIsValid = (False, 'Tipo Invalido')
        
        # CHECK MESSAGE CONTENT IS VALID
        if message.key is None or (len(message.key) < 3 and message.key !=  "*"): messageIsValid = (False, 'Chave Invalida')
        if message.client is None or (len(message.client) < 3 and message.client != "*"): messageIsValid = (False, 'Client Invalido')
        if message.type == 'SET' and len(message.value) == 0: messageIsValid = (False, 'Tipo SET: Valor Invalido')
        
        if not messageIsValid[0]:
            print(f"INVALID MESSAGE: {bodyMsg} ERRO: {messageIsValid[1]}", flush=True)
            self.logger.logMsgError(f"INVALID MESSAGE: {bodyMsg} ERRO: {messageIsValid[1]}")
            return

        
        if message.client != '*' and message.client.upper() != self.baseMsg.client.upper():
            print(f"INVALID CLIENT: {message.client} - {self.baseMsg.client}", flush=True)
            return
        
        self.logger.logMsg(f"MESSAGE RECEIVED: {message.value}")
        
        if message.type == 'SET':
            self.logger.logMsg(f"SET: {message.value}")
            r = redis.Redis(host=host, port=6379)
            r.set(message.key, message.value)
        # SETJS
        elif message.type == 'SETJS':
            self.logger.logMsg(f"SETJS: {message.value}")
            msg = json.loads(message.value)
            if len(list(msg.keys())) != 1:
                self.logger.logMsgError(f"INVALID JS CONFIGURATION: {message.value}")
                return
            keyjs = list(msg.keys())[0]
            value = msg[keyjs]
            key   = message.key
            r = redis.Redis(host=host, port=6379)
            
            if r.exists(key):
                dados = r.get(key).decode('utf-8')
                dados = json.loads(dados)
                dados[keyjs] = value
            else:
                dados = { keyjs: value }
            r.set(key, json.dumps(dados, indent = 4))
        # DEL
        elif message.type == 'DEL':
            self.logger.logMsg(f"DEL: {message.key}")
            # REMOVE CHAVE
            r = redis.Redis(host=host, port=6379)
            r.delete(message.key)
        else:
            self.logger.logMsgError(f"INVALID MESSAGE: {bodyMsg} ERRO: {messageIsValid[1]}")
           
            
    def __init__(self, system: str, client: str, logService: LogService):
        # EXCHANGE TOPICO SOFTGO
        # QUEUE EXCLUSIVA DE CADA CLIENTE
        # ROUTE KEY ENVIA A TODOS OS CLIENTE COM FILTRAGEM INTERNA - MODO PROMISCUOS
        self.logger = logService
        try:
            if len(client) < 2 and client != '*':
                raise Exception(f"INVALID CLIENT CONFIGURATION: {client}")
            self.baseMsg    = MessageModel(system=system, client=client, session=str(uuid.uuid4()))
            self.service    = "keysetter"
            queue           = f"{client}.{system}.keysetter"      
            routeKey        = f"setup.keysetter.{system}" # logs.hash.sg_etl.crescer, status.hash.sg_etl
            params          = pika.URLParameters(logService.amqps)
            params.socket_timeout = 5 
            self.connection = pika.BlockingConnection(params)
            self.channel    = self.connection.channel() # start main channel
            self.channel.exchange_declare(exchange='topic_softgo', exchange_type= 'topic', auto_delete= True)
            self.channel.queue_declare(queue= queue, auto_delete= True, exclusive= True)
            self.channel.queue_bind(exchange= 'topic_softgo', queue= queue, routing_key= routeKey)
            self.channel.basic_consume(queue= queue, auto_ack= True, on_message_callback= self.callBack)
            print(f"RedisConfig: {versao}")
        except Exception as ex:
            self.logger.logMsgError(ex)
            sys.exit(10)
