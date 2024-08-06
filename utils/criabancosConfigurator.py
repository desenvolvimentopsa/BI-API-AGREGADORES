import json, sys, uuid

import pika, redis

from Logger import LogService
from messageModel import MessageModel
from Exchange import QueueExchange
from model import SystemConfig
import redis
# AGUARDA CONFIGURAÇÕES DA GESTAO ENVIADOS PELA CALSE SENDCONFIG
# OUVE NA FILA CLIENTE.SISTEMA.KEYSETTER E FILTRA AS MENSAGENS NA KEY SETUP.SISTEMA.KEYSETTER

versao = "v1.0 06/06/2024 Tiago Freitas Recebe serviços para CRIAR / ALTERAR TABELAS E CRIAR BANCOS"

host  = 'redis'
client = 'sg_gestaobancos'
service = 'sg_receptor'

class criabancosConfig:
    def callBack(self, ch, method, properties, body):
        r = redis.Redis(host=host, port=6379)
        env = SystemConfig.model_validate(
        json.loads(r.get("system_config"))
        )
        # CHECK IF MESSAGE EXTRUCTURE IS VALID
        self.logger.logMsg("SERVICE SG_SIENGE RUNNING")
        self.logger.heartBeat()
        dado = body.decode("utf-8")
        self.logger.logMsg(f" [x] Received {dado}")
        servico = json.loads(dado)
        exchange = QueueExchange(host= env.rabbitmq, client=client,service="sg_receptor")
        
        print(servico,flush=True)
        print(f"sg_{servico['service']}_service_gestaobancos")
        exchange.sendMsg(servico, rkey=f"sg_{servico['service']}_service_sg_gestaobancos")
            
    def __init__(self, system: str, client: str, logService: LogService):
        # EXCHANGE TOPICO SOFTGO
        # QUEUE EXCLUSIVA DE CADA CLIENTE
        # ROUTE KEY ENVIA A TODOS OS CLIENTE COM FILTRAGEM INTERNA - MODO PROMISCUOS
        self.logger = logService
        try:
            if len(client) < 2 and client != '*':
                raise Exception(f"INVALID CLIENT CONFIGURATION: {client}")
            self.baseMsg    = MessageModel(system=system, client=client, session=str(uuid.uuid4()))
            self.service    = service
            queue           = f"{client}.{system}"      
            routeKey        = f"setup.gestaobancos.{system}" # logs.hash.sg_etl.crescer, status.hash.sg_etl
            params          = pika.URLParameters(logService.amqps)
            params.socket_timeout = 5 
            self.connection = pika.BlockingConnection(params)
            self.channel    = self.connection.channel() # start main channel
            self.channel.exchange_declare(exchange='topic_softgo', exchange_type= 'topic', auto_delete= True)
            self.channel.queue_declare(queue= queue, auto_delete= True, exclusive= True)
            self.channel.queue_bind(exchange= 'topic_softgo', queue= queue, routing_key= routeKey)
            self.channel.basic_consume(queue= queue, auto_ack= True, on_message_callback= self.callBack)
            print(f"criaBancosConfig: {versao}")
        except Exception as ex:
            self.logger.logMsgError(ex)
            sys.exit(10)
