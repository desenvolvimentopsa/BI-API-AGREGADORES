import json, sys
import uuid

from Logger import LogService
from messageModel import MessageModel

# ENVIA CONFIGURAÇÃO PARA OS SISTEMAS DISTRIBUIDOS.
# OS SISTEMAS QUE TIVEREM O SERVIÇO KEYSETTER/REDISCONFIG IMPLEMENTADO
# RECEBERA AS CONFIGURAÇÕES ENVIADAS POR ESTA INTERFACE.

versao = "v1.0 20/01/2024 MJ. Envia / Recebe Configuração REDIS utilizando serviço Keysetter remoto"
versao = "v1.1 09/02/2024 MJ. Servico Logger vem instanciado na inicialização"

host  = 'redis'
# CONFIGURACAO DO EXCHANGE
# EXCHANGE TIPO DIRECT
# SET -> grava configuracao / chave / valor
# SETJS -> grava configuracao dentro json em chave / valor
# DEL -> apaga configuracao
class SendConfig:
    def __init__(self, systemTarget: str, systemSource: str, logService: LogService):
        # logger e msg base
        try:
            self.isClientSet= False
            self.baseMsg    = None
            self.target     = systemTarget
            self.session    = str(uuid.uuid4())
            self.service    = "keysetter"
            self.source     = systemSource
            self.logger     = logService
            print(f"SEND CONFIG: {self.target} {self.session} {self.service} {self.source}")
        except Exception as ex:
            print(ex)
            sys.exit(10)
        
    def setClient(self, client:str):
        if len(client) < 3 and client != '*':
            self.logger.logMsgError(f"INVALID CLIENT CONFIGURATION: {client}")
            return
        try:
            self.baseMsg = MessageModel(type='SET',
                                        system=self.target, 
                                        client=client, 
                                        session=self.session, source=self.source)
            self.isClientSet  = True
            print(f"SET CLIENT: {client}")
        except Exception as ex:
            print("deu ruin")
            self.logger.logMsgError(ex)
            sys.exit(10)
  
    # SEND A CONFIGURATION MESSAGE TO THE QUEUE
    def setCnf(self, chave:str, valor:str):
        print(f"SET CONFIGURATION: {chave} - {valor}")
        if not self.isClientSet:
            self.logger.logMsgError(f"CLIENT IS NOT SET")
            return
        
        if len(chave) <= 3 or len(valor) == 0:
            self.logger.logMsgError(f"INVALID CONFIGURATION: {chave} - {valor}")
            return
        
        self.baseMsg.type  = 'SET'
        self.baseMsg.key   = chave
        self.baseMsg.value = valor
        self.logger.exchange.setLogType('setup')
        self.logger.logMsg(self.baseMsg.model_dump_json())
        self.logger.exchange.clrLogType()
        print(f"SENT CONFIGURATION: {chave} - {valor}")
        
    def setCnfJS(self, key:str, keyjs:str, value:str):
        print(f"SET JS CONFIGURATION: {key} - {keyjs}: {value}")
        if not self.isClientSet:
            self.logger.logMsgError(f"CLIENT IS NOT SET")
            return
        
        if len(key) <= 3 or len(value) == 0:
            self.logger.logMsgError(f"INVALID CONFIGURATION: {key} - {keyjs}: {value}")
            return
        if len(keyjs) <= 3:
            self.logger.logMsgError(f"INVALID CONFIGURATION: {key} - {keyjs}: {value}")
            return
 
        self.baseMsg.type  = 'SETJS'
        self.baseMsg.key   = key
        self.baseMsg.value = json.dumps({keyjs: value})
        self.logger.exchange.setLogType('setup')
        self.logger.logMsg(self.baseMsg.model_dump_json())
        self.logger.exchange.clrLogType()
        print(f"SENT CONFIGURATION: {key} - {value}")
            
    def delCnf(self, key:str):
        if not self.isClientSet:
            self.logger.logMsgError(f"CLIENT IS NOT SET")
            return
        
        if len(key) <= 3:
            self.logger.logMsgError(f"INVALID CONFIGURATION: {key}")
            return
        
        self.baseMsg.type  = 'DEL'
        self.baseMsg.key   = key
        self.logger.exchange.setLogType('setup')
        self.logger.logMsg(self.baseMsg.model_dump_json())
        self.logger.exchange.clrLogType()
        