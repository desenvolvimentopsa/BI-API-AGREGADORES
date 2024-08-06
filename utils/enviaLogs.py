from Logger import *
import sys
version = "v1.0 21/06/2024 MJ. Teste de envio de logs"
service = "Logger"
amqps ="amqps://rnraluwi:v6pRbVu6psBABqn7pg81a2nnuChc833d@tall-cyan-dog.rmq4.cloudamqp.com/rnraluwi"
logger = LogService(amqps   = amqps, 
                    system  = "SG_ETL_Logger", 
                    service = service,
                    version = version,
                    origin  = "office",
                    client  = "psa",
                    logType ='logs', 
                    logLocal=False)

logger.logMsg("RouteKey: ", logger.exchange.routeKey)
logger.logMsg("Mensagem sem escopo de função")


def minhaFuncao():
    logger.logMsg("Logando dentro de minhaFuncao")
    logger.logMsgError("Essa mensagem vai para a messageria e para um arquivo em disco")
    outraFuncaoDOGILDENOR()

def outraFuncaoDOGILDENOR():
    logger.logMsg("Logando dentro de outraFuncao")
    logger.logMsgError("Mensagem de erro de OUTRAFUNCAO")
  

if __name__ == "__main__":
    logger.logMsg("INICIO DO SISTEMA")
    minhaFuncao()
    mytuple = (487, "foo", "bar", True)
    for i in range(10):
        logger.logMsg("Contando ", i , "vezes", mytuple)
    logger.logMsg("FIM DO SISTEMA")
  