import json, sys
import redis
import time
from Logger import LogService
from Exchange import QueueExchange

versao = "v1.00 21/02/2024 Thiago. Versao inicial"
versao = "v1.01 23/02/2024 MJ. Tempo de 30s entre execucoes"

# mqcoordinator = {
#     "execute": "nome_servico",
#      ...
# }

def startcoordinator(): # Procura para trabalho a coordenar
    payload  = redis_conn.get('mqcoordinator')
    if not payload:
        return
    logger.logMsg("Trabalho em 'mqcoordinator'...")

    try:
        envmqcoordinator = json.loads(payload)
        service          = envmqcoordinator['execute'] # Nome do serviço que vai executar
        exchange.sendMsg(message=payload, rkey= service)  # Use the exchange instance
        redis_conn.delete('mqcoordinator')

    except Exception as ex:
        logger.logMsgError('Erro na coordenação', ex)

## OPERATIONAL VALUES FROM REDIS
redis_conn       = None 
env              = None
envmqcoordinator = None
try:
    redis_conn = redis.Redis(host='redis', port='6379')
    env        = redis_conn.get('system_config')
    if env:
        env    = json.loads(env)
    else:
        print("REDIS KEY system_config FAILED CONFIG")
        time.sleep(20)
        sys.exit(0)

except Exception as ex:
    print('REDIS SERVICE OFFLINE')
    print(ex)
    time.sleep(15)       # WAIT FOR SERVICE BEFORE RESTART
    sys.exit(1)

## START LOGGER SERVICE ##
try:
    logger  = LogService(amqps   = env['amqps'], 
                         system  = 'sg_etl', 
                         service = 'mqcoordinator',
                         client  = env['nome_cliente'],
                         logLocal= True
                        )

    printlg = logger.logMsg

except Exception as e:
    print("LOGGER SERVICE DOWN")
    print("Erro: " + str(e), flush=True)
    time.sleep(10)
    sys.exit(1)


# START QUEUE SERVICE
try:
    exchange = QueueExchange(service="mqcoordinator")
except Exception  as ex:
    logger.logMsgError('RABBITMQ SERVICE DOWN', ex)
    time.sleep(20)         # WAIT FOR SERVICE BEFORE RESTART
    sys.exit(0)
## START SERVICE ##

startcoordinator()
time.sleep(30)

