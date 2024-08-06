import sys, time, json

import redis

from Logger import LogService
from redisConfig import RedisConfig

versao   = "v1.00 - 01/01/2024 MJ. Versao Inicial do Serviço"
versao   = "v1.10 - 07/02/2024 MJ. Nova Logger e system_config"
versao   = "v1.20 - 21/06/2024 MJ. Novo Logger v1.50 e prefixos system_config"

service  = 'keysetter'

## START KEYSETTER SERVICE SETUP:

## OPERATIONAL VALUES FROM REDIS
redis_conn = None
try:
    redis_conn = redis.Redis(host='redis',port='6379')
    env        = redis_conn.get('system_config')
    if not env:
        msg = f"KEYSETTER FAILED CONFIG"
        print(msg)
        time.sleep(20)
        sys.exit(0)
    env = json.loads(env)
    if 'sistema' in env.keys():
        system = env['sistema']
        print(f"KEYSETTER SYSTEM - {system.upper()}")
    else:
        msg = f"KEYSETTER KEY 'sistema' NOT FOUND"
        print(msg)
        time.sleep(10)
        sys.exit(0)
except Exception as ex:
    print('REDIS SERVICE OFFLINE', ex)
    time.sleep(15)            # WAIT FOR SERVICE BEFORE RESTART
    sys.exit(0)  

## START KEYSETTER SERVICE 
# CONFIGURA LOGGER
print("KEYSETTER SERVICE STARTING....", versao)
try:
    logger  = LogService(amqps   = env['amqps'], 
                         system  = system,
                         service = service,
                         version = versao,
                         origin  = env['origem'],
                         client  = env['nome_cliente'],
                         logType = 'logs',
                         logLocal= True)
    printlg = logger.logMsg
    printlg(f"KEYSETTER SERVICE STARTING....{versao}")
    printlg = logger.logMsg
    if 'nome_cliente' in env.keys():
        logger.client  = env['nome_cliente']
        printlg(f"KEYSETTER SERVICE CLIENT - {logger.client}")
    else:
        msg = f"KEYSETTER {configkey} KEY 'nome_cliente' NOT FOUND"
        logger.logMsgError(msg)
        time.sleep(20)
        sys.exit(0)

    time.sleep(5)
except Exception as err:
    print("KEYSETTER SERVICE UNABLE TO START", err)
    time.sleep(10)
    sys.exit(0)
    
## KeySetter 
try:
    keysetter = RedisConfig(system=system, client=logger.client, logService=logger)
except Exception as err:
    logger.logMsgError(f"KEYSETTER PROBLEM: {err}")
    time.sleep(40)
    sys.exit(0)

printlg(f"KEYSETTER SERVICE STARTED {versao}")
printlg(f"KEYSETTER {keysetter.baseMsg.model_dump_json()}")
keysetter.channel.start_consuming()
