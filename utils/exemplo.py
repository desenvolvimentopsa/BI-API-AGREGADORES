from sendConfig import SendConfig
import time, json


# EXEMPLO DE ENVIO DE CONFIGURAÇÃO
## systemTarget nome do sistema que receberá a configuração,  p.ex sg_etl, sg_intel, sg_erp - sem  menção do cliente
## systemSource nome do sistema que faz a gestão das chaves - De onde está vindo a configuração?
# NOTAS
## para quem espera pela configuração(serviço keysetter): será criado uma queue com o nome {cliente}.{systemTarget}.keysetter
## todos os clientes de um sistema receberão a configuração enviada, mas existe uma filtragem por cliente
##  a menos que o cliente seja *, de forma que todos os clientes receberão a configuração 
try:
    versao = "v1.0 Envia Chaves de configuração para os clientes de um sistema alvo"
    setconf = SendConfig(systemTarget='sg_etl', systemSource='sg_etl_operacao')
    setconf.setClient(client='IQ5')
    setconf.setCnf('versao', versao) # cria uma chave versao com o valor da versao
    setconf.setCnfJS('system_config', 'client', 'sg_etl')
    setconf.setClient(client='bambui')
    setconf.setCnfJS('system_config', 'client', 'MARIA JOANA')
    setconf.setClient(client='*')  # Para todos os clientes
    setconf.setCnf("novidade","PARA TODOS OS GOSTOS")
    setconf.setCnfJS("groupid", "pg_usernew", "baltiago") # SE A CHAVE NAO EXISTE, CRIA
    setconf.setCnfJS("groupid", "pg_userpwd", "38472")    # ACRECENTA NA CHAVE CRIADA
    setconf.delCnf("groupid")
    setconf.delCnf("novidade")
    setconf.delCnf("versao")
except Exception as err:
    print(err)