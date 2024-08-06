import json

import requests, adal
from pydantic import BaseModel, TypeAdapter

from sendConfig import SendConfig

workgroup = {'capacityId': '3A52B349-23B0-413F-BD07-E79C4050E4D1',
            'defaultDatasetStorageFormat': 'Small',
            'id': '92e82b5c-559e-445b-bd9f-e28413fb4b27',
            'isOnDedicatedCapacity': True,
            'isReadOnly': False,
            'name': 'ATTIVA_18',
            'type': 'Workspace'}

class biWorkgroups(BaseModel):
    capacityId: str
    defaultDatasetStorageFormat: str
    id: str
    isOnDedicatedCapacity: bool
    isReadOnly: bool
    name: str
    type: str = None
    
class biWorkgroupsList(BaseModel):
    grupos: list[biWorkgroups]
    
ta = TypeAdapter[biWorkgroupsList]  
    
    
    
def GetTokenBI(clientid,usernamebi,passwordbi):
    print(f"ACCOUNT: {usernamebi}")
    authority_url = 'https://login.microsoftonline.com/common/'
    context = adal.AuthenticationContext(
        authority_url,
        validate_authority=True,
        api_version=None
    )

    token = context.acquire_token_with_username_password(
        resource='https://analysis.windows.net/powerbi/api',
        username=usernamebi,
        password=passwordbi,
        client_id=clientid
    )

    access_token = token['accessToken']
    return access_token

def GetWorkgroups(token):
    try:
        url='https://api.powerbi.com/v1.0/myorg/groups'
        headers = {"authorization": "Bearer %s"%(token)}
        r = requests.get(url, headers=headers)
        status = r.status_code
        if status == 200:
            dados = json.loads(r.text)
            result = [ biWorkgroups.model_validate(item) for item in dados['value'] if item['isOnDedicatedCapacity'] ] 
            return result
    except (Exception) as error:
        print(error)
    
    return None

try:
    versao = "v1.0 Envia Chaves de configuração para os clientes de um sistema alvo"
    conf = SendConfig(systemTarget='sg_etl', systemSource='sg_etl_operacao')
    conf.setClient(client='*')
    clientbi = '79f8a918-70a0-4a77-b5bc-c3c9e7d12484'
    clientid =  clientbi
    usernamebi = 'bi18@psasistemas.com.br'
    passwordbi = 'xgvX9CgKhB7Mn'
    
    conf.setCnfJS('hash_client', 'clientebi', clientbi)
    conf.setCnfJS('hash_client', 'clientid',clientbi)
    conf.setCnfJS('hash_client', 'usernamebi', usernamebi)
    conf.setCnfJS('hash_client', 'passwordbi', passwordbi)
    
except Exception as err:
    print(err)
    
print("COLETANDO WORKGROUPS")
    
tokenBI = GetTokenBI(clientid,usernamebi,passwordbi)
    
#print(f"TOKEN: {tokenBI}")  

relacao = GetWorkgroups(tokenBI)

for work in relacao:
    client = work.name[:-3]
    groupid = work.id
    #print(f"{client} -> {groupid}")
    conf.setClient(client=client)
    conf.setCnfJS('hash_client', 'groupid', groupid)
    
    
    