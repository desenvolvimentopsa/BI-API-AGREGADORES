from typing import Literal
from pydantic import BaseModel


# DADOS ENVIADOS PARA OS SISTEMAS DISTRIBUIDOS
class MessageModel(BaseModel):
    type: Literal['SET','SETJS','DEL'] = 'SET'
    system: str                   # Sistema utilizado pelo cliente
    client: str                   # Nome do cliente
    session: str                  # Identificador da sessao do cliente (uuid)
    key: str = None               # Chave a ser configurada
    value: str = None             # Valor a ser configurado
    source: str = None            # Origem do sistema
    
