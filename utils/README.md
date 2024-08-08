# Exchange

## QueueExchange - Classe para uso em Messageria Interna tipo direct

### Disposição QueueExchange

- Classe dispõe de dois métodos: sendMsg e start_consuming
- Dados de configuração da instância:
  - service:str     Nome do serviço onde a classe é utilizada;
  - callback:object Função de chamada para o consumo de msg

### Exemplo QueueExchange

```python
# Exemplo cria e consome da queue queue_teste (service)
# publica na queue_controller (default)
# publica na queue gateway
from Exchange import QueueExchange

def callback(ch, method, properties, body):
        dado = json.loads(body)
        exchange.sendMsg(dado, 'gateway') # publica em queue gateway
        exchange.sendMsg(dado)            # publica em queue queue_controller
        
try: # consome msgs da queue queue_teste e executa função callback
    exchange = QueueExchange(service= 'queue_teste', callback= callback) 
except Exception as ex:
    print('rabbitmq service offline', ex)
    time.sleep(15)            # WAIT FOR SERVICE BEFORE RESTART
    sys.exit(0)

printlg('QUEUE_CONTROLLER SERVICE READY') 
exchange.start_consuming()
```

## TaskExchange - Envia task round robin para workers

### Disposição TaskExchange

- callback:object   Função de chamada para o consumo de tasks

### Exemplo TaskExchange

```python
# Exemplo envio de tasks
from Exchange import TaskExchange

exchange = TaskExchange()

for i in range(1000):
    msgTask = {'id': i, 'task': 'task', 'data': 'task'+str(i)}
    exchange.sendTask(msgTask)
```

```python
# Exemplo consumo de tasks
from Exchange import TaskExchange

def callback(ch, method, properties, body):
    print(body)
    time.sleep(1)
    ch.basic_ack(delivery_tag = method.delivery_tag)

exchange = TaskExchange(callback= callback)
exchange.start_consuming()
```
