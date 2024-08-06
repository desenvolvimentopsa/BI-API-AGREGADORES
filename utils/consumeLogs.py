import pika, sys, time, json

import pprint

def callback(ch, method, properties, body):
    print(f"{method.routing_key}")
    dados = body.decode('utf-8')
    dados = json.loads(dados)

    pprint.pprint(dados, indent=4)
    print("\n-------------------------------------\n")

#url        = 'amqps://tahccxql:AprsNPNbTfmE_sdwvAfiFTt3zrII5TbC@jackal.rmq.cloudamqp.com/tahccxql'
url        = 'amqps://rnraluwi:v6pRbVu6psBABqn7pg81a2nnuChc833d@tall-cyan-dog.rmq4.cloudamqp.com/rnraluwi'
params     = pika.URLParameters(url)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.exchange_declare(exchange= 'topic_softgo', 
                        exchange_type= 'topic',
                        auto_delete  = True)

result = channel.queue_declare('consumeLogs', exclusive=True)
queue_name = result.method.queue

binding_keys = sys.argv[1:]
if not binding_keys:
    channel.queue_bind( exchange    = 'topic_softgo', 
                        queue       = queue_name, 
                        routing_key = '#.teste-itauba')

else:
    for binding_key in binding_keys:
        channel.queue_bind( exchange    = 'topic_softgo', 
                            queue       = queue_name, 
                            routing_key = binding_key)

print(' [*] Waiting for logs. To exit press CTRL+C')

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
