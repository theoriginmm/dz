#!/usr/bin/env python3

import json
import pika
import zlib


from time import sleep

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq', heartbeat=600,
                                       blocked_connection_timeout=300))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')
channel.queue_declare(queue='rpc_queue_response')


def calc_nonzero_bytes(fileName):
    CHUNKSIZE = 131072
    NONZEROBYTES = 0
    d = zlib.decompressobj(16+zlib.MAX_WBITS)
    f = open(fileName, 'rb')
    buffer = f.read(CHUNKSIZE)
    while buffer:
        outstr = d.decompress(buffer)
        for char in outstr:
            if char != 0:
                NONZEROBYTES += 1
        buffer = f.read(CHUNKSIZE)
    outstr = d.flush()
    f.close()
    return NONZEROBYTES



def on_request(ch, method, props, body):
    s = str(body.decode('UTF-8'))
    task = json.loads(s.replace("'", "\""))
    print(" [.] Obtained task %s" % body)

    if task['type'] == 'map':
        response = calc_nonzero_bytes(task['file'])
        reduced = 1
        print(" %s " % response)
    elif task['type'] == 'reduce':
        response = sum(task['values'])
        reduced = sum(task['reduced'])
    else:
        sleep(10)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return
    message = {"reduced": reduced, "values": response}
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=str(message))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()
