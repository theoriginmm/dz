#!/usr/bin/env python3

import json
import pika
import uuid
import os,sys

from time import sleep


class MapReduceClient(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', heartbeat=600,
                                       blocked_connection_timeout=300))

        self.channel = self.connection.channel()

        self.callback_queue = 'rpc_queue_response'

    def call(self, task):
        print(" [x] Requesting task: %s" % task)
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(task))

    def on_message(self):
        method, properties, body = self.channel.basic_get('rpc_queue_response', auto_ack=False)
        s = str(body.decode('UTF-8'))
        data = json.loads(s.replace("'", "\""))
        return data['values'], data['reduced'], method.delivery_tag

    def response_message_count(self):
        ch = self.channel.queue_declare(queue='rpc_queue_response', passive=True)
        return ch.method.message_count

    def close(self):
        self.connection.close()


if __name__ == "__main__":

    # task_map= {'type': 'map', 'file': 'file.gz'}
    # task_reduce = {'type': 'reduce', 'values': int , 'reduced': int}

    if len(sys.argv) != 2:
        print("Usage: %s /path/to/your/folder/with/gz/files" % sys.argv[0])
        sys.exit()
    else:
        os.system('docker cp '+str(sys.argv[1]) +'/. mrstorage:/files/')

    map_reduce = MapReduceClient()

    task = {'type': 'map'}
    list_of_files = os.listdir(sys.argv[1])
    number_tasks = len(list_of_files)

    for file in list_of_files:
        task['file'] = file
        map_reduce.call(task)

    # Reduce

    while True:
        num, val, red = map_reduce.response_message_count(), [], []

        if num > 1:
            for i in range(0, num):
                msg = map_reduce.on_message()
                val.append(msg[0])
                red.append(msg[1])
                dtag = msg[2]

                map_reduce.channel.basic_ack(delivery_tag=dtag)
            map_reduce.call({'type': 'reduce', 'values': val, 'reduced': red})

        elif num == 1:
            val, red, dtag = map_reduce.on_message()

            if red < number_tasks:
                map_reduce.channel.basic_nack(delivery_tag=dtag, requeue=True)
                sleep(10)
            elif red == number_tasks:
                map_reduce.channel.basic_ack(delivery_tag=dtag)
                print("The answer is %r" % val)
                map_reduce.close()
                sys.exit(0)
            else:
                print("Something goes wrong with internal logic. Aborting")
                sys.exit(1)
        else:
            sleep(10)
