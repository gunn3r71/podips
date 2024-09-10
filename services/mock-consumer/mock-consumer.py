#!/usr/bin/env python3
# coding: utf-8 
# ==========================================================================================
# mock-consumer: A program that reads logs from a queue and pretends to send to a server log 
# @author Flávio Gomes da Silva Lisboa <flavio.lisboa@fgsl.eti.br>
# @license LGPL-2.1
# ==========================================================================================
import json
import os
import stomp
import time

# connection to ActiveMQ
def getQueue():
    print("Loading queue configuration...")
    try:
        with open("queue.config.json", "r") as f:
            queue_config = json.loads(f.read())
    except IOError:
        queue_config = {"QUEUE_USERNAME" : "", "QUEUE_PASSWORD": "", "QUEUE_HOST": "", "QUEUE_PORT": ""}

    print("ActiveMQ configuration loaded from JSON file...")

    queue_username = os.getenv('QUEUE_USERNAME', queue_config['QUEUE_USERNAME'])
    print("queue_username defined...")

    queue_password = os.getenv('QUEUE_PASSWORD', queue_config['QUEUE_PASSWORD'])
    print("queue_password defined...")

    queue_host = os.getenv('QUEUE_HOST', queue_config['QUEUE_HOST'])
    print("queue_host defined...")

    queue_port = int(os.getenv('QUEUE_PORT', queue_config['QUEUE_PORT']))
    print("queue_port defined...")

    print("Trying to connect with queue using " + queue_host + ":" + str(queue_port) + "...")
    queue = stomp.Connection([(queue_host, queue_port)])
    queue.set_listener('logger', LoggerListener())
    queue.connect(queue_username, queue_password, wait=True)
    print("Connected to queue using user " + queue_username + " in host " + queue_host + ":" + str(queue_port))
    queue.subscribe('/podips/pods', "mock-consumer")
    print("Queue /queue/pods subscribed")
    return queue
  
class LoggerListener(stomp.ConnectionListener):
    def on_error(self, frame):
        if hasattr(frame, 'body'):
            print('ERROR: received an error "%s"' % frame.body)
        else:
            print('ERROR: when tried to read messages')
    def on_message(self, frame):
        if hasattr(frame, 'body'):
            message = frame.body
            print('MOCK-CONSUMER: received a message "%s"' % message)
            try:
                json.loads(message)
                print("MOCK-CONSUMER: message loaded as JSON")
            except Exception as e:
                print("ERROR: could not send log...:", e)

# Main function
print("Waiting 10 seconds to start...")
time.sleep(10)

queue = None
while True:
    try:
        if queue is None or not queue.is_connected():
            queue = getQueue()
        print("Waiting for messages...")
        time.sleep(60)
    except Exception as e:
        print("ERROR:", e)
        if queue is not None:
            queue.disconnect()
        queue = None

print("Program finished unexpectedly... and unexplainedly")