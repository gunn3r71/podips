#!/usr/bin/env python3
# coding: utf-8 
# ==========================================================================================
# mock-consumer: A program that reads logs from a queue and pretends to send to a server log 
# @author Flávio Gomes da Silva Lisboa <flavio.lisboa@fgsl.eti.br>
# This implementation specific for MacOS was written by Marcos Fernando Simon (https://github.com/mfsimonbrz)
# @license LGPL-2.1
# ==========================================================================================
from fluent import sender
import json
import os
import stomp
import time
import sys

# connection to ActiveMQ
def getQueue(queue_host, queue_port, queue_username, queue_password):
    print("Trying to connect with queue using " + queue_host + ":" + str(queue_port) + "...")
    queue = stomp.Connection([(queue_host,queue_port)])
    queue.connect(queue_username, queue_password, wait=True)
    print("Connected to queue using user " + queue_username + " in host " + queue_host + ":" + str(queue_port))
    queue.set_listener('logger',     LoggerListener())
    queue.subscribe('/queue/pods',1)
    print("Queue /queue/pods subscribed")
    return queue  
  
class LoggerListener(stomp.ConnectionListener):
    def on_error(self, frame):
        if hasattr(frame, 'body'):
            print('ERROR: received an error "%s"' % frame.body)
        else:
            print('ERROR: when tried to read messages')
    def on_message(self, frame):
        if hasattr(frame,'body'):
            message = frame.body
        print('MOCK-CONSUMER: received a message "%s"' % message)
        try:
            json.loads(message)
            print("MOCK-CONSUMER: message loaded as JSON")
        except Exception as e:
            print("ERROR: could not send log...:",e)

def load_env_vars():
    print("Loading queue configuration...")
    try:
        with open("queue.config.json", "r") as f:
            queue_config = json.loads(f.read())
            print("ActiveMQ configuration loaded from JSON file...")
    except IOError:
        queue_config = {"QUEUE_USERNAME" : "", "QUEUE_PASSWORD": "", "QUEUE_HOST": "", "QUEUE_PORT": ""}

    if  os.getenv('QUEUE_USERNAME') is not None:
        queue_username = os.getenv('QUEUE_USERNAME')
    else:
        queue_username = queue_config['QUEUE_USERNAME']
    print("queue_username defined...")

    if  os.getenv('QUEUE_PASSWORD') is not None:
        queue_password = os.getenv('QUEUE_PASSWORD')
    else:
        queue_password = queue_config['QUEUE_PASSWORD']
    print("queue_password defined...")

    if  os.getenv('QUEUE_HOST') is not None:
        queue_host = os.getenv('QUEUE_HOST')
    else:
        queue_host = queue_config['QUEUE_HOST']    
    print("queue_host defined...")

    if  os.getenv('QUEUE_PORT') is not None:
        queue_port = int(os.getenv('QUEUE_PORT'))
    else:
        queue_port = int(queue_config['QUEUE_PORT']) if queue_config['QUEUE_PORT'].isdigit() else 0
    print("queue_port defined...")

    return queue_host, queue_port, queue_username, queue_password

# # # # # 
# # # #
# # # Main function
# # 
# 
if __name__ == "__main__":
    queue_host, queue_port, queue_username, queue_password = load_env_vars()

    if (not queue_host) or (queue_port == 0) or (not queue_username) or (not queue_password):
        sys.exit("Environment variables not properly defined. Exiting now...")

    print("Waiting 10 seconds to start...")
    time.sleep(10)

    while True:
        try:
            queue = getQueue(queue_host, queue_port, queue_username, queue_password)
            print("Waiting for messages...")
            time.sleep(60)
            queue.disconnect()
        except Exception as e:
            print("ERROR:",e)
