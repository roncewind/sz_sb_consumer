#! /usr/bin/env python3

import argparse
import logging
import os
import sys
import time
import traceback

from azure.servicebus import ServiceBusClient
from senzing import G2Engine

LONG_RECORD = int(os.getenv("LONG_RECORD", default=300))


log_format = "%(asctime)s %(message)s"


# -----------------------------------------------------------------------------
# main

try:

    # set up logging
    log_level_map = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "fatal": logging.FATAL,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    log_level_parameter = os.getenv("SENZING_LOG_LEVEL", "info").lower()
    log_level = log_level_map.get(log_level_parameter, logging.INFO)
    logging.basicConfig(format=log_format, level=log_level)

    # get the environment variables
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--queue", dest="url", required=False, help="queue url")
    parser.add_argument(
        "-i",
        "--info",
        dest="info",
        action="store_true",
        default=False,
        help="produce withinfo messages",
    )
    parser.add_argument(
        "-t",
        "--debugTrace",
        dest="debugTrace",
        action="store_true",
        default=False,
        help="output debug trace information",
    )
    args = parser.parse_args()

    engine_config = os.getenv("SENZING_ENGINE_CONFIGURATION_JSON")
    if not engine_config:
        print(
            "The environment variable SENZING_ENGINE_CONFIGURATION_JSON must be set with a proper JSON configuration.",
            file=sys.stderr,
        )
        print(
            "Please see https://senzing.zendesk.com/hc/en-us/articles/360038774134-G2Module-Configuration-and-the-Senzing-API",
            file=sys.stderr,
        )
        exit(-1)

    # Initialize the G2Engine
    print("Initializing G2 engine")
    g2 = G2Engine()
    g2.init("sz_sb_consumer", engine_config, args.debugTrace)
    logCheckTime = prevTime = time.time()
    print(f"Initializing G2 engine completed: {logCheckTime}")

    # setupt the queue
    connection_str = args.url
    if not connection_str:
        connection_str = os.getenv("SENZING_AZURE_QUEUE_CONNECTION_STRING")
    if not connection_str:
        print(
            "The environment variable SENZING_AZURE_QUEUE_CONNECTION_STRING must be set.",
            file=sys.stderr,
        )
        exit(-1)

    max_workers = int(os.getenv("SENZING_THREADS_PER_PROCESS", 0))
    prefetch = int(os.getenv("SENZING_PREFETCH", -1))

    if not max_workers:  # reset to null for executors
        max_workers = None
        cpu_count = os.cpu_count()
        prefetch = min(32, 0 if cpu_count is None else cpu_count + 4)
    elif prefetch < 0:
        prefetch = max_workers

    print(f"max_workers: {max_workers}")
    print(f"prefetch: {prefetch}")
    print(f"queue_url: {connection_str}")

    queue_name = os.getenv("SENZING_AZURE_QUEUE_NAME")
    if not queue_name:
        print(
            "The environment variable SENZING_AZURE_QUEUE_NAME must be set.",
            file=sys.stderr,
        )
        exit(-1)

    message_count = 0
    # renewer = AutoLockRenewer(max_lock_renewal_duration=3600)
    with ServiceBusClient.from_connection_string(
        conn_str=connection_str
    ) as servicebus_client:
        while True:
            receiver = servicebus_client.get_queue_receiver(
                queue_name=queue_name,
                prefetch_count=prefetch,
            )
            if receiver.session() == None:
                print("Session is None")
            else:
                print(receiver.session())
            received_msgs = receiver.receive_messages(max_message_count=1)
            for msg in received_msgs:
                # print(str(msg))
                message_count += 1
                # receiver.complete_message(msg)
            print(f"Received {message_count} messages")
            receiver.close()
            if not received_msgs:
                print(f"Final received {message_count} messages")
                break
        # with servicebus_client.get_queue_receiver(
        #     queue_name=queue_name,
        #     prefetch_count=prefetch,
        #     receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
        #     # auto_lock_renewer=renewer,
        #     # receive_mode="peek_lock", # not a valid receive_mode error
        #     # receive_mode="PEEK_LOCK", # not a valid receive_mode error?
        #     # receive_mode=ServiceBusReceiveMode.PEEK_LOCK, # not a valid receive_mode error?!
        # ) as receiver:
        #     while True:
        #         # received_msgs = receiver.receive_messages(
        #         #     max_message_count=10, max_wait_time=5
        #         # )
        #         received_msgs = receiver.receive_messages(max_message_count=1)
        #         for msg in received_msgs:
        #             # print(str(msg))
        #             message_count += 1
        #             # receiver.complete_message(msg)
        #         print(f"Received {message_count} messages")
        #         if not received_msgs:
        #             print(f"Final received {message_count} messages")
        #             break
except Exception as err:
    print(err, file=sys.stderr)
    traceback.print_exc()
    exit(-1)

print("Receive is done.")
exit(0)
