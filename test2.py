#! /usr/bin/env python3

import argparse
import json
import logging
import os
import sys
import time
import traceback

from azure.servicebus import ServiceBusClient
from senzing import G2Engine

INTERVAL = 10000
LONG_RECORD = os.getenv("LONG_RECORD", default=300)

TUPLE_MSG = 0
TUPLE_STARTTIME = 1
TUPLE_EXTENDED = 2

log_format = "%(asctime)s %(message)s"


# -----------------------------------------------------------------------------
# add the message to Senzing
def process_msg(engine, msg, info):
    try:
        print("------------")
        print("--> " + str(msg))
        print("------------")
        # record = orjson.loads(msg)
        record = json.loads(str(msg).strip())
        print("DATA_SOURCE: " + record["DATA_SOURCE"])
        print("RECORD_ID: " + record["RECORD_ID"])
        if info:
            response = bytearray()
            engine.addRecordWithInfo(
                record["DATA_SOURCE"], record["RECORD_ID"], msg, response
            )
            return response.decode()
        else:
            print(">>>>> calling addRecord")
            engine.addRecord(
                record["DATA_SOURCE"], record["RECORD_ID"], str(msg).strip()
            )
            print("<<<<<< addRecord")
            return None
    except Exception as err:
        print(f"{err} [{msg}]", file=sys.stderr)
        raise


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
    g2 = G2Engine()
    g2.init("sz_sb_consumer", engine_config, args.debugTrace)
    logCheckTime = prevTime = time.time()

    # senzing_governor = importlib.import_module("senzing_governor")
    # governor = senzing_governor.Governor(hint="sz_sqs_consumer")

    # sqs = boto3.client("sqs")

    # setupt the queue
    connection_str = args.url
    if not connection_str:
        connection_str = os.getenv("SENZING_AZURE_QUEUE_CONNECTION_STRING")

    max_workers = int(os.getenv("SENZING_THREADS_PER_PROCESS", 0))
    prefetch = int(os.getenv("SENZING_PREFETCH", -1))

    if not max_workers:  # reset to null for executors
        max_workers = None

    print(f"max_workers: {max_workers}")
    print(f"prefetch: {prefetch}")
    print(f"queue_url: {connection_str}")

    queue_name = os.environ["SENZING_AZURE_QUEUE_NAME"]

    servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_str)

    with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(queue_name=queue_name)
        with receiver:
            received_msgs = receiver.receive_messages(
                max_message_count=10, max_wait_time=5
            )
            for msg in received_msgs:
                process_msg(g2, msg, False)
                receiver.complete_message(msg)


except Exception as err:
    print(err, file=sys.stderr)
    traceback.print_exc()
    exit(-1)

print("Receive is done.")
