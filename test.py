#! /usr/bin/env python3

import argparse
import logging
import os
import sys
import time
import traceback

INTERVAL = 10000
LONG_RECORD = os.getenv("LONG_RECORD", default=300)

TUPLE_MSG = 0
TUPLE_STARTTIME = 1
TUPLE_EXTENDED = 2

log_format = "%(asctime)s %(message)s"

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
    # g2 = G2Engine()
    # g2.init("sz_sqs_consumer", engine_config, args.debugTrace)
    logCheckTime = prevTime = time.time()

    # senzing_governor = importlib.import_module("senzing_governor")
    # governor = senzing_governor.Governor(hint="sz_sqs_consumer")

    # sqs = boto3.client("sqs")

    # setupt the queue
    queue_url = args.url
    if not queue_url:
        queue_url = os.getenv("SENZING_AZURE_QUEUE_CONNECTION_STRING")

    max_workers = int(os.getenv("SENZING_THREADS_PER_PROCESS", 0))
    prefetch = int(os.getenv("SENZING_PREFETCH", -1))

    if not max_workers:  # reset to null for executors
        max_workers = None

    print(f"max_workers: {max_workers}")
    print(f"prefetch: {prefetch}")
    print(f"queue_url: {queue_url}")

except Exception as err:
    print(err, file=sys.stderr)
    traceback.print_exc()
    exit(-1)
