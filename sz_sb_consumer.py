#! /usr/bin/env python3

import argparse
import concurrent.futures
import logging
import os
import sys
import time
import traceback

import orjson
import senzing_core
from azure.servicebus import AutoLockRenewer, ServiceBusClient
from senzing import SzBadInputError, SzEngineFlags, SzRetryTimeoutExceededError

INTERVAL = 10000
LONG_RECORD = int(os.getenv("LONG_RECORD", default=300))

TUPLE_MSG = 0
TUPLE_STARTTIME = 1
TUPLE_EXTENDED = 2

log_format = "%(asctime)s %(message)s"


# -----------------------------------------------------------------------------
# add the message to Senzing
def process_msg(engine, msg, info):
    try:
        record = orjson.loads(str(msg).strip())
        if info:
            response = engine.add_record(
                record["DATA_SOURCE"], record["RECORD_ID"], str(msg).strip(), SzEngineFlags.SZ_WITH_INFO
            )
            return response
        else:
            engine.add_record(
                record["DATA_SOURCE"], record["RECORD_ID"], str(msg).strip()
            )
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

    # Initialize the Senzing engine using the new SDK
    print("Initializing Senzing engine")
    factory = senzing_core.SzAbstractFactoryCore("sz_sb_consumer", engine_config, verbose_logging=args.debugTrace)
    g2 = factory.create_engine()
    logCheckTime = prevTime = time.time()
    print(f"Initializing Senzing engine completed: {logCheckTime}")

    # setup the queue
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

    renewer = AutoLockRenewer(max_lock_renewal_duration=3600)
    with ServiceBusClient.from_connection_string(
        conn_str=connection_str
    ) as servicebus_client:
        receiver = servicebus_client.get_queue_receiver(
            queue_name=queue_name, prefetch_count=prefetch, auto_lock_renewer=renewer
        )
        messages = 0
        duration = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:

            print(f"Threads: {executor._max_workers}")
            print(f"Prefetch: {prefetch}")
            futures = {}
            try:
                while True:

                    nowTime = time.time()
                    if futures:
                        done, _ = concurrent.futures.wait(
                            futures,
                            timeout=10,
                            return_when=concurrent.futures.FIRST_COMPLETED,
                        )

                        delete_batch = []
                        delete_cnt = 0

                        for fut in done:
                            msg = futures.pop(fut)
                            try:
                                result = fut.result()
                                if result:
                                    print(result)  # we would handle pushing to withinfo queues here
                            except (
                                SzRetryTimeoutExceededError,
                                SzBadInputError,
                            ) as err:
                                print(err)
                                record = orjson.loads(str(msg[TUPLE_MSG]).strip())
                                print(
                                    f'Sending to deadletter: {record["DATA_SOURCE"]} : {record["RECORD_ID"]}'
                                )
                                receiver.dead_letter_message(msg[TUPLE_MSG])
                            delete_batch.append(msg[TUPLE_MSG])
                            delete_cnt += 1
                            if delete_cnt == 10:  # max for delete batch
                                for msg_to_complete in delete_batch:
                                    receiver.complete_message(msg_to_complete)
                                delete_batch = []
                                delete_cnt = 0

                            messages += 1

                            if messages % INTERVAL == 0:  # display rate stats
                                diff = nowTime - prevTime
                                speed = -1
                                if diff > 0.0:
                                    speed = int(INTERVAL / diff)
                                print(
                                    f"Processed {messages} adds, {speed} records per second"
                                )
                                prevTime = nowTime

                        if delete_batch:
                            for msg_to_complete in delete_batch:
                                receiver.complete_message(msg_to_complete)

                        if nowTime > logCheckTime + (
                            LONG_RECORD / 2
                        ):  # log long running records
                            logCheckTime = nowTime

                            response = g2.get_stats()
                            print(f"\n{response}\n")

                            numStuck = 0
                            numRejected = 0
                            for fut, msg in futures.items():
                                if not fut.done():
                                    duration = nowTime - msg[TUPLE_STARTTIME]
                                    if duration > LONG_RECORD * (
                                        msg[TUPLE_EXTENDED] + 1
                                    ):
                                        numStuck += 1
                                        record = orjson.loads(
                                            str(msg[TUPLE_MSG]).strip()
                                        )
                                        times_extended = msg[TUPLE_EXTENDED] + 1
                                        # push out the visibility another 2 LONG_RECORD times intervals
                                        new_time = (times_extended + 2) * LONG_RECORD
                                        receiver.renew_message_lock(msg[TUPLE_MSG])
                                        futures[fut] = (
                                            msg[TUPLE_MSG],
                                            msg[TUPLE_STARTTIME],
                                            times_extended,
                                        )
                                        print(
                                            f'Extended visibility ({duration / 60:.1f} min, extended {times_extended} times): {record["DATA_SOURCE"]} : {record["RECORD_ID"]}'
                                        )
                                if numStuck >= executor._max_workers:
                                    print(
                                        f"All {executor._max_workers} threads are stuck on long running records"
                                    )
                    if len(futures) >= executor._max_workers + prefetch:
                        time.sleep(1)
                        continue

                    while len(futures) < executor._max_workers + prefetch:
                        try:
                            max_msgs = executor._max_workers + prefetch - len(futures)
                            response = receiver.receive_messages(
                                max_message_count=max_msgs, max_wait_time=5
                            )
                            if not response:
                                if len(futures) == 0:
                                    receiver.close()
                                    print("Recreating receiver.")
                                    receiver = servicebus_client.get_queue_receiver(
                                        queue_name=queue_name,
                                        prefetch_count=prefetch,
                                        auto_lock_renewer=renewer,
                                    )
                                break
                            for this_msg in response:
                                futures[
                                    executor.submit(
                                        process_msg, g2, this_msg, args.info
                                    )
                                ] = (this_msg, time.time(), 0)
                        except Exception as err:
                            print(f"{type(err).__name__}: {err}", file=sys.stderr)
                            raise

                    print(f"Processed total of {messages} adds")

            except Exception as err:
                print(
                    f"{type(err).__name__}: Shutting down due to error: {err}",
                    file=sys.stderr,
                )
                traceback.print_exc()
                nowTime = time.time()
                for fut, msg in futures.items():
                    if not fut.done():
                        duration = nowTime - msg[TUPLE_STARTTIME]
                        record = orjson.loads(str(msg[TUPLE_MSG]).strip())
                        print(
                            f'Still processing ({duration / 60:.1f} min: {record["DATA_SOURCE"]} : {record["RECORD_ID"]}'
                        )
                executor.shutdown()
                exit(-1)
    renewer.close()

except Exception as err:
    print(err, file=sys.stderr)
    traceback.print_exc()
    exit(-1)

print("Receive is done.")
