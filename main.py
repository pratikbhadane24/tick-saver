import asyncio
import json
import random
from datetime import datetime, timedelta
from queue import Queue

import websockets
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

from database import REDIS_DB_CLIENT
from helpers.zerodha_helpers import fetch_all_tokens_for_zerodha, parse_binary_ticks, parse_text_message
from ohlc_handler import process_tick, threaded_save
from utils.utils import ping_task

data_queue = Queue()

cirrus_token_to_broker_token_mapping = {}
task_scheduler = AsyncIOScheduler()


async def schedule_task_at_time(target_time: datetime, task: callable, args=None, kwargs=None):
    if kwargs is None:
        kwargs = {}
    if args is None:
        args = []
    random_id = random.randint(0, 100000)
    task_scheduler.add_job(
        task,
        DateTrigger(run_date=target_time),
        args=args,
        kwargs=kwargs,
        id=str(random_id),
    )
    print(f"Task scheduled for {target_time} at {datetime.now()}")
    return str(random_id)


async def send_instruments_to_zerodha(socket, instruments):
    message = {
        "a": "mode",
        "v": ["full", instruments]
    }
    try:
        await socket.send(json.dumps(message))
    except Exception as e:
        print("Error in sending instruments to Zerodha", e)
        try:
            await socket.close()
        except Exception as e:
            print("Error in closing socket", e)
        return


async def close_socket(ws):
    try:
        await ws.close()
    except Exception as e:
        print("Error in closing socket", e)


async def ohlc_flush_task_generator():
    last_scheduled_time = None
    while True:
        now = datetime.now().replace(second=0, microsecond=0)
        next_minute = now + timedelta(minutes=1)
        # if next_minute is after 15:30 PM, skip scheduling


        if last_scheduled_time and next_minute <= last_scheduled_time:
            # print("Skipping scheduling for", next_minute, "as it is already scheduled")
            await asyncio.sleep(10)
            continue
        print("Scheduling OHLC flush at", next_minute)
        await schedule_task_at_time(next_minute, threaded_save)
        last_scheduled_time = next_minute
        await asyncio.sleep(10)


async def fetch_zerodha_market_data(api_key, access_token, broker_token_list_to_share):
    url = f"wss://ws.kite.trade?api_key={api_key}&access_token={access_token}"
    tasks = []
    try:
        ws_conn = None
        async with websockets.connect(url, ping_interval=None) as target:
            ws_conn = target
            ws_ping_task: asyncio.Task = asyncio.create_task(ping_task(5, target))
            data_recv_task: asyncio.Task = asyncio.create_task(handle_received_data(target))

            print(f'Connection established with Zerodha with {api_key} and {len(broker_token_list_to_share)} tokens')
            await send_instruments_to_zerodha(ws_conn, broker_token_list_to_share)
            tasks.append(ws_ping_task)
            tasks.append(data_recv_task)
            await asyncio.gather(*tasks)
    except websockets.exceptions.ConnectionClosedError:
        pass
    except asyncio.CancelledError:
        pass
    except Exception as e:
        pass
    finally:
        for task in tasks:
            task.cancel()
        if ws_conn is not None:
            await ws_conn.close()
            await fetch_zerodha_market_data(api_key, access_token, broker_token_list_to_share)
        else:
            await fetch_zerodha_market_data(api_key, access_token, broker_token_list_to_share)


async def handle_received_data(ws):
    global cirrus_token_to_broker_token_mapping
    try:
        while True:
            result = await ws.recv()
            if isinstance(result, bytes):
                result = parse_binary_ticks(result)
                for res in result:
                    try:
                        c_token = cirrus_token_to_broker_token_mapping[res['token']]
                        res['token'] = c_token
                        process_tick(res)
                    except Exception as e:
                        print(f"Error processing tick data: {e}")
            else:
                parse_text_message(result)
    except websockets.exceptions.ConnectionClosedError:
        print("Zerodha WebSocket connection closed")
    except asyncio.CancelledError:
        print("Task Cancelled")
    except Exception as e:
        print(f"Internal Error: {e}")


async def main():
    task_scheduler.start()
    print("Initializing ZeroDha")
    global cirrus_token_to_broker_token_mapping
    broker_token_list_to_share, cirrus_token_to_broker_token_mapping = fetch_all_tokens_for_zerodha(['CT:1:1:NIFTY'])
    token_limit_per_connection = 3000
    connection_per_account = 3

    tasks = []
    chunk_index = 0

    runner_key_start = 1
    runner_key_end = 2
    runners_names = [f"TS{runner_key}" for runner_key in range(runner_key_start, runner_key_end + 1)]
    print("Runners Names: ", runners_names)

    account_list = REDIS_DB_CLIENT.hmget("ZERODHA_TOKENS_FOR_MINUTE_DATA", runners_names)
    account_list = [json.loads(acc.decode('utf-8')) for acc in account_list if acc is not None]
    if not account_list:
        print("No Zerodha accounts found in Redis")
        return

    max_total_tokens = token_limit_per_connection * connection_per_account * len(account_list)
    tokens_to_process = broker_token_list_to_share[:max_total_tokens]

    for account in account_list:
        client_id = account["api_key"]
        access_token = account["access_token"]

        for _ in range(connection_per_account):
            start = chunk_index * token_limit_per_connection
            end = start + token_limit_per_connection
            if start >= len(tokens_to_process):
                break
            token_chunk = tokens_to_process[start:end]
            t = asyncio.create_task(fetch_zerodha_market_data(client_id, access_token, token_chunk))
            tasks.append(t)
            chunk_index += 1

    print("Total connections created: ", len(tasks))
    saver_task = asyncio.create_task(ohlc_flush_task_generator())
    tasks.append(saver_task)

    await asyncio.gather(*tasks)


asyncio.run(main())
