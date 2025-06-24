import asyncio


async def ping_task(ping_time, target) -> None:
    while True:
        try:
            await asyncio.sleep(ping_time)
            binary_ping = "ping".encode('utf-8')
            await target.send(binary_ping)
        except Exception as e:
            print("ERROR IN PING", e)
            try:
                await target.close()
            except Exception as e:
                print("ERROR IN CLOSE", e)
            return
