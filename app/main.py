import asyncio
from keyvaluestore import KeyValueStore


def parse(command: str):
    return [
        w 
        for w in command.split('\r\n') 
        if w != '' and w[0] not in '*$:+-'
    ]


async def get_response(command: str, store: KeyValueStore, *args):
    if command == 'PING':
        return "+PONG\r\n"

    elif command == 'ECHO':
        return f"+{args[0]}\r\n"

    elif command == 'SET':
        store.set(args[0], args[1])
        if len(args) >= 4 and args[2].upper() == 'PX':
            coro = store.expire(args[0], int(args[3]))
            asyncio.create_task(coro)
        return "+OK\r\n"

    elif command == 'GET':
        value = store.get(args[0])
        return f"+{value}\r\n" if value else "$-1\r\n"

    return "-ERR unknown command\r\n"


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, store: KeyValueStore):
    while data := await reader.read(1024):
        input = parse(data.decode())
        command = input[0].upper()
        args = input[1:]
        response = await get_response(command, store, *args)
        writer.write(response.encode('utf-8'))
        await writer.drain()


async def start_server(port: int, host: str = 'localhost'):
    store = KeyValueStore()
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, store), host, port
    )
    async with server:
        print(f"Listening on {host}, port: {port} ")
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(start_server(6379))
