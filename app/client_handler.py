import asyncio
from app.keyvaluestore import KeyValueStore


def parse_input(command: str):
    return [
        w 
        for w in command.split('\r\n') 
        if w != '' and w[0] not in '*$:+-'
    ]


async def get_response(command: str, store: KeyValueStore, state: KeyValueStore, *args) -> str:
    def str2bulk_string(value: str) -> str:
        return f"${len(value)}\r\n{value}\r\n"


    if command == 'ECHO':
        return f"+{args[0]}\r\n"
    
    elif command == 'GET':
        value = store.get(args[0])
        return f"+{value}\r\n" if value else "$-1\r\n"
    
    elif command == 'INFO':
        return str2bulk_string(
            f"role:{state.get('role')}\n" +
            f"master_replid:{state.get('master_replid')}\n" +
            f"master_repl_offset:{state.get('master_repl_offset')}\n"
        )
    
    elif command == 'PING':
        return "+PONG\r\n"
    
    elif command == 'PSYNC':
        return "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"
    
    elif command == 'REPLCONF':
        return "+OK\r\n"

    elif command == 'SET':
        store.set(args[0], args[1])
        if len(args) >= 4 and args[2].upper() == 'PX':
            coro = store.expire(args[0], int(args[3]))
            asyncio.create_task(coro)
        return "+OK\r\n"

    return "-ERR unknown command\r\n"


async def handle_client(
    reader: asyncio.StreamReader, 
    writer: asyncio.StreamWriter, 
    store: KeyValueStore, 
    state: KeyValueStore
):
    while data := await reader.read(1024):
        input = parse_input(data.decode())
        command = input[0].upper()
        args = input[1:]
        print(f"Received request: {command} {args}")
        response = await get_response(command, store, state, *args)
        writer.write(response.encode('utf-8'))
        await writer.drain()