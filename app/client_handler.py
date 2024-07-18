import asyncio
import base64

from app.keyvaluestore import KeyValueStore
from app.utils import encode_command

RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

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
        return f"+FULLRESYNC {state.get('master_replid')} {state.get('master_repl_offset')}\r\n"
    
    elif command == 'REPLCONF':
        return "+OK\r\n"

    elif command == 'SET':
        store.set(args[0], args[1])
        if len(args) >= 4 and args[2].upper() == 'PX':
            coro = store.expire(args[0], int(args[3]))
            asyncio.create_task(coro)

        if state.get('role') == 'master':
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
        print(f"I am {state.get('role')}, listening on port {state.get('listening_port')}")
        print(f"Received request: {command} {' '.join(args)}")

        if state.get('role') == 'master' and command in ['SET', 'DEL']:
            for slave_writer in state.get('connected_slaves'):
                slave_writer.write(encode_command(command + ' ' + ' '.join(args)))
                await slave_writer.drain()

        response = await get_response(command, store, state, *args)
        print(f"Sending response: {response}")
        writer.write(response.encode())

        if command == 'PSYNC':
            my_slaves = state.get('connected_slaves')
            my_slaves.append(writer)
            state.set('connected_slaves', my_slaves)

            rdb_file = base64.b64decode(RDB_BASE64)
            writer.write(f"${len(rdb_file)}\r\n".encode() + rdb_file)

        await writer.drain()