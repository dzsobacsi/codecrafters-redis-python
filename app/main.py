from argparse import ArgumentParser, Namespace
import asyncio

from app.client_handler import get_response, handle_client, parse_input
from app.keyvaluestore import KeyValueStore
from app.utils import encode_command


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Redis - My own implementation of a Redis-like server")
    parser.add_argument("--port", type=int, default=6379,
        help="Set the listening port")
    parser.add_argument("--replicaof", type=str, default=None,
        help="Start the server in replica mode")
    return parser.parse_args()


async def handshake(reader, writer, args):
    writer.write(encode_command('PING'))
    await reader.read(1024)

    writer.write(encode_command(f'REPLCONF listening-port {args.port}'))
    await reader.read(1024)

    writer.write(encode_command('REPLCONF capa psync2'))
    await reader.read(1024)

    writer.write(encode_command(f'PSYNC ? -1'))
    await reader.read(1024)


async def listen_to_master(reader, store, state):
    while data := await reader.read(1024):

        inputs = [
            parse_input('*' + inp) 
            for inp in data.decode().split('*') 
            if inp
        ]
        for input in inputs:
            command = input[0].upper()
            args = input[1:]

            if command in ['SET', 'DEL']:
                print(f"I am a replica, received command: {command} {' '.join(args)}")
                await get_response(command, store, state, *args)
                print(f"My new store is: {store.store}")


async def start_server(args: Namespace):
    store = KeyValueStore()
    state = KeyValueStore()
    state.set('role', 'master')
    state.set('listening_port', args.port)
    state.set('master_replid', '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb')
    state.set('master_repl_offset', 0)
    state.set('connected_slaves', [])

    if args.replicaof:
        state.set('role', 'slave')
        reader, writer = await asyncio.open_connection(*args.replicaof.split())
        await handshake(reader, writer, args)
        asyncio.create_task(listen_to_master(reader, store, state))

    host = 'localhost'
    port = args.port
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, store, state), host, port
    )
    async with server:
        print(f"Listening on {host}, port: {port} ")
        await server.serve_forever()


if __name__ == "__main__":
    args = get_args()
    asyncio.run(start_server(args))
