from argparse import ArgumentParser, Namespace
import asyncio
import socket

from app.command_handler import parse_input, get_response
from app.keyvaluestore import KeyValueStore


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Redis - My own implementation of a Redis-like server")
    parser.add_argument("--port", type=int, default=6379,
        help="Set the listening port")
    parser.add_argument("--replicaof", type=str, default=None,
        help="Start the server in replica mode")
    return parser.parse_args()


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
        response = await get_response(command, store, state, *args)
        writer.write(response.encode('utf-8'))
        await writer.drain()


def handshake(args):
    connection = socket.create_connection(args.replicaof.split())
    connection.sendall(b"*1\r\n$4\r\nPING\r\n")


async def start_server(args: Namespace):
    store = KeyValueStore()
    state = KeyValueStore()
    state.set('role', 'master')
    state.set('master_replid', '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb')
    state.set('master_repl_offset', 0)

    if args.replicaof:
        state.set('role', 'slave')
        handshake(args)


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
