from argparse import ArgumentParser
import asyncio
from command_handler import parse_input, get_response
from keyvaluestore import KeyValueStore


def get_args():
    parser = ArgumentParser(
        description="Redis - My own implementation of a Redis-like server")
    parser.add_argument("--port", type=int, default=6379,
        help="Set the listening port")
    return parser.parse_args()


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, store: KeyValueStore):
    while data := await reader.read(1024):
        input = parse_input(data.decode())
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
    args = get_args()
    asyncio.run(start_server(args.port))
