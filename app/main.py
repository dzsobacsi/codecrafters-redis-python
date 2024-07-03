import asyncio


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while await reader.read(1024):
        writer.write(b"+PONG\r\n")
        await writer.drain()


async def start_server(port: int, host: str = 'localhost'):
    server = await asyncio.start_server(handle_client, host=host, port=port)
    async with server:
        print(f"Listening on {host}, port: {port} ")
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(start_server(6379))
