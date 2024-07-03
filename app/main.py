import asyncio

def parse(command: str):
    return [w for w in command.split('\r\n') if w != '' and w[0] not in ['*', '$', ':', '+', '-']]


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while data := await reader.read(1024):
        input = parse(data.decode())
        command = input[0]
        args = input[1:]

        if command == 'PING':
            writer.write(b"+PONG\r\n")
            await writer.drain()

        elif command == 'ECHO':
            writer.write(f"+{args[0]}\r\n".encode('utf-8'))
            await writer.drain()


async def start_server(port: int, host: str = 'localhost'):
    server = await asyncio.start_server(handle_client, host=host, port=port)
    async with server:
        print(f"Listening on {host}, port: {port} ")
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(start_server(6379))
