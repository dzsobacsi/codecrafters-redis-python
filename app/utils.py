def encode_command(command: str) -> bytes:
    words = command.split()
    response = f"*{len(words)}\r\n"
    for word in words:
        response += f"${len(word)}\r\n{word}\r\n"
    return response.encode()