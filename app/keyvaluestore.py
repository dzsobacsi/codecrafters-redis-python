import asyncio

class KeyValueStore:
    def __init__(self):
        self.store = {}

    def set(self, key, value):
        self.store[key] = value

    def get(self, key):
        return self.store.get(key)
    
    async def expire(self, key, ms):
        await asyncio.sleep(ms / 1000)
        del self.store[key]