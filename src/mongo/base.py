import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from configs.env_config import Env

class MongoClient:
    def __init__(self, is_test: bool = False):
        self.client = AsyncIOMotorClient(Env.MONGO_URI)
        if is_test:
            self.DATA_DB = self.client.T_DATA
            self.MART_DB = self.client.T_MART
        else:
            self.DATA_DB = self.client.C_DATA
            self.MART_DB = self.client.C_MART