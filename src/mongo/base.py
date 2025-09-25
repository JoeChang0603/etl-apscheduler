"""MongoDB connection helpers backed by Motor."""

from motor.motor_asyncio import AsyncIOMotorClient

from src.configs.env_config import Env
from src.utils.casting import to_bool

IS_TEST = to_bool(Env.IS_TEST)


class MongoClient:
    """Thin wrapper exposing tenant-specific Mongo databases."""

    def __init__(self, is_test: bool = IS_TEST):
        """Initialise the Motor client and select databases.

        :param is_test: Whether to connect to the test database pair.
        """

        self.client = AsyncIOMotorClient(Env.MONGO_URI)
        if is_test:
            self.DATA_DB = self.client.T_DATA
            self.MART_DB = self.client.T_MART
        else:
            self.DATA_DB = self.client.C_DATA
            self.MART_DB = self.client.C_MART
