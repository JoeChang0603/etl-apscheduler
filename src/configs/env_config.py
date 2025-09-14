import os
from dotenv import load_dotenv

load_dotenv()

class Env:
    # MongoDB
    MONGO_URI = os.getenv("MONGO_URI")
    SQLALCHEMY_URL = os.getenv("SQLALCHEMY_URL")
    ETL_PROCESS_WEBHOOK = os.getenv("ETL_PROCESS_WEBHOOK")
    ETL_TOTAL_USD_VALUE_ALERT = os.getenv("ETL_TOTAL_USD_VALUE_ALERT")

    @classmethod
    def validate(cls):
        required_vars = {
            "MONGO_URI": cls.MONGO_URI,
            "SQLALCHEMY_URL": cls.SQLALCHEMY_URL,
            "ETL_PROCESS_WEBHOOK": cls.ETL_PROCESS_WEBHOOK,
            "ETL_TOTAL_USD_VALUE_ALERT": cls.ETL_TOTAL_USD_VALUE_ALERT
        }

        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

Env.validate()
    