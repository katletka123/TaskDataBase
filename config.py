import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    DB_CONFIG = {
        "dbname": os.getenv("dbname"),
        "user": os.getenv("user"),
        "password": os.getenv("password"),
        "host": os.getenv("host"),
        "port": int(os.getenv("port", 5432)),
    }
