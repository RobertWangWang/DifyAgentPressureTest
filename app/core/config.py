import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    MYSQL_URL: str = os.getenv(
        "MYSQL_URL",
        "mysql+pymysql://root:abcd1234@localhost:3306/dify_test?charset=utf8mb4"
    )

settings = Settings()
