import os
from pydantic_settings import BaseSettings
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()


class MySQLSettings(BaseSettings):
    # MySQL 기본 연결 설정
    MYSQL_CONNECTION_TIMEOUT: int = int(os.getenv("MYSQL_CONNECTION_TIMEOUT", "10"))
    MYSQL_MAX_POOL_SIZE: int = int(os.getenv("MYSQL_MAX_POOL_SIZE", "2"))
    MYSQL_MIN_POOL_SIZE: int = int(os.getenv("MYSQL_MIN_POOL_SIZE", "1"))
    MYSQL_POOL_RECYCLE: int = int(os.getenv("MYSQL_POOL_RECYCLE", "3600"))
    MYSQL_QUERY_TIMEOUT: int = int(os.getenv("MYSQL_QUERY_TIMEOUT", "60"))

    @property
    def default_connection_args(self) -> dict:
        """기본 MySQL 연결 설정 반환"""
        return {
            'connect_timeout': self.MYSQL_CONNECTION_TIMEOUT,
            'maxsize': self.MYSQL_MAX_POOL_SIZE,
            'minsize': self.MYSQL_MIN_POOL_SIZE,
            'pool_recycle': self.MYSQL_POOL_RECYCLE,
            'echo': True,
            'charset': 'utf8mb4'
        }

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache()
def get_mysql_settings() -> MySQLSettings:
    return MySQLSettings()


mysql_settings = get_mysql_settings()