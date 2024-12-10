from typing import Optional, Dict, Any, Type, TypeVar
import aiomysql
import asyncio
from functools import wraps
import logging
from dataclasses import dataclass
from configs.mysql_conf import mysql_settings

logger = logging.getLogger(__name__)

T = TypeVar('T', bound='MySQLConnector')

@dataclass
class MySQLConnectionInfo:
    """MySQL 연결 정보"""
    endpoint: str
    port: int
    user: str
    password: str
    database: Optional[str] = None

    @classmethod
    def from_mongo_doc(cls, doc: Dict[str, Any]) -> 'MySQLConnectionInfo':
        """MongoDB 문서에서 연결 정보 생성"""
        return cls(
            endpoint=doc['endpoint'],
            port=doc['port'],
            user=doc['master_username'],
            password=doc.get('password', ''),
            database=None
        )

def ensure_connection(func):
    """데이터베이스 연결을 보장하는 데코레이터"""
    @wraps(func)
    async def wrapper(cls, conn_info: MySQLConnectionInfo, *args, **kwargs):
        pool_key = cls._get_pool_key(conn_info)
        pool = cls._pools.get(pool_key)
        if not pool or not await cls._is_connected(pool):
            await cls._connect(conn_info)
        try:
            return await func(cls, conn_info, *args, **kwargs)
        except Exception as e:
            logger.error(f"MySQL 연결 오류 발생: {e}")
            await cls.reconnect(conn_info)
            return await func(cls, conn_info, *args, **kwargs)
    return wrapper

class MySQLConnector:
    _pools: Dict[str, aiomysql.Pool] = {}
    _connection_attempts = 0
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY = 1

    @classmethod
    def _get_pool_key(cls, conn_info: MySQLConnectionInfo) -> str:
        """연결 풀 키 생성"""
        return f"{conn_info.endpoint}:{conn_info.port}"

    @classmethod
    async def initialize(cls: Type[T], conn_info: MySQLConnectionInfo) -> None:
        """MySQL 커넥터 초기화"""
        pool_key = cls._get_pool_key(conn_info)
        if pool_key not in cls._pools:
            await cls._connect(conn_info)

    @classmethod
    async def _connect(cls: Type[T], conn_info: MySQLConnectionInfo) -> None:
        """MySQL 연결 수행"""
        pool_key = cls._get_pool_key(conn_info)
        settings = mysql_settings.default_connection_args
        last_error = None

        while cls._connection_attempts < cls.MAX_RETRY_ATTEMPTS:
            try:
                pool = await aiomysql.create_pool(
                    host=conn_info.endpoint,
                    port=conn_info.port,
                    user=conn_info.user,
                    password=conn_info.password,
                    db=conn_info.database,
                    **settings
                )

                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT 1")
                        await cursor.fetchone()

                cls._pools[pool_key] = pool
                cls._connection_attempts = 0
                logger.info(f"MySQL 연결 성공: {conn_info.endpoint}:{conn_info.port}")
                return

            except Exception as e:
                last_error = e
                cls._connection_attempts += 1
                if cls._connection_attempts >= cls.MAX_RETRY_ATTEMPTS:
                    logger.error(f"MySQL 연결 실패 (최대 재시도 횟수 초과): {e}")
                    raise ConnectionError(f"MySQL 연결 실패: {str(last_error)}")

                logger.warning(
                    f"MySQL 연결 재시도 중... ({cls._connection_attempts}/{cls.MAX_RETRY_ATTEMPTS})"
                )
                await asyncio.sleep(cls.RETRY_DELAY)

    @classmethod
    async def _is_connected(cls, pool: aiomysql.Pool) -> bool:
        """연결 상태 확인"""
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
                    await cursor.fetchone()
            return True
        except Exception as e:
            logger.error(f"MySQL 연결 확인 실패: {e}")
            return False

    @classmethod
    async def get_connection(cls, conn_info: MySQLConnectionInfo):
        """연결 객체 반환"""
        pool_key = cls._get_pool_key(conn_info)
        pool = cls._pools.get(pool_key)
        if not pool:
            await cls._connect(conn_info)
            pool = cls._pools[pool_key]
        return await pool.acquire()

    @classmethod
    async def reconnect(cls: Type[T], conn_info: MySQLConnectionInfo) -> None:
        """연결 재시도"""
        await cls.close(conn_info)
        await cls._connect(conn_info)

    @classmethod
    async def close(cls: Type[T], conn_info: MySQLConnectionInfo) -> None:
        """특정 연결 종료"""
        pool_key = cls._get_pool_key(conn_info)
        if pool := cls._pools.get(pool_key):
            try:
                pool.close()
                await pool.wait_closed()
                del cls._pools[pool_key]
                logger.info(f"MySQL 연결 종료 완료: {conn_info.endpoint}:{conn_info.port}")
            except Exception as e:
                logger.error(f"MySQL 연결 종료 중 오류 발생: {e}")

    @classmethod
    async def close_all(cls: Type[T]) -> None:
        """모든 연결 종료"""
        for pool_key, pool in cls._pools.items():
            try:
                pool.close()
                await pool.wait_closed()
            except Exception as e:
                logger.error(f"연결 종료 중 오류 발생 ({pool_key}): {e}")
        cls._pools.clear()
        logger.info("모든 MySQL 연결이 종료되었습니다.")