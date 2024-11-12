from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, AsyncGenerator
from configs.mongo_conf import MONGODB_URI, MONGODB_DB_NAME
import logging
import asyncio
from functools import wraps

logger = logging.getLogger(__name__)


def ensure_connection(func):
    """데이터베이스 연결을 보장하는 데코레이터"""

    @wraps(func)
    async def wrapper(cls, *args, **kwargs):
        if cls._client is None or not await cls._is_connected():
            await cls._connect()
        return await func(cls, *args, **kwargs)

    return wrapper


class MongoDBConnector:
    _client: Optional[AsyncIOMotorClient] = None
    _db: Optional[AsyncIOMotorDatabase] = None
    _connection_attempts = 0
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY = 1  # seconds

    @classmethod
    async def initialize(cls, **kwargs) -> None:
        """
        커넥터 초기화 및 연결 설정

        Args:
            **kwargs: MongoDB 클라이언트 추가 설정
        """
        if cls._client is None:
            await cls._connect(**kwargs)

    @classmethod
    @asynccontextmanager
    async def get_session(cls) -> AsyncGenerator:
        """
        MongoDB 세션 컨텍스트 매니저

        Yields:
            ClientSession: MongoDB 세션
        """
        if cls._client is None:
            await cls._connect()

        async with cls._client.start_session() as session:
            try:
                yield session
            except Exception as e:
                await session.abort_transaction()
                raise e

    @classmethod
    @ensure_connection
    async def get_database(cls) -> AsyncIOMotorDatabase:
        """데이터베이스 인스턴스 반환"""
        return cls._db

    @classmethod
    async def reconnect(cls) -> None:
        """연결 재시도"""
        await cls.close()
        await cls._connect()

    @classmethod
    async def close(cls) -> None:
        """연결 종료 및 리소스 정리"""
        if cls._client:
            cls._client.close()
            await cls._client.wait_closed()  # 모든 작업이 완료될 때까지 대기
        cls._client = None
        cls._db = None
        cls._connection_attempts = 0

    @classmethod
    async def _connect(cls, **kwargs) -> None:
        """
        MongoDB 연결 수행

        Args:
            **kwargs: MongoDB 클라이언트 추가 설정
        """
        while cls._connection_attempts < cls.MAX_RETRY_ATTEMPTS:
            try:
                default_settings = {
                    'tls': True,
                    'tlsAllowInvalidCertificates': True,
                    'tlsAllowInvalidHostnames': True,
                    'directConnection': False,
                    'serverSelectionTimeoutMS': 5000,
                    'maxPoolSize': 100,  # 커넥션 풀 크기
                    'minPoolSize': 10,  # 최소 커넥션 유지 수
                    'maxIdleTimeMS': 30000,  # 최대 유휴 시간
                    'retryWrites': True,  # 쓰기 작업 재시도
                }
                settings = {**default_settings, **kwargs}

                cls._client = AsyncIOMotorClient(MONGODB_URI, **settings)
                cls._db = cls._client[MONGODB_DB_NAME]

                # 연결 테스트
                await cls._client.admin.command('ping')
                logger.info("MongoDB에 성공적으로 연결되었습니다.")
                cls._connection_attempts = 0  # 연결 성공 시 카운터 리셋
                return

            except Exception as e:
                cls._connection_attempts += 1
                if cls._connection_attempts >= cls.MAX_RETRY_ATTEMPTS:
                    cls._client = None
                    cls._db = None
                    logger.error(f"MongoDB 연결 실패 (최대 재시도 횟수 초과): {e}")
                    raise

                logger.warning(f"MongoDB 연결 재시도 중... ({cls._connection_attempts}/{cls.MAX_RETRY_ATTEMPTS})")
                await asyncio.sleep(cls.RETRY_DELAY)

    @classmethod
    async def _is_connected(cls) -> bool:
        """연결 상태 확인"""
        try:
            await cls._client.admin.command('ping')
            return True
        except Exception:
            return False

    @classmethod
    @ensure_connection
    async def create_collection(cls, name: str, **kwargs) -> None:
        """
        새로운 컬렉션 생성

        Args:
            name: 컬렉션 이름
            **kwargs: 컬렉션 생성 옵션
        """
        try:
            await cls._db.create_collection(name, **kwargs)
            logger.info(f"컬렉션 생성 완료: {name}")
        except Exception as e:
            logger.error(f"컬렉션 생성 실패: {e}")
            raise

    @classmethod
    @ensure_connection
    async def create_index(cls, collection: str, keys: Dict[str, Any], **kwargs) -> str:
        """
        인덱스 생성

        Args:
            collection: 컬렉션 이름
            keys: 인덱스 키
            **kwargs: 인덱스 생성 옵션

        Returns:
            str: 생성된 인덱스 이름
        """
        try:
            result = await cls._db[collection].create_index(keys, **kwargs)
            logger.info(f"인덱스 생성 완료: {result}")
            return result
        except Exception as e:
            logger.error(f"인덱스 생성 실패: {e}")
            raise
