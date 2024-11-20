from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo.errors import (
    ConnectionFailure,
    ServerSelectionTimeoutError,
    OperationFailure,
    CollectionInvalid,
    InvalidName
)
from contextlib import asynccontextmanager
from typing import TypeVar, Type, Optional, Dict, Any, AsyncGenerator, Union, List
from configs.mongo_conf import MONGODB_URI, MONGODB_DB_NAME
import logging
import asyncio
from functools import wraps
from datetime import datetime

logger = logging.getLogger(__name__)

# 타입 변수 정의
T = TypeVar('T', bound='MongoDBConnector')


def ensure_connection(func):
    """데이터베이스 연결을 보장하는 데코레이터"""

    @wraps(func)
    async def wrapper(cls, *args, **kwargs):
        if cls._client is None or not await cls._is_connected():
            await cls._connect()
        try:
            return await func(cls, *args, **kwargs)
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"MongoDB 연결 오류 발생: {e}")
            await cls.reconnect()
            return await func(cls, *args, **kwargs)

    return wrapper


class MongoDBConnector:
    _client: Optional[AsyncIOMotorClient] = None
    _db: Optional[AsyncIOMotorDatabase] = None
    _connection_attempts = 0
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY = 1  # seconds

    @classmethod
    def _get_default_settings(cls) -> Dict[str, Any]:
        """기본 MongoDB 설정을 반환"""
        return {
            'tls': False,
            'tlsAllowInvalidCertificates': True,
            'tlsAllowInvalidHostnames': True,
            'directConnection': False,
            'serverSelectionTimeoutMS': 5000,
            'maxPoolSize': 100,
            'minPoolSize': 10,
            'maxIdleTimeMS': 30000,
            'retryWrites': True,
            'connectTimeoutMS': 20000,
            'socketTimeoutMS': 35000,
            'waitQueueTimeoutMS': 10000
        }

    @classmethod
    async def validate_settings(cls, **kwargs) -> bool:
        """
        MongoDB 설정 유효성 검증

        Args:
            **kwargs: 검증할 설정값들

        Returns:
            bool: 설정 유효성 여부
        """
        required_settings = {'maxPoolSize', 'minPoolSize', 'maxIdleTimeMS'}

        try:
            settings = {**cls._get_default_settings(), **kwargs}

            # 필수 설정 확인
            missing_settings = required_settings - set(settings.keys())
            if missing_settings:
                logger.error(f"필수 설정 누락: {missing_settings}")
                return False

            # 값 범위 검증
            if not (0 < settings['minPoolSize'] <= settings['maxPoolSize']):
                logger.error("잘못된 풀 크기 설정")
                return False

            if settings['maxIdleTimeMS'] < 1000:
                logger.error("너무 짧은 idle 시간")
                return False

            if settings['serverSelectionTimeoutMS'] < 1000:
                logger.error("너무 짧은 서버 선택 타임아웃")
                return False

            return True

        except Exception as e:
            logger.error(f"설정 검증 중 오류 발생: {e}")
            return False

    @classmethod
    async def initialize(cls: Type[T], **kwargs) -> None:
        """
        커넥터 초기화 및 연결 설정

        Args:
            **kwargs: MongoDB 클라이언트 추가 설정

        Raises:
            ValueError: 설정이 유효하지 않은 경우
        """
        if not await cls.validate_settings(**kwargs):
            raise ValueError("유효하지 않은 MongoDB 설정")

        if cls._client is None:
            await cls._connect(**kwargs)

    @classmethod
    @asynccontextmanager
    async def get_session(cls: Type[T]) -> AsyncGenerator:
        """
        MongoDB 세션 컨텍스트 매니저

        Yields:
            ClientSession: MongoDB 세션

        Raises:
            ConnectionFailure: 연결 실패 시
        """
        if cls._client is None:
            await cls._connect()

        async with cls._client.start_session() as session:
            try:
                yield session
            except Exception as e:
                logger.error(f"세션 작업 중 오류 발생: {e}")
                await session.abort_transaction()
                raise

    @classmethod
    @ensure_connection
    async def get_database(cls: Type[T]) -> AsyncIOMotorDatabase:
        """
        데이터베이스 인스턴스 반환

        Returns:
            AsyncIOMotorDatabase: MongoDB 데이터베이스 객체
        """
        return cls._db

    @classmethod
    @ensure_connection
    async def get_collection(cls: Type[T], name: str) -> AsyncIOMotorCollection:
        """
        컬렉션 객체 반환

        Args:
            name: 컬렉션 이름

        Returns:
            AsyncIOMotorCollection: MongoDB 컬렉션 객체
        """
        return cls._db[name]

    @classmethod
    async def reconnect(cls: Type[T]) -> None:
        """연결 재시도"""
        await cls.close()
        await cls._connect()

    @classmethod
    async def close(cls: Type[T]) -> None:
        """연결 종료 및 리소스 정리"""
        if cls._client:
            try:
                cls._client.close()
                await cls._client.wait_closed()
                logger.info("MongoDB 연결이 정상적으로 종료되었습니다.")
            except Exception as e:
                logger.error(f"MongoDB 연결 종료 중 오류 발생: {e}")
            finally:
                cls._client = None
                cls._db = None
                cls._connection_attempts = 0

    @classmethod
    async def _connect(cls: Type[T], **kwargs) -> None:
        """
        MongoDB 연결 수행

        Args:
            **kwargs: MongoDB 클라이언트 추가 설정

        Raises:
            ConnectionFailure: 최대 재시도 횟수 초과 시
        """
        settings = {**cls._get_default_settings(), **kwargs}
        last_error = None

        while cls._connection_attempts < cls.MAX_RETRY_ATTEMPTS:
            try:
                cls._client = AsyncIOMotorClient(MONGODB_URI, **settings)
                cls._db = cls._client[MONGODB_DB_NAME]

                # 연결 테스트
                await cls._client.admin.command('ping')
                logger.info("MongoDB에 성공적으로 연결되었습니다.")

                # 연결 상태 로깅
                server_info = await cls._client.server_info()
                logger.info(f"MongoDB 버전: {server_info.get('version')}")

                cls._connection_attempts = 0  # 연결 성공 시 카운터 리셋
                return

            except Exception as e:
                last_error = e
                cls._connection_attempts += 1
                if cls._connection_attempts >= cls.MAX_RETRY_ATTEMPTS:
                    cls._client = None
                    cls._db = None
                    logger.error(f"MongoDB 연결 실패 (최대 재시도 횟수 초과): {e}")
                    raise ConnectionFailure(f"MongoDB 연결 실패: {str(last_error)}")

                logger.warning(
                    f"MongoDB 연결 재시도 중... "
                    f"({cls._connection_attempts}/{cls.MAX_RETRY_ATTEMPTS})"
                )
                await asyncio.sleep(cls.RETRY_DELAY)

    @classmethod
    async def _is_connected(cls: Type[T]) -> bool:
        """
        연결 상태 확인

        Returns:
            bool: 연결 상태
        """
        try:
            await cls._client.admin.command('ping')
            return True
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"MongoDB 연결 확인 실패: {e}")
            return False
        except Exception as e:
            logger.error(f"예상치 못한 오류 발생: {e}")
            return False

    @classmethod
    @ensure_connection
    async def create_collection(cls: Type[T], name: str, **kwargs) -> None:
        """
        새로운 컬렉션 생성

        Args:
            name: 컬렉션 이름
            **kwargs: 컬렉션 생성 옵션

        Raises:
            CollectionInvalid: 컬렉션 생성 실패 시
        """
        try:
            await cls._db.create_collection(name, **kwargs)
            logger.info(f"컬렉션 생성 완료: {name}")
        except CollectionInvalid as e:
            logger.error(f"컬렉션 생성 실패: {e}")
            raise
        except Exception as e:
            logger.error(f"예상치 못한 오류 발생: {e}")
            raise

    @classmethod
    @ensure_connection
    async def create_index(
            cls: Type[T],
            collection: str,
            keys: Dict[str, Any],
            **kwargs
    ) -> str:
        """
        인덱스 생성

        Args:
            collection: 컬렉션 이름
            keys: 인덱스 키
            **kwargs: 인덱스 생성 옵션

        Returns:
            str: 생성된 인덱스 이름

        Raises:
            OperationFailure: 인덱스 생성 실패 시
        """
        try:
            result = await cls._db[collection].create_index(keys, **kwargs)
            logger.info(f"인덱스 생성 완료: {result}")
            return result
        except OperationFailure as e:
            logger.error(f"인덱스 생성 실패: {e}")
            raise
        except Exception as e:
            logger.error(f"예상치 못한 오류 발생: {e}")
            raise

    @classmethod
    @ensure_connection
    async def bulk_write(
            cls: Type[T],
            collection: str,
            operations: List[Dict[str, Any]],
            ordered: bool = True
    ) -> Dict[str, Any]:
        """
        벌크 작업 수행

        Args:
            collection: 컬렉션 이름
            operations: 벌크 작업 목록
            ordered: 순서 보장 여부

        Returns:
            Dict[str, Any]: 벌크 작업 결과

        Raises:
            OperationFailure: 벌크 작업 실패 시
        """
        try:
            start_time = datetime.now()
            result = await cls._db[collection].bulk_write(
                operations,
                ordered=ordered
            )
            execution_time = (datetime.now() - start_time).total_seconds()

            result_info = {
                'inserted_count': result.inserted_count,
                'modified_count': result.modified_count,
                'deleted_count': result.deleted_count,
                'execution_time': execution_time
            }

            logger.info(f"벌크 작업 완료: {result_info}")
            return result_info

        except OperationFailure as e:
            logger.error(f"벌크 작업 실패: {e}")
            raise
        except Exception as e:
            logger.error(f"예상치 못한 오류 발생: {e}")
            raise

    @classmethod
    async def get_collection_stats(cls: Type[T], collection: str) -> Dict[str, Any]:
        """
        컬렉션 상태 정보 조회

        Args:
            collection: 컬렉션 이름

        Returns:
            Dict[str, Any]: 컬렉션 상태 정보

        Raises:
            OperationFailure: 상태 조회 실패 시
        """
        try:
            stats = await cls._db.command('collStats', collection)
            return {
                'document_count': stats.get('count', 0),
                'size': stats.get('size', 0),
                'avg_doc_size': stats.get('avgObjSize', 0),
                'storage_size': stats.get('storageSize', 0),
                'indexes': stats.get('nindexes', 0),
                'total_index_size': stats.get('totalIndexSize', 0),
                'ok': stats.get('ok', 0)
            }
        except OperationFailure as e:
            logger.error(f"컬렉션 상태 조회 실패: {e}")
            raise
        except Exception as e:
            logger.error(f"예상치 못한 오류 발생: {e}")
            raise

    @classmethod
    async def cleanup_idle_connections(cls: Type[T]) -> None:
        """
        유휴 연결 정리

        Raises:
            OperationFailure: 연결 정리 실패 시
        """
        if cls._client:
            try:
                # 기존 연결 정보 조회
                server_status = await cls._client.admin.command('serverStatus')
                current_connections = server_status.get('connections', {})

                if current_connections.get('current', 0) > cls._get_default_settings()['minPoolSize']:
                    # 유휴 연결 정리
                    await cls._client.admin.command('connPoolSync')

                    # 정리 후 상태 확인
                    after_status = await cls._client.admin.command('serverStatus')
                    after_connections = after_status.get('connections', {})

                    logger.info(
                        f"유휴 연결 정리 완료: "
                        f"{current_connections.get('current')} -> "
                        f"{after_connections.get('current')}"
                    )

            except OperationFailure as e:
                logger.error(f"유휴 연결 정리 실패: {e}")
                raise
            except Exception as e:
                logger.error(f"예상치 못한 오류 발생: {e}")
                raise
