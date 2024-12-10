from typing import Optional, Dict, Any, NamedTuple
import logging
import asyncio
import getpass
from datetime import datetime
from modules.mongodb_connector import MongoDBConnector
from modules.mysql_connector import MySQLConnector, MySQLConnectionInfo
from configs.mongo_conf import mongo_settings
from bson.objectid import ObjectId

logger = logging.getLogger(__name__)


class DBCredential(NamedTuple):
    """DB 접속 정보"""
    instance_id: str
    env: str
    default_db: str
    primary_endpoint: str
    secondary_endpoint: str
    port: int
    master_user: str
    password: str
    use_yn: str


class DBCredentialsManager:
    def __init__(self):
        self.collection_name = mongo_settings.MONGO_DB_CREDENTIALS_COLLECTION
        self._collection = None
        self._cached_credentials = {}
        self._mysql_connections = {}

    async def _get_collection(self):
        """MongoDB 컬렉션 가져오기"""
        if self._collection is None:
            self._collection = await MongoDBConnector.get_collection(
                self.collection_name
            )
        return self._collection

    async def get_credential(
            self,
            instance_id: str,
            use_secondary: bool = False,
            prompt_if_missing: bool = True,
            store_if_input: bool = False
    ) -> Optional[DBCredential]:
        """
        DB 접속 정보 조회 및 MySQL 연결 생성

        Args:
            instance_id: RDS 인스턴스 ID
            use_secondary: 보조 엔드포인트 사용 여부
            prompt_if_missing: 패스워드가 없을 경우 입력 요청 여부
            store_if_input: 입력받은 패스워드 저장 여부

        Returns:
            Optional[DBCredential]: DB 접속 정보
        """
        try:
            # 캐시 확인
            if instance_id in self._cached_credentials:
                credential = self._cached_credentials[instance_id]
                await self._ensure_mysql_connection(credential, use_secondary)
                return credential

            # DB에서 조회
            collection = await self._get_collection()
            credential_doc = await collection.find_one(
                {
                    "instance_id": instance_id,
                    "use_yn": "Y"
                }
            )

            if credential_doc:
                credential = DBCredential(
                    instance_id=credential_doc['instance_id'],
                    env=credential_doc['env'],
                    default_db=credential_doc['default_db'],
                    primary_endpoint=credential_doc['primary_endpoint'],
                    secondary_endpoint=credential_doc['secondary_endpoint'],
                    port=credential_doc['port'],
                    master_user=credential_doc['master_user'],
                    password=credential_doc['password'],
                    use_yn=credential_doc['use_yn']
                )

                self._cached_credentials[instance_id] = credential
                await self._ensure_mysql_connection(credential, use_secondary)
                return credential

            return None

        except Exception as e:
            logger.error(f"접속 정보 조회 실패 (인스턴스: {instance_id}): {str(e)}")
            raise

    async def _ensure_mysql_connection(self, credential: DBCredential, use_secondary: bool = False):
        """MySQL 연결 확보"""
        connection_key = f"{credential.instance_id}_{use_secondary}"

        if connection_key not in self._mysql_connections:
            endpoint = credential.secondary_endpoint if use_secondary else credential.primary_endpoint

            connection_info = MySQLConnectionInfo(
                endpoint=endpoint,
                port=credential.port,
                user=credential.master_user,
                password=credential.password,
                database=credential.default_db
            )

            try:
                await MySQLConnector.initialize(connection_info)
                self._mysql_connections[connection_key] = connection_info
            except Exception as e:
                logger.error(f"MySQL 연결 실패 ({endpoint}): {str(e)}")
                raise

    async def get_mysql_connection(self, instance_id: str, use_secondary: bool = False):
        """MySQL 연결 객체 반환"""
        credential = await self.get_credential(instance_id, use_secondary)
        if not credential:
            raise ValueError(f"접속 정보를 찾을 수 없습니다: {instance_id}")

        connection_key = f"{instance_id}_{use_secondary}"
        connection_info = self._mysql_connections.get(connection_key)

        if not connection_info:
            raise ValueError(f"MySQL 연결을 찾을 수 없습니다: {instance_id}")

        return await MySQLConnector.get_connection(connection_info)

    async def close_connections(self, instance_id: str):
        """특정 인스턴스의 모든 연결 종료"""
        for use_secondary in [False, True]:
            connection_key = f"{instance_id}_{use_secondary}"
            if connection_info := self._mysql_connections.get(connection_key):
                await MySQLConnector.close(connection_info)
                del self._mysql_connections[connection_key]

    async def close_all_connections(self):
        """모든 MySQL 연결 종료"""
        await MySQLConnector.close_all()
        self._mysql_connections.clear()


# 싱글톤 인스턴스
_credentials_manager = None


def get_credentials_manager() -> DBCredentialsManager:
    """DBCredentialsManager 싱글톤 인스턴스 반환"""
    global _credentials_manager
    if _credentials_manager is None:
        _credentials_manager = DBCredentialsManager()
    return _credentials_manager