# modules/aws_account_module.py

import logging
from datetime import datetime
from typing import List, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from models.aws_account import (
    AWSAccountCreate,
    AWSAccountInDB,
    AWSAccountUpdate,
    EnvironmentType
)
from configs.mongo_conf import mongo_settings

logger = logging.getLogger(__name__)


class AWSAccountModule:
    """AWS 계정 정보 관리 모듈"""

    def __init__(self):
        self.client = AsyncIOMotorClient(mongo_settings.MONGODB_URI)
        self.db = self.client[mongo_settings.MONGODB_DB_NAME]
        self.collection = self.db[mongo_settings.MONGO_AWS_ACCOUNT_COLLECTION]

    async def create_account(self, account: AWSAccountCreate) -> AWSAccountInDB:
        """새로운 AWS 계정 정보 생성"""
        # 중복 계정 확인
        existing = await self.collection.find_one({"aws_account_id": account.aws_account_id})
        if existing:
            raise ValueError(f"Account with ID {account.aws_account_id} already exists")

        account_in_db = AWSAccountInDB(
            **account.dict()
        )

        await self.collection.insert_one(account_in_db.dict())
        return account_in_db

    async def get_account(self, account_id: str) -> Optional[AWSAccountInDB]:
        """특정 AWS 계정 정보 조회"""
        account_data = await self.collection.find_one({"aws_account_id": account_id})
        if account_data:
            return AWSAccountInDB(**account_data)
        return None

    async def get_all_accounts(self) -> List[AWSAccountInDB]:
        """모든 AWS 계정 정보 조회"""
        accounts = []
        async for account in self.collection.find():
            accounts.append(AWSAccountInDB(**account))
        return accounts

    async def update_account(
            self, account_id: str, account_update: AWSAccountUpdate
    ) -> Optional[AWSAccountInDB]:
        """AWS 계정 정보 업데이트"""
        update_data = account_update.dict(exclude_unset=True)
        if not update_data:
            return await self.get_account(account_id)

        update_data["update_at"] = datetime.utcnow()

        result = await self.collection.update_one(
            {"aws_account_id": account_id},
            {"$set": update_data}
        )

        if result.modified_count:
            return await self.get_account(account_id)
        return None

    async def delete_account(self, account_id: str) -> bool:
        """AWS 계정 정보 삭제"""
        result = await self.collection.delete_one({"aws_account_id": account_id})
        return result.deleted_count > 0

    async def get_accounts_by_environment(self, env: EnvironmentType) -> List[AWSAccountInDB]:
        """특정 환경에 속한 AWS 계정 정보 조회

        Args:
            env: 환경 구분 (prd/dev)

        Returns:
            해당 환경에 속한 계정 목록 (both로 설정된 계정도 포함)
        """
        query = {
            "$or": [
                {"environment_type": env},
                {"environment_type": EnvironmentType.BOTH}
            ]
        }

        accounts = []
        async for account in self.collection.find(query):
            accounts.append(AWSAccountInDB(**account))
        return sorted(accounts, key=lambda x: x.aws_account_name)