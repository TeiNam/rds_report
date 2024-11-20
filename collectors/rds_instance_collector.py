# collectors/rds_instance_collector.py

import os
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from motor.motor_asyncio import AsyncIOMotorClient
from models.aws_account import AWSAccountInDB

from modules.aws_session_manager import AWSSessionManager, EnvironmentType
from modules.aws_account_module import AWSAccountModule

# 로깅 설정
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)


class RDSInstanceCollector:
    """AWS RDS 인스턴스 정보 수집기"""

    def __init__(self):
        # MongoDB 설정
        self.mongodb_uri = os.getenv('MONGODB_URI')
        self.mongodb_db_name = os.getenv('MONGODB_DB_NAME')
        self.collection_name = 'aws_rds_instance_all_stat'

        # 시간대 설정
        self.kst = timezone(timedelta(hours=9))
        self.datetime_format = "%Y-%m-%d %H:%M:%S KST"

        # AWS 세션 관리자
        self.session_manager = AWSSessionManager()

        # AWS 계정 모듈
        self.aws_account_module = AWSAccountModule()

    async def get_target_accounts(self, env: str) -> List[AWSAccountInDB]:
        """지정된 환경의 AWS 계정 정보 조회

        Args:
            env: 환경 구분 ('prd' 또는 'dev')

        Returns:
            계정 정보 목록 (계정ID와 리전 정보 포함)
        """
        try:
            accounts = await self.aws_account_module.get_accounts_by_environment(env)
            if not accounts:
                logger.warning(f"No accounts found for environment: {env}")
            else:
                logger.info(f"Found {len(accounts)} accounts for environment: {env}")
                for account in accounts:
                    logger.info(
                        f"Account: {account.aws_account_id} "
                        f"({account.aws_account_name}), "
                        f"Regions: {', '.join(account.regions)}"
                    )
            return accounts
        except Exception as e:
            logger.error(f"Failed to retrieve account information: {e}")
            raise

    def get_kst_time(self) -> str:
        """현재 KST 시간을 포맷에 맞춰 반환"""
        utc_now = datetime.now(timezone.utc)
        return utc_now.astimezone(self.kst).strftime(self.datetime_format)

    def convert_utc_to_kst(self, utc_time: Optional[datetime]) -> Optional[datetime]:
        """UTC 시간을 KST로 변환"""
        if utc_time is None:
            return None
        return utc_time.replace(tzinfo=timezone.utc).astimezone(self.kst)

    def format_datetime(self, dt: Optional[datetime]) -> Optional[str]:
        """datetime 객체를 문자열로 포맷팅"""
        if dt is None:
            return None
        return dt.strftime(self.datetime_format)

    async def get_rds_instances(self, account: AWSAccountInDB) -> List[dict]:
        """특정 계정의 RDS 인스턴스 정보 수집

        Args:
            account: AWS 계정 정보 (계정ID와 리전 목록 포함)
        """
        instances = []
        account_id = account.aws_account_id

        for region in account.regions:
            try:
                # AWS 세션 매니저를 통해 RDS 클라이언트 생성
                rds = self.session_manager.get_client('rds', account_id, region)
                paginator = rds.get_paginator('describe_db_instances')

                for page in paginator.paginate():
                    for db in page['DBInstances']:
                        instance_data = {
                            'AccountId': account_id,
                            'Region': region,
                            'DBInstanceIdentifier': db.get('DBInstanceIdentifier'),
                            'DBInstanceClass': db.get('DBInstanceClass'),
                            'Engine': db.get('Engine'),
                            'EngineVersion': db.get('EngineVersion'),
                            'Endpoint': {
                                'Address': db.get('Endpoint', {}).get('Address'),
                                'Port': db.get('Endpoint', {}).get('Port')
                            } if db.get('Endpoint') else None,
                            'DBInstanceStatus': db.get('DBInstanceStatus'),
                            'MasterUsername': db.get('MasterUsername'),
                            'AllocatedStorage': db.get('AllocatedStorage'),
                            'AvailabilityZone': db.get('AvailabilityZone'),
                            'MultiAZ': db.get('MultiAZ'),
                            'StorageType': db.get('StorageType'),
                            'InstanceCreateTime': self.format_datetime(
                                self.convert_utc_to_kst(db.get('InstanceCreateTime'))
                            ),
                            'Tags': {tag['Key']: tag['Value'] for tag in db.get('TagList', [])}
                        }
                        instances.append(instance_data)

                logger.debug(f"Found {len(instances)} instances in region {region}")

            except ClientError as e:
                logger.error(f"Error fetching RDS instances in account {account_id}, region {region}: {e}")
                continue

        logger.info(f"Total {len(instances)} instances found in account {account_id}")
        return instances

    async def save_to_mongodb(self, instances: List[dict], account_id: str) -> None:
        """MongoDB에 인스턴스 정보 저장"""
        client = AsyncIOMotorClient(self.mongodb_uri)
        db = client[self.mongodb_db_name]
        collection = db[self.collection_name]

        data = {
            'timestamp': self.get_kst_time(),
            'account_id': account_id,
            'total_instances': len(instances),
            'instances': instances
        }

        try:
            await collection.insert_one(data)
            logger.info(f"Saved {len(instances)} RDS instances for account {account_id}")
        except Exception as e:
            logger.error(f"Error saving to MongoDB for account {account_id}: {e}")
        finally:
            client.close()

    async def run(self, env: str = 'prd') -> None:
        """RDS 인스턴스 수집 실행"""
        try:
            # AWS 계정 정보 조회
            accounts = await self.get_target_accounts(env)
            if not accounts:
                logger.warning(f"No accounts to process for environment: {env}")
                return

            logger.info(f"Starting RDS instance collection for {len(accounts)} accounts")

            # 환경에 따른 세션 초기화
            if self.session_manager.environment == EnvironmentType.LOCAL:
                # 로컬 환경에서는 각 계정별로 SSO 세션 생성
                for account in accounts:
                    try:
                        session = self.session_manager._get_sso_session(account.aws_account_id)
                        self.session_manager._sessions[account.aws_account_id] = session
                    except Exception as e:
                        logger.error(f"Failed to create SSO session for account {account.aws_account_id}: {e}")
                        continue
            else:
                # EC2/EKS 환경에서는 각 계정별로 Role 세션 생성
                for account in accounts:
                    try:
                        session = self.session_manager._get_role_session(account.aws_account_id)
                        self.session_manager._sessions[account.aws_account_id] = session
                    except Exception as e:
                        logger.error(f"Failed to create role session for account {account.aws_account_id}: {e}")
                        continue

            # 각 계정별로 RDS 인스턴스 수집
            for account in accounts:
                try:
                    logger.info(
                        f"Processing account {account.aws_account_id} "
                        f"({account.aws_account_name}) "
                        f"in regions: {', '.join(account.regions)}"
                    )

                    instances = await self.get_rds_instances(account)
                    if instances:
                        await self.save_to_mongodb(instances, account.aws_account_id)
                        logger.info(f"Successfully processed account {account.aws_account_id}")
                    else:
                        logger.warning(f"No RDS instances found for account {account.aws_account_id}")

                except Exception as e:
                    logger.exception(f"Error processing account {account.aws_account_id}: {str(e)}")
                    continue

            logger.info("RDS instance collection completed successfully")

        except Exception as e:
            logger.exception(f"Failed to run RDS instance collection: {str(e)}")
            raise


async def main():
    collector = RDSInstanceCollector()
    await collector.run()


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())