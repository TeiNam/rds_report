# collectors/rds_instance_collector.py

import os
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from motor.motor_asyncio import AsyncIOMotorClient

from modules.aws_session_manager import AWSSessionManager, EnvironmentType

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

        # AWS 설정
        aws_regions_str = os.getenv('AWS_REGIONS')
        if not aws_regions_str:
            raise ValueError("AWS_REGIONS is not set")
        try:
            self.aws_regions = json.loads(aws_regions_str)
        except json.JSONDecodeError:
            raise ValueError("AWS_REGIONS is not a valid JSON string")

        # 시간대 설정
        self.kst = timezone(timedelta(hours=9))
        self.datetime_format = "%Y-%m-%d %H:%M:%S KST"

        # AWS 세션 관리자
        self.session_manager = AWSSessionManager()

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

    async def get_rds_instances(self, account_id: str) -> List[dict]:
        """특정 계정의 RDS 인스턴스 정보 수집"""
        instances = []

        for region in self.aws_regions:
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

            except ClientError as e:
                logger.error(f"Error fetching RDS instances in account {account_id}, region {region}: {e}")

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
            # 계정 리스트 정의
            accounts = {
                'prd': ['488659748805', '578868370045', '790631726648',
                        '732250966717', '518026839586', '897374448634'],
                'dev': ['708010261224', '058264293746', '637423179433']
            }

            target_accounts = accounts.get(env, [])
            if not target_accounts:
                logger.warning(f"No accounts defined for environment: {env}")
                return

            logger.info(f"Starting RDS instance collection for {len(target_accounts)} "
                        f"accounts in {env} environment")

            # 환경에 따른 세션 초기화
            if self.session_manager.environment == EnvironmentType.LOCAL:
                # 로컬 환경에서는 각 계정별로 SSO 세션 생성
                for account_id in target_accounts:
                    try:
                        session = self.session_manager._get_sso_session(account_id)
                        self.session_manager._sessions[account_id] = session
                    except Exception as e:
                        logger.error(f"Failed to create SSO session for account {account_id}: {e}")
                        continue
            else:
                # EC2/EKS 환경에서는 각 계정별로 Role 세션 생성
                for account_id in target_accounts:
                    try:
                        session = self.session_manager._get_role_session(account_id)
                        self.session_manager._sessions[account_id] = session
                    except Exception as e:
                        logger.error(f"Failed to create role session for account {account_id}: {e}")
                        continue

            # 각 계정별로 RDS 인스턴스 수집
            for account_id in target_accounts:
                try:
                    logger.info(f"Processing account: {account_id}")

                    instances = await self.get_rds_instances(account_id)
                    if instances:
                        await self.save_to_mongodb(instances, account_id)
                    else:
                        logger.warning(f"No RDS instances found for account {account_id}")

                except Exception as e:
                    logger.exception(f"Unexpected error processing account {account_id}: {str(e)}")

        except Exception as e:
            logger.exception(f"Failed to run RDS instance collection: {str(e)}")
            raise


async def main():
    collector = RDSInstanceCollector()
    await collector.run()


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())