# collectors/rds_instance_collector.py

import os
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from motor.motor_asyncio import AsyncIOMotorClient
from modules.aws_session_manager import AWSSessionManager


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

        # AWS 세션 관리자 초기화
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

    async def get_rds_instances(self, session_manager: AWSSessionManager, account_id: str) -> List[dict]:
        """특정 계정의 RDS 인스턴스 정보 수집"""
        instances = []
        instance_info = session_manager.get_instance_info()
        if not instance_info:
            logger.error(f"No instance information available for account {account_id}")
            return instances

        account_instances = next((acc for acc in instance_info.accounts if acc.account_id == account_id), None)
        if not account_instances:
            logger.error(f"No instances found for account {account_id}")
            return instances

        for instance in account_instances.instances:
            try:
                rds = session_manager.get_client('rds', account_id, instance.region)
                response = rds.describe_db_instances(
                    DBInstanceIdentifier=instance.instance_identifier
                )

                for db in response['DBInstances']:
                    instance_data = {
                        'AccountId': account_id,
                        'Region': instance.region,
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
                        'Tags': instance.tags
                    }
                    instances.append(instance_data)

            except ClientError as e:
                logger.error(
                    f"Error fetching RDS instance {instance.instance_identifier} "
                    f"in account {account_id}, region {instance.region}: {e}"
                )

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

    async def run(self, env: str = 'prd', date: Optional[str] = None) -> None:
        """RDS 인스턴스 수집 실행"""
        try:
            # AWS 세션 매니저 초기화
            await self.session_manager.initialize(env=env, end_date=date)
            instance_info = self.session_manager.get_instance_info()

            if not instance_info or not instance_info.accounts:
                logger.warning(f"No accounts found for environment: {env}")
                return

            logger.info(
                f"Starting RDS instance collection for {len(instance_info.accounts)} accounts "
                f"in {env} environment"
            )

            for account in instance_info.accounts:
                try:
                    account_id = account.account_id
                    logger.info(f"Processing account: {account_id}")

                    instances = await self.get_rds_instances(self.session_manager, account_id)
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