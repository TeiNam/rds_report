# modules/instance_fetcher.py

from typing import List, Dict, Optional, Literal
import logging
import pytz
from motor.motor_asyncio import AsyncIOMotorDatabase
from modules.mongodb_connector import MongoDBConnector
from pydantic import BaseModel, Field
from collections import defaultdict

logger = logging.getLogger(__name__)

class InstanceInfo(BaseModel):
   """인스턴스 정보를 위한 Pydantic 모델"""
   account_id: str = Field(..., alias='AccountId')
   region: str = Field(..., alias='Region')
   instance_identifier: str = Field(..., alias='DBInstanceIdentifier')
   tags: Dict[str, str] = Field(..., alias='Tags')
   timestamp: str

   class Config:
       populate_by_name = True
       arbitrary_types_allowed = True

class AccountInfo(BaseModel):
   """계정 정보를 위한 Pydantic 모델"""
   account_id: str
   instances: List[InstanceInfo]
   instance_count: int

class InstanceQueryResult(BaseModel):
   """인스턴스 조회 결과를 위한 Pydantic 모델"""
   accounts: List[AccountInfo]
   total_instances: int
   latest_date: Optional[str] = None
   env: str  # env 필드 추가

   class Config:
       populate_by_name = True

class InstanceFetcher:
   """데이터베이스에서 RDS 인스턴스 정보를 조회하는 클래스"""

   def __init__(self):
       self.timezone = pytz.timezone('Asia/Seoul')
       self.collection_name = "aws_rds_instance_all_stat"

   async def _get_database(self) -> AsyncIOMotorDatabase:
       """데이터베이스 연결을 가져옵니다."""
       return await MongoDBConnector.get_database()

   async def get_latest_date_pipeline(self, start_date: str, end_date: str) -> List[Dict]:
       """주어진 기간 내의 최신 날짜를 찾는 파이프라인을 반환합니다."""
       return [
           {
               "$addFields": {
                   "date": {"$substr": ["$timestamp", 0, 10]}
               }
           },
           {
               "$match": {
                   "date": {
                       "$gte": start_date,
                       "$lte": end_date
                   }
               }
           },
           {
               "$sort": {"date": -1, "timestamp": -1}
           },
           {
               "$group": {
                   "_id": "$date",
                   "docs": {"$first": "$$ROOT"}
               }
           },
           {
               "$sort": {"_id": -1}
           },
           {"$limit": 1}
       ]

   async def get_instances_pipeline(self, latest_date: str, env: str) -> List[Dict]:
       """특정 날짜와 환경에 대한 인스턴스 조회 파이프라인을 반환합니다."""
       return [
           {
               "$addFields": {
                   "date": {"$substr": ["$timestamp", 0, 10]}
               }
           },
           {
               "$match": {"date": latest_date}
           },
           {"$unwind": "$instances"},
           {
               "$match": {
                   "$or": [
                       {"instances.Tags.env": env},
                       {"instances.Tags.Environment": env}
                   ]
               }
           }
       ]

   def _group_instances_by_account(self, instances: List[InstanceInfo]) -> List[AccountInfo]:
       """인스턴스를 계정별로 그룹화합니다."""
       account_instances = defaultdict(list)

       # 인스턴스를 계정별로 분류
       for instance in instances:
           account_instances[instance.account_id].append(instance)

       # AccountInfo 객체 생성
       accounts = []
       for account_id, instances in account_instances.items():
           account_info = AccountInfo(
               account_id=account_id,
               instances=sorted(instances, key=lambda x: x.instance_identifier),
               instance_count=len(instances)
           )
           accounts.append(account_info)

       # 계정 ID로 정렬하여 반환
       return sorted(accounts, key=lambda x: x.account_id)

   async def get_instance_identifiers(
           self,
           env: Literal['prd', 'dev'],
           start_date: str,
           end_date: str
   ) -> InstanceQueryResult:
       """
       지정된 기간의 마지막 날짜에 해당하는 모든 계정의 특정 환경 인스턴스 정보를 가져옵니다.

       Args:
           env: 환경 구분 ('prd' 또는 'dev')
           start_date: 시작 날짜 (YYYY-MM-DD)
           end_date: 종료 날짜 (YYYY-MM-DD)

       Returns:
           InstanceQueryResult: 계정별로 그룹화된 인스턴스 정보
       """
       try:
           logger.info(f"Fetching {env} instances between {start_date} and {end_date}")

           db = await self._get_database()
           collection = db[self.collection_name]

           # 최신 날짜 찾기
           latest_doc = None
           async for doc in collection.aggregate(await self.get_latest_date_pipeline(start_date, end_date)):
               latest_doc = doc['docs']
               break

           if not latest_doc:
               logger.warning(f"No documents found between {start_date} and {end_date}")
               return InstanceQueryResult(accounts=[], total_instances=0, env=env)  # env 추가

           latest_date = latest_doc['date']
           logger.info(f"Found latest date: {latest_date}")

           # 인스턴스 정보 조회
           instances_list = []
           async for doc in collection.aggregate(await self.get_instances_pipeline(latest_date, env)):
               instance = doc['instances']
               instance_info = InstanceInfo(
                   AccountId=instance['AccountId'],
                   Region=instance['Region'],
                   DBInstanceIdentifier=instance['DBInstanceIdentifier'],
                   Tags=instance['Tags'],
                   timestamp=doc['timestamp']
               )
               instances_list.append(instance_info)

           # 계정별로 그룹화
           grouped_accounts = self._group_instances_by_account(instances_list)
           total_instances = sum(account.instance_count for account in grouped_accounts)

           # 결과 로깅
           if total_instances > 0:
               logger.info(f"Found {total_instances} '{env}' instances from {latest_date}")
               for account in grouped_accounts:
                   logger.info(f"Account {account.account_id}: {account.instance_count} instances")

               for account in grouped_accounts:
                   logger.debug(f"Account: {account.account_id}")
                   for instance in account.instances:
                       logger.debug(
                           f"  - Instance: {instance.instance_identifier} "
                           f"(Region: {instance.region})"
                       )
           else:
               logger.warning(f"No instances found for environment '{env}' at {latest_date}")

           return InstanceQueryResult(
               accounts=grouped_accounts,
               total_instances=total_instances,
               latest_date=latest_date,
               env=env  # env 추가
           )

       except Exception as e:
           logger.error(f"Error fetching instances: {e}", exc_info=True)
           return InstanceQueryResult(accounts=[], total_instances=0, env=env)  # env 추가

   @classmethod
   async def get_instances(
           cls,
           env: Literal['prd', 'dev'],
           start_date: str,
           end_date: str
   ) -> InstanceQueryResult:
       """
       인스턴스 정보를 조회하는 클래스 메서드

       Args:
           env: 환경 구분 ('prd' 또는 'dev')
           start_date: 시작 날짜 (YYYY-MM-DD)
           end_date: 종료 날짜 (YYYY-MM-DD)

       Returns:
           InstanceQueryResult: 계정별로 그룹화된 인스턴스 정보
       """
       fetcher = cls()
       return await fetcher.get_instance_identifiers(env, start_date, end_date)


# 테스트 코드
if __name__ == "__main__":
    import asyncio
    import argparse
    from datetime import datetime, timedelta

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Fetch RDS instances by environment')
    parser.add_argument(
        '--env',
        type=str,
        choices=['prd', 'dev'],
        default='prd',
        help='Environment to fetch instances for'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date (YYYY-MM-DD)'
    )

    args = parser.parse_args()

    # 기본값 설정
    if not args.start_date:
        args.start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if not args.end_date:
        args.end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"Fetching {args.env} instances from {args.start_date} to {args.end_date}")


    async def main():
        result = await InstanceFetcher.get_instances(
            env=args.env,
            start_date=args.start_date,
            end_date=args.end_date
        )

        print(f"\nFound total {result.total_instances} {args.env} instances")
        if result.latest_date:
            print(f"Latest date: {result.latest_date}")

        for account in result.accounts:
            print(f"\nAccount: {account.account_id} ({account.instance_count} instances)")
            for instance in account.instances:
                print(f"  - Instance: {instance.instance_identifier}")
                print(f"    Region: {instance.region}")
                print(f"    Tags: {instance.tags}")


    asyncio.run(main())