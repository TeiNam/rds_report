import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
import logging

logger = logging.getLogger(__name__)

class SlowQueryLoader:
    def __init__(self):
        self.query_collection_name = mongo_settings.MONGO_SLOW_QUERY_COLLECTION
        self.instance_collection_name = mongo_settings.MONGO_RDS_INSTANCE_ALL_STAT_COLLECTION
        self._query_collection = None
        self._instance_collection = None
        self.target_instances = self._get_target_instances()

    def _get_target_instances(self) -> List[str]:
        """환경 변수에서 대상 인스턴스 목록 가져오기"""
        try:
            instances_str = os.getenv('REPORT_TARGET_INSTANCES', '[]')
            return json.loads(instances_str)
        except json.JSONDecodeError:
            logger.error("REPORT_TARGET_INSTANCES 환경 변수 파싱 실패")
            return []

    async def _get_collections(self) -> Tuple[object, object]:
        if self._query_collection is None:
            self._query_collection = await MongoDBConnector.get_collection(
                self.query_collection_name
            )
        if self._instance_collection is None:
            self._instance_collection = await MongoDBConnector.get_collection(
                self.instance_collection_name
            )
        return self._query_collection, self._instance_collection

    async def _get_instance_info(self, instance_id: str) -> Optional[Dict]:
        try:
            _, instance_collection = await self._get_collections()
            pipeline = [
                {
                    "$match": {
                        "instances": {
                            "$elemMatch": {
                                "DBInstanceIdentifier": instance_id
                            }
                        }
                    }
                },
                {"$sort": {"timestamp": -1}},
                {"$limit": 1},
                {
                    "$project": {
                        "instance": {
                            "$filter": {
                                "input": "$instances",
                                "as": "inst",
                                "cond": {
                                    "$eq": ["$$inst.DBInstanceIdentifier", instance_id]
                                }
                            }
                        }
                    }
                },
                {"$unwind": "$instance"}
            ]

            async for doc in instance_collection.aggregate(pipeline):
                if doc and "instance" in doc:
                    instance_info = doc["instance"]
                    return {
                        "endpoint": instance_info["Endpoint"]["Address"],
                        "port": instance_info["Endpoint"]["Port"],
                        "master_username": instance_info["MasterUsername"]
                    }
            return None
        except Exception as e:
            logger.error(f"인스턴스 정보 조회 실패: {str(e)}")
            return None

    async def get_digest_queries(
            self,
            instance_ids: Optional[Union[str, List[str]]] = None,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ) -> List[Dict]:
        """
        다이제스트 쿼리 조회

        Args:
            instance_ids: 특정 인스턴스 ID 또는 ID 목록 (None일 경우 환경 변수의 대상 인스턴스 사용)
            start_date: 시작 날짜
            end_date: 종료 날짜

        Returns:
            List[Dict]: 다이제스트 쿼리 정보 목록
        """
        try:
            query_collection, _ = await self._get_collections()

            # 인스턴스 ID 처리
            if instance_ids is None:
                target_instances = self.target_instances
            elif isinstance(instance_ids, str):
                target_instances = [instance_ids]
            else:
                target_instances = instance_ids

            if not target_instances:
                logger.warning("대상 인스턴스가 지정되지 않았습니다.")
                return []

            # 기본 match 조건
            match_stage = {"instance_id": {"$in": target_instances}}
            if start_date or end_date:
                match_stage["date"] = {}
                if start_date:
                    match_stage["date"]["$gte"] = start_date.strftime("%Y-%m-%d")
                if end_date:
                    match_stage["date"]["$lte"] = end_date.strftime("%Y-%m-%d")

            pipeline = [
                {"$match": match_stage},
                {"$unwind": "$slow_queries"},
                {
                    "$group": {
                        "_id": {
                            "instance_id": "$instance_id",
                            "digest_query": "$slow_queries.digest_query"
                        },
                        "example_queries": {"$first": "$slow_queries.example_queries"},
                        "users": {"$addToSet": "$slow_queries.users"},
                        "avg_time": {"$first": "$slow_queries.avg_time"}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "instance_id": "$_id.instance_id",
                        "digest_query": "$_id.digest_query",
                        "example_query": {"$arrayElemAt": ["$example_queries", 0]},
                        "user": {"$arrayElemAt": [{"$first": "$users"}, 0]},
                        "avg_time": 1
                    }
                },
                {"$sort": {"avg_time": -1}}
            ]

            results = []
            async for doc in query_collection.aggregate(pipeline, allowDiskUse=True):
                instance_info = await self._get_instance_info(doc["instance_id"])
                if instance_info:
                    doc.update({
                        "endpoint": instance_info["endpoint"],
                        "port": instance_info["port"],
                        "master_username": instance_info["master_username"]
                    })
                results.append(doc)

            return results

        except Exception as e:
            logger.error(f"다이제스트 쿼리 조회 실패: {str(e)}")
            raise

_loader_instance = None

def get_query_loader() -> SlowQueryLoader:
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = SlowQueryLoader()
    return _loader_instance

if __name__ == "__main__":
    import asyncio
    from datetime import datetime
    from dotenv import load_dotenv
    import json

    load_dotenv()

    YEAR = 2024
    MONTH = 11
    # 특정 인스턴스만 조회하고 싶을 경우
    #INSTANCE_ID = "orderservice"

    async def main():
        try:
            await MongoDBConnector.initialize()
            loader = get_query_loader()

            start_date = datetime(YEAR, MONTH, 1)
            end_date = datetime(YEAR, MONTH + 1, 1) if MONTH < 12 else datetime(YEAR + 1, 1, 1)

            # 방법 1: 환경 변수의 대상 인스턴스 사용
            all_queries = await loader.get_digest_queries(
                start_date=start_date,
                end_date=end_date
            )
            print("\n환경 변수의 대상 인스턴스 조회 결과:")
            print(f"조회된 쿼리 수: {len(all_queries)}")

            # 방법 2: 특정 인스턴스만 조회
            specific_queries = await loader.get_digest_queries(
                start_date=start_date,
                end_date=end_date
            )
            print("\n특정 인스턴스 조회 결과:")
            print(f"조회된 쿼리 수: {len(specific_queries)}")
            if specific_queries:
                print("\n첫 번째 쿼리 예시:")
                print(json.dumps(specific_queries[0], indent=2, default=str))

        except Exception as e:
            print(f"오류 발생: {str(e)}")
        finally:
            await MongoDBConnector.close()

    asyncio.run(main())