from datetime import datetime
from typing import Dict, List, Optional, Tuple
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
            instance_id: str,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ) -> List[Dict]:
        try:
            query_collection, _ = await self._get_collections()
            instance_info = await self._get_instance_info(instance_id)

            match_stage = {"instance_id": instance_id}
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
    INSTANCE_ID = "orderservice"

    async def main():
        try:
            await MongoDBConnector.initialize()
            loader = get_query_loader()

            start_date = datetime(YEAR, MONTH, 1)
            end_date = datetime(YEAR, MONTH + 1, 1) if MONTH < 12 else datetime(YEAR + 1, 1, 1)

            queries = await loader.get_digest_queries(INSTANCE_ID, start_date, end_date)
            print(json.dumps(queries, indent=2, default=str))

        except Exception as e:
            print(f"오류 발생: {str(e)}")
        finally:
            await MongoDBConnector.close()

    asyncio.run(main())