from datetime import datetime
from typing import Dict, List, Optional
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
import logging
from pymongo import ReplaceOne

logger = logging.getLogger(__name__)


class QueryDigestStore:
    def __init__(self):
        self.collection_name = mongo_settings.MONGO_SLOW_DIGEST_INFO_COLLECTION
        self._collection = None

    async def _get_collection(self):
        if self._collection is None:
            self._collection = await MongoDBConnector.get_collection(
                self.collection_name
            )
        return self._collection

    def _is_valid_digest_query(self, digest_query: str) -> bool:
        """
        다이제스트 쿼리의 유효성을 검사

        Args:
            digest_query: 검사할 다이제스트 쿼리

        Returns:
            bool: 유효성 여부
        """
        if not digest_query:  # None 또는 빈 문자열 체크
            return False

        # 무시할 쿼리 목록
        invalid_queries = {
            'COMMIT;',
            'COMMIT',
            'BEGIN',
            'BEGIN;',
            'ROLLBACK',
            'ROLLBACK;'
        }

        cleaned_query = digest_query.strip().upper()
        if cleaned_query in invalid_queries:
            return False

        return True

    async def store_digest_queries(
            self,
            instance_id: str,
            digests: List[Dict]
    ) -> bool:
        try:
            collection = await self._get_collection()
            now = datetime.now()

            # 벌크 작업 준비
            operations = []
            filtered_count = 0
            total_count = len(digests)

            for digest in digests:
                digest_query = digest.get("digest_query")

                # 유효성 검사
                if not self._is_valid_digest_query(digest_query):
                    filtered_count += 1
                    continue

                document = {
                    "instance_id": instance_id,
                    "created_at": now,
                    "digest_query": digest_query,
                    "example_query": digest.get("example_query"),
                    "user": digest.get("user"),
                    "avg_time": digest.get("avg_time"),
                    "endpoint": digest.get("endpoint"),
                    "port": digest.get("port"),
                    "master_username": digest.get("master_username")
                }

                operations.append(
                    ReplaceOne(
                        {
                            "instance_id": instance_id,
                            "digest_query": digest_query
                        },
                        document,
                        upsert=True
                    )
                )

            if operations:
                await collection.bulk_write(operations, ordered=False)
                logger.info(
                    f"인스턴스 {instance_id}의 다이제스트 쿼리 저장 완료 "
                    f"(전체: {total_count}, 필터링: {filtered_count}, "
                    f"저장: {len(operations)})"
                )
                return True
            return False

        except Exception as e:
            logger.error(f"다이제스트 쿼리 저장 실패: {str(e)}")
            return False

    async def get_stored_digests(
            self,
            instance_id: str,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> List[Dict]:
        try:
            collection = await self._get_collection()

            match_stage = {"instance_id": instance_id}
            if start_date or end_date:
                match_stage["created_at"] = {}
                if start_date:
                    match_stage["created_at"]["$gte"] = start_date
                if end_date:
                    match_stage["created_at"]["$lte"] = end_date

            pipeline = [
                {"$match": match_stage},
                {"$sort": {"created_at": -1, "avg_time": -1}}
            ]

            results = []
            async for doc in collection.aggregate(pipeline, allowDiskUse=True):
                doc.pop("_id", None)
                results.append(doc)

            return results

        except Exception as e:
            logger.error(f"저장된 다이제스트 쿼리 조회 실패: {str(e)}")
            return []


# 싱글톤 인스턴스
_store_instance = None


def get_query_digest_store() -> QueryDigestStore:
    global _store_instance
    if _store_instance is None:
        _store_instance = QueryDigestStore()
    return _store_instance


if __name__ == "__main__":
    import asyncio
    from datetime import datetime
    from dotenv import load_dotenv
    from slowquery_tools.loaders.query_loader import get_query_loader
    import json

    load_dotenv()

    # 설정
    YEAR = 2024
    MONTH = 11
    INSTANCE_ID = "orderservice"


    async def store_query_digests():
        try:
            # MongoDB 초기화
            await MongoDBConnector.initialize()

            # 쿼리 로더에서 데이터 가져오기
            loader = get_query_loader()
            start_date = datetime(YEAR, MONTH, 1)
            end_date = datetime(YEAR, MONTH + 1, 1) if MONTH < 12 else datetime(YEAR + 1, 1, 1)

            queries = await loader.get_digest_queries(INSTANCE_ID, start_date, end_date)
            print(f"쿼리 로더에서 {len(queries)}개의 다이제스트 쿼리를 불러왔습니다.")

            # 다이제스트 스토어에 저장
            store = get_query_digest_store()
            success = await store.store_digest_queries(INSTANCE_ID, queries)
            print(f"저장 결과: {'성공' if success else '실패'}")

            # 저장된 데이터 확인
            stored_queries = await store.get_stored_digests(INSTANCE_ID)
            print(f"저장된 데이터 수: {len(stored_queries)}")

            if stored_queries:
                print("\n첫 번째 저장된 쿼리:")
                print(json.dumps(stored_queries[0], indent=2, default=str))

        except Exception as e:
            print(f"오류 발생: {str(e)}")

        finally:
            await MongoDBConnector.close()


    # 실행
    asyncio.run(store_query_digests())