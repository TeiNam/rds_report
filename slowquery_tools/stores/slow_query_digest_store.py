from datetime import datetime
from typing import Dict, List, Optional, Union
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
import logging
from pymongo import ReplaceOne
from itertools import groupby
from operator import itemgetter

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
        """다이제스트 쿼리의 유효성을 검사"""
        if not digest_query:  # None 또는 빈 문자열 체크
            return False

        invalid_queries = {
            'COMMIT;', 'COMMIT', 'BEGIN', 'BEGIN;', 'ROLLBACK', 'ROLLBACK;'
        }

        cleaned_query = digest_query.strip().upper()
        return cleaned_query not in invalid_queries

    async def store_digest_queries(
            self,
            digests: List[Dict]
    ) -> Dict[str, bool]:
        """
        다이제스트 쿼리 저장

        Args:
            digests: 다이제스트 쿼리 정보 리스트

        Returns:
            Dict[str, bool]: 인스턴스별 저장 결과
        """
        try:
            collection = await self._get_collection()
            now = datetime.now()
            results = {}

            # instance_id로 그룹화
            sorted_digests = sorted(digests, key=itemgetter('instance_id'))
            for instance_id, instance_digests in groupby(sorted_digests, key=itemgetter('instance_id')):
                # 벌크 작업 준비
                operations = []
                instance_digests = list(instance_digests)  # generator를 리스트로 변환
                filtered_count = 0
                total_count = len(instance_digests)

                for digest in instance_digests:
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
                    results[instance_id] = True
                else:
                    logger.warning(f"인스턴스 {instance_id}의 저장할 쿼리가 없습니다.")
                    results[instance_id] = False

            return results

        except Exception as e:
            logger.error(f"다이제스트 쿼리 저장 실패: {str(e)}")
            return {instance_id: False for instance_id in set(d["instance_id"] for d in digests)}

    async def get_stored_digests(
            self,
            instance_ids: Optional[Union[str, List[str]]] = None,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> List[Dict]:
        """
        저장된 다이제스트 쿼리 조회

        Args:
            instance_ids: 조회할 인스턴스 ID 또는 ID 목록
            start_date: 시작 날짜
            end_date: 종료 날짜

        Returns:
            List[Dict]: 다이제스트 쿼리 정보 리스트
        """
        try:
            collection = await self._get_collection()

            match_stage = {}
            if instance_ids:
                if isinstance(instance_ids, str):
                    match_stage["instance_id"] = instance_ids
                else:
                    match_stage["instance_id"] = {"$in": instance_ids}

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


    async def store_query_digests():
        try:
            await MongoDBConnector.initialize()

            # 쿼리 로더에서 데이터 가져오기
            loader = get_query_loader()
            start_date = datetime(YEAR, MONTH, 1)
            end_date = datetime(YEAR, MONTH + 1, 1) if MONTH < 12 else datetime(YEAR + 1, 1, 1)

            # 모든 대상 인스턴스의 쿼리 가져오기
            queries = await loader.get_digest_queries(
                start_date=start_date,
                end_date=end_date
            )
            print(f"쿼리 로더에서 {len(queries)}개의 다이제스트 쿼리를 불러왔습니다.")

            # 다이제스트 스토어에 저장
            store = get_query_digest_store()
            results = await store.store_digest_queries(queries)

            print("\n저장 결과:")
            for instance_id, success in results.items():
                print(f"- {instance_id}: {'성공' if success else '실패'}")

            # 저장된 데이터 확인
            stored_queries = await store.get_stored_digests()
            print(f"\n총 저장된 데이터 수: {len(stored_queries)}")

            # 인스턴스별 저장된 쿼리 수 출력
            instance_counts = {}
            for query in stored_queries:
                instance_id = query["instance_id"]
                instance_counts[instance_id] = instance_counts.get(instance_id, 0) + 1

            print("\n인스턴스별 저장된 쿼리 수:")
            for instance_id, count in instance_counts.items():
                print(f"- {instance_id}: {count}개")

        except Exception as e:
            print(f"오류 발생: {str(e)}")

        finally:
            await MongoDBConnector.close()


    # 실행
    asyncio.run(store_query_digests())