from typing import Dict, Any, List, Optional, Union
import logging
import asyncio
from datetime import datetime, timedelta
from dataclasses import dataclass
from bson import ObjectId
import aiomysql
import json
import os
import argparse
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorCollection

from modules.mysql_connector import MySQLConnector, MySQLConnectionInfo
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
from slowquery_tools.base import BaseSlowQueryTool
from modules.db_credentials_manager import get_credentials_manager, DBCredential
from slowquery_tools.stores.plan_updater import get_plan_updater
from models.slowquery import SlowQueryInfo

logger = logging.getLogger(__name__)


@dataclass
class AnalysisConfig:
    """분석 설정"""
    batch_size: int = 100
    max_concurrent_tasks: int = 5
    query_timeout: float = 30.0
    max_query_length: int = 10000
    max_retries: int = 3
    retry_delay: float = 1.0


class ImprovedQueryPlanAnalyzer(BaseSlowQueryTool):
    """개선된 슬로우 쿼리 실행 계획 분석기"""

    def __init__(self, config: Optional[AnalysisConfig] = None):
        super().__init__()
        self.config = config or AnalysisConfig()
        self.collection_name = mongo_settings.MONGO_SLOW_DIGEST_INFO_COLLECTION
        self._collection = None
        self._plan_updater = get_plan_updater()
        self._connection_pools = {}
        self._credentials_cache = {}

    async def _get_collection(self) -> AsyncIOMotorCollection:
        """MongoDB 컬렉션 가져오기 (BaseSlowQueryTool 추상 메소드 구현)"""
        if self._collection is None:
            self._collection = await MongoDBConnector.get_collection(
                self.collection_name
            )
        return self._collection

    async def initialize(self):
        """분석기 초기화"""
        await MongoDBConnector.initialize()
        self._collection = await self._get_collection()

    async def _get_connection_pool(
            self,
            credential: DBCredential
    ) -> aiomysql.Pool:
        """MySQL 연결 풀 가져오기 (캐시 활용)"""
        pool_key = f"{credential.instance_id}_secondary"

        if pool_key not in self._connection_pools:
            self._connection_pools[pool_key] = await aiomysql.create_pool(
                host=credential.secondary_endpoint,
                port=credential.port,
                user=credential.master_user,
                password=credential.password,
                db=credential.default_db,
                maxsize=10,
                minsize=1,
                autocommit=True
            )

        return self._connection_pools[pool_key]

    async def analyze_slow_queries(
            self,
            start_date: datetime,
            end_date: datetime,
            target_instances: Optional[List[str]] = None,
            save_results: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        final_results = {}

        try:
            match_condition = {
                "created_at": {"$gte": start_date, "$lte": end_date}
            }
            if target_instances:
                match_condition["instance_id"] = {"$in": target_instances}

            logger.info(f"MongoDB 쿼리 조건: {match_condition}")
            doc_count = await self._collection.count_documents(match_condition)
            logger.info(f"조회된 전체 문서 수: {doc_count}")

            instance_queries = await self._group_queries_by_instance(match_condition)

            analysis_tasks = []
            for instance_id, queries in instance_queries.items():
                task = asyncio.create_task(
                    self._process_instance_queries(
                        instance_id,
                        queries,
                        save_results
                    )
                )
                analysis_tasks.append((instance_id, task))

                if len(analysis_tasks) >= self.config.max_concurrent_tasks:
                    results_batch = await asyncio.gather(
                        *[task for _, task in analysis_tasks]
                    )
                    # 여기서 batch 결과를 final_results에 반영
                    for (inst_id, _), result in zip(analysis_tasks, results_batch):
                        final_results[inst_id] = result
                    analysis_tasks = []

            # 남은 태스크 처리
            if analysis_tasks:
                results = await asyncio.gather(*[task for _, task in analysis_tasks])
                for (instance_id, _), result in zip(analysis_tasks, results):
                    final_results[instance_id] = result

            return final_results

        except Exception as e:
            logger.error(f"슬로우 쿼리 분석 중 오류 발생: {str(e)}")
            raise

        finally:
            await self._cleanup_resources()

    async def _group_queries_by_instance(
            self,
            match_condition: Dict
    ) -> Dict[str, List[SlowQueryInfo]]:
        """인스턴스별 쿼리 그룹화"""
        instance_queries = {}

        pipeline = [
            {"$match": match_condition},
            {"$sort": {"instance_id": 1, "avg_time": -1}}
        ]

        unique_instances = set()
        total_docs = 0

        async for doc in self._collection.aggregate(pipeline):
            try:
                total_docs += 1
                instance_id = doc.get('instance_id')
                if instance_id:
                    unique_instances.add(instance_id)

                slow_query = SlowQueryInfo.from_mongo_doc(doc)
                if self._is_select_query(slow_query.example_query):
                    if instance_id not in instance_queries:
                        instance_queries[instance_id] = []
                    instance_queries[instance_id].append(slow_query)
            except Exception as e:
                logger.error(f"쿼리 처리 실패 (ID: {doc.get('_id')}): {str(e)}")

        # 결과 로깅
        logger.info(f"총 처리된 문서 수: {total_docs}")
        logger.info(f"발견된 인스턴스 목록: {sorted(list(unique_instances))}")
        logger.info(f"그룹화된 인스턴스 수: {len(instance_queries)}")
        for inst_id, queries in instance_queries.items():
            logger.info(f"인스턴스 {inst_id}의 SELECT 쿼리 수: {len(queries)}")

        return instance_queries

    async def _process_instance_queries(
            self,
            instance_id: str,
            queries: List[SlowQueryInfo],
            save_results: bool
    ) -> Dict[str, Any]:
        """인스턴스별 쿼리 처리"""
        result = {
            'instance_id': instance_id,
            'total_queries': len(queries),
            'analyzed_queries': 0,
            'failed_queries': 0,
            'top_queries': []
        }

        try:
            # DB 접속 정보 조회 및 연결 풀 생성
            credential = await self._get_credential(instance_id)
            pool = await self._get_connection_pool(credential)

            # 배치 단위로 쿼리 처리
            for i in range(0, len(queries), self.config.batch_size):
                batch = queries[i:i + self.config.batch_size]
                batch_results = await self._process_query_batch(
                    pool,
                    batch,
                    save_results
                )
                self._update_analysis_results(result, batch_results)

            # 상위 쿼리 정렬
            result['top_queries'].sort(
                key=lambda x: x['avg_time'],
                reverse=True
            )
            result['top_queries'] = result['top_queries'][:10]

            return result

        except Exception as e:
            logger.error(f"인스턴스 {instance_id} 처리 실패: {str(e)}")
            return result

    async def _process_query_batch(
            self,
            pool: aiomysql.Pool,
            queries: List[SlowQueryInfo],
            save_results: bool
    ) -> List[Dict[str, Any]]:
        """쿼리 배치 처리"""
        batch_tasks = []
        for query in queries:
            task = asyncio.create_task(
                self._analyze_single_query(pool, query, save_results)
            )
            batch_tasks.append(task)

        results = await asyncio.gather(*batch_tasks)
        return results

    async def _analyze_single_query(
            self,
            pool: aiomysql.Pool,
            query_info: SlowQueryInfo,
            save_results: bool
    ) -> Dict[str, Any]:
        """단일 쿼리 분석"""
        try:
            if self._is_query_too_large(query_info.example_query):
                return None

            explain_results = await self._get_explain_with_retry(
                pool,
                query_info.example_query
            )

            if explain_results:
                result = {
                    'id': str(query_info._id),
                    'digest': query_info.digest_query,
                    'avg_time': query_info.avg_time,
                    'plan': explain_results
                }

                if save_results:
                    await self._save_explain_results(query_info, explain_results)

                return result

        except Exception as e:
            logger.error(f"쿼리 분석 실패 (ID: {query_info._id}): {str(e)}")

        return None

    async def _get_explain_with_retry(
            self,
            pool: aiomysql.Pool,
            query: str
    ) -> Optional[Dict[str, Any]]:
        """재시도 로직이 포함된 EXPLAIN 실행"""
        retries = 0
        while retries < self.config.max_retries:
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        # Warning 메시지 무시 설정
                        await cursor.execute("SET sql_notes = 0")
                        await cursor.execute("SET sql_warnings = 0")

                        explain_results = {}

                        # JSON 형식
                        json_query = f"EXPLAIN FORMAT=JSON {query}"
                        await asyncio.wait_for(
                            cursor.execute(json_query),
                            timeout=self.config.query_timeout
                        )
                        json_result = await cursor.fetchone()
                        if json_result and 'EXPLAIN' in json_result:
                            explain_results['json'] = json_result['EXPLAIN']

                        # TREE 형식
                        tree_query = f"EXPLAIN FORMAT=TREE {query}"
                        await asyncio.wait_for(
                            cursor.execute(tree_query),
                            timeout=self.config.query_timeout
                        )
                        tree_result = await cursor.fetchall()
                        if tree_result:
                            tree_lines = [
                                next(iter(row.values()))
                                for row in tree_result
                                if isinstance(row, dict)
                            ]
                            explain_results['tree'] = '\n'.join(tree_lines)

                        # Warning 설정 복구
                        await cursor.execute("SET sql_notes = 1")
                        await cursor.execute("SET sql_warnings = 1")

                        if explain_results:
                            explain_results['analyzed_at'] = datetime.now()
                            return explain_results

                return None

            except (asyncio.TimeoutError, Exception) as e:
                retries += 1
                if retries == self.config.max_retries:
                    logger.error(f"EXPLAIN 실행 최대 재시도 횟수 초과: {str(e)}")
                    return None
                await asyncio.sleep(self.config.retry_delay * (2 ** retries))

    async def _save_explain_results(
            self,
            query_info: SlowQueryInfo,
            explain_results: Dict[str, Any]
    ):
        """실행 계획 결과 저장"""
        try:
            await self._plan_updater.update_explain_results(
                query_info,
                {
                    'plan': explain_results,
                    'analyzed_at': explain_results['analyzed_at']
                }
            )
        except Exception as e:
            logger.error(f"실행 계획 저장 실패 (ID: {query_info._id}): {str(e)}")

    async def _get_credential(self, instance_id: str) -> DBCredential:
        """DB 접속 정보 조회 (캐시 활용)"""
        if instance_id not in self._credentials_cache:
            credentials_manager = get_credentials_manager()
            credential = await credentials_manager.get_credential(
                instance_id,
                use_secondary=True,
                prompt_if_missing=True,
                store_if_input=True
            )
            if not credential:
                raise ValueError(f"접속 정보를 찾을 수 없음: {instance_id}")
            self._credentials_cache[instance_id] = credential

        return self._credentials_cache[instance_id]

    async def _cleanup_resources(self):
        """리소스 정리"""
        for pool in self._connection_pools.values():
            pool.close()
            await pool.wait_closed()
        self._connection_pools.clear()
        self._credentials_cache.clear()

    def _is_query_too_large(self, query: str) -> bool:
        """쿼리 크기 체크"""
        return len(query) > self.config.max_query_length

    def _is_select_query(self, query: str) -> bool:
        """SELECT 쿼리 여부 확인"""
        return query.strip().upper().startswith('SELECT')

    def _update_analysis_results(
            self,
            result: Dict[str, Any],
            batch_results: List[Dict[str, Any]]
    ):
        """분석 결과 업데이트"""
        for query_result in batch_results:
            if query_result:
                result['analyzed_queries'] += 1
                result['top_queries'].append(query_result)
            else:
                result['failed_queries'] += 1


# 싱글톤 인스턴스
_analyzer_instance = None


def get_improved_query_plan_analyzer(
        config: Optional[AnalysisConfig] = None
) -> ImprovedQueryPlanAnalyzer:
    """ImprovedQueryPlanAnalyzer 싱글톤 인스턴스 반환"""
    global _analyzer_instance
    if _analyzer_instance is None:
        _analyzer_instance = ImprovedQueryPlanAnalyzer(config)
    return _analyzer_instance


def load_test_config() -> AnalysisConfig:
    """테스트용 설정 로드"""
    return AnalysisConfig(
        batch_size=50,  # 한 번에 처리할 쿼리 수
        max_concurrent_tasks=3,  # 최대 동시 처리 태스크
        query_timeout=20.0,  # 쿼리 타임아웃(초)
        max_query_length=8000,  # 최대 쿼리 길이
        max_retries=3,  # 최대 재시도 횟수
        retry_delay=1.0  # 재시도 대기 시간(초)
    )

async def analyze_queries(
        start_date: datetime,
        end_date: datetime,
        target_instances: List[str],
        interactive: bool = False
):
    """쿼리 분석 실행"""
    try:
        print("\n=== 슬로우 쿼리 실행 계획 분석 시작 ===")
        print(f"시작 시간: {datetime.now()}")
        print(f"\n[분석 설정]")
        print(f"- 분석 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
        print(f"- 대상 인스턴스: {', '.join(target_instances)}")

        # MongoDB 초기화 및 분석기 초기화
        await MongoDBConnector.initialize()
        analyzer = get_improved_query_plan_analyzer(load_test_config())
        await analyzer.initialize()

        # 분석 실행
        print("\n쿼리 분석 시작...")
        results = await analyzer.analyze_slow_queries(
            start_date=start_date,
            end_date=end_date,
            target_instances=target_instances,
            save_results=True
        )

        # 종합 리포트 출력
        await print_analysis_report(results)

    except Exception as e:
        print(f"\n오류 발생: {str(e)}")
        logger.error(f"쿼리 분석 중 오류 발생: {str(e)}")
        raise

    finally:
        print("\n리소스 정리 중...")
        await MongoDBConnector.close()
        print("분석 완료!")

async def _process_instance_queries(
        self,
        instance_id: str,
        queries: List[SlowQueryInfo],
        save_results: bool
) -> Dict[str, Any]:
    """인스턴스별 쿼리 처리"""
    result = {
        'instance_id': instance_id,
        'total_queries': len(queries),
        'analyzed_queries': 0,
        'failed_queries': 0
    }

    try:
        credential = await self._get_credential(instance_id)
        pool = await self._get_connection_pool(credential)

        batch_size = self.config.batch_size
        total_batches = (len(queries) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(queries))
            batch = queries[start_idx:end_idx]

            logger.info(f"인스턴스 {instance_id} - 배치 {batch_num + 1}/{total_batches} 처리 중")

            batch_results = await self._process_query_batch(
                pool,
                batch,
                save_results
            )

            for query_result in batch_results:
                if query_result is not None:
                    result['analyzed_queries'] += 1
                else:
                    result['failed_queries'] += 1

        logger.info(f"인스턴스 {instance_id} 처리 완료: "
                    f"총 {result['total_queries']}, "
                    f"성공 {result['analyzed_queries']}, "
                    f"실패 {result['failed_queries']}")

        return result

    except Exception as e:
        logger.error(f"인스턴스 {instance_id} 처리 오류: {str(e)}")
        result['failed_queries'] = len(queries)
        return result


async def print_analysis_report(results: Dict[str, Dict[str, Any]]):
    """분석 결과 리포트 출력"""
    print("\n=== 슬로우 쿼리 분석 종합 리포트 ===")

    if not results:
        print("처리된 결과가 없습니다.")
        return

    total_stats = {
        'total_queries': 0,
        'analyzed_queries': 0,
        'failed_queries': 0
    }

    # 각 인스턴스별 결과 출력 및 전체 통계 집계
    for instance_id in sorted(results.keys()):
        result = results[instance_id]
        print(f"\n[인스턴스: {instance_id}]")
        print(f"- 총 쿼리 수: {result['total_queries']:,}개")
        print(f"- 분석 성공: {result['analyzed_queries']:,}개")
        print(f"- 분석 실패: {result['failed_queries']:,}개")

        # 전체 통계 업데이트
        total_stats['total_queries'] += result['total_queries']
        total_stats['analyzed_queries'] += result['analyzed_queries']
        total_stats['failed_queries'] += result['failed_queries']

    print("\n=== 전체 통계 ===")
    print(f"- 총 쿼리 수: {total_stats['total_queries']:,}개")
    print(f"- 총 분석 성공: {total_stats['analyzed_queries']:,}개")
    print(f"- 총 분석 실패: {total_stats['failed_queries']:,}개")
    print(f"- 완료 시간: {datetime.now()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='슬로우 쿼리 실행 계획 분석')
    parser.add_argument('--days', type=int, default=30, help='분석할 기간(일)')
    parser.add_argument('--instances', type=str, help='분석할 인스턴스 목록(쉼표로 구분)')
    parser.add_argument('--interactive', action='store_true', help='대화형 모드 실행')
    parser.add_argument('--start-date', type=str, help='시작 날짜 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='종료 날짜 (YYYY-MM-DD)')
    args = parser.parse_args()

    # 환경변수 로드
    load_dotenv()

    # 날짜 설정
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(
            hour=23, minute=59, second=59
        )
    else:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)
        start_date = start_date.replace(hour=0, minute=0, second=0)

    # 인스턴스 설정
    if args.instances:
        target_instances = args.instances.split(',')
    else:
        target_instances = json.loads(os.getenv("REPORT_TARGET_INSTANCES", "[]"))

    if not target_instances:
        print("\n경고: 분석할 인스턴스가 지정되지 않았습니다.")
        exit(1)

    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 분석 실행
    asyncio.run(
        analyze_queries(
            start_date=start_date,
            end_date=end_date,
            target_instances=target_instances,
            interactive=args.interactive
        )
    )