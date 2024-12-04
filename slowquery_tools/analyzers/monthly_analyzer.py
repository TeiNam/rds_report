from typing import Dict, List
from collections import defaultdict
from datetime import datetime
from slowquery_tools.loaders.stats_loader import get_stats_loader
from slowquery_tools.stores.slow_query_statistics_store import SlowQueryStatisticsStore
from modules.mongodb_connector import MongoDBConnector
import sqlparse
import logging

logger = logging.getLogger(__name__)


class MonthlySlowQueryAnalyzer:
    def __init__(self):
        self.stats_loader = get_stats_loader()

    async def analyze_and_store_all_instances(
            self,
            instances: List[str],
            year: int,
            month: int
    ) -> None:
        """모든 인스턴스의 통계 분석 및 저장"""
        try:
            # MongoDB 초기화
            await MongoDBConnector.initialize()
            await SlowQueryStatisticsStore.initialize_collection()

            for instance_id in instances:
                print(f"\n{'=' * 50}")
                print(f"인스턴스 {instance_id} 분석 및 저장")
                print(f"{'=' * 50}")

                try:
                    # 통계 분석 수행
                    stats = await self.analyze_monthly_stats(instance_id, year, month)

                    if stats:
                        # MongoDB에 저장
                        await SlowQueryStatisticsStore.store_statistics(
                            instance_id, year, month, stats
                        )
                        await self.print_analysis_results(stats)
                    else:
                        print(f"\n{year}년 {month}월 데이터가 없습니다.")

                except Exception as e:
                    logger.error(f"인스턴스 {instance_id} 처리 중 오류 발생: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"전체 처리 중 오류 발생: {str(e)}")
            raise

        finally:
            await MongoDBConnector.close()

    async def analyze_monthly_stats(
            self,
            instance_id: str,
            year: int,
            month: int
    ) -> Dict:
        """월간 슬로우 쿼리 통계 분석"""
        try:
            # 월간 데이터 조회
            queries = await self.stats_loader.get_instance_queries(
                instance_id,
                start_date=datetime(year, month, 1),
                end_date=datetime(year, month + 1, 1) if month < 12 else datetime(year + 1, 1, 1)
            )

            if not queries:
                logger.warning(f"{year}년 {month}월 데이터가 없습니다.")
                return {}

            # 전체 통계 계산
            total_stats = self._calculate_total_stats(queries)

            # 사용자별 통계 계산
            user_stats = self._calculate_user_stats(queries)

            # 다이제스트별 통계 계산
            digest_stats = self._calculate_digest_stats(queries)

            return {
                'total_stats': total_stats,
                'user_stats': user_stats,
                'digest_stats': digest_stats
            }

        except Exception as e:
            logger.error(f"월간 통계 분석 실패: {str(e)}")
            raise

    def _calculate_total_stats(self, queries: List[Dict]) -> Dict:
        """전체 통계 계산"""
        # 쿼리 타입별 카운트
        read_queries = sum(1 for q in queries if q['digest_query'].strip().upper().startswith('SELECT'))
        write_queries = sum(1 for q in queries if any(
            q['digest_query'].strip().upper().startswith(op)
            for op in ('INSERT', 'UPDATE', 'DELETE')
        ))
        ddl_queries = sum(1 for q in queries if q['digest_query'].strip().upper().startswith('ALTER'))

        return {
            'total_slow_queries': len(queries),
            'total_execution_count': sum(q['execution_count'] for q in queries),
            'total_execution_time': sum(q['total_time'] for q in queries),
            'avg_execution_time': sum(q['total_time'] for q in queries) / sum(q['execution_count'] for q in queries),
            'total_examined_rows': sum(q['avg_rows_examined'] * q['execution_count'] for q in queries),
            'unique_digest_count': len(set(q['digest_query'] for q in queries)),
            'read_queries': read_queries,
            'write_queries': write_queries,
            'ddl_queries': ddl_queries
        }

    def _calculate_user_stats(self, queries: List[Dict]) -> List[Dict]:
        """사용자별 통계 계산"""
        user_stats = defaultdict(lambda: {
            'slow_query_count': 0,
            'total_execution_count': 0,
            'total_execution_time': 0,
            'total_examined_rows': 0,
            'unique_digests': set()
        })

        # 각 쿼리의 사용자별 통계 집계
        for query in queries:
            for user in query['users']:
                stats = user_stats[user]
                stats['slow_query_count'] += 1
                stats['total_execution_count'] += query['execution_count']
                stats['total_execution_time'] += query['total_time']
                stats['total_examined_rows'] += query['avg_rows_examined'] * query['execution_count']
                stats['unique_digests'].add(query['digest_query'])

        # 딕셔너리를 리스트로 변환하고 실행 횟수로 정렬
        return [
            {
                'user': user,
                **{k: v for k, v in stats.items() if k != 'unique_digests'},
                'avg_execution_time': stats['total_execution_time'] / stats['total_execution_count'],
                'unique_digest_count': len(stats['unique_digests'])
            }
            for user, stats in sorted(
                user_stats.items(),
                key=lambda x: x[1]['total_execution_count'],
                reverse=True
            )
        ]

    def _calculate_digest_stats(self, queries: List[Dict]) -> List[Dict]:
        """다이제스트별 통계 계산"""
        digest_stats = defaultdict(lambda: {
            'execution_count': 0,
            'total_time': 0,
            'total_examined_rows': 0,
            'users': set(),
            'first_seen': None,
            'last_seen': None
        })

        # 각 다이제스트별 통계 집계
        for query in queries:
            digest = query['digest_query']
            stats = digest_stats[digest]

            stats['execution_count'] += query['execution_count']
            stats['total_time'] += query['total_time']
            stats['total_examined_rows'] += query['avg_rows_examined'] * query['execution_count']
            stats['users'].update(query['users'])

            first_seen = datetime.fromisoformat(query['first_seen'])
            last_seen = datetime.fromisoformat(query['last_seen'])

            if stats['first_seen'] is None or first_seen < stats['first_seen']:
                stats['first_seen'] = first_seen
            if stats['last_seen'] is None or last_seen > stats['last_seen']:
                stats['last_seen'] = last_seen

        # 딕셔너리를 리스트로 변환하고 실행 시간으로 정렬
        return [
            {
                'digest_query': digest,
                'execution_count': stats['execution_count'],
                'total_time': stats['total_time'],
                'avg_time': stats['total_time'] / stats['execution_count'],
                'total_examined_rows': stats['total_examined_rows'],
                'avg_examined_rows': stats['total_examined_rows'] / stats['execution_count'],
                'unique_users': len(stats['users']),
                'users': list(stats['users']),
                'first_seen': stats['first_seen'].isoformat(),
                'last_seen': stats['last_seen'].isoformat()
            }
            for digest, stats in sorted(
                digest_stats.items(),
                key=lambda x: x[1]['total_time'],
                reverse=True
            )
        ]

    async def print_analysis_results(self, stats: Dict) -> None:
        """분석 결과 출력"""
        if not stats:
            print("\n데이터가 없습니다.")
            return

        # 전체 통계 출력
        print("\n=== 전체 통계 ===")
        total = stats['total_stats']
        print(f"고유 다이제스트 수: {total['unique_digest_count']:,}")
        print(f"전체 슬로우 쿼리 수: {total['total_slow_queries']:,}")
        print(f"전체 실행 횟수: {total['total_execution_count']:,}")
        print(f"전체 실행 시간: {total['total_execution_time']:.2f}초")
        print(f"평균 실행 시간: {total['avg_execution_time']:.3f}초")
        print(f"전체 조회 행 수: {total['total_examined_rows']:,}")
        print("쿼리 타입별 통계:")
        print(f"  읽기 쿼리 (SELECT): {total['read_queries']:,}")
        print(f"  쓰기 쿼리 (INSERT/UPDATE/DELETE): {total['write_queries']:,}")
        print(f"  DDL 쿼리 (ALTER): {total['ddl_queries']:,}")

        # 사용자별 통계 출력
        print("\n=== 사용자별 통계 ===")
        for user_stat in stats['user_stats']:
            print(f"\n사용자: {user_stat['user']}")
            print(f"  고유 다이제스트 수: {user_stat['unique_digest_count']:,}")
            print(f"  슬로우 쿼리 수: {user_stat['slow_query_count']:,}")
            print(f"  전체 실행 횟수: {user_stat['total_execution_count']:,}")
            print(f"  전체 실행 시간: {user_stat['total_execution_time']:.2f}초")
            print(f"  평균 실행 시간: {user_stat['avg_execution_time']:.3f}초")
            print(f"  전체 조회 행 수: {user_stat['total_examined_rows']:,}")

        # 상위 10개 다이제스트 통계 출력
        print("\n=== 상위 10개 다이제스트 통계 ===")
        for i, digest in enumerate(stats['digest_stats'][:10], 1):
            print(f"\n{i}. 다이제스트:")
            # SQL 포맷팅
            formatted_sql = sqlparse.format(
                digest['digest_query'],
                reindent=True,
                keyword_case='upper',
                indent_width=2
            )
            print("쿼리:")
            print("-" * 80)  # 구분선 추가
            print(formatted_sql)
            print("-" * 80)  # 구분선 추가
            print(f"  실행 횟수: {digest['execution_count']:,}")
            print(f"  전체 실행 시간: {digest['total_time']:.2f}초")
            print(f"  평균 실행 시간: {digest['avg_time']:.3f}초")
            print(f"  평균 조회 행 수: {digest['avg_examined_rows']:.1f}")
            print(f"  사용자 목록: {', '.join(digest['users'])}")
            print(f"  첫 실행: {digest['first_seen']}")
            print(f"  마지막 실행: {digest['last_seen']}")


if __name__ == "__main__":
    import asyncio
    import argparse
    import os
    from datetime import datetime
    from dotenv import load_dotenv
    import json

    # .env 파일 로드
    load_dotenv()

    # 기본 타겟 년월 설정
    DEFAULT_TARGET_YEAR = 2024
    DEFAULT_TARGET_MONTH = 11

    # 커맨드 라인 인자 파싱
    parser = argparse.ArgumentParser(description='월간 슬로우 쿼리 분석 및 저장')
    parser.add_argument('--year', type=int, default=DEFAULT_TARGET_YEAR,
                        help=f'분석 년도 (기본값: {DEFAULT_TARGET_YEAR})')
    parser.add_argument('--month', type=int, default=DEFAULT_TARGET_MONTH,
                        help=f'분석 월 (기본값: {DEFAULT_TARGET_MONTH})')

    args = parser.parse_args()

    # 인스턴스 목록 가져오기
    target_instances = json.loads(os.getenv('REPORT_TARGET_INSTANCES', '[]'))

    if not target_instances:
        print("분석할 인스턴스가 없습니다. REPORT_TARGET_INSTANCES 환경 변수를 확인해주세요.")
        exit(1)

    print(f"\n{args.year}년 {args.month}월 슬로우 쿼리 분석 및 저장 시작\n")

    # MonthlySlowQueryAnalyzer 인스턴스 생성 및 실행
    analyzer = MonthlySlowQueryAnalyzer()
    asyncio.run(analyzer.analyze_and_store_all_instances(
        target_instances, args.year, args.month
    ))