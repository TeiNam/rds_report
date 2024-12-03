# collectors/cloudwatch_slowquery_collector.py

import logging
import re
import os
import sys
import pytz
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from modules.aws_session_manager import AWSSessionManager
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
from configs.report_settings import ReportSettings

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
kst = pytz.timezone('Asia/Seoul')


class RDSCloudWatchSlowQueryCollector:
    """RDS CloudWatch 슬로우 쿼리 수집기"""

    def __init__(self, session_manager: AWSSessionManager):
        """
        Args:
            session_manager: AWS 세션 관리자 인스턴스
        """
        self.session_manager = session_manager
        self._instance_info = self.session_manager.get_instance_info()
        self.target_instances = ReportSettings.get_report_target_instances()
        self._query_pattern = re.compile(
            r"# User@Host: (?P<user>.*?)\[.*?\] @ (?P<host>.*?)"
            r"# Query_time: (?P<query_time>\d+\.\d+)\s+"
            r"Lock_time: (?P<lock_time>\d+\.\d+)\s+"
            r"Rows_sent: (?P<rows_sent>\d+)\s+"
            r"Rows_examined: (?P<rows_examined>\d+)"
            r".*?SET timestamp=(?P<timestamp>\d+);"
            r"(?P<query>.*?)(?=# User@Host:|$)",
            re.DOTALL
        )

        if self.target_instances:
            logger.info(f"수집 대상 인스턴스: {', '.join(self.target_instances)}")
        else:
            logger.warning("수집 대상 인스턴스가 설정되지 않았습니다.")

    @property
    def collection_name(self) -> str:
        """MongoDB 컬렉션 이름 반환"""
        return mongo_settings.MONGO_SLOW_QUERY_COLLECTION

    async def collect_metrics_daily(
            self,
            target_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        일 단위 슬로우 쿼리 수집

        Args:
            target_date: 수집 대상 날짜 (기본값: 어제)

        Returns:
            Dict[str, Any]: 계정별, 인스턴스별 수집된 슬로우 쿼리
        """
        try:
            if not self._instance_info:
                raise ValueError("인스턴스 정보가 초기화되지 않았습니다")

            if not self.target_instances:
                logger.warning("수집 대상 인스턴스가 없습니다")
                return {}

            # 기본값으로 어제 날짜 사용
            if target_date is None:
                target_date = datetime.now(kst).date() - timedelta(days=1)
            elif isinstance(target_date, datetime):
                target_date = target_date.date()

            start_date = datetime.combine(target_date, datetime.min.time())
            end_date = datetime.combine(target_date, datetime.max.time())

            logger.info(
                f"{target_date.strftime('%Y-%m-%d')} 슬로우 쿼리 수집 시작: "
                f"{len(self._instance_info.accounts)}개 계정"
            )

            account_queries = {}
            for account in self._instance_info.accounts:
                queries = await self._collect_account_slow_queries(
                    account=account,
                    start_date=start_date,
                    end_date=end_date
                )
                if queries:
                    account_queries[account.account_id] = queries

            if not account_queries:
                logger.warning("수집된 슬로우 쿼리가 없습니다")
                return {}

            await self._save_daily_metrics(
                account_queries=account_queries,
                target_date=target_date
            )

            return account_queries

        except Exception as e:
            logger.error(f"일간 슬로우 쿼리 수집 중 오류 발생: {e}")
            raise

    async def _collect_account_slow_queries(
            self,
            account: Any,
            start_date: datetime,
            end_date: datetime
    ) -> Dict[str, List[Dict]]:
        """계정별 슬로우 쿼리 수집"""
        try:
            instance_queries = {}

            # 대상 인스턴스만 필터링
            target_instances = [
                instance for instance in account.instances
                if instance.instance_identifier in self.target_instances
            ]

            if not target_instances:
                logger.debug(
                    f"계정 {account.account_id}에 "
                    f"대상 인스턴스가 없습니다."
                )
                return {}

            for instance in target_instances:
                logger.info(
                    f"[{account.account_id}] "
                    f"인스턴스 {instance.instance_identifier} 슬로우 쿼리 수집 시작"
                )

                try:
                    logs = await self._get_slow_query_logs(
                        account_id=account.account_id,
                        instance=instance,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if logs:
                        # 쿼리 분석 및 통계 계산
                        analyzed_queries = self._analyze_slow_queries(logs)
                        if analyzed_queries:
                            instance_queries[instance.instance_identifier] = analyzed_queries
                            logger.info(
                                f"✓ {instance.instance_identifier}: "
                                f"{len(analyzed_queries)} 개의 슬로우 쿼리 분석 완료"
                            )
                    else:
                        logger.info(
                            f"- {instance.instance_identifier}: "
                            f"수집된 슬로우 쿼리 없음"
                        )

                except Exception as e:
                    logger.error(f"인스턴스 {instance.instance_identifier} 처리 중 오류 발생: {e}")
                    continue

            return instance_queries

        except Exception as e:
            logger.error(f"계정 {account.account_id}의 슬로우 쿼리 수집 중 오류: {e}")
            raise

    async def _get_slow_query_logs(
            self,
            account_id: str,
            instance: Any,
            start_date: datetime,
            end_date: datetime
    ) -> List[Dict]:
        """CloudWatch Logs에서 슬로우 쿼리 로그 조회"""
        try:
            logs_client = self.session_manager.get_client(
                'logs',
                account_id,
                instance.region
            )

            log_group_name = f"/aws/rds/instance/{instance.instance_identifier}/slowquery"

            try:
                # 로그 스트림 조회
                streams_response = await asyncio.to_thread(
                    logs_client.describe_log_streams,
                    logGroupName=log_group_name,
                    orderBy='LastEventTime',
                    descending=True,
                    limit=50  # 스트림 개수 증가
                )
            except Exception as e:
                logger.warning(f"로그 스트림 조회 실패 ({log_group_name}): {e}")
                return []

            log_events = []
            next_token = None

            # 모든 로그 이벤트 수집
            for stream in streams_response.get('logStreams', []):
                while True:
                    try:
                        # get_log_events 파라미터 설정
                        params = {
                            'logGroupName': log_group_name,
                            'logStreamName': stream['logStreamName'],
                            'startTime': int(start_date.timestamp() * 1000),
                            'endTime': int(end_date.timestamp() * 1000),
                            'limit': 10000  # 최대 로그 이벤트 수 증가
                        }

                        if next_token:
                            params['nextToken'] = next_token

                        events_response = await asyncio.to_thread(
                            logs_client.get_log_events,
                            **params
                        )

                        if events_response.get('events'):
                            log_events.extend(events_response['events'])

                        # 다음 페이지 확인
                        next_token = events_response.get('nextForwardToken')

                        # 더 이상 로그가 없거나 토큰이 같으면 종료
                        if not events_response.get('events') or next_token == params.get('nextToken'):
                            break

                    except Exception as e:
                        logger.warning(f"로그 이벤트 조회 실패 ({stream['logStreamName']}): {e}")
                        break

            logger.info(f"총 {len(log_events)}개의 로그 이벤트 수집됨")
            return log_events

        except Exception as e:
            logger.error(f"로그 조회 중 오류 발생: {e}")
            return []

    def _analyze_slow_queries(self, logs: List[Dict]) -> List[Dict]:
        """
        슬로우 쿼리 로그 분석
        - 모든 다이제스트 쿼리 패턴 수집 (시스템 계정 제외)
        - 각 다이제스트 당 example_queries는 10개로 제한
        """
        query_stats = {}
        total_query_count = len(logs)
        processed_query_count = 0
        excluded_users = {'rdsadmin', 'event_scheduler'}  # 제외할 시스템 계정 목록

        for log in logs:
            match = self._query_pattern.search(log.get('message', ''))
            if not match:
                continue

            data = match.groupdict()
            # 시스템 계정이 실행한 쿼리는 제외
            if any(user in data['user'].lower() for user in excluded_users):
                continue

            normalized_query = self._normalize_query(data['query'])
            processed_query_count += 1

            if normalized_query not in query_stats:
                query_stats[normalized_query] = {
                    'digest_query': normalized_query,
                    'example_queries': set(),
                    'execution_count': 0,
                    'total_time': 0.0,
                    'lock_time': 0.0,
                    'rows_sent': 0,
                    'rows_examined': 0,
                    'users': set(),
                    'hosts': set(),
                    'first_seen': None,
                    'last_seen': None
                }

            stats = query_stats[normalized_query]
            stats['execution_count'] += 1
            stats['total_time'] += float(data['query_time'])
            stats['lock_time'] += float(data['lock_time'])
            stats['rows_sent'] += int(data['rows_sent'])
            stats['rows_examined'] += int(data['rows_examined'])
            stats['users'].add(data['user'])
            stats['hosts'].add(data['host'])

            # example_queries는 10개까지만 저장
            if len(stats['example_queries']) < 10:
                stats['example_queries'].add(data['query'].strip())

            timestamp = datetime.fromtimestamp(int(data['timestamp']))
            if not stats['first_seen'] or timestamp < stats['first_seen']:
                stats['first_seen'] = timestamp
            if not stats['last_seen'] or timestamp > stats['last_seen']:
                stats['last_seen'] = timestamp

        # 결과 정리
        system_query_count = total_query_count - processed_query_count
        logger.info(
            f"전체 쿼리 수: {total_query_count}, "
            f"시스템 계정 쿼리 수: {system_query_count}, "
            f"처리된 쿼리 수: {processed_query_count}, "
            f"고유 다이제스트 수: {len(query_stats)}"
        )

        results = []
        for stats in query_stats.values():
            results.append({
                'digest_query': stats['digest_query'],
                'example_queries': list(stats['example_queries']),  # 이미 10개로 제한됨
                'execution_count': stats['execution_count'],
                'avg_time': stats['total_time'] / stats['execution_count'],
                'total_time': stats['total_time'],
                'avg_lock_time': stats['lock_time'] / stats['execution_count'],
                'avg_rows_sent': stats['rows_sent'] / stats['execution_count'],
                'avg_rows_examined': stats['rows_examined'] / stats['execution_count'],
                'users': list(stats['users']),
                'hosts': list(stats['hosts']),
                'first_seen': stats['first_seen'].isoformat(),
                'last_seen': stats['last_seen'].isoformat()
            })

        # 평균 실행 시간으로 정렬 (제한 없음)
        return sorted(results, key=lambda x: x['avg_time'], reverse=True)

    def _normalize_query(self, query: str) -> str:
        """쿼리 정규화 (변수 값을 플레이스홀더로 대체)"""
        # 문자열 리터럴 제거
        query = re.sub(r"'[^']*'", "?", query)
        query = re.sub(r'"[^"]*"', "?", query)

        # 숫자 리터럴 제거
        query = re.sub(r'\b\d+\b', "?", query)

        # 불필요한 공백 제거
        query = " ".join(query.split())

        return query

    async def _save_daily_metrics(
            self,
            account_queries: Dict[str, Dict],
            target_date: datetime.date
    ) -> None:
        """일간 슬로우 쿼리 데이터를 MongoDB에 저장"""
        try:
            db = await MongoDBConnector.get_database()
            collection = db[self.collection_name]

            for account_id, instance_queries in account_queries.items():
                for instance_id, queries in instance_queries.items():
                    document = {
                        "env": self._instance_info.env,
                        "date": target_date.strftime('%Y-%m-%d'),
                        "year": target_date.year,
                        "month": target_date.month,
                        "day": target_date.day,
                        "account_id": account_id,
                        "instance_id": instance_id,
                        "slow_queries": queries,
                        "created_at": datetime.now(kst).isoformat()
                    }

                    filter_doc = {
                        "env": document["env"],
                        "date": document["date"],
                        "account_id": account_id,
                        "instance_id": instance_id
                    }

                    try:
                        result = await collection.update_one(
                            filter_doc,
                            {"$set": document},
                            upsert=True
                        )

                        operation = "업데이트" if result.modified_count else "생성"
                        logger.info(
                            f"인스턴스 {instance_id}의 "
                            f"{target_date.strftime('%Y-%m-%d')} "
                            f"슬로우 쿼리 도큐먼트 {operation} 완료"
                        )

                    except Exception as e:
                        logger.error(
                            f"인스턴스 {instance_id} 슬로우 쿼리 저장 실패: {str(e)}"
                        )
                        continue

        except Exception as e:
            logger.error(f"MongoDB 저장 중 오류 발생: {e}")
            raise

    async def collect_metrics_monthly(
            self,
            year: int,
            month: int
    ) -> Dict[str, Any]:
        """
        월 단위 슬로우 쿼리 수집

        Args:
            year: 수집 연도
            month: 수집 월

        Returns:
            Dict[str, Any]: 계정별, 인스턴스별 수집된 슬로우 쿼리
        """
        try:
            if not self._instance_info:
                raise ValueError("인스턴스 정보가 초기화되지 않았습니다")

            if not self.target_instances:
                logger.warning("수집 대상 인스턴스가 없습니다")
                return {}

            # 해당 월의 시작일과 마지막 일 계산
            start_date = datetime(year, month, 1, tzinfo=kst)
            if month == 12:
                end_date = datetime(year + 1, 1, 1, tzinfo=kst) - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1, tzinfo=kst) - timedelta(days=1)

            logger.info(
                f"{year}년 {month}월 슬로우 쿼리 수집 시작: "
                f"{len(self._instance_info.accounts)}개 계정"
            )

            # 일별 데이터 수집
            all_account_queries = {}
            current_date = start_date

            while current_date <= end_date:
                logger.info(f"{current_date.strftime('%Y-%m-%d')} 데이터 수집 중...")

                # 일별 데이터 수집
                daily_queries = await self.collect_metrics_daily(current_date)

                # 데이터 병합
                for account_id, instance_queries in daily_queries.items():
                    if account_id not in all_account_queries:
                        all_account_queries[account_id] = {}

                    for instance_id, queries in instance_queries.items():
                        if instance_id not in all_account_queries[account_id]:
                            all_account_queries[account_id][instance_id] = []

                        all_account_queries[account_id][instance_id].extend(queries)

                current_date += timedelta(days=1)

            if not all_account_queries:
                logger.warning("수집된 슬로우 쿼리가 없습니다")
                return {}

            # 월간 데이터 정리 및 저장
            await self._save_monthly_metrics(
                account_queries=all_account_queries,
                year=year,
                month=month
            )

            return all_account_queries

        except Exception as e:
            logger.error(f"월간 슬로우 쿼리 수집 중 오류 발생: {e}")
            raise

    async def _save_monthly_metrics(
            self,
            account_queries: Dict[str, Dict],
            year: int,
            month: int
    ) -> None:
        """월간 슬로우 쿼리 데이터를 MongoDB에 저장"""
        try:
            db = await MongoDBConnector.get_database()
            collection = db[f"{self.collection_name}_monthly"]  # 월간 데이터는 별도 컬렉션에 저장

            for account_id, instance_queries in account_queries.items():
                for instance_id, queries in instance_queries.items():
                    # 쿼리 다이제스트별 통계 계산
                    digest_stats = {}
                    for query in queries:
                        digest = query['digest_query']
                        if digest not in digest_stats:
                            digest_stats[digest] = {
                                'digest_query': digest,
                                'example_queries': set(),
                                'execution_count': 0,
                                'total_time': 0.0,
                                'lock_time': 0.0,
                                'rows_sent': 0,
                                'rows_examined': 0,
                                'users': set(),
                                'hosts': set(),
                                'first_seen': None,
                                'last_seen': None
                            }

                        stats = digest_stats[digest]
                        stats['execution_count'] += query['execution_count']
                        stats['total_time'] += query['total_time']
                        stats['lock_time'] += query['avg_lock_time'] * query['execution_count']
                        stats['rows_sent'] += query['avg_rows_sent'] * query['execution_count']
                        stats['rows_examined'] += query['avg_rows_examined'] * query['execution_count']
                        stats['example_queries'].update(query['example_queries'])
                        stats['users'].update(query['users'])
                        stats['hosts'].update(query['hosts'])

                        first_seen = datetime.fromisoformat(query['first_seen'])
                        last_seen = datetime.fromisoformat(query['last_seen'])

                        if not stats['first_seen'] or first_seen < stats['first_seen']:
                            stats['first_seen'] = first_seen
                        if not stats['last_seen'] or last_seen > stats['last_seen']:
                            stats['last_seen'] = last_seen

                    # 통계 변환
                    monthly_stats = []
                    for stats in digest_stats.values():
                        monthly_stats.append({
                            'digest_query': stats['digest_query'],
                            'example_queries': list(stats['example_queries'])[:10],
                            'execution_count': stats['execution_count'],
                            'avg_time': stats['total_time'] / stats['execution_count'],
                            'total_time': stats['total_time'],
                            'avg_lock_time': stats['lock_time'] / stats['execution_count'],
                            'avg_rows_sent': stats['rows_sent'] / stats['execution_count'],
                            'avg_rows_examined': stats['rows_examined'] / stats['execution_count'],
                            'users': list(stats['users']),
                            'hosts': list(stats['hosts']),
                            'first_seen': stats['first_seen'].isoformat(),
                            'last_seen': stats['last_seen'].isoformat()
                        })

                    # 평균 실행 시간 기준 정렬
                    monthly_stats.sort(key=lambda x: x['avg_time'], reverse=True)

                    document = {
                        "env": self._instance_info.env,
                        "year": year,
                        "month": month,
                        "account_id": account_id,
                        "instance_id": instance_id,
                        "slow_queries": monthly_stats,
                        "created_at": datetime.now(kst).isoformat()
                    }

                    filter_doc = {
                        "env": document["env"],
                        "year": year,
                        "month": month,
                        "account_id": account_id,
                        "instance_id": instance_id
                    }

                    try:
                        result = await collection.update_one(
                            filter_doc,
                            {"$set": document},
                            upsert=True
                        )

                        operation = "업데이트" if result.modified_count else "생성"
                        logger.info(
                            f"인스턴스 {instance_id}의 "
                            f"{year}년 {month}월 "
                            f"슬로우 쿼리 도큐먼트 {operation} 완료"
                        )

                    except Exception as e:
                        logger.error(
                            f"인스턴스 {instance_id} 월간 슬로우 쿼리 저장 실패: {str(e)}"
                        )
                        continue

        except Exception as e:
            logger.error(f"MongoDB 저장 중 오류 발생: {e}")
            raise


async def collect_slow_queries(
        target_date: Optional[datetime] = None,
        mode: str = 'daily'
) -> None:
    """
    RDS 슬로우 쿼리 수집 실행

    Args:
        target_date: 수집할 날짜 (기본값: 어제)
        mode: 수집 모드 ('daily' 또는 'monthly')
    """
    try:
        target_instances = ReportSettings.get_report_target_instances()
        if not target_instances:
            logger.error("수집 대상 인스턴스가 설정되지 않았습니다")
            return

        # 기본값으로 어제 날짜 사용
        if target_date is None:
            target_date = datetime.now(kst) - timedelta(days=1)

        # AWS 세션 매니저 환경 설정
        env = os.getenv('ENV', 'prd')  # 기본값으로 'prd' 사용

        if mode == 'daily':
            logger.info(
                f"{target_date.strftime('%Y-%m-%d')} "
                f"RDS 슬로우 쿼리 수집 시작 (환경: {env})\n"
                f"대상 인스턴스: {', '.join(target_instances)}"
            )
        else:
            logger.info(
                f"{target_date.strftime('%Y년 %m월')} "
                f"RDS 슬로우 쿼리 수집 시작 (환경: {env})\n"
                f"대상 인스턴스: {', '.join(target_instances)}"
            )

        # AWS 세션 매니저 초기화
        session_manager = AWSSessionManager()
        await session_manager.initialize(env)

        # 인스턴스 정보 조회
        instance_info = session_manager.get_instance_info()

        if not instance_info or not instance_info.accounts:
            logger.error(f"인스턴스 정보를 찾을 수 없습니다. (환경: {env})")
            return

        # MongoDB 연결
        await MongoDBConnector.initialize()

        try:
            # 슬로우 쿼리 수집기 생성 및 실행
            collector = RDSCloudWatchSlowQueryCollector(session_manager)

            if mode == 'daily':
                account_queries = await collector.collect_metrics_daily(target_date)
            else:
                account_queries = await collector.collect_metrics_monthly(
                    target_date.year,
                    target_date.month
                )

            # 결과 요약
            total_instances = sum(
                len(instance_queries)
                for instance_queries in account_queries.values()
            )
            collected_instances = sorted([
                instance_id
                for queries in account_queries.values()
                for instance_id in queries.keys()
            ])

            if mode == 'daily':
                logger.info(
                    f"{target_date.strftime('%Y-%m-%d')} 슬로우 쿼리 수집 완료: "
                    f"{len(account_queries)}개 계정, {total_instances}개 인스턴스"
                )
            else:
                logger.info(
                    f"{target_date.strftime('%Y년 %m월')} 슬로우 쿼리 수집 완료: "
                    f"{len(account_queries)}개 계정, {total_instances}개 인스턴스"
                )

            if collected_instances:
                logger.info(f"수집된 인스턴스: {', '.join(collected_instances)}")
            else:
                logger.warning("수집된 인스턴스가 없습니다")

        except Exception as e:
            logger.error(f"슬로우 쿼리 수집 중 오류 발생: {e}")
            raise  # 상위 예외 처리기로 전파

        finally:
            try:
                await MongoDBConnector.close()
            except Exception as e:
                logger.error(f"MongoDB 연결 종료 중 오류 발생: {e}")

    except Exception as e:
        logger.error(f"프로그램 실행 중 오류 발생: {e}")
        sys.exit(1)


if __name__ == "__main__":
    import asyncio
    import argparse
    from datetime import datetime, timedelta

    # 수동 설정 (테스트용)
    # MANUAL_DATE = "2024-11-07"  # 원하는 날짜로 설정 (YYYY-MM-DD)
    # MANUAL_MODE = "daily"  # 수집 모드 설정 ('daily' 또는 'monthly')
    # MANUAL_PERIOD = None  # 월간 수집시 기간 설정 (YYYY-MM 형식)

    # 월간 수집
    MANUAL_DATE = None
    MANUAL_MODE = "monthly"
    MANUAL_PERIOD = "2024-11"

    # MANUAL_DATE = None          # 수동 설정을 사용하지 않을 때는 None으로 설정
    # MANUAL_MODE = None
    # MANUAL_PERIOD = None

    # 커맨드 라인 인자 파서 설정
    parser = argparse.ArgumentParser(description='RDS 슬로우 쿼리 수집기')
    parser.add_argument(
        '--date',
        type=str,
        help='수집할 날짜 (YYYY-MM-DD 형식). 미입력시 어제 날짜 사용'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['daily', 'monthly'],
        default='daily',
        help='수집 모드 선택 (기본값: daily)'
    )
    parser.add_argument(
        '--period',
        type=str,
        help='월간 수집 시 기간 (YYYY-MM 형식)'
    )
    parser.add_argument(
        '--env',
        type=str,
        default='prd',
        choices=['dev', 'prd'],
        help='실행 환경 (기본값: prd)'
    )

    args = parser.parse_args()

    # 모드 처리
    mode = MANUAL_MODE if MANUAL_MODE else args.mode

    # 날짜 처리
    if mode == 'monthly' and (MANUAL_PERIOD or args.period):
        # 월간 수집 모드일 때
        period_str = MANUAL_PERIOD or args.period
        try:
            target_date = datetime.strptime(period_str, '%Y-%m')
            logger.info(f"월간 수집 기간: {period_str}")
        except ValueError:
            logger.error(f"잘못된 기간 형식: {period_str} (YYYY-MM 형식으로 입력)")
            sys.exit(1)
    else:
        # 일간 수집 모드이거나 기간이 지정되지 않았을 때
        if MANUAL_DATE:
            try:
                target_date = datetime.strptime(MANUAL_DATE, '%Y-%m-%d')
                logger.info(f"수동 설정된 날짜 사용: {MANUAL_DATE}")
            except ValueError:
                logger.error(f"잘못된 수동 날짜 형식: {MANUAL_DATE} (YYYY-MM-DD 형식으로 입력)")
                sys.exit(1)
        elif args.date:
            try:
                target_date = datetime.strptime(args.date, '%Y-%m-%d')
            except ValueError:
                logger.error(f"잘못된 날짜 형식: {args.date} (YYYY-MM-DD 형식으로 입력)")
                sys.exit(1)
        else:
            # 환경 변수에서 날짜 정보 가져오기 (옵션)
            target_date_str = os.getenv('TARGET_DATE')
            if target_date_str:
                try:
                    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
                except ValueError:
                    logger.error(f"잘못된 날짜 형식: {target_date_str} (YYYY-MM-DD 형식으로 입력)")
                    sys.exit(1)
            else:
                # 기본값: 어제
                target_date = datetime.now(kst) - timedelta(days=1)

    # 환경 설정
    os.environ['ENV'] = args.env

    # 로깅
    if mode == 'daily':
        logger.info(f"일간 수집 시작: 날짜={target_date.strftime('%Y-%m-%d')}, 환경={args.env}")
    else:
        logger.info(f"월간 수집 시작: 기간={target_date.strftime('%Y년 %m월')}, 환경={args.env}")

    # Windows에서 실행 시 필요한 설정
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # 비동기 이벤트 루프 실행
    asyncio.run(collect_slow_queries(target_date, mode=mode))