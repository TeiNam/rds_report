# scripts/generate_monthly_report.py
import asyncio
import os
import json
import logging
from calendar import monthrange
from datetime import date, datetime
from zoneinfo import ZoneInfo
from report_tools.instance_statistics import InstanceStatisticsTool
from report_tools.generators.instance_report import ReportGenerator
from report_tools.generators.base import BaseReportGenerator
from report_tools.generators.metric_visualizer import MetricVisualizer
from report_tools.generators.instance_trend import InstanceTrendGenerator
from modules.mongodb_connector import MongoDBConnector
from configs.report_settings import ReportSettings
from configs.mongo_conf import mongo_settings

logger = logging.getLogger(__name__)

class MonthlyReportGenerator(BaseReportGenerator):
    """월간 리포트 생성기"""

    def __init__(self, year: int = None, month: int = None):
        # 년월 설정
        today = date.today()
        self.year = year or today.year
        self.month = month or today.month

        # 대상 연월 형식의 디렉토리 이름 설정
        target_date = f"{self.year}{self.month:02d}"

        # BaseReportGenerator 초기화
        # output_dir을 reports/YYYYMM으로 직접 지정
        report_dir = os.path.join(self._find_project_root(), "reports", target_date)
        super().__init__(output_dir=report_dir)

        # 리포트 날짜를 대상 연월로 설정
        self.report_date = target_date


async def generate_monthly_report(year: int = None, month: int = None) -> dict:
    """월간 인스턴스 리포트 생성"""
    try:
        # 리포트 생성기 초기화
        generator = MonthlyReportGenerator(year, month)

        # 시작일과 종료일 설정
        start_date, end_date = get_month_date_range(year, month)

        print(f"\n=== {year}년 {month}월 RDS 인스턴스 리포트 생성 ===")
        print(f"기간: {start_date} ~ {end_date}")
        print(f"출력 경로: {generator.output_dir}")

        # 기간별 통계 수집
        stats_tool = InstanceStatisticsTool(start_date=start_date, end_date=end_date)
        period_stats = await stats_tool.get_period_statistics()

        print("\n1. 기간별 통계 수집 완료")
        print(f"- 시작 시점 인스턴스 수: {period_stats['total_instances_start']}")
        print(f"- 종료 시점 인스턴스 수: {period_stats['total_instances_end']}")
        print(f"- 추가된 인스턴스: {len(period_stats['instances_added'])}개")
        print(f"- 제거된 인스턴스: {len(period_stats['instances_removed'])}개")

        # 마지막 날짜의 상세 통계 수집
        last_date = datetime.strptime(period_stats['data_range']['end'], "%Y-%m-%d")
        daily_stats = await stats_tool.get_daily_statistics(target_date=last_date)

        print("\n2. 상세 통계 수집 완료")
        print(f"- 총 인스턴스 수: {daily_stats['total_instances']}")
        print(f"- 개발 인스턴스: {daily_stats['dev_instances']}")
        print(f"- 운영 인스턴스: {daily_stats['prd_instances']}")

        # 통계 데이터 결합
        report_data = {
            **daily_stats,
            "period_statistics": period_stats
        }

        # JSON 파일로 저장
        json_file = generator.get_report_path("statistics.json")
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        print(f"\n3. 통계 데이터 저장 완료: {json_file}")

        # MongoDB에 저장
        await save_to_mongodb(report_data, year, month)

        print("\n4. MongoDB 저장 완료")

        # 리포트 및 그래프 생성
        report_generator = ReportGenerator(generator.output_dir)
        # await 추가
        report_file = await report_generator.create_report(daily_stats)

        print("\n5. 리포트 생성 완료")
        print(f"- 리포트 파일: {report_file}")
        print(f"- 그래프 경로: {report_generator.graphs_dir}")

        # rds_report.md 파일을 대상 월의 파일명으로 변경
        report_date = f"{year}{month:02d}"
        target_file = generator.get_report_path(f"rds_report_{report_date}.md")
        os.rename(report_file, target_file)
        report_file = target_file

        # 월간 변경사항 추가
        with open(report_file, "a", encoding="utf-8") as f:
            f.write(
                f"\n## 3. 월간 변경사항 ({period_stats['data_range']['start']} ~ {period_stats['data_range']['end']})\n\n")
            f.write(f"- 시작 시점 인스턴스 수: {period_stats['total_instances_start']}\n")
            f.write(f"- 종료 시점 인스턴스 수: {period_stats['total_instances_end']}\n")
            f.write(f"- 순증감: {period_stats['total_instances_end'] - period_stats['total_instances_start']}\n\n")

            if period_stats['instances_added']:
                f.write("### 신규 생성된 인스턴스\n")
                # 생성일 기준으로 정렬 (오래된 순)
                sorted_added = sorted(
                    period_stats['instances_added'],
                    key=lambda x: x['created_at']
                )
                for instance in sorted_added:
                    f.write(f"- {instance['id']} (생성일: {instance['created_at']})\n")
                f.write("\n")

            if period_stats['instances_removed']:
                f.write("### 삭제된 인스턴스\n")
                # 삭제일 기준으로 정렬 (오래된 순)
                sorted_removed = sorted(
                    period_stats['instances_removed'],
                    key=lambda x: x['deleted_at']
                )
                for instance in sorted_removed:
                    f.write(f"- {instance['id']} (삭제일: {instance['deleted_at']})\n")
                f.write("\n")

        print("\n6-2. 인스턴스 변동 현황 추가 시작")
        trend_data = []

        # MongoDB에서 3개월치 데이터 조회
        trend_data = []
        # MongoDB 데이터베이스 연결
        db = await MongoDBConnector.get_database()

        # 컬렉션 이름 동적으로 설정
        collection_name = mongo_settings.MONGO_MONTHLY_INSTANCE_STATISTICS_COLLECTION
        collection = db[collection_name]

        for m in range(month - 2, month + 1):
            y = year
            if m <= 0:
                y -= 1
                m += 12
            elif m > 12:
                y += 1
                m -= 12

            try:
                data = await collection.find_one(
                    {
                        "year": y,
                        "month": m
                    },
                    {
                        "year": 1,
                        "month": 1,
                        "statistics": {
                            "total_instances": 1,
                            "period_statistics": {
                                "instances_added": 1,
                                "instances_removed": 1
                            }
                        }
                    }
                )
                if data:
                    trend_data.append(data)
            except Exception as e:
                logger.error(f"{y}년 {m}월 데이터 조회 실패: {e}")

        if trend_data:
            print(f"- {len(trend_data)}개월 데이터 조회 완료")
            # 날짜순 정렬
            trend_data.sort(key=lambda x: (x['year'], x['month']))

            # 변동 추이 생성기 초기화 및 실행
            trend_generator = InstanceTrendGenerator(generator.output_dir)
            trend_generator.append_trend_section(target_file, trend_data)
            print("- 변동 추이 분석 추가 완료")
        else:
            logger.warning("변동 추이를 분석할 데이터가 없습니다")
            with open(target_file, "a", encoding="utf-8") as f:
                f.write("\n### 3. 인스턴스 변동 추이\n\n")
                f.write("> ⚠️ 분석할 데이터가 없습니다.\n\n")

        print("\n6. 월간 변경사항 추가 완료")

        # 메트릭 시각화 추가
        print("\n7. 메트릭 시각화 추가 시작")

        # MongoDB에서 메트릭 데이터 조회
        db = await MongoDBConnector.get_database()
        collection = db[mongo_settings.MONGO_MONTHLY_CW_RDS_METRIC_COLLECTION]

        # 대상 인스턴스 정보 가져오기
        target_instances = ReportSettings.get_report_target_instances()
        print(f"리포트 대상 인스턴스: {target_instances}")

        if not target_instances:
            print("분석할 대상 인스턴스가 지정되지 않았습니다.")
            return

        # 이번 달을 포함한 3개월치 데이터 조회
        metric_data = []
        for m in range(month - 2, month + 1):
            y = year
            if m <= 0:
                y -= 1
                m += 12
            elif m > 12:
                y += 1
                m -= 12

            try:
                cursor = collection.find({
                    "env": "prd",
                    "year": y,
                    "month": m,
                    "instance_id": {"$in": target_instances}
                })

                async for doc in cursor:
                    # 연도-월 정보를 추가
                    doc['yearmonth'] = f"{y}-{m:02d}"  # 추가된 부분
                    print(f"- {y}년 {m}월 {doc['instance_id']} 메트릭 데이터 조회 완료")
                    metric_data.append(doc)

            except Exception as e:
                logger.error(f"{y}년 {m}월 메트릭 데이터 조회 실패: {e}")

        if metric_data:
            print(f"총 {len(metric_data)}개월의 메트릭 데이터 조회됨")

            # 연도와 월 기준으로 정렬
            metric_data.sort(key=lambda x: (x['year'], x['month']))

            # 시각화 생성
            visualizer = MetricVisualizer(generator.output_dir)
            visualizer.create_metric_visualizations(
                target_instances,
                metric_data,  # 정렬된 데이터 전달
                report_file
            )
            print("메트릭 시각화 완료")
        else:
            print("메트릭 데이터가 없습니다")
            with open(report_file, "a", encoding="utf-8") as f:
                f.write("\n## 4. 메트릭 분석\n\n")
                f.write("> ⚠️ 분석할 메트릭 데이터가 없습니다.\n\n")

        # 리포트 파일 경로
        report_date = f"{year}{month:02d}"
        target_file = generator.get_report_path(f"rds_report_{report_date}.md")

        # 처리 결과 반환
        return {
            "output_dir": generator.output_dir,
            "report_file": target_file,
            "report_date": report_date,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            }
        }

    except Exception as e:
        print(f"\n오류 발생: {str(e)}")
        raise


def get_month_date_range(year: int, month: int) -> tuple[str, str]:
    """해당 월의 시작일과 마지막일 반환

    Args:
        year: 년도
        month: 월

    Returns:
        tuple[str, str]: (시작일, 마지막일) - YYYY-MM-DD 형식
    """
    # monthrange는 해당 월의 1일의 요일과 총 일수를 반환
    _, last_day = monthrange(year, month)

    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day}"

    return start_date, end_date


def get_previous_month(current_date: date = None) -> tuple[int, int]:
    """전월의 년도와 월을 반환

    Args:
        current_date: 기준 날짜 (기본값: 오늘)

    Returns:
        tuple[int, int]: (년도, 월)
    """
    if current_date is None:
        current_date = date.today()

    if current_date.month == 1:
        return current_date.year - 1, 12
    else:
        return current_date.year, current_date.month - 1

async def save_to_mongodb(report_data: dict, year: int, month: int):
    """MongoDB에 통계 데이터 저장"""

    try:
        # MongoDB 데이터베이스 연결
        db = await MongoDBConnector.get_database()

        # 컬렉션 이름 동적으로 설정
        collection_name = mongo_settings.MONGO_MONTHLY_INSTANCE_STATISTICS_COLLECTION
        collection = db[collection_name]

        # 저장할 데이터 준비
        data_to_save = {
            "year": year,
            "month": month,
            "report_date": f"{year}-{month:02d}",
            "statistics": report_data,
            "created_at": datetime.now(ZoneInfo("Asia/Seoul"))  # KST 시간
        }

        # 데이터 저장 (기존 데이터가 있으면 업데이트, 없으면 삽입)
        await collection.update_one(
            {"year": year, "month": month},  # 중복 방지 조건
            {"$set": data_to_save},  # 데이터를 덮어씀
            upsert=True  # 문서가 없으면 삽입
        )

        print(f"MongoDB에 월간 통계 저장 완료: {year}-{month:02d}")

    except Exception as e:
        print(f"MongoDB 저장 중 오류 발생: {str(e)}")
        raise


if __name__ == "__main__":
    import sys
    from datetime import date

    # 인자가 없으면 전월 사용
    if len(sys.argv) == 1:
        year, month = get_previous_month()
        print(f"날짜가 지정되지 않아 전월({year}년 {month}월)의 리포트를 생성합니다.")

    # YYYYMM 형식으로 입력받기
    elif len(sys.argv) == 2:
        try:
            date_str = sys.argv[1]
            if len(date_str) != 6:
                raise ValueError("날짜는 YYYYMM 형식으로 입력해주세요. (예: 202403)")
            year = int(date_str[:4])
            month = int(date_str[4:])
            if not (1 <= month <= 12):
                raise ValueError("월은 1-12 사이의 값이어야 합니다.")
        except ValueError as e:
            print(f"오류: {str(e)}")
            sys.exit(1)
    else:
        print("사용법: python generate_monthly_report.py [YYYYMM]")
        print("예: python generate_monthly_report.py 202403")
        print("입력이 없으면 전월의 리포트를 생성합니다.")
        sys.exit(1)

    print(f"\n{year}년 {month}월의 리포트를 생성합니다...")
    asyncio.run(generate_monthly_report(year, month))