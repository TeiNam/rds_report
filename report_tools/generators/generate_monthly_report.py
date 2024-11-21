# scripts/generate_monthly_report.py
import asyncio
import os
import json
from datetime import datetime, date
from pathlib import Path
from report_tools.instance_statistics import InstanceStatisticsTool
from report_tools.generators.instance_report import ReportGenerator
from report_tools.generators.base import BaseReportGenerator


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


async def generate_monthly_report(year: int = None, month: int = None):
    """월간 인스턴스 리포트 생성"""
    try:
        # 리포트 생성기 초기화
        generator = MonthlyReportGenerator(year, month)

        # 시작일과 종료일 설정
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year + 1}-01-01"
        else:
            end_date = f"{year}-{month + 1:02d}-01"

        print(f"\n=== {year}년 {month}월 RDS 인스턴스 리포트 생성 ===")
        print(f"기간: {start_date} ~ {end_date}")
        print(f"출력 경로: {generator.output_dir}")

        # 기간별 통계 수집
        stats_tool = InstanceStatisticsTool(start_date=start_date, end_date=end_date)
        period_stats = await stats_tool.get_period_statistics()

        print("\n1. 기간별 통계 수집 완료")
        print(f"- 시작 시점 인스턴스 수: {period_stats['total_instances_start']}")
        print(f"- 종료 시점 인스턴스 수: {period_stats['total_instances_end']}")
        print(f"- 추가된 인스턴스: {period_stats['instances_added']}개")
        print(f"- 제거된 인스턴스: {period_stats['instances_removed']}개")

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

        # 리포트 및 그래프 생성
        report_generator = ReportGenerator(generator.output_dir)
        report_file = report_generator.create_report(daily_stats)

        print("\n4. 리포트 생성 완료")
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

            if period_stats['added_instances']:
                f.write("### 신규 생성된 인스턴스\n")
                for instance in sorted(period_stats['added_instances']):
                    f.write(f"- {instance}\n")
                f.write("\n")

            if period_stats['removed_instances']:
                f.write("### 삭제된 인스턴스\n")
                for instance in sorted(period_stats['removed_instances']):
                    f.write(f"- {instance}\n")
                f.write("\n")

        print("\n5. 월간 변경사항 추가 완료")
        print("\n작업이 완료되었습니다!")

    except Exception as e:
        print(f"\n오류 발생: {str(e)}")
        raise

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