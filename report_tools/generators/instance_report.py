# report_tools/generators/instance_report.py
from typing import Dict, Any, List
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.font_manager as fm
import os
from pathlib import Path
from datetime import date, datetime
import logging
from report_tools.generators.base import BaseReportGenerator
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings

logger = logging.getLogger(__name__)


class ReportGenerator(BaseReportGenerator):
    """RDS 인스턴스 리포트 생성기"""

    # 그래프 색상
    COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#82ca9d']

    def __init__(self, output_dir: str = None):
        """
        Args:
            output_dir: 리포트 출력 디렉토리 (기본값: 프로젝트 루트의 reports)
        """
        super().__init__(output_dir)

        # 그래프 디렉토리 설정
        self.graphs_dir = self.create_subdirectory("graphs")

        # AWS 계정 정보 저장을 위한 딕셔너리
        self.account_names = {}

        # 한글 폰트 설정
        self._set_korean_font()

        # 그래프 스타일 설정
        sns.set_theme(style="whitegrid")
        plt.style.use('default')
        sns.set_palette(self.COLORS)

    def _set_korean_font(self):
        """한글 폰트 설정"""
        try:
            # 현재 파일의 위치를 기준으로 폰트 파일 경로 설정
            current_dir = Path(__file__).parent
            font_path = current_dir / 'fonts' / 'MaruBuri.ttf'

            if not font_path.exists():
                raise FileNotFoundError(f"폰트 파일을 찾을 수 없습니다: {font_path}")

            # 폰트 매니저에 폰트 추가
            fm.fontManager.addfont(str(font_path))
            font_name = 'MaruBuri'

            # matplotlib 폰트 설정
            plt.rcParams['font.family'] = font_name
            plt.rcParams['axes.unicode_minus'] = False

            # seaborn 폰트 설정
            sns.set_style("whitegrid", {
                'font.family': font_name,
            })

            logger.info("한글 폰트 설정 완료")
        except Exception as e:
            logger.error(f"한글 폰트 설정 실패: {str(e)}")
            raise

    async def load_account_names(self):
        """MongoDB에서 AWS 계정 정보 로드"""
        try:
            collection = await MongoDBConnector.get_collection(
                mongo_settings.MONGO_AWS_ACCOUNT_COLLECTION
            )
            async for doc in collection.find({}, {'aws_account_id': 1, 'aws_account_name': 1}):
                self.account_names[doc['aws_account_id']] = doc['aws_account_name']
            logger.info(f"AWS 계정 정보 로드 완료: {len(self.account_names)}개")
        except Exception as e:
            logger.error(f"AWS 계정 정보 로드 실패: {str(e)}")
            raise

    def get_account_name(self, account_id: str) -> str:
        """계정 ID에 해당하는 이름 반환"""
        return self.account_names.get(account_id, account_id)

    async def create_report(self, data: Dict[str, Any]) -> str:
        """리포트 생성"""
        try:
            # AWS 계정 정보 로드
            await self.load_account_names()

            # 계정 정보 매핑
            for account in data['accounts']:
                account_id = account['account_id']
                account['display_name'] = self.get_account_name(account_id)

            logger.info("데이터 확인:")
            logger.info(f"- total_instances: {data.get('total_instances')}")
            logger.info(f"- dev_instances: {data.get('dev_instances')}")
            logger.info(f"- prd_instances: {data.get('prd_instances')}")
            logger.info(f"- accounts count: {len(data.get('accounts', []))}")
            logger.info(f"- regions count: {len(data.get('regions', []))}")

            # 그래프 생성
            self._create_pie_chart(
                data['accounts'], 'display_name', 'instance_count',
                "계정별 인스턴스 분포", "account_distribution.png"
            )

            self._create_pie_chart(
                data['regions'], 'region', 'instance_count',
                "리전별 인스턴스 분포", "region_distribution.png"
            )

            env_data = [
                {'name': '개발', 'count': data['dev_instances']},
                {'name': '운영', 'count': data['prd_instances']}
            ]
            self._create_pie_chart(
                env_data, 'name', 'count',
                "개발/운영 환경별 분포", "env_distribution.png"
            )

            class_data = [{'name': k, 'count': v} for k, v in data['instance_classes'].items()]
            class_data.sort(key=lambda x: x['count'], reverse=True)
            self._create_bar_chart(
                class_data, "인스턴스 클래스별 분포", "class_distribution.png"
            )

            # 마크다운 리포트 생성
            report_file = self.get_report_path("rds_report.md")
            await self._create_markdown_report(data, report_file)

            return report_file

        except Exception as e:
            logger.error(f"리포트 생성 중 오류 발생: {str(e)}")
            raise

    def _create_pie_chart(self, data: List[Dict], label_key: str, value_key: str,
                          title: str, filename: str) -> str:
        """파이 차트 생성"""
        plt.figure(figsize=(10, 8))

        # 데이터 준비 및 유효성 검사
        valid_data = []
        for item in data:
            value = item[value_key]
            if value and value > 0:  # None, 0, NaN 체크
                valid_data.append((item[label_key], value))

        # 폰트 설정
        font_prop = fm.FontProperties(fname=str(Path(__file__).parent / 'fonts' / 'MaruBuri.ttf'))

        if not valid_data:
            plt.text(0.5, 0.5, '데이터 없음',
                     horizontalalignment='center',
                     verticalalignment='center',
                     transform=plt.gca().transAxes,
                     fontproperties=font_prop)
            plt.axis('off')
        else:
            try:
                labels, values = zip(*valid_data)
                total = sum(values)
                if total <= 0:
                    raise ValueError("Total sum of values is 0 or negative")

                plt.pie(values,
                        labels=labels,
                        autopct='%1.1f%%',
                        colors=self.COLORS[:len(valid_data)],
                        textprops={'fontproperties': font_prop})

                plt.title(title, pad=20, size=14, fontproperties=font_prop)

                legend = plt.legend(labels,
                                    title="항목",
                                    loc="upper left",
                                    bbox_to_anchor=(1, 0, 0.5, 1),
                                    prop=font_prop)
                legend.get_title().set_fontproperties(font_prop)

            except Exception as e:
                logger.error(f"파이 차트 생성 중 오류 발생: {str(e)}")
                plt.clf()
                plt.text(0.5, 0.5, '데이터 없음',
                         horizontalalignment='center',
                         verticalalignment='center',
                         transform=plt.gca().transAxes,
                         fontproperties=font_prop)
                plt.axis('off')

        output_path = os.path.join(self.graphs_dir, filename)
        plt.savefig(output_path, bbox_inches='tight', dpi=300)
        plt.close()

        return os.path.join("graphs", filename)

    def _create_bar_chart(self, data: List[Dict], title: str, filename: str) -> str:
        """바 차트 생성"""
        plt.figure(figsize=(12, 6))

        names = [item['name'] for item in data]
        counts = [item['count'] for item in data]

        font_prop = fm.FontProperties(fname=str(Path(__file__).parent / 'fonts' / 'MaruBuri.ttf'))

        bars = plt.bar(range(len(names)), counts, color=self.COLORS[:len(data)])

        plt.xticks(range(len(names)), names, rotation=45, ha='right', fontproperties=font_prop)

        for i, count in enumerate(counts):
            plt.text(i, count, str(count), ha='center', va='bottom')

        plt.title(title, pad=20, size=14, fontproperties=font_prop)
        plt.xlabel("인스턴스 클래스", fontproperties=font_prop)
        plt.ylabel("인스턴스 수", fontproperties=font_prop)

        plt.tight_layout()

        output_path = os.path.join(self.graphs_dir, filename)
        plt.savefig(output_path, bbox_inches='tight', dpi=300)
        plt.close()

        return os.path.join("graphs", filename)

    def _calculate_percentage(self, value: int, total: int) -> str:
        """백분율 계산"""
        if total == 0:
            return "0.0"
        return f"{(value / total) * 100:.1f}"

    async def _create_markdown_report(self, data: Dict[str, Any], output_file: str):
        """마크다운 형식의 리포트 생성"""
        report_date = data.get('date', date.today().strftime("%Y-%m-%d"))
        try:
            formatted_date = datetime.strptime(report_date, "%Y-%m-%d").strftime("%Y-%m")
        except:
            formatted_date = date.today().strftime("%Y-%m")

        total_instances = data['total_instances']

        report_content = f"""# RDS 인스턴스 분석 리포트 ({formatted_date})

## 1. 요약
- 총 인스턴스 수: {total_instances}
- 개발 인스턴스: {data['dev_instances']}
- 운영 인스턴스: {data['prd_instances']}
- 총 계정 수: {data['account_count']}
- 총 리전 수: {data['region_count']}

## 2. 인스턴스 분포 현황

### 2.1 개발/운영 환경별 분포
![개발/운영 환경별 분포](graphs/env_distribution.png)

| 환경 | 인스턴스 수 | 비율(%) |
|------|------------|---------|
| 개발 | {data['dev_instances']} | {self._calculate_percentage(data['dev_instances'], total_instances)} |
| 운영 | {data['prd_instances']} | {self._calculate_percentage(data['prd_instances'], total_instances)} |

### 2.2 계정별 인스턴스 현황
![계정별 분포](graphs/account_distribution.png)

| Account Name | Account ID | Instance Count | 비율(%) |
|-------------|------------|----------------|---------|"""

        for account in sorted(data['accounts'], key=lambda x: x['instance_count'], reverse=True):
            percentage = self._calculate_percentage(account['instance_count'], total_instances)
            report_content += f"\n| {account['display_name']} | {account['account_id']} | {account['instance_count']} | {percentage} |"

        report_content += "\n\n### 2.3 리전별 인스턴스 현황\n"
        report_content += "![리전별 분포](graphs/region_distribution.png)\n\n"
        report_content += "| Region | Instance Count | 비율(%) |\n"
        report_content += "|--------|----------------|----------|\n"

        for region in sorted(data['regions'], key=lambda x: x['instance_count'], reverse=True):
            percentage = self._calculate_percentage(region['instance_count'], total_instances)
            report_content += f"| {region['region']} | {region['instance_count']} | {percentage} |\n"

        report_content += "\n### 2.4 인스턴스 클래스별 현황\n"
        report_content += "![인스턴스 클래스별 분포](graphs/class_distribution.png)\n\n"
        report_content += "| Instance Class | Count | 비율(%) |\n"
        report_content += "|----------------|-------|----------|\n"

        sorted_classes = sorted(data['instance_classes'].items(), key=lambda x: x[1], reverse=True)
        for class_name, count in sorted_classes:
            percentage = self._calculate_percentage(count, total_instances)
            report_content += f"| {class_name} | {count} | {percentage} |\n"

        # 리포트 파일 저장
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(report_content)


if __name__ == "__main__":
    import asyncio
    from report_tools.instance_statistics import InstanceStatisticsTool

    async def test_generate():
        try:
            # MongoDB 초기화
            await MongoDBConnector.initialize()

            # 통계 데이터 수집
            stats_tool = InstanceStatisticsTool()
            daily_stats = await stats_tool.get_daily_statistics()

            # 리포트 생성
            generator = ReportGenerator()
            report_file = await generator.create_report(daily_stats)
            print(f"리포트가 성공적으로 생성되었습니다: {report_file}")
            print(f"그래프 파일들이 {generator.graphs_dir} 디렉토리에 저장되었습니다.")

        except Exception as e:
            print(f"리포트 생성 중 오류가 발생했습니다: {str(e)}")
            raise

        finally:
            # MongoDB 연결 종료
            await MongoDBConnector.close()

            # asyncio 이벤트 루프 실행
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        asyncio.run(test_generate())