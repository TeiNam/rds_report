import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
from datetime import datetime
import os
from pathlib import Path
import pandas as pd
from typing import List, Dict, Any, TextIO
from .base import BaseReportGenerator


class MetricVisualizer(BaseReportGenerator):
    """CloudWatch 메트릭 시각화 도구"""

    # 관심 메트릭 정의
    TARGET_METRICS = [
        'CPUUtilization',
        'DatabaseConnections',
        'ReadIOPS',
        'WriteIOPS',
        'NetworkReceiveThroughput',
        'NetworkTransmitThroughput'
    ]

    def __init__(self, output_dir: str = None):
        super().__init__(output_dir)
        self.graphs_dir = self.create_subdirectory("graphs")
        self._setup_plot_style()

    def _setup_plot_style(self):
        """그래프 스타일 설정"""
        # 폰트 설정
        font_path = Path(__file__).parent / 'fonts' / 'MaruBuri.ttf'
        if not font_path.exists():
            raise FileNotFoundError(f"폰트 파일을 찾을 수 없습니다: {font_path}")

        fm.fontManager.addfont(str(font_path))
        plt.rcParams['font.family'] = 'MaruBuri'
        plt.rcParams['axes.unicode_minus'] = False

        # 스타일 설정
        sns.set_theme(style="whitegrid")
        plt.style.use('default')

    def create_metric_visualizations(
            self,
            instance_ids: List[str],
            metric_data: List[Dict[str, Any]],
            report_file: str
    ) -> None:
        """
        지정된 인스턴스들의 메트릭 시각화

        Args:
            instance_ids: 대상 인스턴스 ID 목록
            metric_data: MongoDB에서 조회한 메트릭 데이터 목록
            report_file: 리포트 파일 경로
        """
        # 데이터 전처리
        daily_metrics = self._prepare_daily_metrics(instance_ids, metric_data)
        monthly_summary = self._prepare_monthly_summary(instance_ids, metric_data)

        # 그래프 생성
        for metric_name in self.TARGET_METRICS:
            self._create_line_plot(
                daily_metrics[metric_name],
                metric_name,
                f"metric_{metric_name.lower()}.png"
            )

        # 마크다운 추가
        self._append_to_report(
            report_file,
            daily_metrics,
            monthly_summary
        )

    def _prepare_daily_metrics(
            self,
            instance_ids: List[str],
            metric_data: List[Dict[str, Any]]
    ) -> Dict[str, pd.DataFrame]:
        """일별 메트릭 데이터 준비"""
        metric_frames = {}

        print("\n=== 일별 메트릭 데이터 준비 ===")
        print(f"분석 대상 인스턴스: {instance_ids}")

        for metric_name in self.TARGET_METRICS:
            data_list = []

            for doc in metric_data:
                instance_id = doc['instance_id']

                # 정확히 일치하는 인스턴스만 처리
                if instance_id not in instance_ids:
                    print(f"- {instance_id}: 대상 인스턴스({', '.join(instance_ids)})가 아님")
                    continue

                print(f"\n처리 중: {doc['year']}년 {doc['month']}월 {instance_id}")
                daily_metrics = doc.get('daily_metrics', {})

                # 각 날짜별 메트릭 처리
                for date, metrics in daily_metrics.items():
                    if metric_name in metrics:
                        metric_info = metrics[metric_name]
                        data_list.append({
                            'date': datetime.strptime(date, '%Y-%m-%d'),
                            'instance_id': instance_id,
                            'value': metric_info['avg']
                        })
                        print(f"- {date} {metric_name} 데이터 처리 완료")

            if data_list:
                df = pd.DataFrame(data_list)
                df = df.pivot(index='date', columns='instance_id', values='value')
                metric_frames[metric_name] = df
                print(f"\n{metric_name} 데이터프레임 생성 완료:")
                print(f"- 기간: {df.index.min()} ~ {df.index.max()}")
                print(f"- 인스턴스: {df.columns.tolist()}")

        return metric_frames

    def _prepare_monthly_summary(
            self,
            instance_ids: List[str],
            metric_data: List[Dict[str, Any]]
    ) -> Dict[str, pd.DataFrame]:
        """월별 요약 통계 준비"""
        summary_data = {metric: [] for metric in self.TARGET_METRICS}

        # 연도와 월 정보로 정렬
        sorted_metric_data = sorted(
            metric_data,
            key=lambda x: x['yearmonth']  # yearmonth로 정렬
        )

        for doc in sorted_metric_data:
            instance_id = doc['instance_id']
            year = doc['year']
            month = doc['month']
            yearmonth = doc['yearmonth']  # 추가

            if instance_id not in instance_ids:
                continue

            monthly_summary = doc.get('monthly_summary', {})

            for metric_name in self.TARGET_METRICS:
                metric_summary = monthly_summary.get(metric_name, {})
                if metric_summary:
                    summary_data[metric_name].append({
                        'year': year,
                        'month': month,
                        'yearmonth': yearmonth,  # 추가
                        'instance_id': instance_id,
                        'avg': metric_summary.get('avg', 0),
                        'max': metric_summary.get('max', {}).get('value', 0)
                    })

        summary_frames = {}
        for metric_name, data in summary_data.items():
            if data:
                df = pd.DataFrame(data)
                df = df.sort_values('yearmonth')  # yearmonth로 정렬
                summary_frames[metric_name] = df

        return summary_frames

    def _create_line_plot(self, df: pd.DataFrame, metric_name: str, filename: str):
        """라인 그래프 생성"""
        plt.figure(figsize=(12, 6))

        # 폰트 설정
        font_path = Path(__file__).parent / 'fonts' / 'MaruBuri.ttf'
        font_prop = fm.FontProperties(fname=str(font_path))

        # 라인 플롯 생성
        for column in df.columns:
            plt.plot(df.index, df[column], label=column, marker='o', markersize=4)

        # 한글 제목 매핑
        title_map = {
            'CPUUtilization': 'CPU 사용률 (%)',
            'DatabaseConnections': 'DB 연결 수',
            'ReadIOPS': '읽기 IOPS',
            'WriteIOPS': '쓰기 IOPS',
            'NetworkReceiveThroughput': '네트워크 수신량 (Bytes/s)',
            'NetworkTransmitThroughput': '네트워크 송신량 (Bytes/s)'
        }

        # 그래프 스타일링 - 폰트 명시적 지정
        plt.title(title_map.get(metric_name, metric_name),
                  pad=20, size=14, fontproperties=font_prop)
        plt.xlabel('날짜', fontproperties=font_prop)
        plt.ylabel('값', fontproperties=font_prop)

        # x축 주요 날짜 설정 (1, 5, 10, 15, 20, 25일)
        all_dates = df.index
        major_days = [1, 5, 10, 15, 20, 25]

        # 주요 날짜에 해당하는 인덱스와 레이블 준비
        major_dates = []
        major_date_labels = []

        for date in all_dates:
            if date.day in major_days:
                major_dates.append(date)
                # MM/DD 형식으로 표시
                major_date_labels.append(date.strftime('%m/%d'))

        # x축 설정
        plt.gca().set_xticks(major_dates)  # 주요 날짜 위치 설정
        plt.gca().set_xticklabels(major_date_labels,
                                  rotation=45,
                                  ha='right',
                                  fontproperties=font_prop)

        # 모든 날짜에 대해 그리드 라인은 유지
        plt.gca().set_xticks(all_dates, minor=True)
        # 주요 날짜 그리드
        plt.grid(True, which='major', linestyle='-', alpha=0.3)
        # 나머지 날짜 그리드
        plt.grid(True, which='minor', linestyle=':', alpha=0.1)

        # 범례 설정
        legend = plt.legend(bbox_to_anchor=(1.05, 1),
                            loc='upper left',
                            prop=font_prop)

        # 범례 제목 폰트 설정
        if legend:
            legend.get_title().set_fontproperties(font_prop)

        # y축 레이블 값 폰트 설정
        plt.gca().yaxis.set_tick_params(labelsize=9)
        for label in plt.gca().yaxis.get_ticklabels():
            label.set_fontproperties(font_prop)

        # 여백 조정
        plt.tight_layout()

        # 저장
        output_path = os.path.join(self.graphs_dir, filename)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()

    def _append_to_report(
            self,
            report_file: str,
            daily_metrics: Dict[str, pd.DataFrame],
            monthly_summary: Dict[str, pd.DataFrame]
    ):
        """리포트에 메트릭 분석 추가"""
        with open(report_file, "a", encoding="utf-8") as f:
            f.write("\n## 4. 상세 메트릭 분석\n\n")

            for metric_name in self.TARGET_METRICS:
                title_map = {
                    'CPUUtilization': 'CPU 사용률',
                    'DatabaseConnections': 'DB 연결 수',
                    'ReadIOPS': '읽기 IOPS',
                    'WriteIOPS': '쓰기 IOPS',
                    'NetworkReceiveThroughput': '네트워크 수신량',
                    'NetworkTransmitThroughput': '네트워크 송신량'
                }

                index = list(self.TARGET_METRICS).index(metric_name) + 1
                metric_title = title_map.get(metric_name, metric_name)

                f.write(f"\n### 4.{index}. {metric_title}\n")
                f.write(f"![{metric_name}](graphs/metric_{metric_name.lower()}.png)\n\n")

                if metric_name in monthly_summary:
                    df = monthly_summary[metric_name]
                    if not df.empty:
                        self._write_monthly_statistics(f, metric_name, df)

    def _write_monthly_statistics(
            self,
            f: TextIO,
            metric_name: str,
            df: pd.DataFrame
    ):
        """월별 통계 테이블 작성"""
        f.write("#### 월별 통계\n\n")

        for instance_id in df['instance_id'].unique():
            instance_data = df[df['instance_id'] == instance_id].copy()

            # 헤더 작성
            header = "| 분류 | 인스턴스 |"
            separator = "|------|----------|"

            for _, row in instance_data.iterrows():
                header += f" {row['yearmonth']}월 |"
                separator += "-------------|"

            f.write(f"{header}\n")
            f.write(f"{separator}\n")

            # 평균값 행
            avg_row = f"| 평균 | {instance_id} |"
            prev_avg = None
            for _, row in instance_data.iterrows():
                current_avg = row['avg']
                if prev_avg is not None:
                    diff = current_avg - prev_avg
                    diff_text = self._format_change(diff, metric_name)
                else:
                    diff_text = ""
                avg_row += f" {self._format_metric_value(current_avg, metric_name)} {diff_text} |"
                prev_avg = current_avg
            f.write(f"{avg_row}\n")

            # 최대값 행도 동일한 방식으로 작성
            max_row = f"| 최대값 | {instance_id} |"
            prev_max = None
            for _, row in instance_data.iterrows():
                current_max = row['max']
                if prev_max is not None:
                    diff = current_max - prev_max
                    diff_text = self._format_change(diff, metric_name)
                else:
                    diff_text = ""
                max_row += f" {self._format_metric_value(current_max, metric_name)} {diff_text} |"
                prev_max = current_max
            f.write(f"{max_row}\n\n")

    def _group_instances(self, instance_ids: List[str]) -> Dict[str, List[str]]:
        """인스턴스를 그룹화"""
        groups = {
            "프로덕션 서비스": [],
            "읽기 전용 인스턴스": [],
            "기타 인스턴스": []
        }

        for instance_id in sorted(instance_ids):
            if instance_id.startswith("prd-") and "read" in instance_id:
                groups["읽기 전용 인스턴스"].append(instance_id)
            elif not instance_id.startswith("prd-"):
                groups["프로덕션 서비스"].append(instance_id)
            else:
                groups["기타 인스턴스"].append(instance_id)

        return {k: v for k, v in groups.items() if v}  # 비어있지 않은 그룹만 반환


    def _format_change(self, change: float, metric_name: str) -> str:
        """변동폭 포맷팅"""
        if change is None:
            return ""

        # 네트워크 메트릭의 경우 MB/s로 변환
        if 'NetworkReceiveThroughput' in metric_name or 'NetworkTransmitThroughput' in metric_name:
            change = change / (1024 * 1024)

        # 변동폭이 0이면 빈 문자열 반환
        if abs(change) < 0.01:
            return ""

        formatted_change = f"{abs(change):.2f}"

        if change > 0:
            return f" (<span style='color: #FF4B4B'>▲{formatted_change}</span>)"  # 빨간색
        else:
            return f" (<span style='color: #4B8AFC'>▼{formatted_change}</span>)"  # 파란색

    def _format_metric_value(self, value: float, metric_name: str) -> str:
        """메트릭 값 포맷팅"""
        if 'NetworkReceiveThroughput' in metric_name or 'NetworkTransmitThroughput' in metric_name:
            # bytes/s를 MB/s로 변환하고 소수점 2자리까지 표시
            return f"{value / (1024 * 1024):.2f}"
        return f"{value:.2f}"

    def _format_cell_content(
            self,
            content: str,
            max_length: int,
            align: str = 'left'
    ) -> str:
        """셀 내용 포맷팅"""
        content = str(content)
        if len(content) > max_length:
            content = content[:max_length - 3] + '...'
        else:
            padding = max_length - len(content)
            if align == 'right':
                content = ' ' * padding + content
            else:
                content = content + ' ' * padding
        return content