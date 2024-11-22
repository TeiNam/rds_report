import matplotlib.pyplot as plt
import numpy as np
import os
from pathlib import Path
from typing import List, Dict, Any
import matplotlib.font_manager as fm
import seaborn as sns
from .base import BaseReportGenerator


class InstanceTrendGenerator(BaseReportGenerator):
    """인스턴스 변동 추이 생성기"""

    # 그래프 색상
    COLORS = ['#2E86C1', '#28B463', '#E74C3C']  # 전체, 증가, 감소 순

    def __init__(self, output_dir: str = None):
        super().__init__(output_dir)
        self.graphs_dir = self.create_subdirectory("graphs")

        # 한글 폰트 설정
        self._set_korean_font()

        # 그래프 스타일 설정
        sns.set_theme(style="whitegrid")
        plt.style.use('default')
        sns.set_palette(self.COLORS)

    def _set_korean_font(self):
        """한글 폰트 설정"""
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

    def create_trend_chart(self, data_list: List[Dict[str, Any]], filename: str = "instance_trend.png") -> str:
        """
        인스턴스 변동 추이 차트 생성

        Args:
            data_list: MongoDB에서 조회한 월별 통계 데이터 목록
            filename: 저장할 파일명

        Returns:
            str: 저장된 파일의 상대 경로
        """
        plt.figure(figsize=(10, 6))

        # 폰트 설정
        font_path = Path(__file__).parent / 'fonts' / 'MaruBuri.ttf'
        font_prop = fm.FontProperties(fname=str(font_path))

        # 데이터 준비
        months = [f"{d['year']}-{d['month']:02d}" for d in data_list]
        totals = [d['statistics']['total_instances'] for d in data_list]
        added = [len(d['statistics']['period_statistics']['instances_added']) for d in data_list]
        removed = [len(d['statistics']['period_statistics']['instances_removed']) for d in data_list]

        # 막대 위치 설정
        x = np.arange(len(months))
        width = 0.25

        # 막대 그래프 생성
        bars1 = plt.bar(x - width, totals, width,
                        label='전체', color=self.COLORS[0])
        bars2 = plt.bar(x, added, width,
                        label='증가', color=self.COLORS[1])
        bars3 = plt.bar(x + width, removed, width,
                        label='감소', color=self.COLORS[2])

        # 그래프 스타일링
        plt.title('월별 인스턴스 변동 현황', pad=20, size=14, fontproperties=font_prop)
        plt.xlabel('연월', fontproperties=font_prop)
        plt.ylabel('인스턴스 수', fontproperties=font_prop)
        plt.xticks(x, months, rotation=0, fontproperties=font_prop)

        # 범례에 폰트 적용
        plt.legend(prop=font_prop)

        # 각 막대 위에 값 표시
        def add_value_labels(bars):
            for bar in bars:
                height = bar.get_height()
                if height > 0:  # 0보다 큰 경우만 표시
                    plt.text(
                        bar.get_x() + bar.get_width() / 2,
                        height,
                        str(int(height)),
                        ha='center',
                        va='bottom',
                        fontproperties=font_prop
                    )

        add_value_labels(bars1)
        add_value_labels(bars2)
        add_value_labels(bars3)

        # 여백 조정
        plt.tight_layout()

        # 저장
        output_path = os.path.join(self.graphs_dir, filename)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()

        return os.path.join("graphs", filename)

    def append_trend_section(self, report_file: str, data_list: List[Dict[str, Any]]) -> None:
        """
        리포트에 변동 현황 섹션 추가

        Args:
            report_file: 리포트 파일 경로
            data_list: MongoDB에서 조회한 월별 통계 데이터 목록
        """
        month_count = len(data_list)

        with open(report_file, "a", encoding="utf-8") as f:
            f.write("\n### 3. 인스턴스 변동 추이\n\n")

            if month_count < 3:
                missing_months = 3 - month_count
                f.write(f"> ℹ️ 이전 {missing_months}개월 데이터가 없어 {month_count}개월 데이터만 표시합니다.\n\n")

            # 그래프 생성 및 참조 추가
            graph_path = self.create_trend_chart(data_list)
            f.write(f"![인스턴스 변동 추이]({graph_path})\n\n")

            # 테이블 추가
            f.write("| 연월 | 전체 | 증가 | 감소 | 순증감 |\n")
            f.write("|------|------|------|------|--------|\n")

            for data in data_list:
                year = data['year']
                month = data['month']
                total = data['statistics']['total_instances']
                added = len(data['statistics']['period_statistics']['instances_added'])
                removed = len(data['statistics']['period_statistics']['instances_removed'])
                net_change = added - removed

                net_change_str = f"{net_change:+d}" if net_change != 0 else "0"
                f.write(f"| {year}-{month:02d} | {total} | {added} | {removed} | {net_change_str} |\n")