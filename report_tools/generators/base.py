# report_tools/generators/base.py
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional


class BaseReportGenerator:
    """리포트 생성 기본 클래스"""

    def __init__(self, output_dir: Optional[str] = None):
        """
        Args:
            output_dir: 리포트 출력 디렉토리 (기본값: 프로젝트 루트의 reports)
        """
        self.root_dir = self._find_project_root()
        self.base_output_dir = output_dir if output_dir else os.path.join(self.root_dir, "reports")

        # reports 폴더가 없는 경우에만 생성
        if not os.path.exists(self.base_output_dir):
            os.makedirs(self.base_output_dir)
            print(f"reports 폴더가 생성되었습니다: {self.base_output_dir}")

        # output_dir이 직접 지정된 경우 해당 경로 사용
        if output_dir:
            self.output_dir = output_dir
        else:
            # 기본값으로 현재 날짜 폴더 사용
            self.report_date = datetime.now().strftime("%Y%m%d")
            self.output_dir = os.path.join(self.base_output_dir, self.report_date)

        # 출력 디렉토리 초기화
        self._initialize_output_directory()

    def _initialize_output_directory(self):
        """출력 디렉토리 초기화"""
        # 디렉토리가 없는 경우에만 생성
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"출력 폴더가 생성되었습니다: {self.output_dir}")
        else:
            print(f"기존 출력 폴더를 사용합니다: {self.output_dir}")

    def _find_project_root(self) -> str:
        """프로젝트 루트 디렉토리 찾기

        Returns:
            str: 프로젝트 루트 디렉토리 경로
        """
        current_dir = Path(__file__).resolve().parent
        while current_dir.as_posix() != '/':
            if (current_dir / 'requirements.txt').exists() or (current_dir / '.git').exists():
                return str(current_dir)
            current_dir = current_dir.parent

            if current_dir.as_posix() == '/':
                return os.getcwd()

        return os.getcwd()

    def _initialize_date_directory(self) -> str:
        """날짜별 디렉토리 초기화

        Returns:
            str: 날짜별 디렉토리 경로
        """
        date_dir = os.path.join(self.base_output_dir, self.report_date)

        # 이미 존재하는 경우 삭제 후 재생성
        if os.path.exists(date_dir):
            shutil.rmtree(date_dir)
            print(f"기존 {self.report_date} 폴더를 삭제했습니다.")

        os.makedirs(date_dir)
        print(f"새로운 {self.report_date} 폴더가 생성되었습니다: {date_dir}")

        return date_dir

    def create_subdirectory(self, subdir_name: str) -> str:
        """서브 디렉토리 생성

        Args:
            subdir_name: 생성할 서브 디렉토리 이름

        Returns:
            str: 생성된 서브 디렉토리 경로
        """
        subdir_path = os.path.join(self.output_dir, subdir_name)
        os.makedirs(subdir_path, exist_ok=True)
        return subdir_path

    def get_report_path(self, filename: str) -> str:
        """리포트 파일 경로 반환

        Args:
            filename: 리포트 파일 이름

        Returns:
            str: 리포트 파일의 전체 경로
        """
        return os.path.join(self.output_dir, filename)