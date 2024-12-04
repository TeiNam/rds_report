# RDS Report Automation

## 클라우드 메트릭 수집
- 로컬 SSO 사용
- Ec2 IAM 권한 사용

## 환경설정
```dotenv
MONGODB_URI=""
MONGODB_DB_NAME=mgmt_db

## 리포트 대상 인스턴스 지정
REPORT_TARGET_INSTANCES=["","",""]

## Ai Api Key Settings
OPENAI_API_KEY=your-openai-key
ANTHROPIC_API_KEY=your-anthropic-key
OLLAMA_BASE_URL=http://localhost:11434/api
```

### 프로젝트의 소스 코드 트리 구조
```angular2html
📦 rds_report
├── 📄 README.md
├── 📄 __init__.py
├── 📁 apis
│   ├── 📄 __init__.py
│   └── 📁 v1
│       ├── 📄 __init__.py
│       ├── 📄 aws_account.py
│       ├── 📄 generate_report.py
│       └── 📄 monthly_report.py
├── 📁 collectors
│   ├── 📄 __init__.py
│   ├── 📄 cloudwatch_metric_collector.py
│   ├── 📄 cloudwatch_slowquery_collector.py
│   └── 📄 rds_instance_collector.py
├── 📁 configs
│   ├── 📄 __init__.py
│   ├── 📄 ai_conf.py
│   ├── 📄 cloudwatch_conf.py
│   ├── 📄 mongo_conf.py
│   └── 📄 report_settings.py
├── 📄 main.py
├── 📁 models
│   └── 📄 aws_account.py
├── 📁 modules
│   ├── 📄 __init__.py
│   ├── 📁 ai
│   │   ├── 📄 __init__.py
│   │   ├── 📄 exceptions.py
│   │   ├── 📄 factory.py
│   │   └── 📁 models
│   │       ├── 📄 __init__.py
│   │       ├── 📄 bedrock.py
│   │       ├── 📄 claude.py
│   │       ├── 📄 interface.py
│   │       ├── 📄 ollama.py
│   │       └── 📄 openai.py
│   ├── 📄 aws_account_module.py
│   ├── 📄 aws_session_manager.py
│   ├── 📄 instance_fetcher.py
│   └── 📄 mongodb_connector.py
├── 📁 report_tools
│   ├── 📄 __init__.py
│   ├── 📄 base.py
│   ├── 📁 generators
│   │   ├── 📄 __init__.py
│   │   ├── 📄 base.py
│   │   ├── 📁 fonts
│   │   │   └── 📄 MaruBuri.ttf
│   │   ├── 📄 generate_monthly_report.py
│   │   ├── 📄 instance_report.py
│   │   ├── 📄 instance_trend.py
│   │   └── 📄 metric_visualizer.py
│   └── 📄 instance_statistics.py
├── 📁 slowquery_tools
│   ├── 📄 __init__.py
│   ├── 📁 analyzers
│   │   ├── 📄 __init__.py
│   │   └── 📄 monthly_analyzer.py
│   ├── 📄 base.py
│   ├── 📁 loaders
│   │   ├── 📄 __init__.py
│   │   └── 📄 stats_loader.py
│   └── 📁 stores
│       ├── 📄 __init__.py
│       └── 📄 slow_query_statistics_store.py
├── 📄 requirements.txt
└── 📄 test_main.http
```
### 프로젝트 구조는 다음과 같은 주요 컴포넌트로 구성
1. APIs (apis/)
- REST API 엔드포인트 정의
- 버전별 API 구현 (v1)
2. 데이터 수집기 (collectors/)
- CloudWatch 메트릭 수집
- SlowQuery 수집
- RDS 인스턴스 정보 수집
3. 설정 (configs/)
- AI, CloudWatch, MongoDB, 리포트 설정 관리 
4. 모델 (models/)
- 데이터 모델 정의
5. 핵심 모듈 (modules/)
- AI 기능 (여러 AI 모델 지원)
- AWS 세션 관리
- 데이터베이스 연결
6. 리포트 도구 (report_tools/)
- 리포트 생성기
- 데이터 시각화
- 통계 분석
7. 생성된 리포트 (reports/)
- 월별 리포트
- 그래프와 통계 데이터
8. 기타
- 메인 애플리케이션 (main.py)
- 의존성 관리 (requirements.txt)
- API 테스트 (test_main.http)