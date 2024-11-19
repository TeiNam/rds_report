# api/endpoints.py

@router.get("/collect-monthly-metrics")
async def collect_monthly_metrics(
        year: int = Query(..., description="수집 연도"),
        month: int = Query(..., ge=1, le=12, description="수집 월 (1-12)"),
        env: str = Query('prd', description="환경 ('prd' 또는 'dev')")
):
    """월간 RDS CloudWatch 메트릭 수집 API"""
    try:
        # AWS 세션 매니저 초기화
        session_manager = AWSSessionManager()

        # 해당 월의 마지막 날짜 계산
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)

        await session_manager.initialize(
            env=env,
            end_date=end_date.strftime("%Y-%m-%d")
        )

        # CloudWatch 메트릭 수집기 초기화
        collector = RDSCloudWatchCollector(session_manager)

        # 메트릭 수집 및 저장
        metrics = await collector.collect_metrics_monthly(
            year=year,
            month=month
        )

        return {
            "message": f"{year}년 {month}월의 CloudWatch 메트릭이 성공적으로 저장되었습니다.",
            "data": metrics
        }

    except Exception as e:
        logger.error(f"메트릭 수집 중 오류 발생: {e}")
        raise HTTPException(status_code=500, detail=str(e))