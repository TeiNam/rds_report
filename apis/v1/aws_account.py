# apis/v1/aws_account.py

from typing import List
from fastapi import APIRouter, HTTPException
from models.aws_account import (
    AWSAccountCreate,
    AWSAccountResponse,
    AWSAccountUpdate
)
from modules.aws_account_module import AWSAccountModule

router = APIRouter(prefix="/aws-accounts", tags=["aws-accounts"])
aws_account_module = AWSAccountModule()

@router.post("/", response_model=AWSAccountResponse)
async def create_aws_account(account: AWSAccountCreate):
    """AWS 계정 등록"""
    try:
        created_account = await aws_account_module.create_account(account)
        return AWSAccountResponse(**created_account.dict())
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={
                "status": "error",
                "message": str(e)
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"계정 등록 중 오류가 발생했습니다: {str(e)}"
            }
        )

@router.get("/", response_model=List[AWSAccountResponse])
async def get_all_aws_accounts():
    """등록된 AWS 계정 목록 조회"""
    try:
        accounts = await aws_account_module.get_all_accounts()
        return [AWSAccountResponse(**account.dict()) for account in accounts]
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"계정 목록 조회 중 오류가 발생했습니다: {str(e)}"
            }
        )

@router.put("/{account_id}", response_model=AWSAccountResponse)
async def update_aws_account(account_id: str, account_update: AWSAccountUpdate):
    """AWS 계정 정보 수정"""
    updated_account = await aws_account_module.update_account(account_id, account_update)
    if not updated_account:
        raise HTTPException(
            status_code=404,
            detail={
                "status": "error",
                "message": f"계정을 찾을 수 없습니다: {account_id}"
            }
        )
    return AWSAccountResponse(**updated_account.dict())

@router.delete("/{account_id}")
async def delete_aws_account(account_id: str):
    """AWS 계정 삭제"""
    deleted = await aws_account_module.delete_account(account_id)
    if not deleted:
        raise HTTPException(
            status_code=404,
            detail={
                "status": "error",
                "message": f"계정을 찾을 수 없습니다: {account_id}"
            }
        )
    return {
        "status": "success",
        "message": f"계정이 성공적으로 삭제되었습니다: {account_id}"
    }