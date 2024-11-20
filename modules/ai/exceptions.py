# ai/exceptions.py
class AIModuleException(Exception):
    """AI 모듈 관련 기본 예외"""
    pass

class APIKeyNotFoundError(AIModuleException):
    """API 키가 없을 때 발생하는 예외"""
    pass

class ModelNotFoundError(AIModuleException):
    """요청한 AI 모델을 찾을 수 없을 때 발생하는 예외"""
    pass

class ModelNotAvailableError(AIModuleException):
    """AI 모델이 현재 사용 불가능할 때 발생하는 예외"""
    pass