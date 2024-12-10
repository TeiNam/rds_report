from typing import Dict, Any
import logging
from datetime import datetime
from dataclasses import dataclass
from bson import ObjectId

logger = logging.getLogger(__name__)


@dataclass
class SlowQueryInfo:
    """슬로우 쿼리 정보"""
    _id: ObjectId
    instance_id: str
    digest_query: str
    example_query: str
    avg_time: float
    endpoint: str
    port: int
    master_username: str
    created_at: datetime

    @classmethod
    def from_mongo_doc(cls, doc: Dict[str, Any]) -> 'SlowQueryInfo':
        """MongoDB 문서에서 슬로우 쿼리 정보 생성"""
        return cls(
            _id=doc['_id'],
            instance_id=doc['instance_id'],
            digest_query=doc['digest_query'],
            example_query=doc['example_query'],
            avg_time=doc.get('avg_time', 0.0),
            endpoint=doc['endpoint'],
            port=doc['port'],
            master_username=doc['master_username'],
            created_at=doc['created_at']
        )