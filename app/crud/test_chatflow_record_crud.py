from typing import List, Optional, Any, Dict
from sqlalchemy import select, update, delete
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.models import TestRecord, TestStatus

class TestRecordCRUD:

    @staticmethod
    def create(
        session: Session,
        filename: str,
        status: TestStatus = TestStatus.INIT,
        duration: Optional[int] = None,
        result: Optional[str] = None,
        concurrency: Optional[int] = 1,
        dify_api_url: Optional[str] = None,
        dify_api_key: Optional[str] = None,
        dify_username: Optional[str] = None,
        chatflow_query: Optional[str] = None
    ) -> TestRecord:
        record = TestRecord(
            filename=filename,
            status=status,
            duration=duration,
            result=result,
            concurrency=concurrency,
            dify_api_url=dify_api_url,
            dify_api_key=dify_api_key,
            dify_username=dify_username,
            chatflow_query=chatflow_query
        )
        try:
            session.add(record)
            session.commit()
            session.refresh(record)
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        return record

    @staticmethod
    def get_by_uuid(session: Session, uuid_str: str) -> Optional[TestRecord]:
        stmt = select(TestRecord).where(TestRecord.uuid == uuid_str)
        return session.scalars(stmt).first()

    @staticmethod
    def list_all(
        session: Session,
        limit: int = 100,
        offset: int = 0
    ) -> List[TestRecord]:
        stmt = (
            select(TestRecord)
            .order_by(TestRecord.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        return list(session.scalars(stmt).all())

    @staticmethod
    def update_by_uuid(
        session: Session,
        uuid_str: str,
        **kwargs: Any
    ) -> Optional[TestRecord]:
        update_data: Dict[str, Any] = {k: v for k, v in kwargs.items() if v is not None}
        if not update_data:
            return TestRecordCRUD.get_by_uuid(session, uuid_str)

        stmt = (
            update(TestRecord)
            .where(TestRecord.uuid == uuid_str)
            .values(**update_data)
            .execution_options(synchronize_session="fetch")
        )
        try:
            session.execute(stmt)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e

        return TestRecordCRUD.get_by_uuid(session, uuid_str)

    @staticmethod
    def delete_by_uuid(session: Session, uuid_str: str) -> bool:
        stmt = delete(TestRecord).where(TestRecord.uuid == uuid_str)
        try:
            result = session.execute(stmt)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e

        return result.rowcount is not None and result.rowcount > 0
