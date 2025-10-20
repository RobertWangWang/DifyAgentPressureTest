from typing import List, Optional, Any, Dict
from sqlalchemy import select, update, delete, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.models import TestRecord, TestStatus
from app.core.database import SessionLocal


class TestRecordCRUD:

    @staticmethod
    def create(
        session: Session,
        *,
        filename: str,
        dify_api_url: str,
        dify_bearer_token: str,
        dify_test_agent_id: str,
        dify_username: str,
        chatflow_query: str,
        status: TestStatus = TestStatus.INIT,
        duration: Optional[int] = None,
        result: Optional[str] = None,
        concurrency: int = 1,
        dify_api_key: Optional[str] = None,
    ) -> TestRecord:
        """
        创建一条新的测试记录。
        所有 nullable=False 的字段必须提供。
        """

        record = TestRecord(
            filename=filename,
            status=status,
            duration=duration,
            result=result,
            concurrency=concurrency,
            dify_api_url=dify_api_url,
            dify_bearer_token=dify_bearer_token,
            dify_test_agent_id=dify_test_agent_id,
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
    def list_all(session: Session, limit: int = 100, offset: int = 0) -> List[TestRecord]:
        stmt = (
            select(TestRecord)
            .order_by(TestRecord.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        return list(session.scalars(stmt).all())

    @staticmethod
    def update_by_uuid(session: Session, uuid_str: str, **kwargs: Any) -> Optional[TestRecord]:
        """
        根据 uuid 更新字段。只更新传入的非 None 值。
        """
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

    @staticmethod
    def increment_success_count(uuid_str: str):
        with SessionLocal() as session:
            session.execute(
                text("""
                    UPDATE test_chatflow_records
                    SET success_count = success_count + 1
                    WHERE uuid = :uuid_str
                """),
                {"uuid_str": uuid_str}
            )
            session.commit()

    @staticmethod
    def increment_failure_count(uuid_str: str):
        with SessionLocal() as session:
            session.execute(
                text("""
                    UPDATE test_chatflow_records
                    SET failure_count = failure_count + 1
                    WHERE uuid = :uuid_str
                """),
                {"uuid_str": uuid_str}
            )
            session.commit()
