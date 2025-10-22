from typing import List, Optional, Any, Dict
from sqlalchemy import select, update, delete, text, func
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import pandas as pd

from app.models import TestRecord, TestStatus
from app.core.database import SessionLocal
from app.schemas.test_record_schema import TestRecordRead
from app.utils.pressure_test_util import dify_get_account_id,dify_api_url_2_account_profile_url

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
        dify_account_id: str,
        task_name: str,
        judge_prompt: str,
        chatflow_query: str,
        agent_type: str,
        agent_name: str,
        status: TestStatus = TestStatus.INIT,
        duration: Optional[int] = None,
        result: Optional[str] = None,
        concurrency: int = 1,
        dify_api_key: Optional[str] = None,
        judge_model: str = None,
        judge_model_provider_name: str = None,
    ) -> TestRecord:
        """
        创建一条新的测试记录。
        所有 nullable=False 的字段必须提供。
        """

        record = TestRecord(
            filename=filename,
            status=status,
            agent_type=agent_type,
            agent_name=agent_name,
            task_name=task_name,
            duration=duration,
            result=result,
            concurrency=concurrency,
            dify_account_id=dify_account_id,
            dify_api_url=dify_api_url,
            dify_bearer_token=dify_bearer_token,
            dify_test_agent_id=dify_test_agent_id,
            dify_api_key=dify_api_key,
            dify_username=dify_username,
            chatflow_query=chatflow_query,
            judge_prompt=judge_prompt,
            judge_model=judge_model,
            judge_model_provider_name=judge_model_provider_name,
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
    def get_by_agent_id(session: Session, agent_id: str) -> Optional[TestRecord]:
        stmt = select(TestRecord).where(TestRecord.dify_test_agent_id == agent_id)
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
                    UPDATE test_records
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
                    UPDATE test_records
                    SET failure_count = failure_count + 1
                    WHERE uuid = :uuid_str
                """),
                {"uuid_str": uuid_str}
            )
            session.commit()

    @staticmethod
    def get_all_records_by_agent_id(input_agent_id: str, page: int, page_size: int):
        with SessionLocal() as session:
            # 分页查询数据
            stmt = (
                select(TestRecord)
                .where(TestRecord.dify_test_agent_id == input_agent_id)
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            records = session.scalars(stmt).all()

            # 单独统计总数
            total_stmt = select(func.count()).select_from(TestRecord).where(
                TestRecord.dify_test_agent_id == input_agent_id
            )
            total = session.scalar(total_stmt)

            return {
                "page": page,
                "page_size": page_size,
                "total": total,
                "records": [TestRecordRead.model_validate(r) for r in records],
            }

    @staticmethod
    def get_all_records_by_task_name(input_task_name: str, page: int, page_size: int):
        with SessionLocal() as session:
            # 分页查询数据
            stmt = (
                select(TestRecord)
                .where(TestRecord.task_name == input_task_name)
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            records = session.scalars(stmt).all()

            # 单独统计总数
            total_stmt = select(func.count()).select_from(TestRecord).where(
                TestRecord.task_name == input_task_name
            )
            total = session.scalar(total_stmt)

            return {
                "page": page,
                "page_size": page_size,
                "total": total,
                "records": [TestRecordRead.model_validate(r) for r in records],
            }

    @staticmethod
    def get_dataset_first_three_lines(input_uuid:str):

        with SessionLocal() as session:
            record = TestRecordCRUD.get_by_uuid(SessionLocal(), input_uuid)
            dataset_file_name = record.filename
            dataset_path = Path("uploads/" + dataset_file_name).resolve()
            if dataset_path.__str__().endswith(".csv"):
                df = pd.read_csv(dataset_path)
            elif dataset_path.__str__().endswith(".xlsx"):
                df = pd.read_excel(dataset_file_name, engine="openpyxl")
            else:
                raise ValueError(
                    "Unsupported file type. Only .csv and .xlsx test files are supported."
                )

            return df.head(3).to_dict(orient="records")

    @staticmethod
    def get_records_by_uuid_and_bearer_token(input_agent_id: str, bearer_token: str):

        with SessionLocal() as session:

            record = TestRecordCRUD.get_by_agent_id(session, input_agent_id)
            input_dify_url = record.dify_api_url
            input_dify_account_url = dify_api_url_2_account_profile_url(input_dify_url.__str__())
            dify_account_id = dify_get_account_id(input_dify_account_url, bearer_token)

            stmt = (
                select(TestRecord)
                .where(TestRecord.dify_account_id == dify_account_id).where(
                    TestRecord.dify_test_agent_id == input_agent_id
                )
            )
            records = session.scalars(stmt).all()
            return [TestRecordRead.model_validate(r) for r in records]

    @staticmethod
    def update_judge_model(input_uuid: str, judge_model_name: str):
        with SessionLocal() as session:
            record = TestRecordCRUD.get_by_uuid(session, input_uuid)
            record.judge_model_name = judge_model_name
            TestRecordCRUD.update_by_uuid(session, input_uuid,**{"judge_model": judge_model_name})