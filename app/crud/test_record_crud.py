from typing import List, Optional, Any, Dict
from sqlalchemy import select, update, delete, text, func, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import pandas as pd
from fastapi.responses import JSONResponse

from app.models import TestRecord, TestStatus
from app.core.database import SessionLocal
from app.schemas.test_record_schema import TestRecordRead
from app.utils.pressure_test_util import dify_get_account_id, dify_api_url_2_account_profile_url


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
        dataset_absolute_path: Optional[str] = None,
        is_deleted: bool = False,
    ) -> TestRecord:
        """创建测试记录"""
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
            dataset_absolute_path=dataset_absolute_path,
            is_deleted=is_deleted,  # ✅ 新增：默认未删除
        )

        try:
            session.add(record)
            session.commit()
            session.refresh(record)
        except SQLAlchemyError as e:
            session.rollback()
            raise e

        return record

    # ---------------------- 查询操作 ----------------------

    @staticmethod
    def get_by_uuid(session: Session, uuid_str: str) -> Optional[TestRecord]:
        stmt = select(TestRecord).where(
            TestRecord.uuid == uuid_str,
            TestRecord.is_deleted.is_(False),  # ✅ 排除软删除
        )
        return session.scalars(stmt).first()

    @staticmethod
    def get_by_agent_id(session: Session, agent_id: str) -> Optional[TestRecord]:
        stmt = select(TestRecord).where(
            TestRecord.dify_test_agent_id == agent_id,
            TestRecord.is_deleted.is_(False),
        )
        return session.scalars(stmt).first()

    @staticmethod
    def list_all(session: Session, limit: int = 100, offset: int = 0) -> List[TestRecord]:
        stmt = (
            select(TestRecord)
            .where(TestRecord.is_deleted.is_(False))  # ✅ 排除软删除
            .order_by(TestRecord.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        return list(session.scalars(stmt).all())

    # ---------------------- 更新操作 ----------------------

    @staticmethod
    def update_by_uuid(session: Session, uuid_str: str, **kwargs: Any) -> Optional[TestRecord]:
        """根据 uuid 更新字段（只更新非 None 值）"""
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

    # ---------------------- 删除/恢复操作 ----------------------

    @staticmethod
    def delete_by_uuid(session: Session, uuid_str: str) -> bool:
        """软删除（将 is_deleted 设置为 True）"""
        stmt = (
            update(TestRecord)
            .where(TestRecord.uuid == uuid_str)
            .values(is_deleted=True)
            .execution_options(synchronize_session="fetch")
        )
        try:
            result = session.execute(stmt)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        return result.rowcount is not None and result.rowcount > 0

    @staticmethod
    def restore_by_uuid(session: Session, uuid_str: str) -> bool:
        """恢复软删除的记录"""
        stmt = (
            update(TestRecord)
            .where(TestRecord.uuid == uuid_str)
            .values(is_deleted=False)
            .execution_options(synchronize_session="fetch")
        )
        try:
            result = session.execute(stmt)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        return result.rowcount is not None and result.rowcount > 0

    # ---------------------- 统计操作 ----------------------

    @staticmethod
    def increment_success_count(uuid_str: str):
        with SessionLocal() as session:
            session.execute(
                text("""
                    UPDATE test_records
                    SET success_count = success_count + 1
                    WHERE uuid = :uuid_str AND is_deleted = 0
                """),
                {"uuid_str": uuid_str},
            )
            session.commit()

    @staticmethod
    def increment_failure_count(uuid_str: str):
        with SessionLocal() as session:
            session.execute(
                text("""
                    UPDATE test_records
                    SET failure_count = failure_count + 1
                    WHERE uuid = :uuid_str AND is_deleted = 0
                """),
                {"uuid_str": uuid_str},
            )
            session.commit()

    # ---------------------- 分页查询 ----------------------

    @staticmethod
    def get_all_records_by_agent_id(input_agent_id: str, page: int, page_size: int):
        with SessionLocal() as session:
            stmt = (
                select(TestRecord)
                .where(
                    TestRecord.dify_test_agent_id == input_agent_id,
                    TestRecord.is_deleted.is_(False),
                )
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            records = session.scalars(stmt).all()

            total_stmt = select(func.count()).select_from(TestRecord).where(
                TestRecord.dify_test_agent_id == input_agent_id,
                TestRecord.is_deleted.is_(False),
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
            stmt = (
                select(TestRecord)
                .where(
                    TestRecord.task_name == input_task_name,
                    TestRecord.is_deleted.is_(False),
                )
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            records = session.scalars(stmt).all()

            total_stmt = select(func.count()).select_from(TestRecord).where(
                TestRecord.task_name == input_task_name,
                TestRecord.is_deleted.is_(False),
            )
            total = session.scalar(total_stmt)

            return {
                "page": page,
                "page_size": page_size,
                "total": total,
                "records": [TestRecordRead.model_validate(r) for r in records],
            }

    # ---------------------- 数据集预览 ----------------------

    @staticmethod
    def get_dataset_first_three_lines(input_uuid: str):
        with SessionLocal() as session:
            record = TestRecordCRUD.get_by_uuid(session, input_uuid)
            if not record:
                raise ValueError("Record not found or deleted.")

            dataset_path = Path("uploads/" + record.filename).resolve()
            if dataset_path.suffix == ".csv":
                df = pd.read_csv(dataset_path)
            elif dataset_path.suffix == ".xlsx":
                df = pd.read_excel(dataset_path, engine="openpyxl")
            else:
                raise ValueError("Unsupported file type. Only .csv and .xlsx are supported.")
            return df.head(3).to_dict(orient="records")

    # ---------------------- 根据 agent + bearer_token 查询 ----------------------

    @staticmethod
    def get_records_by_uuid_and_bearer_token(input_agent_id: str, bearer_token: str):
        with SessionLocal() as session:
            record = TestRecordCRUD.get_by_agent_id(session, input_agent_id)
            if not record:
                return []

            input_dify_url = record.dify_api_url
            input_dify_account_url = dify_api_url_2_account_profile_url(str(input_dify_url))
            dify_account_id = dify_get_account_id(input_dify_account_url, bearer_token)

            stmt = (
                select(TestRecord)
                .where(
                    TestRecord.dify_account_id == dify_account_id,
                    TestRecord.dify_test_agent_id == input_agent_id,
                    TestRecord.is_deleted.is_(False),
                )
            )
            records = session.scalars(stmt).all()
            return [TestRecordRead.model_validate(r) for r in records]

    # ---------------------- 特殊更新 ----------------------

    @staticmethod
    def update_judge_model(input_uuid: str, judge_model_name: str):
        with SessionLocal() as session:
            TestRecordCRUD.update_by_uuid(session, input_uuid, judge_model=judge_model_name)

    @staticmethod
    def get_by_uuid_include_deleted(session: Session, uuid_str: str) -> Optional[TestRecord]:
        """允许返回已软删除的记录"""
        stmt = select(TestRecord).where(TestRecord.uuid == uuid_str)
        return session.scalars(stmt).first()

    @staticmethod
    def get_uuid_task_status(input_uuid: str):
        with SessionLocal() as session:
            record = TestRecordCRUD.get_by_uuid(session, input_uuid)
            if not record:
                raise ValueError("Record not found or deleted.")
            return record

    @staticmethod
    def get_records_by_keyword(key_word: str, page: int, page_size: int):
        """支持关键字模糊搜索（task_name 或 agent_name），可为空"""

        with SessionLocal() as session:
            # 构造基础查询
            stmt = select(TestRecord).where(TestRecord.is_deleted.is_(False))

            # 如果 key_word 非空，则添加模糊匹配
            if key_word:
                like_pattern = f"%{key_word}%"
                stmt = stmt.where(
                    or_(
                        TestRecord.task_name.ilike(like_pattern),
                        TestRecord.agent_name.ilike(like_pattern),
                    )
                )

            # 分页
            stmt = stmt.order_by(TestRecord.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
            records = session.scalars(stmt).all()

            # 总数统计
            count_stmt = select(func.count()).select_from(TestRecord).where(TestRecord.is_deleted.is_(False))
            if key_word:
                count_stmt = count_stmt.where(
                    or_(
                        TestRecord.task_name.ilike(like_pattern),
                        TestRecord.agent_name.ilike(like_pattern),
                    )
                )
            total = session.scalar(count_stmt)

            return {
                "page": page,
                "page_size": page_size,
                "total": total,
                "records": [TestRecordRead.model_validate(r) for r in records],
            }