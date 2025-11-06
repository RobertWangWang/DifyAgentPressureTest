from typing import List, Optional, Any, Dict
from sqlalchemy import select, update, delete, text, func, or_
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import pandas as pd
import os

from app.models import TestRecord, TestStatus
from app.core.database import SessionLocal,AsyncSessionLocal
from app.schemas.dataset_schema import DatasetRead
from app.schemas.test_record_schema import TestRecordRead, TestRecordStatus
from app.utils.pressure_test_util import (
    dify_get_account_id,
    dify_api_url_2_account_profile_url,
    download_from_tos
)
from app.utils.logger import logger


class TestRecordCRUD:
    # ---------------------- 创建 ----------------------

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
        agent_type: str,
        agent_name: str,
        status: TestStatus = TestStatus.INIT,
        duration: Optional[int] = None,
        result: Optional[str] = None,
        concurrency: int = 1,
        dify_api_key: Optional[str] = None,
        judge_model: Optional[str] = None,
        judge_model_provider_name: Optional[str] = None,
        dataset_uuid: Optional[str] = None,  # ✅ 新增
        dataset_file_md5: Optional[str] = None,
        dataset_tos_key: Optional[str] = None,
        dataset_tos_url: Optional[str] = None,
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
            judge_prompt=judge_prompt,
            judge_model=judge_model,
            judge_model_provider_name=judge_model_provider_name,
            dataset_uuid=dataset_uuid,  # ✅ 新外键
            dataset_file_md5=dataset_file_md5,
            dataset_tos_key=dataset_tos_key,
            dataset_tos_url=dataset_tos_url,
            is_deleted=is_deleted,
        )

        try:
            session.add(record)
            session.commit()
            session.refresh(record)
            logger.info(f"✅ 创建测试记录成功 uuid={record.uuid}, dataset_uuid={dataset_uuid}")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"❌ 创建测试记录失败: {e}")
            raise e

        return record

    # ---------------------- 查询 ----------------------

    @staticmethod
    def get_by_uuid(session: Session, uuid_str: str) -> Optional[TestRecord]:
        stmt = (
            select(TestRecord)
            .options(joinedload(TestRecord.dataset))  # ✅ 自动关联 Dataset
            .where(TestRecord.uuid == uuid_str, TestRecord.is_deleted.is_(False))
        )
        return session.scalars(stmt).first()

    @staticmethod
    def get_by_agent_id(session: Session, agent_id: str) -> Optional[TestRecord]:
        stmt = (
            select(TestRecord)
            .options(joinedload(TestRecord.dataset))
            .where(TestRecord.dify_test_agent_id == agent_id, TestRecord.is_deleted.is_(False))
        )
        return session.scalars(stmt).first()

    @staticmethod
    def list_all(session: Session, limit: int = 100, offset: int = 0) -> List[TestRecord]:
        stmt = (
            select(TestRecord)
            .options(joinedload(TestRecord.dataset))
            .where(TestRecord.is_deleted.is_(False))
            .order_by(TestRecord.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        return list(session.scalars(stmt).all())

    # ---------------------- 更新 ----------------------

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
            logger.info(f"🛠️ 更新测试记录 {uuid_str} 字段: {list(update_data.keys())}")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"❌ 更新测试记录失败: {e}")
            raise e

        return TestRecordCRUD.get_by_uuid(session, uuid_str)

    # ---------------------- 删除/恢复 ----------------------

    @staticmethod
    def delete_by_uuid(session: Session, uuid_str: str) -> bool:
        """软删除"""
        stmt = (
            update(TestRecord)
            .where(TestRecord.uuid == uuid_str)
            .values(is_deleted=True)
            .execution_options(synchronize_session="fetch")
        )
        try:
            result = session.execute(stmt)
            session.commit()
            logger.info(f"🗑️ 已软删除测试记录: {uuid_str}")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"❌ 删除测试记录失败: {e}")
            raise e
        return result.rowcount > 0

    @staticmethod
    def restore_by_uuid(session: Session, uuid_str: str) -> bool:
        """恢复软删除"""
        stmt = (
            update(TestRecord)
            .where(TestRecord.uuid == uuid_str)
            .values(is_deleted=False)
            .execution_options(synchronize_session="fetch")
        )
        try:
            result = session.execute(stmt)
            session.commit()
            logger.info(f"♻️ 已恢复测试记录: {uuid_str}")
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        return result.rowcount > 0

    # ---------------------- 分页查询 ----------------------

    @staticmethod
    def get_all_records_by_agent_id(input_agent_id: str, page: int, page_size: int):
        with SessionLocal() as session:
            stmt = (
                select(TestRecord)
                .options(joinedload(TestRecord.dataset))
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

    # ---------------------- 数据集预览 ----------------------

    @staticmethod
    def get_dataset_first_three_lines(input_uuid: str):
        """
        根据测试记录 UUID 获取其关联数据集的前 3 行内容。
        自动从 TOS 下载文件预览。
        """
        with SessionLocal() as session:
            # ✅ 查询 TestRecord
            record = session.query(TestRecord).filter(TestRecord.uuid == input_uuid).first()
            if not record:
                raise ValueError("Record not found or deleted.")

            # ✅ 优先使用 Dataset 外键关联
            dataset = record.dataset
            if not dataset:
                raise ValueError("No dataset linked to this test record.")

            object_key = dataset.tos_key
            suffix = dataset.file_suffix or ".csv"
            tmp_dir = Path("uploads/previews")
            tmp_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = tmp_dir / f"preview_{dataset.file_md5}{suffix}"

            try:
                # ✅ 从火山 TOS 下载文件
                download_from_tos(object_key, str(tmp_path))
                logger.info(f"✅ 从 TOS 下载完成: {tmp_path}")

                # ✅ 根据文件类型读取
                if suffix == ".csv":
                    df = pd.read_csv(tmp_path)
                elif suffix in [".xls", ".xlsx"]:
                    df = pd.read_excel(tmp_path, engine="openpyxl")
                else:
                    raise ValueError(f"Unsupported file type: {suffix}")

                preview = df.head(3).to_dict(orient="records")
                logger.info(f"✅ 预览成功，共 {len(df)} 行，取前 3 行展示")

                return preview

            except Exception as e:
                logger.error(f"❌ 文件预览失败: {e}")
                raise RuntimeError(f"Failed to preview dataset: {e}")

            finally:
                # ✅ 清理临时文件
                try:
                    if tmp_path.exists():
                        os.remove(tmp_path)
                        logger.info(f"🧹 已删除临时文件: {tmp_path}")
                except Exception as e:
                    logger.warning(f"⚠️ 删除临时文件失败: {e}")

    # ---------------------- 模糊搜索 ----------------------

    @staticmethod
    def get_records_by_keyword(key_word: str, page: int, page_size: int):
        with SessionLocal() as session:
            # 通用过滤条件（排除删除、排除实验状态）
            base_conditions = [
                TestRecord.is_deleted.is_(False),
                TestRecord.status != TestStatus.EXPERIMENT
            ]

            if key_word:
                # 模糊匹配查询
                like_pattern = f"%{key_word}%"
                stmt = (
                    select(TestRecord)
                    .options(joinedload(TestRecord.dataset))
                    .where(
                        *base_conditions,
                        or_(
                            TestRecord.task_name.ilike(like_pattern),
                            TestRecord.agent_name.ilike(like_pattern),
                        )
                    )
                    .order_by(TestRecord.created_at.desc())
                )
                # 搜索模式下不分页，直接返回匹配记录
                records = session.scalars(stmt).all()
                total = len(records)  # 当前记录数
            else:
                # 无关键词时分页获取所有记录
                stmt = (
                    select(TestRecord)
                    .options(joinedload(TestRecord.dataset))
                    .where(*base_conditions)
                    .order_by(TestRecord.created_at.desc())
                    .offset((page - 1) * page_size)
                    .limit(page_size)
                )
                records = session.scalars(stmt).all()

                # 仅当keyword为空时计算总数
                count_stmt = select(func.count()).select_from(TestRecord).where(*base_conditions)
                total = session.scalar(count_stmt)

            return {
                "page": page,
                "page_size": page_size,
                "total": total,
                "records": [TestRecordRead.model_validate(r) for r in records],
            }

    @staticmethod
    async def increment_success_count(uuid_str: str) -> bool:
        """
        ✅ 异步版本：成功次数 +1
        """
        async with AsyncSessionLocal() as session:
            try:
                await session.execute(
                    text("""
                         UPDATE bots_eval_test_records
                         SET success_count = success_count + 1
                         WHERE uuid = :uuid_str
                           AND is_deleted = 0
                         """),
                    {"uuid_str": uuid_str},
                )
                await session.commit()
                logger.debug(f"✅ 成功次数 +1，uuid={uuid_str}")
                return True
            except SQLAlchemyError as e:
                await session.rollback()
                logger.error(f"❌ 成功次数更新失败: {e}")
                return False

    @staticmethod
    async def increment_failure_count(uuid_str: str) -> bool:
        """
        ❌ 异步版本：失败次数 +1
        """
        async with AsyncSessionLocal() as session:
            try:
                await session.execute(
                    text("""
                         UPDATE bots_eval_test_records
                         SET failure_count = failure_count + 1
                         WHERE uuid = :uuid_str
                           AND is_deleted = 0
                         """),
                    {"uuid_str": uuid_str},
                )
                await session.commit()
                logger.debug(f"⚠️ 失败次数 +1，uuid={uuid_str}")
                return True
            except SQLAlchemyError as e:
                await session.rollback()
                logger.error(f"❌ 失败次数更新失败: {e}")
                return False

    @staticmethod
    def get_records_by_uuid_and_bearer_token(agent_id: str, bearer_token: str):
        """
        ✅ 根据 TestRecord UUID 和 Bearer Token 获取该 dify_account 下的所有测试记录
        """
        with SessionLocal() as session:
            # 1️⃣ 获取指定记录
            record = session.scalar(
                select(TestRecord).where(
                    TestRecord.dify_test_agent_id == agent_id,
                    TestRecord.is_deleted.is_(False)
                )
            )
            if not record:
                logger.warning(f"❌ 未找到agent_id的对应记录: {agent_id}")
                return []

            # 2️⃣ 获取 Dify Account ID
            try:
                profile_url = dify_api_url_2_account_profile_url(record.dify_api_url)
                dify_account_id = dify_get_account_id(profile_url, bearer_token)
                logger.info(f"✅ 获取 dify_account_id 成功: {dify_account_id}")
            except Exception as e:
                logger.error(f"❌ 获取 dify_account_id 失败: {e}")
                return []

            # 3️⃣ 查询该账号下所有测试记录
            stmt = (
                select(TestRecord)
                .where(
                    TestRecord.dify_account_id == dify_account_id,
                    TestRecord.status != TestStatus.EXPERIMENT,
                    TestRecord.is_deleted.is_(False)
                )
                .order_by(TestRecord.created_at.desc())
            )
            records = session.scalars(stmt).all()

            # 4️⃣ 转换为 Pydantic 模型列表
            return [TestRecordRead.model_validate(r) for r in records]

    @staticmethod
    def get_by_uuid_include_deleted(session, uuid_str: str, as_dict: bool = False) -> Optional[TestRecord]:
        """
        根据 UUID 获取测试记录（包括软删除的记录）
        - 支持加载 Dataset 外键
        - 可选返回 dict
        """
        try:
            stmt = (
                select(TestRecord)
                .options(joinedload(TestRecord.dataset))
                .where(TestRecord.uuid == uuid_str)
            )
            record = session.scalars(stmt).first()

            if not record:
                logger.warning(f"⚠️ 未找到记录: uuid={uuid_str}")
                return None

            logger.info(
                f"✅ 查询到记录 uuid={record.uuid}, is_deleted={record.is_deleted}, "
                f"dataset_uuid={getattr(record.dataset, 'uuid', None)}"
            )

            if as_dict:
                data = record.to_dict(exclude_none=True)
                if record.dataset:
                    data["dataset"] = record.dataset.to_dict(exclude_none=True)
                return data

            return record

        except Exception as e:
            logger.error(f"❌ 查询记录失败: uuid={uuid_str}, 错误={e}")
            raise

    @staticmethod
    def get_uuid_task_status(uuid: str) -> TestRecordStatus:
        """
        根据 UUID 获取测试任务状态（包含 dataset 信息）
        """
        with SessionLocal() as session:
            try:
                stmt = (
                    select(TestRecord)
                    .options(joinedload(TestRecord.dataset))
                    .where(TestRecord.uuid == uuid)
                )
                record = session.scalars(stmt).first()

                if not record:
                    logger.warning(f"⚠️ 未找到测试任务: uuid={uuid}")
                    raise ValueError("Record not found or deleted.")

                # ✅ 组装结果
                result = {
                    "uuid": record.uuid,
                    "status": record.status,
                    "task_name": record.task_name,
                    "agent_name": record.agent_name,
                    "is_deleted": record.is_deleted,
                }

                # ✅ 如果有关联数据集，则附带数据集摘要
                if record.dataset:
                    result["dataset"] = DatasetRead.model_validate(record.dataset)

                logger.info(f"✅ 查询任务状态成功: uuid={uuid}, status={record.status}")
                return TestRecordStatus(**result)

            except Exception as e:
                logger.error(f"❌ 获取任务状态失败: {e}")
                raise

    @staticmethod
    def update_judge_model(input_uuid: str, judge_model_name: str):
        with SessionLocal() as session:
            TestRecordCRUD.update_by_uuid(session, input_uuid, judge_model=judge_model_name)

    @staticmethod
    def get_datasets_by_agent_and_bearer_token(agent_id: str, bearer_token: str, page: int = 1, page_size: int = 10):
        """
        ✅ 根据 agent_id + bearer_token 查询该账户下的所有测试记录及其对应数据集（分页）
        """
        with SessionLocal() as session:
            try:
                # 1️⃣ 查找一条记录以获取 dify_api_url
                record = session.scalar(
                    select(TestRecord).where(
                        TestRecord.dify_test_agent_id == agent_id,
                        TestRecord.is_deleted.is_(False),
                        TestRecord.dataset_uuid.is_not(None)
                    )
                )
                if not record:
                    logger.warning(f"❌ 未找到对应 agent_id 的记录: {agent_id}")
                    return {
                        "page": page,
                        "page_size": page_size,
                        "total": 0,
                        "records": [],
                    }

                # 2️⃣ 将 bearer_token 转换为 account_id
                try:
                    profile_url = dify_api_url_2_account_profile_url(record.dify_api_url)
                    dify_account_id = dify_get_account_id(profile_url, bearer_token)
                    logger.info(f"✅ 获取 dify_account_id 成功: {dify_account_id}")
                except Exception as e:
                    logger.error(f"❌ 获取 dify_account_id 失败: {e}")
                    return {
                        "page": page,
                        "page_size": page_size,
                        "total": 0,
                        "records": [],
                    }

                # 3️⃣ 计算总数
                total_stmt = (
                    select(func.count())
                    .select_from(TestRecord)
                    .where(
                        TestRecord.dify_account_id == dify_account_id,
                        TestRecord.dify_test_agent_id == agent_id,
                        TestRecord.is_deleted.is_(False),
                        TestRecord.dataset_uuid.is_not(None)
                    )
                )
                total = session.scalar(total_stmt)

                # 4️⃣ 分页查询记录
                stmt = (
                    select(TestRecord)
                    .options(joinedload(TestRecord.dataset))
                    .where(
                        TestRecord.dify_account_id == dify_account_id,
                        TestRecord.dify_test_agent_id == agent_id,
                        TestRecord.is_deleted.is_(False),
                        TestRecord.dataset_uuid.is_not(None)
                    )
                    .order_by(TestRecord.created_at.desc())
                    .offset((page - 1) * page_size)
                    .limit(page_size)
                )
                records = session.scalars(stmt).all()

                # 5️⃣ 组装返回结果
                result_items = []
                for rec in records:
                    try:
                        dataset_obj = (
                            DatasetRead.model_validate(rec.dataset)
                            if rec.dataset else None
                        )
                        record_obj = TestRecordRead.model_validate(rec)
                        result_items.append({
                            "record": record_obj,
                            "dataset": dataset_obj
                        })
                    except Exception as e:
                        logger.warning(f"⚠️ 转换记录失败: uuid={rec.uuid}, 错误={e}")

                logger.info(f"✅ 分页查询成功: page={page}, page_size={page_size}, total={total}")
                return {
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "records": result_items,
                }

            except Exception as e:
                logger.error(f"❌ 查询失败: {e}")
                raise

    @staticmethod
    def detach_dataset(uuid_str: str) -> bool:
        """将 TestRecord 的 dataset 解绑（dataset_uuid 置空）"""
        with SessionLocal() as db:
            record = db.query(TestRecord).filter(
                TestRecord.uuid == uuid_str,
                TestRecord.is_deleted == False
            ).first()

            if not record:
                return False

            # 清空外键引用
            record.dataset_uuid = None
            db.commit()
            return True