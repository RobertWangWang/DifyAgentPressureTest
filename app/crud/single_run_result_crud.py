from sqlalchemy import select, update, delete, desc, func
from loguru import logger
from typing import List, Optional, Dict, Any
from app.models.single_run_result import SingleRunResult
from app.core.database import SessionLocal


class SingleRunResultCRUD:
    """
    CRUD 操作类：SingleRunResult
    自动管理数据库 Session，所有操作均为静态方法。
    """

    # === CREATE ===
    @staticmethod
    def create(**kwargs) -> SingleRunResult:
        """
        创建一条 SingleRunResult 记录。
        支持动态传入字段：input_task_uuid, chatflow_query, test_params, ...
        """
        try:
            with SessionLocal() as session:
                record = SingleRunResult(**kwargs)
                session.add(record)
                session.commit()
                session.refresh(record)
                logger.info(f"✅ Created SingleRunResult(record_id={record.record_id}, task_uuid={record.input_task_uuid})")
                return record
        except Exception as e:
            logger.exception(f"❌ Failed to create SingleRunResult: {e}")
            raise

    # === READ ===
    @staticmethod
    def get_by_id(record_id: str, include_deleted: bool = False) -> Optional[SingleRunResult]:
        """根据 record_id 获取记录"""
        with SessionLocal() as session:
            stmt = select(SingleRunResult).where(SingleRunResult.record_id == record_id)
            if not include_deleted:
                stmt = stmt.where(SingleRunResult.is_deleted == False)
            return session.scalar(stmt)

    @staticmethod
    def get_all(limit: int = 100, include_deleted: bool = False) -> List[SingleRunResult]:
        """获取所有记录（带 limit）"""
        with SessionLocal() as session:
            stmt = select(SingleRunResult).limit(limit)
            if not include_deleted:
                stmt = stmt.where(SingleRunResult.is_deleted == False)
            return list(session.scalars(stmt))

    # === UPDATE ===
    @staticmethod
    def update(record_id: str, **kwargs) -> Optional[SingleRunResult]:
        """
        更新指定记录字段。
        kwargs 可包含任意模型字段名，如 input_score=0.95, test_params={'temperature': 0.7}
        """
        try:
            with SessionLocal() as session:
                stmt = (
                    update(SingleRunResult)
                    .where(SingleRunResult.record_id == record_id)
                    .where(SingleRunResult.is_deleted == False)
                    .values(**kwargs)
                    .execution_options(synchronize_session="fetch")
                )
                result = session.execute(stmt)
                session.commit()

                if result.rowcount == 0:
                    logger.warning(f"⚠️ No record updated (record_id={record_id})")
                    return None

                logger.info(f"✅ Updated SingleRunResult(record_id={record_id}, fields={list(kwargs.keys())})")

                # 返回更新后的记录
                stmt_get = select(SingleRunResult).where(SingleRunResult.record_id == record_id)
                return session.scalar(stmt_get)
        except Exception as e:
            logger.exception(f"❌ Failed to update SingleRunResult({record_id}): {e}")
            raise

    # === DELETE（软删除）===
    @staticmethod
    def delete(record_id: str) -> bool:
        """
        软删除指定记录（设置 is_deleted=True）
        """
        try:
            with SessionLocal() as session:
                stmt = (
                    update(SingleRunResult)
                    .where(SingleRunResult.record_id == record_id)
                    .values(is_deleted=True)
                )
                result = session.execute(stmt)
                session.commit()

                if result.rowcount > 0:
                    logger.info(f"🗑️ Soft deleted SingleRunResult(record_id={record_id})")
                    return True
                else:
                    logger.warning(f"⚠️ No record found for deletion: {record_id}")
                    return False
        except Exception as e:
            logger.exception(f"❌ Failed to soft delete SingleRunResult({record_id}): {e}")
            raise

    # === HARD DELETE（仅用于清理）===
    @staticmethod
    def hard_delete(record_id: str) -> bool:
        """物理删除指定记录（危险操作）"""
        try:
            with SessionLocal() as session:
                stmt = delete(SingleRunResult).where(SingleRunResult.record_id == record_id)
                result = session.execute(stmt)
                session.commit()
                if result.rowcount > 0:
                    logger.info(f"❗ Hard deleted SingleRunResult(record_id={record_id})")
                    return True
                return False
        except Exception as e:
            logger.exception(f"❌ Failed to hard delete SingleRunResult({record_id}): {e}")
            raise

    # === RESTORE ===
    @staticmethod
    def restore_deleted(record_id: str) -> bool:
        """恢复被软删除的记录"""
        try:
            with SessionLocal() as session:
                stmt = (
                    update(SingleRunResult)
                    .where(SingleRunResult.record_id == record_id)
                    .values(is_deleted=False)
                )
                result = session.execute(stmt)
                session.commit()
                if result.rowcount > 0:
                    logger.info(f"♻️ Restored SingleRunResult(record_id={record_id})")
                    return True
                return False
        except Exception as e:
            logger.exception(f"❌ Failed to restore SingleRunResult({record_id}): {e}")
            raise

    # === LATEST 3 ===
    @staticmethod
    def get_latest_three_by_task_id(task_id: str) -> List[SingleRunResult]:
        """根据 task_id 查询最新的 3 条记录（按 create_time 倒序）"""
        try:
            with SessionLocal() as session:
                stmt = (
                    select(SingleRunResult)
                    .where(SingleRunResult.input_task_uuid == task_id)
                    .where(SingleRunResult.is_deleted == False)
                    .order_by(desc(SingleRunResult.create_time))
                    .limit(3)
                )
                results = list(session.scalars(stmt))
                logger.info(f"✅ Latest 3 results queried for task_id={task_id}, found={len(results)}")
                return results
        except Exception as e:
            logger.exception(f"❌ Failed to query latest 3 SingleRunResult(task_id={task_id}): {e}")
            raise

    # === PAGINATION ===
    @staticmethod
    def get_paginated_by_task_id(task_id: str, page: int = 1, page_size: int = 10) -> Dict[str, Any]:
        """根据 task_id 分页查询（排除软删除记录，按 create_time 倒序）"""
        try:
            with SessionLocal() as session:
                offset = (page - 1) * page_size

                total_stmt = (
                    select(func.count())
                    .select_from(SingleRunResult)
                    .where(SingleRunResult.input_task_uuid == task_id)
                    .where(SingleRunResult.is_deleted == False)
                )
                total = session.scalar(total_stmt) or 0

                data_stmt = (
                    select(SingleRunResult)
                    .where(SingleRunResult.input_task_uuid == task_id)
                    .where(SingleRunResult.is_deleted == False)
                    .order_by(desc(SingleRunResult.create_time))
                    .offset(offset)
                    .limit(page_size)
                )

                records = list(session.scalars(data_stmt))

                logger.info(
                    f"✅ Paginated query task_id={task_id}, page={page}, page_size={page_size}, total={total}, returned={len(records)}"
                )

                return {
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "records": records,
                }
        except Exception as e:
            logger.exception(f"❌ Failed paginated query SingleRunResult(task_id={task_id}): {e}")
            raise
