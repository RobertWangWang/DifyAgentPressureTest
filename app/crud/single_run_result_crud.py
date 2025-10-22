from sqlalchemy import select, update, delete, desc, func
from loguru import logger
from typing import List, Optional, Dict, Any
from app.models.single_run_result import SingleRunResult
from app.core.database import SessionLocal  # ✅ 导入你的数据库会话工厂


class SingleRunResultCRUD:
    """
    CRUD 操作类：SingleRunResult
    所有方法均为 @staticmethod，自动管理数据库 Session。
    """

    # === CREATE ===
    @staticmethod
    def create(
        input_task_uuid: str,
        input_time_consumption: Optional[float] = None,
        input_score: Optional[float] = None,
        input_tps: Optional[float] = None,
        input_generated_answer: Optional[str] = None,
    ) -> SingleRunResult:
        """创建一条记录"""
        try:
            with SessionLocal() as session:
                record = SingleRunResult(
                    input_task_uuid=input_task_uuid,
                    input_time_consumption=input_time_consumption,
                    input_score=input_score,
                    input_tps=input_tps,
                    input_generated_answer=input_generated_answer,
                )
                session.add(record)
                session.commit()
                session.refresh(record)
                logger.info(f"✅ Created SingleRunResult: {record.record_id}")
                return record
        except Exception as e:
            logger.error(f"❌ Failed to create SingleRunResult: {e}")
            raise

    # === READ ===
    @staticmethod
    def get_by_id(record_id: str) -> Optional[SingleRunResult]:
        """根据 record_id 获取记录"""
        with SessionLocal() as session:
            stmt = select(SingleRunResult).where(SingleRunResult.record_id == record_id)
            result = session.scalar(stmt)
            return result

    @staticmethod
    def get_all(limit: int = 100) -> List[SingleRunResult]:
        """获取所有记录（带 limit）"""
        with SessionLocal() as session:
            stmt = select(SingleRunResult).limit(limit)
            return list(session.scalars(stmt))

    # === UPDATE ===
    @staticmethod
    def update(record_id: str, **kwargs) -> Optional[SingleRunResult]:
        """
        更新指定记录的字段。
        kwargs 可包含任意模型字段名。
        """
        try:
            with SessionLocal() as session:
                stmt = (
                    update(SingleRunResult)
                    .where(SingleRunResult.record_id == record_id)
                    .values(**kwargs)
                    .execution_options(synchronize_session="fetch")
                )
                session.execute(stmt)
                session.commit()
                logger.info(f"✅ Updated SingleRunResult: {record_id} with {kwargs}")
                # 返回更新后的记录
                stmt_get = select(SingleRunResult).where(SingleRunResult.record_id == record_id)
                return session.scalar(stmt_get)
        except Exception as e:
            logger.error(f"❌ Failed to update SingleRunResult({record_id}): {e}")
            raise

    # === DELETE ===
    @staticmethod
    def delete(record_id: str) -> bool:
        """删除指定记录"""
        try:
            with SessionLocal() as session:
                stmt = delete(SingleRunResult).where(SingleRunResult.record_id == record_id)
                result = session.execute(stmt)
                session.commit()
                if result.rowcount > 0:
                    logger.info(f"🗑️ Deleted SingleRunResult: {record_id}")
                    return True
                else:
                    logger.warning(f"⚠️ No record found for deletion: {record_id}")
                    return False
        except Exception as e:
            logger.error(f"❌ Failed to delete SingleRunResult({record_id}): {e}")
            raise

    @staticmethod
    def get_latest_three_by_task_id(task_id: str) -> List[SingleRunResult]:
        """
        根据 task_id (input_task_uuid) 查询创建时间倒序的前三条记录
        """
        try:
            with SessionLocal() as session:
                stmt = (
                    select(SingleRunResult)
                    .where(SingleRunResult.input_task_uuid == task_id)
                    .order_by(desc(SingleRunResult.create_time))
                    .limit(3)
                )
                results = list(session.scalars(stmt))
                logger.info(
                    f"✅ Query latest 3 SingleRunResult by task_id={task_id}, found={len(results)}"
                )
                return results
        except Exception as e:
            logger.error(f"❌ Failed to query SingleRunResult for task_id={task_id}: {e}")
            raise

    @staticmethod
    def get_paginated_by_task_id(task_id: str, page: int = 1, page_size: int = 10) -> Dict[str, Any]:
        """
        根据 task_id (input_task_uuid) 分页查询记录（按 create_time 倒序）
        返回结构包含总数 total、页码 page、页大小 page_size、records 列表
        """
        try:
            with SessionLocal() as session:
                offset = (page - 1) * page_size

                # 1️⃣ 获取总数
                total_stmt = (
                    select(func.count())
                    .select_from(SingleRunResult)
                    .where(SingleRunResult.input_task_uuid == task_id)
                )
                total = session.scalar(total_stmt) or 0

                # 2️⃣ 获取分页数据
                data_stmt = (
                    select(SingleRunResult)
                    .where(SingleRunResult.input_task_uuid == task_id)
                    .order_by(desc(SingleRunResult.create_time))
                    .offset(offset)
                    .limit(page_size)
                )

                records = list(session.scalars(data_stmt))

                logger.info(
                    f"✅ 分页查询 task_id={task_id}, page={page}, page_size={page_size}, total={total}, returned={len(records)}"
                )

                return {
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "records": records,
                }

        except Exception as e:
            logger.error(f"❌ 查询 SingleRunResult 分页失败 (task_id={task_id}): {e}")
            raise