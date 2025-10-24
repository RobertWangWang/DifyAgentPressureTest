from typing import Any, Optional, Dict, List
from sqlalchemy.orm import Session
from sqlalchemy import select, update, delete
from sqlalchemy.exc import SQLAlchemyError

from app.models.dataset import Dataset  # ✅ ORM 模型
from app.schemas.dataset_schema import DatasetCreate


class DatasetCRUD:
    """Dataset 表的 CRUD 操作类"""

    # 🟢 创建数据集记录
    @staticmethod
    def create(session: Session, data: DatasetCreate) -> Dataset:
        """
        创建新的数据集记录
        """
        dataset = Dataset(
            filename=data.filename,
            file_md5=data.file_md5,
            file_suffix=data.file_suffix,
            tos_key=data.tos_key,
            tos_url=data.tos_url,
            preview_rows=data.preview_rows,
            uploaded_by=data.uploaded_by,
        )

        try:
            session.add(dataset)
            session.commit()
            session.refresh(dataset)
            return dataset
        except SQLAlchemyError as e:
            session.rollback()
            raise e

    # 🟡 根据 UUID 获取数据集
    @staticmethod
    def get_by_uuid(session: Session, uuid_str: str) -> Optional[Dataset]:
        stmt = select(Dataset).where(Dataset.uuid == uuid_str, Dataset.is_deleted == False)
        result = session.execute(stmt).scalar_one_or_none()
        return result

    # 🟡 根据 MD5 获取数据集
    @staticmethod
    def get_by_md5(session: Session, file_md5: str) -> Optional[Dataset]:
        stmt = select(Dataset).where(Dataset.file_md5 == file_md5, Dataset.is_deleted == False)
        result = session.execute(stmt).scalar_one_or_none()
        return result

    # 🟢 列出所有数据集（可选过滤）
    @staticmethod
    def list_all(session: Session, uploaded_by: Optional[str] = None, limit: int = 100) -> List[Dataset]:
        stmt = select(Dataset).where(Dataset.is_deleted == False)
        if uploaded_by:
            stmt = stmt.where(Dataset.uploaded_by == uploaded_by)
        stmt = stmt.order_by(Dataset.created_at.desc()).limit(limit)
        results = session.execute(stmt).scalars().all()
        return results

    # 🟣 根据 UUID 更新（仅更新非 None 值）
    @staticmethod
    def update_by_uuid(session: Session, uuid_str: str, **kwargs: Any) -> Optional[Dataset]:
        """
        根据 UUID 更新字段（仅更新非 None）
        """
        update_data: Dict[str, Any] = {k: v for k, v in kwargs.items() if v is not None}
        if not update_data:
            return DatasetCRUD.get_by_uuid(session, uuid_str)

        stmt = (
            update(Dataset)
            .where(Dataset.uuid == uuid_str)
            .values(**update_data)
            .execution_options(synchronize_session="fetch")
        )

        try:
            session.execute(stmt)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e

        return DatasetCRUD.get_by_uuid(session, uuid_str)

    # 🔴 逻辑删除（软删除）
    @staticmethod
    def soft_delete(session: Session, uuid_str: str) -> bool:
        stmt = (
            update(Dataset)
            .where(Dataset.uuid == uuid_str)
            .values(is_deleted=True)
            .execution_options(synchronize_session="fetch")
        )

        try:
            session.execute(stmt)
            session.commit()
            return True
        except SQLAlchemyError as e:
            session.rollback()
            raise e

    # 🔴 硬删除（彻底删除）
    @staticmethod
    def hard_delete(session: Session, uuid_str: str) -> bool:
        stmt = delete(Dataset).where(Dataset.uuid == uuid_str)
        try:
            session.execute(stmt)
            session.commit()
            return True
        except SQLAlchemyError as e:
            session.rollback()
            raise e
