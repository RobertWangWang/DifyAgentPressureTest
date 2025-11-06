from typing import Any, Optional, Dict, List
from sqlalchemy.orm import Session
from sqlalchemy import select, update, delete, func, or_
from sqlalchemy.exc import SQLAlchemyError

from app.models.dataset import Dataset
from app.schemas.dataset_schema import DatasetCreate


class DatasetCRUD:
    """Dataset 表的 CRUD 操作类"""

    # 🟢 创建（若存在则复用）
    @staticmethod
    def create(session: Session, data: DatasetCreate) -> Dataset:
        try:
            stmt = select(Dataset).where(
                Dataset.file_md5 == data.file_md5,
                Dataset.uploaded_by == data.uploaded_by,
                Dataset.agent_id == data.agent_id,
                Dataset.is_deleted == False,
            )
            # existing = session.execute(stmt).scalar_one_or_none()
            # if existing:
                ## return existing

            dataset = Dataset(
                filename=data.filename,
                file_md5=data.file_md5,
                file_suffix=data.file_suffix,
                tos_key=data.tos_key,
                tos_url=data.tos_url,
                preview_rows=data.preview_rows,
                uploaded_by=data.uploaded_by,
                agent_id=data.agent_id,
                is_deleted=False,
            )
            session.add(dataset)
            session.commit()
            session.refresh(dataset)
            return dataset
        except SQLAlchemyError as e:
            session.rollback()
            raise e

    # 🟡 获取单条记录
    @staticmethod
    def get_by_uuid(session: Session, uuid_str: str) -> Optional[Dataset]:
        stmt = select(Dataset).where(Dataset.uuid == uuid_str, Dataset.is_deleted == False)
        return session.execute(stmt).scalar_one_or_none()

    # 🟡 根据 MD5 查询
    @staticmethod
    def get_by_md5(
        session: Session,
        file_md5: str,
        agent_id: Optional[str] = None,
        uploaded_by: Optional[str] = None,
    ) -> Optional[Dataset]:
        stmt = select(Dataset).where(Dataset.file_md5 == file_md5)
        if agent_id:
            stmt = stmt.where(Dataset.agent_id == agent_id)
        if uploaded_by:
            stmt = stmt.where(Dataset.uploaded_by == uploaded_by)
        return session.execute(stmt).scalars().first()

    # 🟢 列表查询（分页）
    @staticmethod
    def list_all(
        session: Session,
        uploaded_by: Optional[str] = None,
        agent_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dataset]:
        stmt = select(Dataset).where(or_(Dataset.is_deleted == False, Dataset.is_deleted == 0, Dataset.is_deleted == None))
        if uploaded_by:
            stmt = stmt.where(Dataset.uploaded_by == uploaded_by)
        if agent_id:
            stmt = stmt.where(Dataset.agent_id == agent_id)
        stmt = stmt.order_by(Dataset.created_at.desc()).limit(limit).offset(offset)
        return session.execute(stmt).scalars().all()

    # 🧮 计数
    @staticmethod
    def count(session: Session, uploaded_by: Optional[str] = None, agent_id: Optional[str] = None) -> int:
        stmt = select(func.count()).select_from(Dataset).where(Dataset.is_deleted == False)
        if uploaded_by:
            stmt = stmt.where(Dataset.uploaded_by == uploaded_by)
        if agent_id:
            stmt = stmt.where(Dataset.agent_id == agent_id)
        return session.execute(stmt).scalar_one()

    # 🟣 更新
    @staticmethod
    def update_by_uuid(session: Session, uuid_str: str, **kwargs: Any) -> Optional[Dataset]:
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
            return DatasetCRUD.get_by_uuid(session, uuid_str)
        except SQLAlchemyError as e:
            session.rollback()
            raise e

    # 🧹 软删除
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

    # 💣 硬删除
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
