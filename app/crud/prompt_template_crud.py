from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select
from datetime import datetime

from app.core.database import SessionLocal  # 假设你在 database.py 中定义了 SessionLocal
from app.models.prompt_template import PromptTemplate


class PromptTemplateCRUD:

    @staticmethod
    def create(content: str, prompt_name:str) -> PromptTemplate:
        """创建新的 PromptTemplate 记录"""
        with SessionLocal() as session:
            obj = PromptTemplate(content=content, prompt_name=prompt_name)
            session.add(obj)
            session.commit()
            session.refresh(obj)
            return obj

    @staticmethod
    def get_by_id(record_id: int) -> Optional[PromptTemplate]:
        """根据 record_id 查询"""
        with SessionLocal() as session:
            return session.get(PromptTemplate, record_id)

    @staticmethod
    def get_by_uuid(uuid: str) -> Optional[PromptTemplate]:
        """根据 uuid 查询"""
        with SessionLocal() as session:
            stmt = select(PromptTemplate).where(PromptTemplate.uuid == uuid)
            return session.scalars(stmt).first()

    @staticmethod
    def list_all(include_deleted: bool = False) -> List[PromptTemplate]:
        """列出全部记录"""
        with SessionLocal() as session:
            stmt = select(PromptTemplate)
            if not include_deleted:
                stmt = stmt.where(PromptTemplate.deleted_at.is_(False))
            return list(session.scalars(stmt).all())

    @staticmethod
    def update(record_id: int, content: Optional[str] = None) -> Optional[PromptTemplate]:
        """更新指定记录内容"""
        with SessionLocal() as session:
            obj = session.get(PromptTemplate, record_id)
            if not obj:
                return None
            if content is not None:
                obj.content = content
            session.commit()
            session.refresh(obj)
            return obj

    @staticmethod
    def soft_delete(record_id: int) -> bool:
        """软删除（设置 deleted_at=True）"""
        with SessionLocal() as session:
            obj = session.get(PromptTemplate, record_id)
            if not obj:
                return False
            obj.deleted_at = True
            session.commit()
            return True

    @staticmethod
    def hard_delete(record_id: int) -> bool:
        """硬删除（从数据库彻底删除）"""
        with SessionLocal() as session:
            obj = session.get(PromptTemplate, record_id)
            if not obj:
                return False
            session.delete(obj)
            session.commit()
            return True
