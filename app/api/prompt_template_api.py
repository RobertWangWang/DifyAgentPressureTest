from fastapi import APIRouter, HTTPException
from typing import List
from app.schemas.propmt_template_schema import PromptTemplateCreate, PromptTemplateRead
from app.crud.prompt_template_crud import PromptTemplateCRUD

router = APIRouter(prefix="/prompts", tags=["PromptTemplate"])

@router.post("/", response_model=PromptTemplateRead)
def create_prompt(data: PromptTemplateCreate):
    """创建 Prompt 模板"""
    obj = PromptTemplateCRUD.create(content=data.content)
    return obj


@router.get("/", response_model=List[PromptTemplateRead])
def list_prompts(include_deleted: bool = False):
    """获取所有 Prompt 模板"""
    objs = PromptTemplateCRUD.list_all(include_deleted=include_deleted)
    return objs


@router.get("/{record_id}", response_model=PromptTemplateRead)
def get_prompt(record_id: int):
    """根据 ID 获取 Prompt 模板"""
    obj = PromptTemplateCRUD.get_by_id(record_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Prompt not found")
    return obj


@router.put("/{record_id}", response_model=PromptTemplateRead)
def update_prompt(record_id: int, data: PromptTemplateCreate):
    """更新 Prompt 模板"""
    obj = PromptTemplateCRUD.update(record_id, content=data.content)
    if not obj:
        raise HTTPException(status_code=404, detail="Prompt not found")
    return obj


@router.delete("/{record_id}")
def delete_prompt(record_id: int, soft: bool = True):
    """删除 Prompt 模板（支持软删除）"""
    if soft:
        ok = PromptTemplateCRUD.soft_delete(record_id)
    else:
        ok = PromptTemplateCRUD.hard_delete(record_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Prompt not found")
    return {"status": "success", "soft_deleted": soft}
