from fastapi import APIRouter, HTTPException, status, Query
from typing import List, Optional
from loguru import logger

from app.schemas.single_run_result_schema import (
    SingleRunResultCreate,
    SingleRunResultRead,
)
from app.crud.single_run_result_crud import SingleRunResultCRUD


router = APIRouter(prefix="/single_run_result", tags=["SingleRunResult"])


# === 1️⃣ 创建记录 ===
@router.post("/", response_model=SingleRunResultRead, status_code=status.HTTP_201_CREATED)
def create_single_run_result(payload: SingleRunResultCreate):
    """创建一条运行结果记录"""
    try:
        record = SingleRunResultCRUD.create(
            input_task_uuid=payload.input_task_uuid,
            input_task_mode=payload.input_task_mode,
            input_time_consumption=payload.input_time_consumption,
            input_score=payload.input_score,
            input_tps=payload.input_tps,
            input_generated_answer=payload.input_generated_answer,
        )
        return record
    except Exception as e:
        logger.error(f"❌ 创建 SingleRunResult 失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建失败: {str(e)}")


# === 2️⃣ 获取单条记录 ===
@router.get("/{record_id}", response_model=SingleRunResultRead)
def get_single_run_result(record_id: str):
    """根据 record_id 获取单条记录"""
    record = SingleRunResultCRUD.get_by_id(record_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Record {record_id} not found")
    return record


# === 3️⃣ 获取所有记录 ===
@router.get("/", response_model=List[SingleRunResultRead])
def get_all_single_run_results(limit: Optional[int] = 100):
    """获取所有运行结果记录"""
    records = SingleRunResultCRUD.get_all(limit=limit)
    return records


# === 4️⃣ 更新记录 ===
@router.put("/{record_id}", response_model=SingleRunResultRead)
def update_single_run_result(record_id: str, payload: SingleRunResultCreate):
    """更新指定 record_id 的记录"""
    updated = SingleRunResultCRUD.update(record_id, **payload.dict(exclude_unset=True))
    if not updated:
        raise HTTPException(status_code=404, detail=f"Record {record_id} not found")
    return updated


# === 5️⃣ 删除记录 ===
@router.delete("/{record_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_single_run_result(record_id: str):
    """删除指定记录"""
    success = SingleRunResultCRUD.delete(record_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Record {record_id} not found")
    return {"message": f"Record {record_id} deleted successfully"}

@router.get("/latest/{task_id}", response_model=List[SingleRunResultRead])
def get_latest_three_by_task_id(task_id: str):
    """
    根据 task_id (input_task_uuid) 获取按创建时间倒序的最新三条记录
    """
    try:
        records = SingleRunResultCRUD.get_latest_three_by_task_id(task_id)
        if not records:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No records found for task_id={task_id}",
            )
        logger.info(f"✅ 返回 task_id={task_id} 的最新 3 条记录，共 {len(records)} 条")
        return records
    except Exception as e:
        logger.error(f"❌ 查询 task_id={task_id} 的记录失败: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"查询失败: {e}",
        )

@router.get("/page/{task_id}", response_model=dict)
def get_paginated_by_task_id(
    task_id: str,
    page: int = Query(1, ge=1, description="页码（从1开始）"),
    page_size: int = Query(10, ge=1, le=100, description="每页条数（默认10）"),
):
    """
    根据 task_id 分页查询记录（按创建时间倒序）
    返回分页结构：total + page + page_size + records
    """
    try:
        result = SingleRunResultCRUD.get_paginated_by_task_id(task_id, page=page, page_size=page_size)
        if result["total"] == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No records found for task_id={task_id}",
            )

        # ✅ 将 ORM 对象转换为可 JSON 序列化形式
        result["records"] = [
            SingleRunResultRead.from_orm(r).dict() for r in result["records"]
        ]
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 查询分页记录失败 (task_id={task_id}): {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"查询失败: {e}",
        )