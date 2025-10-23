from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import StreamingResponse
from io import BytesIO
import pandas as pd
from typing import List, Optional, Dict, Any


from app.schemas.single_run_result_schema import (
    SingleRunResultCreate,
    SingleRunResultRead,
)
from app.crud.single_run_result_crud import SingleRunResultCRUD
from app.utils.logger import logger

router = APIRouter(prefix="/single_run_result", tags=["SingleRunResult"])


# === 1️⃣ 创建记录 ===
@router.post("/", response_model=SingleRunResultRead, status_code=status.HTTP_201_CREATED)
def create_single_run_result(payload: SingleRunResultCreate):
    """创建一条运行结果记录"""
    try:
        record = SingleRunResultCRUD.create(**payload.dict(exclude_unset=True))
        logger.info(f"✅ 创建成功: record_id={record.record_id}, task_uuid={record.input_task_uuid}")
        return record
    except Exception as e:
        logger.exception(f"❌ 创建 SingleRunResult 失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建失败: {str(e)}")


# === 2️⃣ 获取单条记录 ===
@router.get("/{record_id}", response_model=SingleRunResultRead)
def get_single_run_result(record_id: str):
    """根据 record_id 获取单条记录"""
    record = SingleRunResultCRUD.get_by_id(record_id)
    if not record:
        logger.warning(f"⚠️ 记录不存在: record_id={record_id}")
        raise HTTPException(status_code=404, detail=f"Record {record_id} not found")
    return record


# === 3️⃣ 获取所有记录 ===
@router.get("/", response_model=List[SingleRunResultRead])
def get_all_single_run_results(limit: Optional[int] = 100):
    """获取所有运行结果记录"""
    try:
        records = SingleRunResultCRUD.get_all(limit=limit)
        return records
    except Exception as e:
        logger.exception(f"❌ 获取所有记录失败: {e}")
        raise HTTPException(status_code=500, detail=f"查询失败: {e}")


# === 4️⃣ 更新记录 ===
@router.put("/{record_id}", response_model=SingleRunResultRead)
def update_single_run_result(record_id: str, payload: SingleRunResultCreate):
    """更新指定 record_id 的记录"""
    try:
        updated = SingleRunResultCRUD.update(record_id, **payload.dict(exclude_unset=True))
        if not updated:
            raise HTTPException(status_code=404, detail=f"Record {record_id} not found")
        return updated
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ 更新记录失败 record_id={record_id}: {e}")
        raise HTTPException(status_code=500, detail=f"更新失败: {e}")


# === 5️⃣ 删除记录（软删除）===
@router.delete("/{record_id}", status_code=status.HTTP_200_OK)
def delete_single_run_result(record_id: str):
    """软删除指定记录（is_deleted=True）"""
    try:
        success = SingleRunResultCRUD.delete(record_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Record {record_id} not found")
        return {"message": f"Record {record_id} soft deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ 删除记录失败 record_id={record_id}: {e}")
        raise HTTPException(status_code=500, detail=f"删除失败: {e}")


# === 6️⃣ 恢复被删除的记录 ===
@router.post("/restore/{record_id}", status_code=status.HTTP_200_OK)
def restore_deleted_record(record_id: str):
    """恢复被软删除的记录"""
    try:
        success = SingleRunResultCRUD.restore_deleted(record_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Record {record_id} not found or not deleted")
        return {"message": f"Record {record_id} restored successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ 恢复记录失败 record_id={record_id}: {e}")
        raise HTTPException(status_code=500, detail=f"恢复失败: {e}")


# === 7️⃣ 物理删除（仅管理员使用）===
@router.delete("/hard/{record_id}", status_code=status.HTTP_200_OK)
def hard_delete_single_run_result(record_id: str):
    """物理删除记录（危险操作）"""
    try:
        success = SingleRunResultCRUD.hard_delete(record_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Record {record_id} not found")
        return {"message": f"Record {record_id} permanently deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ 硬删除记录失败 record_id={record_id}: {e}")
        raise HTTPException(status_code=500, detail=f"硬删除失败: {e}")


# === 8️⃣ 查询最新 3 条记录 ===
@router.get("/latest/{task_id}", response_model=List[SingleRunResultRead])
def get_latest_three_by_task_id(task_id: str):
    """根据 task_id 获取按创建时间倒序的最新三条记录"""
    try:
        records = SingleRunResultCRUD.get_latest_three_by_task_id(task_id)
        if not records:
            raise HTTPException(status_code=404, detail=f"No records found for task_id={task_id}")
        logger.info(f"✅ 返回 task_id={task_id} 的最新 3 条记录，共 {len(records)} 条")
        return records
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ 查询 task_id={task_id} 失败: {e}")
        raise HTTPException(status_code=500, detail=f"查询失败: {e}")


# === 9️⃣ 分页查询 ===
@router.get("/page/{task_id}", response_model=Dict[str, Any])
def get_paginated_by_task_id(
    task_id: str,
    page: int = Query(1, ge=1, description="页码（从1开始）"),
    page_size: int = Query(10, ge=1, le=100, description="每页条数（默认10）"),
):
    """根据 task_id 分页查询记录（按创建时间倒序）"""
    try:
        result = SingleRunResultCRUD.get_paginated_by_task_id(task_id, page=page, page_size=page_size)
        if result["total"] == 0:
            raise HTTPException(status_code=404, detail=f"No records found for task_id={task_id}")

        # ✅ ORM → Pydantic → dict 转换
        result["records"] = [SingleRunResultRead.from_orm(r).dict() for r in result["records"]]
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ 查询分页记录失败 task_id={task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"查询失败: {e}")

@router.get("/export/{task_id}")
def export_single_run_results_to_excel(task_id: str):
    """
    根据 input_task_uuid 导出所有结果为 Excel 文件。
    """
    try:
        # 1️⃣ 从数据库查询所有记录
        records = SingleRunResultCRUD.get_paginated_by_task_id(task_id, page=1, page_size=99999)["records"]

        if not records:
            raise HTTPException(status_code=404, detail=f"No records found for task_id={task_id}")

        # 2️⃣ 将 ORM 对象转换为字典列表
        data = []
        for r in records:
            data.append({
                "chatflow_query": r.chatflow_query,
                "测试参数": str(r.test_params) if r.test_params else "",
                "时间消耗": r.input_time_consumption,
                "大模型评分": r.input_score,
                "TPS": r.input_tps,
                "生成的答案": r.input_generated_answer,
            })

        # 3️⃣ 转换为 DataFrame
        df = pd.DataFrame(data)

        # 4️⃣ 写入 Excel 文件到内存
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="results")
        output.seek(0)

        # 5️⃣ 生成响应
        filename = f"single_run_results_{task_id}.xlsx"
        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"'
        }

        logger.info(f"✅ 导出 Excel 成功 task_id={task_id}, rows={len(df)}")

        return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ 导出 Excel 失败 task_id={task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")