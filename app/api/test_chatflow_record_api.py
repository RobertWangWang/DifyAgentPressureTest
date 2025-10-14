from pathlib import Path
from typing import List

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    status,
)
from sqlalchemy.orm import Session

from app.crud.test_chatflow_record_crud import TestRecordCRUD
from app.schemas.test_record_schema import (
    TestRecordCreate,
    TestRecordRead,
    TestRecordUpdate,
    TestStatus,
)
from app.core.config import settings
from app.core.database import SessionLocal
from app.services.test_record_services import test_chatflow_non_stream_pressure_wrapper

router = APIRouter(prefix="/test_chatflow_records", tags=["TestChatflowRecords"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/", response_model=TestRecordRead, status_code=status.HTTP_201_CREATED)
async def create_record(
    file: UploadFile = File(...),
    status: TestStatus = Form(TestStatus.init),
    duration: int | None = Form(None),
    result: str | None = Form(None),
    concurrency: int | None = Form(1),
    dify_api_url: str = Form(...),
    dify_api_key: str = Form(...),
    dify_username: str = Form(...),
    chatflow_query: str = Form(...),
    db: Session = Depends(get_db),
):
    upload_dir = Path(settings.FILE_UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)

    original_filename = Path(file.filename or "").name
    if not original_filename:
        raise HTTPException(status_code=400, detail="Uploaded file must have a name.")

    stem = Path(original_filename).stem
    suffix = Path(original_filename).suffix
    candidate_name = original_filename
    candidate_path = upload_dir / candidate_name
    counter = 1
    while candidate_path.exists():
        candidate_name = f"{stem}_{counter}{suffix}"
        candidate_path = upload_dir / candidate_name
        counter += 1

    file_bytes = await file.read()
    candidate_path.write_bytes(file_bytes)

    record_data = TestRecordCreate(
        filename=candidate_name,
        status=status,
        duration=duration,
        result=result,
        concurrency=concurrency,
        dify_api_url=dify_api_url,
        dify_api_key=dify_api_key,
        dify_username=dify_username,
        chatflow_query=chatflow_query,
    )
    created = TestRecordCRUD.create(db, **record_data.dict())
    return created


@router.get("/{uuid_str}", response_model=TestRecordRead)
def get_record(uuid_str: str, db: Session = Depends(get_db)):
    rec = TestRecordCRUD.get_by_uuid(db, uuid_str)
    if rec is None:
        raise HTTPException(status_code=404, detail="Record not found")
    return rec


@router.get("/", response_model=List[TestRecordRead])
def list_records(limit: int = 100, db: Session = Depends(get_db)):
    return TestRecordCRUD.list_all(db, limit=limit)


@router.patch("/{uuid_str}", response_model=TestRecordRead)
def update_record(uuid_str: str, payload: TestRecordUpdate, db: Session = Depends(get_db)):
    existing = TestRecordCRUD.get_by_uuid(db, uuid_str)
    if existing is None:
        raise HTTPException(status_code=404, detail="Record not found")
    updated = TestRecordCRUD.update_by_uuid(db, uuid_str, **payload.dict(exclude_unset=True))
    return updated


@router.delete("/{uuid_str}", status_code=status.HTTP_204_NO_CONTENT)
def delete_record(uuid_str: str, db: Session = Depends(get_db)):
    existing = TestRecordCRUD.get_by_uuid(db, uuid_str)
    if existing is None:
        raise HTTPException(status_code=404, detail="Record not found")
    success = TestRecordCRUD.delete_by_uuid(db, uuid_str)
    if not success:
        raise HTTPException(status_code=500, detail="Delete failed")
    return None


@router.post("/{uuid_str}/run", status_code=status.HTTP_200_OK)
def run_record(uuid_str: str, db: Session = Depends(get_db)):
    existing = TestRecordCRUD.get_by_uuid(db, uuid_str)
    if existing is None:
        raise HTTPException(status_code=404, detail="Record not found")
    result = test_chatflow_non_stream_pressure_wrapper(existing)
    return result
