from pathlib import Path
from typing import List

from starlette.datastructures import UploadFile as StarletteUploadFile
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, status
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.crud.test_chatflow_record_crud import TestRecordCRUD
from app.schemas.test_record_schema import (
    TestRecordCreate,
    TestRecordRead,
    TestRecordUpdate,
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
async def create_record(request: Request, db: Session = Depends(get_db)):
    content_type = request.headers.get("content-type", "").lower()

    if not content_type.startswith("multipart/form-data"):
        raise HTTPException(
            status_code=415,
            detail="Request content type must be multipart/form-data with a file upload.",
        )

    async def _persist_upload(upload: UploadFile) -> str:
        upload_dir = Path(settings.FILE_UPLOAD_DIR)
        upload_dir.mkdir(parents=True, exist_ok=True)

        original_filename = Path(upload.filename or "").name
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

        file_bytes = await upload.read()
        candidate_path.write_bytes(file_bytes)
        await upload.close()
        return candidate_name

    form = await request.form()
    upload = form.get("file")
    print("FORM KEYS:", form.keys())
    print("UPLOAD TYPE:", type(upload), upload)

    if not isinstance(upload, StarletteUploadFile):
        raise HTTPException(
            status_code=400,
            detail="A 'file' upload is required and must be provided as a file.",
        )

    payload_data: dict[str, str] = {}
    for key, value in form.multi_items():
        if key == "file":
            continue
        payload_data[key] = value

    try:
        record_payload = TestRecordCreate(**payload_data)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    filename = await _persist_upload(upload)

    created = TestRecordCRUD.create(db, filename=filename, **record_payload.dict())
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


@router.post("/run_test/{uuid_str}", status_code=status.HTTP_200_OK)
def run_record(uuid_str: str, db: Session = Depends(get_db)):
    existing = TestRecordCRUD.get_by_uuid(db, uuid_str)
    if existing is None:
        raise HTTPException(status_code=404, detail="Record not found")
    result = test_chatflow_non_stream_pressure_wrapper(existing)
    return result
