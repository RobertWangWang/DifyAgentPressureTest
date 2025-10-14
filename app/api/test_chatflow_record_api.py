from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from app.crud.test_chatflow_record_crud import TestRecordCRUD
from app.schemas.test_record_schema import (
    TestRecordCreate, TestRecordRead, TestRecordUpdate
)
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
def create_record(record: TestRecordCreate, db: Session = Depends(get_db)):
    created = TestRecordCRUD.create(db, **record.dict())
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
