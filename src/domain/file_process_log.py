from sqlalchemy import Column, DateTime, Integer, String, JSON, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class FileProcessLog(Base):
    __tablename__ = 'file_process_log'
    __table_args__ = {"schema": "cbx"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String, nullable=False)
    input_s3_url = Column(String, nullable=True)
    output_s3_url = Column(String, nullable=True)
    file_name = Column(String, nullable=True)
    file_type = Column(String, nullable=True)
    request_origin = Column(String, nullable=True)
    logs = Column(JSON, nullable=True)
    errors = Column(JSON, nullable=True)
    client_id = Column(Integer, nullable=True)
    executed_at = Column(DateTime(timezone=False), nullable=False)
    executed_by = Column(Integer, nullable=True)