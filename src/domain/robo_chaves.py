from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class RoboChaves(Base):
    __table_args__ = {"schema": "cbx"}
    __tablename__ = 'robo_chaves'

    transaction_id = Column(String, primary_key=True)
    key_nf = Column(String, primary_key=True)