from typing import List, TypeVar
from domain.file_process_log import FileProcessLog
from interfaces.file_process_log_repository_interface import IFileProcessLogRepository
from repositories.base_repository import BaseRepository
from sqlalchemy import select, update, text, exc

T = TypeVar('T')

class FileProcessLogRepository(BaseRepository, IFileProcessLogRepository):
    def __init__(self):        
        super().__init__(FileProcessLog)
            
    def update(self, entity: FileProcessLog) -> None:
        with self.SessionSync() as session:
            try:
                stmt = (
                    update(FileProcessLog)
                    .where(FileProcessLog.id == entity.id)
                    .values(
                        executed_at = entity.executed_at,
                        executed_by = entity.executed_by)
                )
                result = session.execute(stmt)
                session.commit()
                if result.rowcount == 0:
                    raise Exception(f"File Process Log {entity.ie_id} não foi atualizado.")
            except Exception as ex:
                session.rollback()
                raise ex