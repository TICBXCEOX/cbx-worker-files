from datetime import datetime
from domain.file_process_log import FileProcessLog
from repositories.file_process_log_repository import FileProcessLogRepository

class FileProcessLogService:    
    def __init__(self):
        self.repository = FileProcessLogRepository()
    
    def log(self, 
            request_origin,
            transaction_id, 
            file_name, 
            file_type, 
            input_s3_url, 
            output_s3_url,             
            errors, 
            logs, 
            user_id, 
            client_id):
        error: str = ''
        try:
            entity = FileProcessLog()
            entity.transaction_id = transaction_id
            entity.file_name = file_name
            entity.file_type = file_type
            entity.input_s3_url = input_s3_url
            entity.output_s3_url = output_s3_url
            entity.request_origin = request_origin
            entity.errors = errors if errors else None
            entity.logs = logs if logs else None
            entity.client_id = client_id        
            entity.executed_by = user_id
            entity.executed_at = datetime.now()
            
            self.create(entity)
        except Exception as ex:
            error = f"Erro ao registrar processo BD. Arquivo: {file_name} - Erro: {str(ex)}"
        return error

    def create(self, entity: FileProcessLog) -> None:                
        self.repository.create(entity)
            
    def update(self, entity: FileProcessLog) -> None:
        self.repository.update(entity)
        
    def delete(self, id: int) -> None:
        self.repository.delete(id)