from abc import ABC, abstractmethod
from typing import List, TypeVar

from domain.file_process_log import FileProcessLog
    
T = TypeVar('T')

class IFileProcessLogRepository(ABC):
    @abstractmethod
    def update(self, entity: FileProcessLog) -> None:
        pass
