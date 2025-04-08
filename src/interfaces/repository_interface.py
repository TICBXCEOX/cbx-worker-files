from abc import ABC, abstractmethod
from typing import Type, TypeVar
from pandas import DataFrame
    
T = TypeVar('T')

class IRepository(ABC):
    @abstractmethod
    def create(self, entity: Type[T]) -> None:
        pass
    
    @abstractmethod
    def delete(self, id: int) -> None:    
        pass

    @abstractmethod
    def query_by_where(self, table: str, where: str) -> 'list[T]':
        pass
        
    @abstractmethod
    def insert_chunk(self, table: str, chunk: DataFrame ) -> str:        
        pass    