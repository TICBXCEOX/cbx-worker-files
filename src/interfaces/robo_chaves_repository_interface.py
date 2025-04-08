from abc import ABC, abstractmethod
from typing import List, TypeVar

from domain.robo_chaves import RoboChaves
    
T = TypeVar('T')

class IRoboChavesRepository(ABC):       
    @abstractmethod
    def get_by_transaction_id(self, transaction_id: str) -> List[RoboChaves]:
        pass
    
    @abstractmethod
    def get_by_pk(self, transaction_id: str, key_nf: str) -> RoboChaves:
        pass
    
    @abstractmethod
    def delete_by_transaction_id(self, transaction_id: str) -> None:
        pass    