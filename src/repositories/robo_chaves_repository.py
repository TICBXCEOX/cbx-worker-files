from typing import List, TypeVar
from sqlalchemy import delete, select
from domain.robo_chaves import RoboChaves
from interfaces.robo_chaves_repository_interface import IRoboChavesRepository
from repositories.base_repository import BaseRepository

T = TypeVar('T')

class RoboChavesRepository(BaseRepository, IRoboChavesRepository):
    def __init__(self):        
        super().__init__(RoboChaves)
                    
    def get_by_transaction_id(self, transaction_id: str) -> List[RoboChaves]:
        with self.SessionSync() as session:
            try:            
                result = session.query(RoboChaves).filter(RoboChaves.transaction_id == transaction_id).all()
                return result
            except Exception as ex:
                session.rollback()
                raise ex            

    def get_by_pk(self, transaction_id: str, key_nf: str) -> RoboChaves:
        with self.SessionSync() as session:            
            try:            
                result = session.query(RoboChaves).filter(RoboChaves.transaction_id == transaction_id, RoboChaves.key_nf == key_nf).first()
                return result
            except Exception as ex:
                session.rollback()
                raise ex            

    def delete_by_transaction_id(self, transaction_id: str) -> None:
        with self.SessionSync() as session:
            try:
                # Step 1: Lock the rows
                stmt = select(RoboChaves).where(RoboChaves.transaction_id == transaction_id).with_for_update()
                locked_rows = session.execute(stmt).scalars().all()  # Fetch locked rows

                # Step 2: Delete the locked rows
                for user in locked_rows:
                    session.delete(user)  # Mark for deletion
                
                session.commit()
                
                #stmt = delete(RoboChaves).where(RoboChaves.transaction_id == transaction_id)
                #result = session.execute(stmt)
                #session.commit()
                #if result.rowcount == 0:
                #    raise Exception(f"Entidade {RoboChaves.__name__} Transaction ID: {str(transaction_id)} não foi deletado")
            except Exception as ex:
                session.rollback()
                raise ex

    def delete_by_pk(self, transaction_id: str, key_nf: str) -> None:
        with self.SessionSync() as session:
            try:
                # Step 1: Lock the rows
                stmt = select(RoboChaves).where(RoboChaves.transaction_id == transaction_id and RoboChaves.key_nf == key_nf).with_for_update()
                locked_rows = session.execute(stmt).scalars().all()  # Fetch locked rows

                # Step 2: Delete the locked rows
                for user in locked_rows:
                    session.delete(user)  # Mark for deletion
                
                session.commit()
                
                #stmt = delete(RoboChaves).where(RoboChaves.transaction_id == transaction_id and RoboChaves.key_nf == key_nf)
                #result = session.execute(stmt)
                #session.commit()
                #if result.rowcount == 0:
                #    raise Exception(f"Entidade {RoboChaves.__name__} Transaction ID: {str(transaction_id)} não foi deletado")
            except Exception as ex:
                session.rollback()
                raise ex
        