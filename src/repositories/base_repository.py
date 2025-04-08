from pandas import DataFrame
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, delete, text
from typing import List, Type, TypeVar
from configs import SQLALCHEMY_DATABASE_URI
from interfaces.repository_interface import IRepository

T = TypeVar('T')

class BaseRepository(IRepository):
    def __init__(self, entity_type: Type[T]):
        self.entity_type = entity_type
        self.engine = create_engine(SQLALCHEMY_DATABASE_URI)
        self.SessionSync = sessionmaker(bind=self.engine)
           
    def create(self, entity: T) -> T:
        with self.SessionSync() as session:
            try:
                session.add(entity)
                session.commit()
                session.refresh(entity)  # Ensures the entity is fully updated from the database
                return entity  # Return the entity with updated fields                
            except Exception as ex:
                session.rollback()
                raise ex

    def delete(self, id: int) -> None:
        with self.SessionSync() as session:
            try:
                stmt = delete(self.entity_type).where(self.entity_type.id == id)
                result = session.execute(stmt)
                session.commit()
                if result.rowcount == 0:
                    raise Exception(f"Entidade {self.entity_type.__name__} ID: {str(id)} não foi deletado")
            except Exception as ex:
                session.rollback()
                raise ex

    def query_by_where(self, table: str, where: str, order: str = '', fields: str = '', join: str = '') -> 'list[T]':
        with self.SessionSync() as session:
            try:
                sql = f"""
                    select
                    {f'{fields}' if fields else '*'} 
                    from cbx.{table} 
                    {f'{join}' if join else ''} 
                    {f'where {where}' if where else ''} 
                    {f'order by {order}' if order else ''}
                """
                result = session.execute(text(sql)).all()
                return result
            except Exception as ex:
                session.rollback()
                raise ex

    def insert_chunk(self, table: str, chunk: DataFrame ) -> str:        
        try:
            chunk.to_sql(table, self.engine, schema='cbx', if_exists='append', index=False)
        except Exception as ex:
            raise ex