from typing import List
from pandas import DataFrame
from domain.robo_chaves import RoboChaves
from repositories.robo_chaves_repository import RoboChavesRepository

class RoboChavesService:
    def __init__(self):
        self.robo_chaves_repository = RoboChavesRepository()

    def create(self, robo: RoboChaves) -> None:
        self.robo_chaves_repository.create(robo)

    def get_by_transaction_id(self, transaction_id: str) -> List[RoboChaves]:
        return self.robo_chaves_repository.get_by_transaction_id(transaction_id)

    def get_by_pk(self, transaction_id: str, key_nf: str) -> RoboChaves:
        return self.robo_chaves_repository.get_by_pk(transaction_id, key_nf)
             
    def delete_by_transaction_id(self, transaction_id: str) -> None:
        self.robo_chaves_repository.delete_by_transaction_id(transaction_id)
    
    def delete_by_pk(self, transaction_id: str, key_nf: str) -> None:
        self.robo_chaves_repository.delete_by_pk(transaction_id, key_nf)
        
    def insert_chunk(self, table: str, chunk: DataFrame) -> str:
        return self.robo_chaves_repository.insert_chunk(table, chunk)
    
    def sync_key_nf(self, transaction_id: str, df: DataFrame, column_key_nf: str = 'key_nf') -> DataFrame:
        """
        Sincroniza as chaves do DataFrame com o banco de dados.

        Recebe um DataFrame com as chaves que devem ser sincronizadas e o id da transacao.
        Verifica quais chaves do DataFrame ja existem no banco de dados e as elimina do DataFrame.
        As chaves restantes sao inseridas no banco de dados.

        Parameters
        ----------
        transaction_id : str
            Id da transacao.
        df : DataFrame
            DataFrame com as chaves a serem sincronizadas.
        column_key_nf : str, optional
            Nome da coluna do DataFrame que contem as chaves. Default: 'key_nf'.

        Returns
        -------
        DataFrame
            DataFrame com as chaves que foram sincronizadas.
        str
            Mensagem de erro caso ocorra algum erro durante a sincronizacao.
        """
        error: str = ''
        try:
            # garantir que a coluna key_nf seja chamada key_nf
            df = df.rename(columns={column_key_nf: 'key_nf'})
            
            # pega as chaves do bd robo_chaves e nf que existem no df (diminuir result do banco de dados)
            
            keys_nf_str = ", ".join(f"'{key}'" for key in df['key_nf'].to_list())
            
            # robo_chaves
            chaves = self.robo_chaves_repository.query_by_where('robo_chaves', f"key_nf in ({keys_nf_str})", fields='key_nf')
            #nf
            nfs = self.robo_chaves_repository.query_by_where('nf', f"key_nf in ({keys_nf_str})", fields='key_nf')
                        
            # Extrai os valores únicos da coluna 'key_nf' de cada conjunto de dados
            chaves_set = set(row[0] for row in chaves) if len(chaves) > 0 else set()
            nfs_set = set(row[0] for row in nfs) if len(nfs) > 0 else set()

            # Garante que não há valores duplicados
            result = chaves_set.union(nfs_set)  

            # Elimina as chaves existentes no BD do df
            df = df[~df['key_nf'].isin(result)]
            
            # Remover possíveis duplicatas dentro do próprio df
            df = df.drop_duplicates(subset=['key_nf'], keep='first')
            
            if not df.empty:
                # adicionar coluna transaction id na primeira posição
                df.insert(0, 'transaction_id', transaction_id)            
                # transformar df na estrutura que o banco de dados espera
                df = df[['transaction_id', 'key_nf']]
                # inserir no banco de dados as chaves do df que nao existem no bd
                self.insert_chunk('robo_chaves', df)
            return df, error
        except Exception as ex:
            error = f"Erro ao sincronizar chaves com o banco de dados. Erro: {str(ex)}"
        return df, error
