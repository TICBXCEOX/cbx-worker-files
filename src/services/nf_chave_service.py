import pandas as pd
from pathlib import Path
from configs import DEBUG
from services.file_service import FileService

class NotaFiscalChaveService:
    def __init__(self):
        pass
            
    def processar_chaves(self, pasta, filename):
        erros = []
        try:
            dados = []
                        
            files = list(Path(pasta).rglob('*.txt')) + list(Path(pasta).rglob('*.csv'))
           
            fileService = FileService()
            #for f in files:
            i = 0
            total = len(files)             
            while i < total:
                if DEBUG:
                    print(f'{i} de {total}')
                    
                f = files[i]            
            
                if "__MACOSX" not in str(f):
                    try:
                        sucesso, content = fileService.open_file(f, False)
                        if sucesso:
                            lines = content.splitlines()
                            dados.extend(lines)
                            if DEBUG:
                                print(f'arquivo {str(f)} - {len(lines)} chaves.')
                    except Exception as ex:
                        erros.append(f"erro arquivo chave {str(f)}: {str(ex)}")
                i += 1

            df = pd.DataFrame(dados, columns=["CHAVE"])
            
            if df.empty:
                erros.append(f"Sem chaves válidas para processar - Arquivo: {str(filename)}")
                return {
                    "status": False, 
                    "erros": erros, 
                    "total_files": len(files) if files else 0
                }
                                                                                                                               
            return {
                "status": True, 
                "erros": erros, 
                "total_files": len(files) if files else 0,
                "df": df
            }        
        except Exception as ex:
            erros.append(f"Erro ao processar SEFAZ. Arquivo: {str(filename)} - Erro: {str(ex)}")
            return {
                "status": False, 
                "erros": erros, 
                "total_files": len(files) if files else 0
            }