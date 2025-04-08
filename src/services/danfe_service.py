import pdfplumber
import traceback
import pandas as pd
import re
from pathlib import Path
from configs import *

class DanfeService:
    def __init__(self):
        pass

    def processar_danfes(self, folder_zip_extracted, full_path_zip_filename):
        try:
            erros = []
            dados = []
            pdf_folder = Path(folder_zip_extracted).rglob('*.pdf')
            files = [x for x in pdf_folder]
            i = 0
            total = len(files) 
            while i < total:
                if DEBUG:
                    print(f'{i} de {total}')
                    
                f = files[i]
                #for f in files:
                if "__MACOSX" not in str(f):
                    try:
                        _chave, _cpf_cnpj, _message, _total_pages, _ie_destinatario, _ie_emissor, _cpf_cnpj_emissor, \
                        tipo, numeronf, destinatario_data_emissao = self.process_danfe_file_string(f)

                        _ano_mes_emissao = ""

                        if _chave is not None:
                            _chave = _chave.replace(" ", "").strip()
                            if len(_chave) == 44:
                                _cpf_cnpj_emissor = _chave[6:20]
                                _ano_mes_emissao = _chave[2:6]
                                numeronf = _chave[25:34]
                                
                            dados.append([_cpf_cnpj, _chave, _ie_destinatario, _ie_emissor, _cpf_cnpj_emissor,
                                        _ano_mes_emissao, tipo, numeronf, destinatario_data_emissao, str(f), _message, _total_pages])
                        else:
                            erros.append(f'Chave nula para o arquivo {str(f)}: {_message}')

                    except Exception as ex:
                        #traceback.print_exc()
                        erros.append(f'Erro no arquivo {str(f)}: {str(ex)}')
                i += 1
            
            if not dados:
                erros.append(f"Sem xmls válidos para processar. Arquivo: {str(full_path_zip_filename)}")
                return {
                    "status": False, 
                    "erros": erros, 
                    "total_files": len(files) if files else 0,
                    "df": None
                }            
                        
            df = pd.DataFrame(dados)
            df.columns = ['CPF/CNPJ', 'CHAVE', "IE_DESTINATARIO", "IE_EMISSOR", "CPF/CNPJ EMISSOR", 
                        "ANO/MES EMISSAO", "TIPO PROCESSAMENTO", "NR. NOTA FISCAL", "DATA EMISSÃO",
                        "ARQUIVO", "STATUS", "Nr de Páginas"]
            
            return {
                "status": True, 
                "erros": erros, 
                "total_files": len(files) if files else 0,
                "df": df
            }            
        except Exception:
            erros.append({f"Erro ao processar xmls. Pasta de origem: {str(folder_zip_extracted)}", f"exception: {str(ex)}"})
            return {
                "status": False, 
                "erros": erros, 
                "total_files": len(files) if files else 0
            }                                    

    def process_danfe_file_string(self, filename):
        _chave = None
        _cpf_cnpj = None
        _message = None
        _total_pages = 0
        _ie_destinatario = None
        _ie_emissor = None
        ie_destinatario = None
        ie_emissor = None
        _cpf_cnpj_emissor = None
        tipo = None

        try:
            with pdfplumber.open(filename) as pdf:
                if DEBUG:
                    print("\n\nprocessando ", filename)
                _total_pages = len(pdf.pages)
                page = pdf.pages[0]  # Pegar a primeira página

                #print(page.width, page.height)

                text = page.extract_text()

                # para debug, imprimir o TXT do PDF
                with open(str(filename)+".txt", 'w', encoding='utf-8') as f:
                    f.write(text)

                # Se não houver texto ou o texto extraído for muito curto
                if not text or len(text.strip()) < 5:
                    return None, None, "o arquivo é uma imagem", None, None, None, None, None, None, None

                # Tente extrair Nº. 000.001.823
                numero_pattern = re.compile(r'Nº\.\s*([\d\.]+)')
                numero_match = numero_pattern.search(text)
                numero = numero_match.group(1) if numero_match else None

                # Tente extrair inscrições
                inscricoes_patterns = [
                    re.compile(
                        r'INSCRIÇÃO ESTADUAL INSCRIÇÃO MUNICIPAL INSCRIÇÃO ESTADUAL DO SUBST\. TRIBUT\. CNPJ\n+\s(0*\d+)'),
                    re.compile(
                        r'INSCRIÇÃO ESTADUAL INSCRIÇÃO MUNICIPAL INSCRIÇÃO ESTADUAL DO SUBST\. TRIBUT\. CNPJ\n+\s(0*\d+)'),
                    re.compile(
                        r'INSCRIÇÃO ESTADUAL INSCRIÇÃO ESTADUAL DO SUBSTITUTO TRIBUTÁRIO CNPJ\n+\s(0*\d+)'),
                    re.compile(
                        r'INSCRICAO ESTADUAL INSCRICAO ESTADUAL DO SUBST. TRIBUT. CNPJ\n+\s(0*\d+)'),
                    re.compile(
                        r'INSCRIÇÃO ESTADUAL INSCRIÇÃO ESTADUAL DO SUBST. TRIBUT. CNPJ\n+\s(0*\d+)'),
                    re.compile(
                        r'INSCRIÇÃO ESTADUAL INSCRIÇÃO ESTADUAL DO SUBSTITUTO TRIBUTÁRIO CNPJ / CPF\n+\s(0*\d+)'),
                    re.compile(
                        r"INSCRIÇÃO ESTADUAL[^\d]+(\d{6,})")
                ]
                ie_emissor = None
                for pattern in inscricoes_patterns:
                    inscricoes_match = pattern.search(text)
                    if inscricoes_match:
                        ie_emissor = inscricoes_match.group(1).split()
                        break

                # Tente extrair informações do destinatário
                destinatario_patterns = [
                    re.compile(
                        r'NOME / RAZÃO SOCIAL CNPJ / CPF DATA DA EMISSÃO\n(.+?)\s(\d{3}\.\d{3}\.\d{3}-\d{2})\s(\d{2}/\d{2}/\d{4})'),
                    re.compile(
                        r'NOME / RAZAO SOCIAL CNPJ / CPF DATA DA EMISSAO\n(.+?)\s(\d{3}\.\d{3}\.\d{3}-\d{2})\s(\d{2}/\d{2}/\d{4})'),
                    re.compile(
                        r'NOME / RAZAO SOCIAL CNPJ / CPF DATA DA EMISSAO\n(.+?)\s(\d{3}\.\d{3}\.\d{3}-\d{2})\s(\d{2}/\d{2}/\d{4})'),
                    re.compile(
                        r'NOME / RAZÃO SOCIAL CNPJ / CPF DATA DA EMISSÃO\n(.+?)\s(\d{3}\.\d{3}\.\d{3}-\d{2})\s(\d{2}/\d{2}/\d{4})')
                ]
                destinatario_nome = destinatario_cpf = destinatario_data_emissao = None
                for pattern in destinatario_patterns:
                    destinatario_match = pattern.search(text)
                    if destinatario_match:
                        destinatario_nome, destinatario_cpf, destinatario_data_emissao = destinatario_match.groups()
                        break

                # Extrair chave de acesso
                                        
                # ex.: 2522 0141 0807 2200 0504 5500 1000 1671 7010 4643 5774
                chave_acesso_pattern = re.compile(r'\d{4} \d{4} \d{4} \d{4} \d{4} \d{4} \d{4} \d{4} \d{4} \d{4} \d{4}')
                chave_acesso_match = chave_acesso_pattern.search(text)
                chave_acesso = chave_acesso_match.group() if chave_acesso_match else None            
                if not chave_acesso:
                    # ex.: 25220141080722000504550010001671701046435774
                    chave_acesso_pattern = re.compile(r'\d{44}')
                    chave_acesso_match = chave_acesso_pattern.search(text)
                    chave_acesso = chave_acesso_match.group() if chave_acesso_match else None
                if not chave_acesso:
                    # ex.: 25-2201-41.080.722/0005-04-55-001-000.167.170-104.643.577-4
                    chave_acesso_pattern = re.compile(r'(\d{2})[-](\d{4})[-](\d{2})[.](\d{3})[.](\d{3})[/](\d{4})[-](\d{2})[-](\d{2})[-](\d{3})[-](\d{3})[.](\d{3})[.](\d{3})[-](\d{3})[.](\d{3})[.](\d{3})[-](\d{1})')
                    chave_acesso_match = chave_acesso_pattern.search(text)
                    chave_acesso = chave_acesso_match.group() if chave_acesso_match else None
                
                # Remove all non-numeric characters
                if chave_acesso:
                    chave_acesso = re.sub(r'\D', '', str(chave_acesso))

                ies_emissor_patterns = [
                    re.compile(r"MUNICÍPIO UF .+ INSCRIÇÃO ESTADUAL HORA DA SAÍDA.+\n.+\s(0*\d+)"),
                    re.compile(r"MUNICÍPIO UF .+ INSCRIÇÃO ESTADUAL HORA DA SAÍDA\n.+\s(0*\d+)"),
                    re.compile(r"MUNICIPIO .+ UF INSCRICAO ESTADUAL HORA DA SAIDA\n.+\s(0*\d+)"),
                    re.compile(r"MUNICÍPIO UF TELEFONE / FAX INSCRIÇÃO ESTADUAL HORA DA SAÍDA\n.+\s(0*\d+)"),
                    # r"ENDERECO MUNICIPIO UF INSCRICAO ESTADUAL\n.+\s(0*\d+)"
                    re.compile(r"INSCRIÇÃO ESTADUAL\s+(\d{9,})\s+\d{2}:\d{2}:\d{2}")
                ]

                inscricoes = None
                for pattern_str in ies_emissor_patterns:
                    padrao_chave_match = pattern.search(text)
                    if padrao_chave_match:
                        inscricoes = padrao_chave_match.group(1).split()
                        break

                if DEBUG:
                    print("Número:", numero)
                    print("Inscrições:", inscricoes)
                    print("Destinatário Nome:", destinatario_nome)
                    print("Destinatário CPF:", destinatario_cpf)
                    print("Destinatário Data de Emissão:", destinatario_data_emissao)
                    print("IE Emissor", ie_emissor)
                    print("Chave de acesso", chave_acesso)

                tipo = "regex"

                return chave_acesso, destinatario_cpf, _message, _total_pages, inscricoes, ie_emissor, _cpf_cnpj_emissor, tipo, numero, destinatario_data_emissao

        except:
            traceback.format_exc()
            return None, None, "erro ao processar arquivo", None, None, None, None, None, None, None