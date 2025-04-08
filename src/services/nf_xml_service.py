from datetime import datetime
from typing import List
import re
import xmltodict
import json
import pandas as pd
import numpy as np
import pytz

from collections import Counter
from pathlib import Path
from domain.ncms import get_ncms
from configs import *
from sqlalchemy import text

from services.utils import get_db_connection, get_engine, is_number

class NotaFiscalXmlService:
    def __init__(self):
        self.ALLOWED_EXTENSIONS = {'zip', 'pdf', 'json'}       

    def parser_nf_insumos(self, dados, xml_path):
        try:
            with open(xml_path, encoding='utf-8-sig') as arquivo:
                file_content = arquivo.read()

                file_content = self.remove_xml_header(file_content)
                doc = xmltodict.parse(file_content)
                    
                NATOP = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "ide", "natOp"])
                tpNF = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "ide", "tpNF"])
                CHAVE = self.safe_get(doc, ["nfeProc", "protNFe", "infProt", "chNFe"])
                nNF = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "ide", "nNF"])
                DATA_EMISSAO = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "ide", "dhEmi"])

                CNPJ = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "emit", "CNPJ"])
                IE = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "emit", "IE"])
                CPF = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "emit", "CPF"])
                NOME = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "emit", "xNome"])
                FANTASIA = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "emit", "xFant"])

                DEST_CPF_CNPJ = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "dest", "CNPJ"]) or \
                                self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "dest", "CPF"])
                DEST_NOME = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "dest", "xNome"])
                DEST_IE = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "dest", "IE"])

                infAdFisco = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "infAdic", "infAdFisco"])
                info_adicionais = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "infAdic", "infCpl"])

                PLACA = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "transp", "veicTransp", "placa"])
                UF = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "transp", "veicTransp", "UF"])

                itens = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "det"])

                verifica_lista = isinstance(itens, list)

                CFOPs = []
                PRODUTOS = []
                TOTAL = 0
                cProd = ""                
                cEAN = ""
                NCM = ""
                CFOP = ""
                xProd = ""
                uCom = ""
                qCom = ""
                vUnCom = ""
                vProd = ""
                uTrib = ""
                qTrib = ""
                vUnTrib = ""

                if verifica_lista:
                    for itens in itens:
                        for key, value in itens.items():
                            if key == "prod":
                                for prod_key, prod_value in value.items():
                                    if prod_key == "uCom":
                                        uCom = prod_value
                                    if prod_key == "qCom":
                                        qCom = prod_value
                                        TOTAL += float(qCom)
                                    if prod_key == "xProd":
                                        xProd = prod_value
                                    if prod_key == "CFOP":
                                        CFOP = prod_value
                                        CFOPs.append(CFOP)
                                    if prod_key == "cProd":
                                        cProd = prod_value
                                    if prod_key == "cEAN":
                                        cEAN = prod_value
                                    if prod_key == "NCM":
                                        NCM = prod_value
                                    if prod_key == "vUnCom":
                                        vUnCom = prod_value
                                    if prod_key == "vProd":
                                        vProd = prod_value
                                    if prod_key == "uTrib":
                                        uTrib = prod_value
                                    if prod_key == "qTrib":
                                        qTrib = prod_value
                                    if prod_key == "vUnTrib":
                                        vUnTrib = prod_value
                                PRODUTOS.append(
                                    {"cProd": cProd, 
                                     "cEAN": cEAN, 
                                     "NCM": NCM,
                                     "CFOP": CFOP,
                                     "xProd": xProd,
                                     "uCom": uCom, 
                                     "qCom": self.safe_number(qCom), # float(qCom), 
                                     "vUnCom": self.safe_number(vUnCom), # float(vUnCom), 
                                     "vProd": self.safe_number(vProd),  #float(vProd),
                                     "uTrib": uTrib, 
                                     "qTrib": self.safe_number(qTrib), #float(qTrib), 
                                     "vUnTrib": self.safe_number(vUnTrib), #float(vUnTrib)
                                     })
                else:
                    for key, value in itens.items():
                        if key == "prod":
                            for prod_key, prod_value in value.items():
                                if prod_key == "uCom":
                                    uCom = prod_value
                                if prod_key == "qCom":
                                    qCom = prod_value
                                    TOTAL += float(qCom)
                                if prod_key == "xProd":
                                    xProd = prod_value
                                if prod_key == "CFOP":
                                    CFOP = prod_value
                                    CFOPs.append(CFOP)
                                if prod_key == "cProd":
                                    cProd = prod_value
                                if prod_key == "cEAN":
                                    cEAN = prod_value
                                if prod_key == "NCM":
                                    NCM = prod_value
                                if prod_key == "vUnCom":
                                    vUnCom = prod_value
                                if prod_key == "vProd":
                                    vProd = prod_value
                                if prod_key == "uTrib":
                                    uTrib = prod_value
                                if prod_key == "qTrib":
                                    qTrib = prod_value
                                if prod_key == "vUnTrib":
                                    vUnTrib = prod_value
                            PRODUTOS.append(
                                {
                                    "cProd": cProd, 
                                    "cEAN": cEAN, 
                                    "NCM": NCM,
                                    "CFOP": CFOP,
                                    "xProd": xProd,
                                    "uCom": uCom, 
                                    "qCom": self.safe_number(qCom), # float(qCom), 
                                    "vUnCom": self.safe_number(vUnCom), # float(vUnCom), 
                                    "vProd": self.safe_number(vProd),  #float(vProd),
                                    "uTrib": uTrib, 
                                    "qTrib": self.safe_number(qTrib), #float(qTrib), 
                                    "vUnTrib": self.safe_number(vUnTrib), #float(vUnTrib)
                                })
                            
                for produto in PRODUTOS:
                    dados.append({
                        "arquivo": xml_path,
                        "NF": nNF,
                        "INSUMO": " ",
                        "TIPO NF": tpNF,
                        "CHAVE": CHAVE,
                        "DATA EMISSAO": datetime.fromisoformat(DATA_EMISSAO).astimezone(pytz.utc) if DATA_EMISSAO else '',
                        "NATUREZA OPERACAO": NATOP,
                        "CFOPs": Counter(CFOPs),
                        "FORNECEDOR": NOME,
                        "NOME FANTASIA": FANTASIA,
                        "CPF": CPF,
                        "CNPJ": CNPJ,
                        "IE": IE,
                        "COMPRADOR_CPF_CNPJ": DEST_CPF_CNPJ,
                        "COMPRADOR": DEST_NOME,
                        "COMPRADOR_IE": DEST_IE,
                        "VEICULO": PLACA,
                        "VEICULO UF": UF,
                        "INFORMACOES ADICIONAIS": info_adicionais,
                        "INFORMACOES FISCO": infAdFisco,
                        "cProd": produto["cProd"],
                        "cEAN": produto["cEAN"],
                        "NCM": produto["NCM"],
                        "CFOP": produto["CFOP"],
                        "xProd": produto["xProd"],
                        "uCom": produto["uCom"],
                        "qCom": produto["qCom"],
                        "vUnCom": produto["vUnCom"],
                        "vProd": produto["vProd"],
                        "uTrib": produto["uTrib"],
                        "qTrib": produto["qTrib"],
                        "vUnTrib": produto["vUnTrib"]
                    })
                return dados

        except Exception as ex:
            raise ex
    
    def processar_nfs_insumos(self, pasta, filename):
        erros = []
        try:
            dados = []
            xml_folder = Path(pasta).rglob('*.xml')            
            files = [x for x in xml_folder]
            
            for f in files:
                if "__MACOSX" not in str(f):
                    try:
                        dados_nfs = self.parser_nf_insumos(dados, f)
                        if dados_nfs is not None:
                            dados = dados_nfs
                    except Exception as ex:
                        erros.append(f"erro arquivo {str(f)} - exception: {str(ex)}")

            df = pd.DataFrame(dados)
            
            if df.empty:
                erros.append(f"Sem xmls válidos para processar - Arquivo: {str(filename)}")
                return {
                    "status": False, 
                    "erros": erros, 
                    "total_files": len(files) if files else 0
                }
            
            # consultar NCMs
            ncms, _ = get_ncms()
            columns_ncm = ['id', 'name', 'status', 'properties', 'ncm', 'tipo', 'grupo']
            df_ncms = pd.DataFrame(ncms, columns=columns_ncm)

            df_ncms = df_ncms.drop(['id', 'status', 'properties'], axis=1)
            df_ncms['ncm'] = df_ncms['ncm'].astype(int)

            df['NCM'] = df['NCM'].astype(int)
            df = df.join(df_ncms.set_index('ncm'), on='NCM')

            # TODO tratar tipo das colunas
            df['NF'] = df['NF'].astype(int)
            df['DATA EMISSAO'] = pd.to_datetime(df['DATA EMISSAO'])
            df['DATA EMISSAO'] = pd.to_datetime(
                df['DATA EMISSAO']).dt.strftime('%Y-%m-%d %H:%M:%S')
            df = df.sort_values(by='DATA EMISSAO', ascending=True)
            df['DATA EMISSAO'] = pd.to_datetime(
                df['DATA EMISSAO'])
            df['DATA EMISSAO'] = pd.to_datetime(
                df['DATA EMISSAO']).dt.strftime('%d/%m/%Y')

            df["arquivo"] = df["arquivo"].astype(str)
            condicao = df["arquivo"].str.contains("cancelada", case=False)
            df["INSUMO"] = np.where(condicao, "NF cancelada", df["INSUMO"])
            
            # reordenar colunas
            nova_ordem = [
                'NF',
                'CHAVE',
                'DATA EMISSAO',
                'FORNECEDOR',
                'COMPRADOR_IE',
                'CFOP',
                'name',
                'uCom',            
                'qCom',
                'IE',
                'NCM',
                'INSUMO',
                'NATUREZA OPERACAO',
                'NOME FANTASIA',
                'COMPRADOR',                      
                'VEICULO',
                'VEICULO UF',
                'INFORMACOES ADICIONAIS',
                'INFORMACOES FISCO',                     
                'xProd',
                'tipo',
                'grupo',
                'TIPO NF',
                'CFOPs',
                'CPF',
                'CNPJ',
                'COMPRADOR_CPF_CNPJ',
                'vUnCom',
                'vProd',
                'uTrib',
                'qTrib',
                'vUnTrib',
                'cProd',
                'cEAN',
                'arquivo']
            
            df = df.reindex(columns=nova_ordem)
            df = df.sort_values(by='grupo', ascending=True)
                                                                                                                           
            return {
                "status": True, 
                "erros": erros, 
                "total_files": len(files) if files else 0,
                "df": df
            }        
        except Exception as ex:
            erros.append(f"Erro ao processar XMLs. Arquivo: {str(filename)} - Erro: {str(ex)}")
            return {
                "status": False, 
                "erros": erros, 
                "total_files": len(files) if files else 0
            }
                                           
    def processar_nfs_cbios(self, pasta, filename):
        try:
            erros = []
            dados = []
            xml_folder = Path(pasta).rglob('*.xml')
            files = [x for x in xml_folder]

            for f in files:
                if "__MACOSX" not in str(f):
                    try:
                        dados_nfs = self.parser_nf_milho(dados, f)
                        if dados_nfs is not None:
                            dados = dados_nfs
                    except Exception as ex:
                        erros.append({"erro arquivo": str(f), "exception": str(ex)})

            df = pd.DataFrame(dados)

            if df.empty:
                erros.append(f"Sem xmls válidos para processar. Arquivo: {str(filename)}")
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
            erros.append(f"Erro ao processar XMLs. Arquivo: {str(filename)} - Erro: {str(ex)}")
            return {
                "status": False, 
                "erros": erros, 
                "total_files": len(files) if files else 0
            }                                    
            
    def parser_nf_milho(self, dados, xml_path):
        try:
            with open(xml_path, encoding='utf-8-sig') as arquivo:

                file_content = arquivo.read()

                CONTRATO = ""
                PLACA = ""
                UF = ""
                PRODUTOS = []
                CFOPs = []
                TOTAL = 0
                CFOP = ""
                FORNECEDOR_NOME = ""
                RETIRADA_DE = ""
                FORNECEDOR_IE = ""
                FORNECEDOR_NF = ""
                EMITIDA_EM = ""

                file_content = self.remove_xml_header(file_content)
                doc = xmltodict.parse(file_content)
                
                NATOP = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "ide", "natOp"])
                tpNF = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "ide", "tpNF"])
                CHAVE = self.safe_get(doc, ["nfeProc", "protNFe", "infProt", "chNFe"])
                nNF = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "ide", "nNF"])
                DATA_EMISSAO = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "ide", "dhEmi"])

                CNPJ = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "emit", "CNPJ"])
                IE = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "emit", "IE"])
                CPF = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "emit", "CPF"])
                NOME = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "emit", "xNome"])
                FANTASIA = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "emit", "xFant"])

                DEST_CNPJ = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "dest", "CNPJ"])                            
                DEST_CPF = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "dest", "CPF"])
                DEST_NOME = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "dest", "xNome"])

                infAdFisco = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "infAdic", "infAdFisco"])
                info_adicionais = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "infAdic", "infCpl"])
                
                if "MERCADORIA ADQUIRIDA E RETIRADA DE" in info_adicionais:
                    FORNECEDOR_NOME = info_adicionais[
                        info_adicionais.index("MERCADORIA ADQUIRIDA E RETIRADA DE"):info_adicionais.index(
                            "CONFORME NF")]
                    FORNECEDOR_NOME = FORNECEDOR_NOME[len(
                        "MERCADORIA ADQUIRIDA E RETIRADA DE "):FORNECEDOR_NOME.index(",")]

                    if "RETIRADO" in info_adicionais:
                        RETIRADA_DE = info_adicionais[info_adicionais.index(
                            "MERCADORIA ADQUIRIDA E RETIRADA DE"):]
                        FORNECEDOR_IE = RETIRADA_DE[RETIRADA_DE.index(
                            " IE:") + 5:RETIRADA_DE.index(" IE:") + 5 + 9]
                        FORNECEDOR_NF = RETIRADA_DE[
                            RETIRADA_DE.index(" CONFORME NF") + len(" CONFORME NF"):RETIRADA_DE.index("SERIE")]
                        EMITIDA_EM = RETIRADA_DE[RETIRADA_DE.index(" EMITIDA EM") + len(" EMITIDA EM"):RETIRADA_DE.index(
                            " EMITIDA EM") + len(" EMITIDA EM") + 11]
                        PLACA = RETIRADA_DE[
                            RETIRADA_DE.index(" PLACA") + len(" PLACA"):RETIRADA_DE.index(" PLACA") + len(" PLACA") + 8]
                        RETIRADA_DE = RETIRADA_DE[
                            RETIRADA_DE.index("RETIRADO ") + len("RETIRADO "):RETIRADA_DE.rfind(" CNPJ")]
                    else:
                        RETIRADA_DE = info_adicionais[info_adicionais.index(
                            "MERCADORIA ADQUIRIDA E RETIRADA DE"):]
                        FORNECEDOR_IE = RETIRADA_DE[RETIRADA_DE.index(
                            " IE:") + 5:RETIRADA_DE.index(" IE:") + 5 + 9]
                        FORNECEDOR_NF = RETIRADA_DE[
                            RETIRADA_DE.index(" CONFORME NF") + len(" CONFORME NF"):RETIRADA_DE.index("SERIE")]
                        EMITIDA_EM = RETIRADA_DE[RETIRADA_DE.index(" EMITIDA EM") + len(" EMITIDA EM"):RETIRADA_DE.index(
                            " EMITIDA EM") + len(" EMITIDA EM") + 11]
                        PLACA = RETIRADA_DE[
                            RETIRADA_DE.index(" PLACA") + len(" PLACA"):RETIRADA_DE.index(" PLACA") + len(" PLACA") + 8]

                elif "MERCADORIA ADQUIRIDA DE" in info_adicionais:
                    FORNECEDOR_NOME = info_adicionais[
                        info_adicionais.index("MERCADORIA ADQUIRIDA DE"):info_adicionais.index("CONFORME NF")]
                    FORNECEDOR_NOME = FORNECEDOR_NOME[len(
                        "MERCADORIA ADQUIRIDA DE "):FORNECEDOR_NOME.index(",")]

                    if "RETIRADO" in info_adicionais:
                        RETIRADA_DE = info_adicionais[info_adicionais.index(
                            "MERCADORIA ADQUIRIDA DE"):]
                        FORNECEDOR_IE = RETIRADA_DE[RETIRADA_DE.index(
                            " IE:") + 5:RETIRADA_DE.index(" IE:") + 5 + 9]
                        FORNECEDOR_NF = RETIRADA_DE[
                            RETIRADA_DE.index(" CONFORME NF") + len(" CONFORME NF"):RETIRADA_DE.index("SERIE")]
                        EMITIDA_EM = RETIRADA_DE[RETIRADA_DE.index(" EMITIDA EM") + len(" EMITIDA EM"):RETIRADA_DE.index(
                            " EMITIDA EM") + len(" EMITIDA EM") + 11]
                        PLACA = RETIRADA_DE[
                            RETIRADA_DE.index(" PLACA") + len(" PLACA"):RETIRADA_DE.index(" PLACA") + len(" PLACA") + 8]
                        RETIRADA_DE = RETIRADA_DE[
                            RETIRADA_DE.index("RETIRADO ") + len("RETIRADO "):RETIRADA_DE.rfind(" CNPJ")]
                    else:
                        RETIRADA_DE = info_adicionais[info_adicionais.index(
                            "MERCADORIA ADQUIRIDA DE"):]
                        FORNECEDOR_IE = RETIRADA_DE[RETIRADA_DE.index(
                            " IE:") + 5:RETIRADA_DE.index(" IE:") + 5 + 9]
                        FORNECEDOR_NF = RETIRADA_DE[
                            RETIRADA_DE.index(" CONFORME NF") + len(" CONFORME NF"):RETIRADA_DE.index("SERIE")]
                        EMITIDA_EM = RETIRADA_DE[RETIRADA_DE.index(" EMITIDA EM") + len(" EMITIDA EM"):RETIRADA_DE.index(
                            " EMITIDA EM") + len(" EMITIDA EM") + 11]
                        PLACA = RETIRADA_DE[
                            RETIRADA_DE.index(" PLACA") + len(" PLACA"):RETIRADA_DE.index(" PLACA") + len(" PLACA") + 8]

                elif "MERCADORIA ADQUIRIDA/RETIRADA DE" in info_adicionais:
                    FORNECEDOR_NOME = info_adicionais[
                        info_adicionais.index("MERCADORIA ADQUIRIDA/RETIRADA DE"):info_adicionais.index(
                            "CONFORME NF")]
                    FORNECEDOR_NOME = FORNECEDOR_NOME[len(
                        "MERCADORIA ADQUIRIDA/RETIRADA DE "):FORNECEDOR_NOME.index(",")]

                    if "RETIRADO" in info_adicionais:
                        RETIRADA_DE = info_adicionais[info_adicionais.index(
                            "MERCADORIA ADQUIRIDA/RETIRADA DE"):]
                        FORNECEDOR_IE = RETIRADA_DE[RETIRADA_DE.index(
                            " IE:") + 5:RETIRADA_DE.index(" IE:") + 5 + 9]
                        FORNECEDOR_NF = RETIRADA_DE[
                            RETIRADA_DE.index(" CONFORME NF") + len(" CONFORME NF"):RETIRADA_DE.index("SERIE")]
                        EMITIDA_EM = RETIRADA_DE[RETIRADA_DE.index(" EMITIDA EM") + len(" EMITIDA EM"):RETIRADA_DE.index(
                            " EMITIDA EM") + len(" EMITIDA EM") + 11]
                        PLACA = RETIRADA_DE[
                            RETIRADA_DE.index(" PLACA") + len(" PLACA"):RETIRADA_DE.index(" PLACA") + len(" PLACA") + 8]
                        RETIRADA_DE = RETIRADA_DE[
                            RETIRADA_DE.index("RETIRADO ") + len("RETIRADO "):RETIRADA_DE.rfind(" CNPJ")]
                    else:
                        RETIRADA_DE = info_adicionais[info_adicionais.index(
                            "MERCADORIA ADQUIRIDA/RETIRADA DE"):]
                        FORNECEDOR_IE = RETIRADA_DE[RETIRADA_DE.index(
                            " IE:") + 5:RETIRADA_DE.index(" IE:") + 5 + 9]
                        FORNECEDOR_NF = RETIRADA_DE[
                            RETIRADA_DE.index(" CONFORME NF") + len(" CONFORME NF"):RETIRADA_DE.index("SERIE")]
                        EMITIDA_EM = RETIRADA_DE[RETIRADA_DE.index(" EMITIDA EM") + len(" EMITIDA EM"):RETIRADA_DE.index(
                            " EMITIDA EM") + len(" EMITIDA EM") + 11]
                        PLACA = RETIRADA_DE[
                            RETIRADA_DE.index(" PLACA") + len(" PLACA"):RETIRADA_DE.index(" PLACA") + len(" PLACA") + 8]

                else:
                    if "NF" in info_adicionais:
                        posicao_nf = info_adicionais.index("NF")
                        info_nf = info_adicionais[posicao_nf + 3:]
                        FORNECEDOR_NF = info_nf[:info_nf.index(" ")]
                        FORNECEDOR_NOME = info_nf[info_nf.index(" "):]
                    elif "NOTA":
                        posicao_nf = info_adicionais.index("NOTA")
                        info_nf = info_adicionais[posicao_nf + 3:]
                        FORNECEDOR_NF = info_nf[:info_nf.index("Valor aproximado")]
                        FORNECEDOR_NOME = info_nf[info_nf.index(
                            "Valor aproximado"):]

                list_of_words = info_adicionais.split()
                if "CONTRATO" in list_of_words:
                    CONTRATO = list_of_words[list_of_words.index("CONTRATO") + 1]

                veic_transp = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "transp", "veicTransp"])
                if veic_transp:
                    PLACA = self.safe_get(veic_transp, ["placa"], default='')
                    UF = self.safe_get(veic_transp, ["UF"], default='')                        

                itens = self.safe_get(doc, ["nfeProc", "NFe", "infNFe", "det"]) # doc["nfeProc"]["NFe"]["infNFe"]["det"]
                verifica_lista = isinstance(itens, list)

                if verifica_lista:
                    for itens in itens:
                        for key, value in itens.items():
                            if key == "prod":
                                for prod_key, prod_value in value.items():
                                    if prod_key == "uCom":
                                        uCom = prod_value
                                    if prod_key == "qCom":
                                        qCom = prod_value
                                        TOTAL += self.safe_number(qCom) # float(qCom)
                                    if prod_key == "xProd":
                                        xProd = prod_value
                                    if prod_key == "CFOP":
                                        CFOP = prod_value
                                PRODUTOS.append(
                                    {"uCom": uCom, "qCom": qCom, "xProd": xProd, "CFOP": CFOP})
                else:
                    for key, value in itens.items():
                        if key == "prod":
                            for prod_key, prod_value in value.items():
                                if prod_key == "uCom":
                                    uCom = prod_value
                                if prod_key == "qCom":
                                    qCom = prod_value
                                    TOTAL += self.safe_number(qCom) #float(qCom)
                                if prod_key == "xProd":
                                    xProd = prod_value
                                if prod_key == "CFOP":
                                    CFOP = prod_value
                            PRODUTOS.append(
                                {"uCom": uCom, "qCom": qCom, "xProd": xProd, "CFOP": CFOP})

            CFOPs.append(CFOP)
            
            dados.append({        
                "NF": nNF,
                "CHAVE": CHAVE,
                "DATA EMISSAO": datetime.fromisoformat(DATA_EMISSAO).astimezone(pytz.utc) if DATA_EMISSAO else '',
                "FORNECEDOR_NOME": FORNECEDOR_NOME,
                "FORNECEDOR_IE": FORNECEDOR_IE,
                "FORNECEDOR_NF": FORNECEDOR_NF,
                "CFOP": CFOP,
                "IE": IE,
                "INSUMO": " ",
                "NATUREZA OPERACAO": NATOP,
                "CNPJ": CNPJ,
                "CPF": CPF,
                "PRODUTOR": NOME,
                "FANTASIA": FANTASIA,
                "DEST_CNPJ": DEST_CNPJ,
                "DEST_CPF": DEST_CPF,
                "DEST_NOME": DEST_NOME,
                "VEICULO": PLACA,
                "VEICULO UF": UF,
                "INFORMACOES ADICIONAIS": info_adicionais,
                "INFORMACOES FISCO": infAdFisco,
                "PRODUTOS": PRODUTOS,
                "QUANTIDADE_TOTAL": TOTAL,
                "CONTRATO": CONTRATO,
                "RETIRADA_DE": RETIRADA_DE,
                "EMITIDA_EM": EMITIDA_EM,
                "TIPO NF": tpNF,
                "arquivo": xml_path,
            })
            return dados
        except Exception as ex:
            raise ex

    def processar_nfs_milho(self, pasta, filename):
        try:
            erros = []
            total_sucesso = 0
            dados = []
            xml_folder = Path(pasta).rglob('*.xml')
            files = [x for x in xml_folder]

            for f in files:
                if "__MACOSX" not in str(f):
                    try:
                        dados_nfs = self.parser_nf_milho(dados, f)
                        if dados_nfs is not None:
                            dados = dados_nfs
                        total_sucesso += 1
                    except Exception as ex:
                        erros.append({"arquivo": str(f), "exception": str(ex)})

            df = pd.DataFrame(dados)
            
            if df.empty:
                erros.append(f"Sem xmls válidos para processar. Arquivo: {str(filename)}")
                return {
                    "status": False, 
                    "erros": erros, 
                    "total_files": len(files) if files else 0
                }            
            
            df['NF'] = df['NF'].astype(int)
            df['DATA EMISSAO'] = pd.to_datetime(df['DATA EMISSAO'])
            df['DATA EMISSAO'] = pd.to_datetime(
                df['DATA EMISSAO']).dt.strftime('%Y-%m-%d %H:%M:%S')
            df = df.sort_values(by='DATA EMISSAO', ascending=True)
            df['DATA EMISSAO'] = pd.to_datetime(
                df['DATA EMISSAO'])
            df['DATA EMISSAO'] = pd.to_datetime(
                df['DATA EMISSAO']).dt.strftime('%d/%m/%Y')

            df["arquivo"] = df["arquivo"].astype(str)
            condicao = df["arquivo"].str.contains("cancelada", case=False)
            df["INSUMO"] = np.where(condicao, "NF cancelada", df["INSUMO"])
                        
            return {
                "status": True, 
                "erros": erros, 
                "total_files": len(files) if files else 0,
                "df": df
            }
        except Exception as ex:
            erros.append(f"Erro ao processar XMLs. Arquivo: {str(filename)} - Erro: {str(ex)}")
            return {
                "status": False, 
                "erros": erros, 
                "total_files": len(files) if files else 0
            }            

    def remove_xml_header(self, xml_content):        
        # Regular expression pattern to match XML declaration
        # This pattern matches <?xml ... ?> declarations with optional attributes
        pattern = r'^\s*<\?xml\s+[^>]*\?>\s*'
        # Remove XML declaration using regular expression
        cleaned_content = re.sub(pattern, '', xml_content, count=1, flags=re.DOTALL)
        return cleaned_content
    
    def save_xmls(self, pasta, client_id):
        nf = []
        nf_view = []
        erros = []
        xml_folder = Path(pasta).rglob('*.xml')
        files = [x for x in xml_folder]
                
        if not files:
            erros.append('Não há XML para salvar no banco de dados')
            return erros, 0, len(erros), 0, 0
                
        # gera a lista de arrays
        self.process_files(files, nf, nf_view, client_id, erros)
                                    
        df1 = pd.DataFrame(nf)
        df2 = pd.DataFrame(nf_view)
        df1 = df1.drop_duplicates(subset=['key_nf']).reset_index(drop=True)
        
        if df1.empty:
            erros.append('Falha ao extrair dados dos XMLs')
            return erros, 0, len(erros), 0, 0
                
        try:
            with get_db_connection() as con:
                # Obtém os valores únicos de key_nf do df1
                keys_nf = set(df1['key_nf']) # set(pd.concat([df1['key_nf'], df2['key_nf']]))

                # Se a lista não estiver vazia, monta o WHERE corretamente
                if keys_nf:
                    # Formata os valores para a consulta SQL
                    keys_nf_str = ", ".join(f"'{key}'" for key in keys_nf)  # Certifique-se de usar aspas se for texto
                    where = f"WHERE key_nf IN ({keys_nf_str})"

                    # Executa a consulta no banco
                    result = con.execute(f"SELECT key_nf FROM cbx.nf {where}").fetchall()

                    # Extrai os valores únicos retornados do banco
                    chaves_set = {row[0] for row in result}
                else:
                    chaves_set = set()

                # Filtra os DataFrames eliminando as chaves existentes no BD
                df1_final = df1[~df1['key_nf'].isin(chaves_set)]
                df2_final = df2[~df2['key_nf'].isin(chaves_set)]
                
                # # get all keys that are in database from the keys_nf
                #where = f" where {format_in('key_nf', keys_nf, True)}"
                #keys_nf_bd = con.execute(f"select key_nf from cbx.nf {where}").all()

                # df_key_nf = pd.DataFrame(keys_nf, columns=['key_nf'])
                # df_key_nf_bd = pd.DataFrame(keys_nf_bd, columns=['key_nf'])
                
                # # get just the key_nfs that are not in the database
                # df_keys_bulk = df_key_nf[~df_key_nf['key_nf'].isin(df_key_nf_bd['key_nf'])]
                
                # filter df1 and df2 to keep rows where 'key_nf' is in df_keys_bulk['key_nf']
                # df1_final = df1[df1['key_nf'].isin(df_keys_bulk['key_nf'])]
                # df2_final = df2[df2['key_nf'].isin(df_keys_bulk['key_nf'])]
                
                # deleta todas as chaves da nf view que vão ser inseridas 
                if not df2_final.empty:
                    keys_nf_list = df2_final['key_nf'].tolist()
                    con.execute(
                        text("DELETE FROM cbx.nf WHERE key_nf IN :keys_nf"),
                        {"keys_nf": tuple(keys_nf_list)}
                    )
        except Exception as e:
            erros.append(f"Erro ao extrair dados dos XMLs: {str(e)}")
            return erros, 0, len(erros), 0, 0
        
        if df1_final.empty:
            erros.append(f"Não há dados XML válidos para registrar no bando de dados.")
            return erros, 0, len(erros), 0, 0
        
        engine = get_engine()
        
        chunk_size = 1000     
        total_nf_bd = 0   
        total_nf_view_bd = 0            
        for start in range(0, max(len(df1_final), len(df2_final)), chunk_size):
            df1_chunk = df1_final[start:start + chunk_size]
            df2_chunk = df2_final[start:start + chunk_size]
    
            try:
                if not df1_chunk.empty:                    
                    df1_chunk.to_sql('nf', con=engine, schema='cbx', if_exists='append', index=False, method='multi')
                    total_nf_bd += len(df1_chunk)
            except Exception as e:
                erros.append(f"Erro ao inserir NFs por partes (chunk): {str(e)}")

            try:
                if not df2_chunk.empty:
                    df2_chunk.to_sql('nf_view', con=engine, schema='cbx', if_exists='append', index=False, method='multi')
                    total_nf_view_bd += len(df2_chunk)
            except Exception as e:
                erros.append(f"Erro ao inserir NCMs por partes (chunk): {str(e)}")
                            
        return erros, \
            len(files) if files else 0, \
            len(erros), \
            total_nf_bd if total_nf_bd else 0, \
            total_nf_view_bd if total_nf_view_bd else 0
    
    def process_files(self, files: List[Path], nf: list, nf_view: list, client_id, erros: list):
        for file in files:
            try:
                with open(file, 'r', encoding='utf-8-sig') as xml_file:
                    content_xml = xml_file.read()
                    if not content_xml:
                        continue
                    content_xml = self.remove_xml_header(content_xml)
                    content_json = xmltodict.parse(content_xml)
                
                if 'procEventoNFe' in content_json:
                    self.process_event_nfe(content_json, content_xml, client_id, nf, nf_view, file)
                else:
                    self.process_standard_nfe(content_json, content_xml, client_id, nf, nf_view, file)
            except Exception as e:
                erros.append(f"Erro ao extrair dados do XML: {Path(file).name}: {e}")

    def process_event_nfe(self, content_json, content_xml, client_id, nf: list, nf_view: list, file: Path):
        infEvento = self.safe_get(content_json, ['procEventoNFe', 'evento', 'infEvento'], {})
        if infEvento:
            nf.append({
                'client_id': client_id,
                'key_nf': infEvento.get('chNFe', ''),
                'status': 'cancelad' not in file.name.lower() and 'canc' not in file.name.lower(),
                'situacao': f"Evento de Cancelamento para o CPF: {self.safe_get(infEvento, ['CPF'], 'nao informado')}",
                'content_json': json.dumps(content_json),
                'content_xml': content_xml,
            })
            desc_evento = f"{self.safe_get(infEvento, ['detEvento', 'descEvento'], '')} - {self.safe_get(infEvento, ['detEvento', 'xJust'], '')}"
            nf_view.append({
                'key_nf': infEvento.get('chNFe', ''),
                'data_emissao': self.safe_get(infEvento, ['dhEvento'], ''),
                'razao_social_emissor': desc_evento,
                'razao_social_destinatario': desc_evento,
                'client_id': client_id,
            })

    def process_standard_nfe(self, content_json, content_xml, client_id, nf: list, nf_view: list, file: Path):
        nf_json = self.safe_get(content_json, ['nfeProc', 'NFe'], self.safe_get(content_json, ['NFe'], {}))
        infNFe = self.safe_get(nf_json, ['infNFe'], {})
        ide = self.safe_get(infNFe, ['ide'], {})
        
        ie_emissor = self.safe_get(infNFe, ['emit', 'IE'], '')
        ie_dest = self.safe_get(infNFe, ['dest', 'IE'], '')
        cpf_emissor = self.safe_get(infNFe, ['emit', 'CPF'], self.safe_get(infNFe, ['emit', 'CNPJ'], ''))
        cpf_dest = self.safe_get(infNFe, ['dest', 'CPF'], self.safe_get(infNFe, ['dest', 'CNPJ'], ''))
        
        razao_emissor = self.safe_get(infNFe, ['emit', 'xNome'], '')
        razao_dest = self.safe_get(infNFe, ['dest', 'xNome'], '')            
        
        nf.append({
            'client_id': client_id,
            'date': ide.get('dhEmi', ''),
            'key_nf': infNFe.get('@Id', '')[3:],
            'status': 'cancelad' not in file.name.lower(),
            'situacao': self.safe_get(content_json, ['nfeProc', 'protNFe', 'infProt', 'xMotivo'], 'Não Informado'),
            'content_json': json.dumps(content_json),
            'content_xml': content_xml,
            'ie_emissor': ie_emissor,
            'ie_destinatario': ie_dest,
            'cnpj_cpf_emissor': cpf_emissor,
            'cnpj_cpf_destinatario': cpf_dest,
            'razao_social_emissor': razao_emissor,
            'razao_social_destinatario': razao_dest,
            'fantasia_emissor': self.safe_get(infNFe, ['emit', 'xFant'], ''),
            'email_destinatario': self.safe_get(infNFe, ['dest', 'email'], ''),
        })
        
        self.process_products(infNFe, ide, ie_emissor, ie_dest, cpf_emissor, cpf_dest, razao_emissor, razao_dest, nf_view, client_id)

    def process_products(self, infNFe, ide, ie_emissor, ie_dest, cpf_emissor, cpf_dest, razao_emissor, razao_dest, nf_view, client_id):
        prod_list = self.safe_get(infNFe, ['det'], [])
        if not isinstance(prod_list, list):
            prod_list = [prod_list]
        for item in prod_list:
            prod = self.safe_get(item, ['prod'], {})

            local_entrega = self.safe_get(infNFe, ['entrega'], {})
            local_retirada = self.safe_get(infNFe, ['retirada'], {})
            
            nome_entrega = self.safe_get(local_entrega, ['xNome'], '')
            cnpj_entrega = self.safe_get(local_entrega, ['CNPJ'], '')
            cpf_entrega = self.safe_get(local_entrega, ['CPF'], '')
            cpf_entrega = f' CPF: {cpf_entrega}' if cpf_entrega else f' CNPJ: {cnpj_entrega}' if cnpj_entrega else ''
            ie_entrega = self.safe_get(local_entrega, ['IE'], '')            
            ie_entrega = f' IE: {ie_entrega}' if ie_entrega else ''
            
            nome_retirada = self.safe_get(local_retirada, ['xNome'], '')
            cnpj_retirada = self.safe_get(local_retirada, ['CNPJ'], '')
            cpf_retirada = self.safe_get(local_retirada, ['CPF'], '')
            cpf_retirada = f' CPF: {cpf_retirada}' if cpf_retirada else f' CNPJ: {cnpj_retirada}' if cnpj_retirada else ''
            ie_retirada = self.safe_get(local_retirada, ['IE'], '')
            ie_retirada = f' IE: {ie_retirada}' if ie_retirada else ''
            
            full_retirada = f"{nome_retirada}{cpf_retirada}{ie_retirada}"
            full_entrega = f"{nome_entrega}{cpf_entrega}{ie_entrega}"
            full_retirada = full_retirada.strip()
            full_entrega = full_entrega.strip()
            
            nf_view.append({
                'key_nf': infNFe.get('@Id', '')[3:],
                'nro_nota': ide.get('nNF', ''),
                'tipo_nota': ide.get('tpNF', ''),
                'data_emissao': ide.get('dhEmi', ''),
                'ie_emissor': ie_emissor,
                'cnpj_cpf_emissor': cpf_emissor,
                'razao_social_emissor': razao_emissor,
                'ie_destinatario': ie_dest,
                'cnpj_cpf_destinatario': cpf_dest,
                'razao_social_destinatario': razao_dest,
                'cfop': self.safe_get(prod, ['CFOP'], ''),
                'ncm': self.safe_get(prod, ['NCM'], ''),
                'nome_produto': self.safe_get(prod, ['xProd'], ''),
                'quantidade': self.safe_number(self.safe_get(prod, ['qCom'], 0)),
                'unidade_medida': self.safe_get(prod, ['uCom'], ''),
                'client_id': client_id,
                'local_retirada': full_retirada,
                'local_entrega': full_entrega
            })

    def safe_get(self, d, keys, default=''):
        for key in keys:
            if isinstance(d, dict) and key in d:
                d = d[key]
            else:
                return default
        return d
    
    def safe_number(self, value, return_value=0):
        return float(value) if is_number(value) else return_value
    
    def allowed_file(self, filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in self.ALLOWED_EXTENSIONS