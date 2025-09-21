"""
Processamento de dados do SIH (Sistema de Informações Hospitalares)
"""

import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict, Any
from pysus.ftp.databases.sih import SIH
from .utils import DataUtils


class SIHProcessor:
    """Processador de dados do SIH para altas por óbito com pneumonia"""
    
    def __init__(self, cache_dir: str = "./data/_pysus_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.sih = SIH().load()
        self.utils = DataUtils()
    
    def load_pneumonia_deaths(
        self, 
        years: List[int], 
        ufs: Optional[List[str]] = None,
        months: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        Carrega altas por óbito com pneumonia (J12-J18) do SIH/RD
        
        Args:
            years: Lista de anos para processar
            ufs: Lista de UFs (None para todas)
            months: Lista de meses (None para todos)
            
        Returns:
            DataFrame com altas por óbito padronizadas
        """
        if ufs is None:
            ufs = self.utils.all_ufs(self.sih)
        
        if months is None:
            months = list(range(1, 13))
        
        frames = []
        
        for uf in ufs:
            for year in years:
                try:
                    # Busca arquivos do SIH/RD para a UF, ano e meses
                    files = self.sih.get_files("RD", uf=uf, year=year, month=months)
                    if not files:
                        print(f"Nenhum arquivo encontrado para {uf} {year}")
                        continue
                    
                    # Download e conversão para DataFrame
                    df = self.utils.download_to_df(
                        self.sih.download(files, local_dir=str(self.cache_dir))
                    )
                    
                    if df.empty:
                        continue
                    
                    # Processa altas por óbito com pneumonia
                    df_processed = self._process_sih_dataframe(df, year)
                    if not df_processed.empty:
                        frames.append(df_processed)
                        
                except Exception as e:
                    print(f"Erro ao processar {uf} {year}: {e}")
                    continue
        
        if frames:
            return pd.concat(frames, ignore_index=True)
        else:
            return pd.DataFrame(columns=[
                "mun6", "ano", "sexo", "idade_anos", "data_saida", "cid3"
            ])
    
    def _process_sih_dataframe(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """Processa um DataFrame do SIH para extrair altas por óbito com pneumonia"""
        
        # Aplica filtros de qualidade
        df = self._apply_quality_filters(df)
        
        # Filtra óbitos na internação
        if "MORTE" in df.columns:
            df = df[pd.to_numeric(df["MORTE"], errors="coerce") == 1]
        
        if df.empty:
            return pd.DataFrame()
        
        # Identifica coluna de diagnóstico principal
        diag_col = self._get_diagnosis_column(df)
        if diag_col is None:
            return pd.DataFrame()
        
        # Filtra pneumonia J12–J18
        mask_pne = self.utils.is_pneumonia_cid(df[diag_col])
        df = df.loc[mask_pne].copy()
        
        if df.empty:
            return pd.DataFrame()
        
        # Processa campos padronizados
        result = pd.DataFrame()
        
        # Município de residência (6 dígitos)
        if "MUNIC_RES" in df.columns:
            result["mun6"] = self.utils.mun_to6(df["MUNIC_RES"])
        else:
            result["mun6"] = pd.NA
        
        # Ano
        result["ano"] = year
        
        # Data de saída
        if "DT_SAIDA" in df.columns:
            result["data_saida"] = self.utils.parse_yyyymmdd_col(df["DT_SAIDA"])
        else:
            result["data_saida"] = pd.NaT
        
        # Sexo padronizado (SIH: 1=M, 3=F)
        if "SEXO" in df.columns:
            sih_sex = pd.to_numeric(df["SEXO"], errors="coerce")
            result["sexo"] = self.utils.standardize_sex(sih_sex, "sih")
        else:
            result["sexo"] = "I"
        
        # Idade em anos
        result["idade_anos"] = self._calculate_age(df)
        
        # CID-10 (3 caracteres)
        result["cid3"] = self.utils.extract_cid3(df[diag_col])
        
        return result
    
    def _apply_quality_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica filtros de qualidade aos dados do SIH"""
        
        # Filtro por tipo de AIH (geral)
        if "IDENT" in df.columns:
            df = df[df["IDENT"].astype(str).str.strip() == "1"]
        
        return df
    
    def _get_diagnosis_column(self, df: pd.DataFrame) -> Optional[str]:
        """Identifica coluna de diagnóstico principal"""
        for col in ["DIAG_PRINC", "DIAG_PRINCIPAL"]:
            if col in df.columns:
                return col
        return None
    
    def _calculate_age(self, df: pd.DataFrame) -> pd.Series:
        """Calcula idade em anos a partir de data de nascimento ou usa campo IDADE"""
        
        # Tenta calcular a partir de data de nascimento
        if "NASC" in df.columns and "DT_SAIDA" in df.columns:
            nasc = self.utils.parse_yyyymmdd_col(df["NASC"])
            saida = self.utils.parse_yyyymmdd_col(df["DT_SAIDA"])
            idade_calc = ((saida - nasc).dt.days / 365.25).where(
                nasc.notna() & saida.notna()
            )
        else:
            idade_calc = pd.Series(index=df.index, dtype=float)
        
        # Fallback para campo IDADE
        if "IDADE" in df.columns:
            idade_raw = pd.to_numeric(df["IDADE"], errors="coerce")
        else:
            idade_raw = pd.Series(index=df.index, dtype=float)
        
        return idade_calc.fillna(idade_raw)
