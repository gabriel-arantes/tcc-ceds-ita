"""
Processamento de dados do SIM (Sistema de Informações sobre Mortalidade)
"""

import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict, Any
from pysus.ftp.databases.sim import SIM
from pysus.preprocessing.decoders import translate_variables_SIM
from .utils import DataUtils


class SIMProcessor:
    """Processador de dados do SIM para óbitos por pneumonia"""
    
    def __init__(self, cache_dir: str = "./data/_pysus_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.sim = SIM().load()
        self.utils = DataUtils()
    
    def load_pneumonia_deaths(
        self, 
        years: List[int], 
        ufs: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Carrega óbitos por pneumonia (J12-J18) do SIM
        
        Args:
            years: Lista de anos para processar
            ufs: Lista de UFs (None para todas)
            
        Returns:
            DataFrame com óbitos por pneumonia padronizados
        """
        if ufs is None:
            ufs = self.utils.all_ufs(self.sim)
        
        frames = []
        
        for uf in ufs:
            for year in years:
                try:
                    # Busca arquivos do SIM para a UF e ano
                    files = self.sim.get_files("CID10", uf=uf, year=year)
                    if not files:
                        print(f"Nenhum arquivo encontrado para {uf} {year}")
                        continue
                    
                    # Download e conversão para DataFrame
                    df = self.utils.download_to_df(
                        self.sim.download(files, local_dir=str(self.cache_dir))
                    )
                    
                    if df.empty:
                        continue
                    
                    # Decodificação oficial das variáveis
                    df = translate_variables_SIM(df, age_classes=False, classify_cid10_chapters=True)
                    
                    # Processa óbitos por pneumonia
                    df_processed = self._process_sim_dataframe(df, year)
                    if not df_processed.empty:
                        frames.append(df_processed)
                        
                except Exception as e:
                    print(f"Erro ao processar {uf} {year}: {e}")
                    continue
        
        if frames:
            return pd.concat(frames, ignore_index=True)
        else:
            return pd.DataFrame(columns=[
                "mun6", "ano", "sexo", "idade_anos", "escolaridade", 
                "data_obito", "cid3", "faixa_etaria"
            ])
    
    def _process_sim_dataframe(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """Processa um DataFrame do SIM para extrair óbitos por pneumonia"""
        
        # Identifica coluna de causa básica
        cause_col = self._get_cause_column(df)
        if cause_col is None:
            return pd.DataFrame()
        
        # Filtra pneumonia J12–J18
        mask_pne = self.utils.is_pneumonia_cid(df[cause_col])
        df = df.loc[mask_pne].copy()
        
        if df.empty:
            return pd.DataFrame()
        
        # Processa campos padronizados
        result = pd.DataFrame()
        
        # Município de residência (6 dígitos)
        sim_mun_col = self._get_municipality_column(df)
        if sim_mun_col:
            result["mun6"] = self.utils.mun_to6(df[sim_mun_col])
        else:
            result["mun6"] = pd.NA
        
        # Ano
        result["ano"] = year
        
        # Data do óbito
        date_col = self._get_death_date_column(df)
        if date_col:
            result["data_obito"] = self.utils.parse_yyyymmdd_col(df[date_col])
        else:
            result["data_obito"] = pd.NaT
        
        # Sexo padronizado
        if "SEXO" in df.columns:
            result["sexo"] = self.utils.standardize_sex(df["SEXO"], "sim")
        else:
            result["sexo"] = "I"
        
        # Idade em anos
        if "IDADE_ANOS" in df.columns:
            result["idade_anos"] = pd.to_numeric(df["IDADE_ANOS"], errors="coerce")
        else:
            result["idade_anos"] = pd.NA
        
        # Escolaridade
        esc_cols = [c for c in df.columns if c.upper().startswith("ESC")]
        if esc_cols:
            result["escolaridade"] = df[esc_cols[0]]
        else:
            result["escolaridade"] = pd.NA
        
        # CID-10 (3 caracteres)
        result["cid3"] = self.utils.extract_cid3(df[cause_col])
        
        # Faixa etária
        result["faixa_etaria"] = self.utils.create_age_groups(result["idade_anos"])
        
        return result
    
    def _get_cause_column(self, df: pd.DataFrame) -> Optional[str]:
        """Identifica coluna de causa básica do óbito"""
        for col in ["CAUSABAS", "CB_PRE"]:
            if col in df.columns:
                return col
        return None
    
    def _get_municipality_column(self, df: pd.DataFrame) -> Optional[str]:
        """Identifica coluna de município de residência"""
        for col in ["CODMUNRES", "MUNCODDV", "MUNCOD"]:
            if col in df.columns:
                return col
        return None
    
    def _get_death_date_column(self, df: pd.DataFrame) -> Optional[str]:
        """Identifica coluna de data do óbito"""
        for col in ["DTOBITO", "DT_OBITO"]:
            if col in df.columns:
                return col
        return None
    
    def get_death_profiles(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Gera perfis dos óbitos por diferentes dimensões"""
        
        profiles = {}
        
        # Perfil por faixa etária
        if "faixa_etaria" in df.columns:
            profiles["idade"] = (
                df.groupby(["ano", "faixa_etaria"], dropna=False)
                .size()
                .rename("obitos")
                .reset_index()
            )
        
        # Perfil por sexo
        if "sexo" in df.columns:
            profiles["sexo"] = (
                df.groupby(["ano", "sexo"], dropna=False)
                .size()
                .rename("obitos")
                .reset_index()
            )
        
        # Perfil por escolaridade
        if "escolaridade" in df.columns:
            profiles["escolaridade"] = (
                df.groupby(["ano", "escolaridade"], dropna=False)
                .size()
                .rename("obitos")
                .reset_index()
            )
        
        return profiles
