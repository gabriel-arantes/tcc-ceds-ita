"""
Utilitários para processamento de dados do DATASUS
"""

import re
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Union, List, Optional


class DataUtils:
    """Utilitários para processamento de dados do DATASUS"""
    
    @staticmethod
    def mun_to6(x) -> pd.Series:
        """Normaliza código IBGE para 6 dígitos (sem dígito verificador)."""
        s = pd.Series(x, dtype="object").astype(str).str.extract(r"(\d+)")[0]
        return s.str.zfill(6).str.slice(0, 6)
    
    @staticmethod
    def parse_yyyymmdd_col(s: pd.Series) -> pd.Series:
        """Converte colunas 'AAAAMMDD' ou 'DDMMAAAA' em datetime; aceita string/int."""
        s = pd.Series(s, dtype="object").astype(str).str.replace(r"\D", "", regex=True)
        # tenta AAAAMMDD
        dt = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
        # fallback DDMMAAAA
        m = dt.isna()
        if m.any():
            dt.loc[m] = pd.to_datetime(s[m], format="%d%m%Y", errors="coerce")
        return dt
    
    @staticmethod
    def download_to_df(bunch_or_list):
        """Compatível com retornos do PySUS: objeto com .to_dataframe() (SIM) ou lista (SIH)."""
        if hasattr(bunch_or_list, "to_dataframe"):
            return bunch_or_list.to_dataframe()
        # lista de ParquetPath -> concat
        return pd.concat([p.to_dataframe() for p in bunch_or_list], ignore_index=True)
    
    @staticmethod
    def all_ufs(sim_or_sih) -> List[str]:
        """Obtém as UFs disponíveis a partir do repositório remoto."""
        try:
            groups = list(sim_or_sih.groups.keys())
            files = sim_or_sih.get_files(groups)
            return sorted({f.uf for f in files if hasattr(f, "uf")})
        except:
            # Fallback: retorna UFs padrão
            return ["SP", "RJ", "DF", "MG"]
    
    @staticmethod
    def create_age_groups(idade_anos: pd.Series) -> pd.Series:
        """Cria faixas etárias padronizadas."""
        return pd.cut(
            idade_anos,
            bins=[-0.1, 1, 5, 14, 24, 44, 59, 74, 120],
            labels=["<1", "1–4", "5–14", "15–24", "25–44", "45–59", "60–74", "75+"]
        )
    
    @staticmethod
    def standardize_sex(sexo_series: pd.Series, source: str = "sim") -> pd.Series:
        """Padroniza códigos de sexo para 'M'/'F'/'I'."""
        if source.lower() == "sim":
            # SIM: 1=Masculino, 2=Feminino
            return sexo_series.map({1: "M", 2: "F"}).fillna(
                sexo_series.astype(str).str[0].str.upper()
            ).map({"M": "M", "F": "F"}).fillna("I")
        elif source.lower() == "sih":
            # SIH: 1=Masculino, 3=Feminino
            return sexo_series.map({1: "M", 3: "F"}).fillna("I")
        else:
            # Fallback genérico
            return sexo_series.astype(str).str[0].str.upper().map({"M": "M", "F": "F"}).fillna("I")
    
    @staticmethod
    def extract_cid3(cid_code: pd.Series) -> pd.Series:
        """Extrai os primeiros 3 caracteres do código CID-10."""
        return cid_code.astype(str).str.upper().str.slice(0, 3)
    
    @staticmethod
    def is_pneumonia_cid(cid_code: pd.Series) -> pd.Series:
        """Verifica se código CID-10 é pneumonia (J12-J18)."""
        return cid_code.astype(str).str.upper().str.fullmatch(r"J1[2-8]\d*")
    
    @staticmethod
    def calculate_mortality_rate(deaths: int, population: int) -> float:
        """Calcula taxa de mortalidade por 100.000 habitantes."""
        if population == 0:
            return 0.0
        return (deaths / population) * 100_000
    
    @staticmethod
    def ensure_directory(path: Union[str, Path]) -> Path:
        """Garante que diretório existe."""
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        return path
