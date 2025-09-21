#!/usr/bin/env python3
"""
Pipeline real de análise de mortalidade por pneumonia usando PySUS
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Optional, Dict, Any
import pysus
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class RealPneumoniaAnalysisPipeline:
    """Pipeline real de análise de mortalidade por pneumonia usando PySUS"""
    
    def __init__(
        self, 
        years: List[int], 
        ufs: Optional[List[str]] = None,
        cache_dir: str = "./data/_pysus_cache",
        results_dir: str = "./data/_resultados"
    ):
        self.years = years
        self.ufs = ufs or ["SP", "RJ", "MG"]
        self.cache_dir = Path(cache_dir)
        self.results_dir = Path(results_dir)
        
        # Cria diretórios
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # Inicializa PySUS
        self.sim = pysus.SIM()
        self.sih = pysus.SIH()
        
        # Carrega metadados
        self.sim.load()
        self.sih.load()
        
        # Dados carregados
        self.sim_data = None
        self.sih_data = None
        self.population_data = None
        self.mortality_rates = None
        self.death_profiles = None
        self.linkage_result = None
        
        # Códigos CID-10 para pneumonia
        self.pneumonia_codes = ['J12', 'J13', 'J14', 'J15', 'J16', 'J17', 'J18']
        
        print(f"PySUS SIM groups: {self.sim.groups}")
        print(f"PySUS SIH groups: {self.sih.groups}")
    
    def load_sim_data(self) -> pd.DataFrame:
        """Carrega dados reais do SIM"""
        print("Carregando dados reais do SIM...")
        
        frames = []
        
        for uf in self.ufs:
            for year in self.years:
                try:
                    print(f"  Processando SIM {uf} {year}...")
                    
                    # Busca arquivos do SIM para a UF e ano
                    files = self.sim.get_files('CID10', uf=uf, year=year)
                    
                    if not files:
                        print(f"    Nenhum arquivo encontrado para {uf} {year}")
                        continue
                    
                    print(f"    Encontrados {len(files)} arquivos")
                    
                    # Download dos arquivos
                    downloaded_files = self.sim.download(files, local_dir=str(self.cache_dir))
                    
                    # Processa o arquivo (ParquetSet)
                    try:
                        # Verifica se é um objeto ParquetSet
                        if hasattr(downloaded_files, 'to_dataframe'):
                            df = downloaded_files.to_dataframe()
                        else:
                            # Se for uma lista, processa cada item
                            if isinstance(downloaded_files, list):
                                for file_path in downloaded_files:
                                    if hasattr(file_path, 'to_dataframe'):
                                        df = file_path.to_dataframe()
                                    else:
                                        df = pd.read_csv(file_path, sep=';', encoding='latin-1', low_memory=False)
                            else:
                                df = pd.read_csv(downloaded_files, sep=';', encoding='latin-1', low_memory=False)
                        
                        # Filtra pneumonia J12–J18
                        if 'CAUSABAS' in df.columns:
                            mask_pne = df['CAUSABAS'].astype(str).str.upper().str.match(r'J1[2-8]')
                            df_pne = df[mask_pne].copy()
                        else:
                            print(f"    Coluna CAUSABAS não encontrada")
                            continue
                        
                        if df_pne.empty:
                            continue
                        
                        # Processa dados
                        df_processed = self._process_sim_dataframe(df_pne, year, uf)
                        if not df_processed.empty:
                            frames.append(df_processed)
                            print(f"    Processados {len(df_processed)} óbitos")
                        
                    except Exception as e:
                        print(f"    Erro ao processar arquivo: {e}")
                        continue
                
                except Exception as e:
                    print(f"  Erro ao processar {uf} {year}: {e}")
                    continue
        
        if frames:
            result = pd.concat(frames, ignore_index=True)
            print(f"Total de óbitos por pneumonia carregados: {len(result)}")
            return result
        else:
            print("Nenhum dado do SIM foi carregado")
            return pd.DataFrame(columns=[
                "mun6", "ano", "sexo", "idade_anos", "escolaridade", 
                "data_obito", "cid3", "faixa_etaria", "uf"
            ])
    
    def load_sih_data(self) -> pd.DataFrame:
        """Carrega dados reais do SIH"""
        print("Carregando dados reais do SIH...")
        
        frames = []
        
        for uf in self.ufs:
            for year in self.years:
                for month in range(1, 13):  # Todos os meses
                    try:
                        print(f"  Processando SIH {uf} {year}/{month:02d}...")
                        
                        # Busca arquivos do SIH para a UF, ano e mês
                        files = self.sih.get_files('RD', uf=uf, year=year, month=month)
                        
                        if not files:
                            continue
                        
                        print(f"    Encontrados {len(files)} arquivos")
                        
                        # Download dos arquivos
                        downloaded_files = self.sih.download(files, local_dir=str(self.cache_dir))
                        
                        # Processa o arquivo (ParquetSet)
                        try:
                            # Verifica se é um objeto ParquetSet
                            if hasattr(downloaded_files, 'to_dataframe'):
                                df = downloaded_files.to_dataframe()
                            else:
                                # Se for uma lista, processa cada item
                                if isinstance(downloaded_files, list):
                                    for file_path in downloaded_files:
                                        if hasattr(file_path, 'to_dataframe'):
                                            df = file_path.to_dataframe()
                                        else:
                                            df = pd.read_csv(file_path, sep=';', encoding='latin-1', low_memory=False)
                                else:
                                    df = pd.read_csv(downloaded_files, sep=';', encoding='latin-1', low_memory=False)
                            
                            # Filtra óbitos na internação
                            if 'MORTE' in df.columns:
                                df = df[df['MORTE'] == 1]
                            
                            # Filtra pneumonia J12–J18
                            if 'DIAG_PRINC' in df.columns:
                                mask_pne = df['DIAG_PRINC'].astype(str).str.upper().str.match(r'J1[2-8]')
                                df_pne = df[mask_pne].copy()
                            else:
                                continue
                            
                            if df_pne.empty:
                                continue
                            
                            # Processa dados
                            df_processed = self._process_sih_dataframe(df_pne, year, uf)
                            if not df_processed.empty:
                                frames.append(df_processed)
                                print(f"    Processadas {len(df_processed)} altas por óbito")
                            
                        except Exception as e:
                            print(f"    Erro ao processar arquivo: {e}")
                            continue
                    
                    except Exception as e:
                        print(f"  Erro ao processar {uf} {year}/{month:02d}: {e}")
                        continue
        
        if frames:
            result = pd.concat(frames, ignore_index=True)
            print(f"Total de altas por óbito com pneumonia carregadas: {len(result)}")
            return result
        else:
            print("Nenhum dado do SIH foi carregado")
            return pd.DataFrame(columns=[
                "mun6", "ano", "sexo", "idade_anos", "data_saida", "cid3", "uf"
            ])
    
    def _process_sim_dataframe(self, df: pd.DataFrame, year: int, uf: str) -> pd.DataFrame:
        """Processa um DataFrame do SIM"""
        
        result = pd.DataFrame()
        
        # Município de residência (6 dígitos)
        if 'CODMUNRES' in df.columns:
            result['mun6'] = df['CODMUNRES'].astype(str).str.zfill(6).str.slice(0, 6)
        else:
            result['mun6'] = pd.NA
        
        # Ano
        result['ano'] = year
        
        # UF
        result['uf'] = uf
        
        # Data do óbito
        if 'DTOBITO' in df.columns:
            result['data_obito'] = pd.to_datetime(df['DTOBITO'], format='%d%m%Y', errors='coerce')
        else:
            result['data_obito'] = pd.NaT
        
        # Sexo
        if 'SEXO' in df.columns:
            result['sexo'] = df['SEXO'].map({1: 'M', 2: 'F'}).fillna('I')
        else:
            result['sexo'] = 'I'
        
        # Idade - LÓGICA CORRIGIDA
        if 'IDADE' in df.columns:
            idade_anos = []
            for idade in df['IDADE']:
                if pd.isna(idade):
                    idade_anos.append(pd.NA)
                else:
                    try:
                        idade_str = str(idade).zfill(3)
                        tipo = int(idade_str[0])
                        valor = int(idade_str[1:])
                        
                        if tipo == 0:  # Anos
                            idade_anos.append(valor)
                        elif tipo == 1:  # Meses
                            idade_anos.append(valor / 12)
                        elif tipo == 2:  # Dias
                            idade_anos.append(valor / 365.25)
                        elif tipo == 3:  # Horas
                            idade_anos.append(0)
                        elif tipo == 4:  # Anos
                            idade_anos.append(valor)
                        elif tipo == 5:  # Segundos
                            idade_anos.append(0)
                        elif tipo == 9:  # Anos
                            idade_anos.append(valor)
                        else:
                            idade_anos.append(pd.NA)
                    except (ValueError, IndexError):
                        idade_anos.append(pd.NA)
            result['idade_anos'] = idade_anos
        else:
            result['idade_anos'] = pd.NA
        
        # Escolaridade
        if 'ESC' in df.columns:
            result['escolaridade'] = df['ESC']
        else:
            result['escolaridade'] = pd.NA
        
        # CID-10 (3 caracteres)
        if 'CAUSABAS' in df.columns:
            result['cid3'] = df['CAUSABAS'].astype(str).str.upper().str.slice(0, 3)
        else:
            result['cid3'] = pd.NA
        
        # Faixa etária
        result['faixa_etaria'] = self._get_age_group(pd.Series(result['idade_anos']))
        
        return result
    
    def _process_sih_dataframe(self, df: pd.DataFrame, year: int, uf: str) -> pd.DataFrame:
        """Processa um DataFrame do SIH"""
        
        result = pd.DataFrame()
        
        # Município de residência (6 dígitos)
        if 'MUNIC_RES' in df.columns:
            result['mun6'] = df['MUNIC_RES'].astype(str).str.zfill(6).str.slice(0, 6)
        else:
            result['mun6'] = pd.NA
        
        # Ano
        result['ano'] = year
        
        # UF
        result['uf'] = uf
        
        # Data de saída
        if 'DT_SAIDA' in df.columns:
            result['data_saida'] = pd.to_datetime(df['DT_SAIDA'], format='%Y%m%d', errors='coerce')
        else:
            result['data_saida'] = pd.NaT
        
        # Sexo (SIH: 1=M, 3=F)
        if 'SEXO' in df.columns:
            result['sexo'] = df['SEXO'].map({1: 'M', 3: 'F'}).fillna('I')
        else:
            result['sexo'] = 'I'
        
        # Idade
        if 'IDADE' in df.columns:
            result['idade_anos'] = pd.to_numeric(df['IDADE'], errors='coerce')
        else:
            result['idade_anos'] = pd.NA
        
        # CID-10 (3 caracteres)
        if 'DIAG_PRINC' in df.columns:
            result['cid3'] = df['DIAG_PRINC'].astype(str).str.upper().str.slice(0, 3)
        else:
            result['cid3'] = pd.NA
        
        return result
    
    def _get_age_group(self, idade_anos: pd.Series) -> pd.Series:
        """Converte idade em faixa etária"""
        def age_to_group(idade):
            if pd.isna(idade):
                return "Ignorado"
            if idade < 1:
                return "<1"
            elif idade < 5:
                return "1–4"
            elif idade < 15:
                return "5–14"
            elif idade < 25:
                return "15–24"
            elif idade < 45:
                return "25–44"
            elif idade < 60:
                return "45–59"
            elif idade < 75:
                return "60–74"
            else:
                return "75+"
        
        return idade_anos.apply(age_to_group)
    
    def load_population_data(self) -> pd.DataFrame:
        """Carrega dados de população do IBGE"""
        print("Carregando dados de população do IBGE...")
        
        try:
            # Usa dados do IBGE via PySUS
            ibge = pysus.IBGEDATASUS()
            ibge.load()
            
            frames = []
            for year in self.years:
                try:
                    # Busca dados de população por município
                    files = ibge.get_files('POP', year=year)
                    if files:
                        downloaded_files = ibge.download(files, local_dir=str(self.cache_dir))
                        for file_path in downloaded_files:
                            # Verifica se é um objeto ParquetSet
                            if hasattr(file_path, 'to_dataframe'):
                                df = file_path.to_dataframe()
                            else:
                                df = pd.read_csv(file_path, sep=';', encoding='latin-1')
                            if not df.empty:
                                frames.append(df)
                except Exception as e:
                    print(f"  Erro ao carregar população {year}: {e}")
                    continue
            
            if frames:
                population = pd.concat(frames, ignore_index=True)
                print(f"Dados de população carregados: {len(population)} registros")
                return population
            else:
                print("Nenhum dado de população encontrado")
                return pd.DataFrame(columns=["mun6", "ano", "pop"])
                
        except Exception as e:
            print(f"Erro ao carregar dados de população: {e}")
            return pd.DataFrame(columns=["mun6", "ano", "pop"])
    
    def calculate_mortality_rates(self) -> pd.DataFrame:
        """Calcula taxas municipais de mortalidade"""
        print("Calculando taxas de mortalidade...")
        
        if self.sim_data is None:
            return pd.DataFrame()
        
        # Agrupa óbitos por município e ano
        deaths = (self.sim_data
                  .groupby(['mun6', 'ano'], as_index=False)
                  .size()
                  .rename(columns={'size': 'obitos_pneumonia'}))
        
        if self.population_data is None or self.population_data.empty:
            print("Nenhum dado de população disponível - criando taxas sem população")
            rates = deaths.copy()
            rates['pop'] = None
            rates['tx_pneu_100k'] = None
            return rates.sort_values(['ano', 'mun6'])
        
        # Mescla com dados de população
        rates = deaths.merge(
            self.population_data, 
            on=['mun6', 'ano'], 
            how='left'
        )
        
        # Calcula taxa por 100.000 habitantes (apenas onde há população)
        rates['tx_pneu_100k'] = None
        mask = rates['pop'].notna() & (rates['pop'] > 0)
        rates.loc[mask, 'tx_pneu_100k'] = (rates.loc[mask, 'obitos_pneumonia'] / rates.loc[mask, 'pop']) * 100000
        
        return rates.sort_values(['ano', 'mun6'])
    
    def get_death_profiles(self) -> Dict[str, pd.DataFrame]:
        """Gera perfis dos óbitos"""
        print("Gerando perfis dos óbitos...")
        
        profiles = {}
        
        # Perfil por faixa etária
        profiles['idade'] = (
            self.sim_data.groupby(['ano', 'faixa_etaria'], dropna=False)
            .size()
            .rename('obitos')
            .reset_index()
        )
        
        # Perfil por sexo
        profiles['sexo'] = (
            self.sim_data.groupby(['ano', 'sexo'], dropna=False)
            .size()
            .rename('obitos')
            .reset_index()
        )
        
        # Perfil por escolaridade
        profiles['escolaridade'] = (
            self.sim_data.groupby(['ano', 'escolaridade'], dropna=False)
            .size()
            .rename('obitos')
            .reset_index()
        )
        
        return profiles
    
    def run_analysis(self) -> Dict[str, Any]:
        """Executa pipeline completo"""
        print("=== INICIANDO ANÁLISE REAL DE MORTALIDADE POR PNEUMONIA ===")
        print(f"Período: {min(self.years)}-{max(self.years)}")
        print(f"UFs: {', '.join(self.ufs)}")
        print()
        
        # 1. Carrega dados reais
        self.sim_data = self.load_sim_data()
        self.sih_data = self.load_sih_data()
        self.population_data = self.load_population_data()
        
        print(f"\nDados carregados:")
        print(f"  Óbitos SIM: {len(self.sim_data):,}")
        print(f"  Altas SIH: {len(self.sih_data):,}")
        print(f"  População: {len(self.population_data):,}")
        
        if self.sim_data.empty:
            print("⚠️  Nenhum dado do SIM foi carregado. Verifique a disponibilidade dos dados.")
            return {"erro": "Nenhum dado do SIM disponível"}
        
        # 2. Calcula taxas de mortalidade
        self.mortality_rates = self.calculate_mortality_rates()
        
        # 3. Gera perfis dos óbitos
        self.death_profiles = self.get_death_profiles()
        
        # 4. Salva resultados
        self.save_results()
        
        # 5. Retorna resumo
        return self.get_summary()
    
    def save_results(self):
        """Salva resultados em arquivos"""
        print("Salvando resultados...")
        
        # Taxas de mortalidade
        if self.mortality_rates is not None and not self.mortality_rates.empty:
            self.mortality_rates.to_parquet(
                self.results_dir / "taxas_pneumonia_municipio_ano_real.parquet", 
                index=False
            )
        
        # Perfis dos óbitos
        if self.death_profiles:
            for profile_name, profile_data in self.death_profiles.items():
                if not profile_data.empty:
                    profile_data.to_parquet(
                        self.results_dir / f"perfil_{profile_name}_real.parquet", 
                        index=False
                    )
        
        # Dados brutos
        if self.sim_data is not None and not self.sim_data.empty:
            self.sim_data.to_parquet(
                self.results_dir / "sim_pneumonia_real.parquet", 
                index=False
            )
        
        if self.sih_data is not None and not self.sih_data.empty:
            self.sih_data.to_parquet(
                self.results_dir / "sih_pneumonia_real.parquet", 
                index=False
            )
    
    def get_summary(self) -> Dict[str, Any]:
        """Retorna resumo da análise"""
        summary = {
            "periodo": f"{min(self.years)}-{max(self.years)}",
            "ufs": self.ufs,
            "obitos_sim": len(self.sim_data) if self.sim_data is not None else 0,
            "altas_sih": len(self.sih_data) if self.sih_data is not None else 0,
            "municipios_com_taxas": len(self.mortality_rates) if self.mortality_rates is not None else 0,
        }
        
        return summary

def main():
    """Função principal"""
    # Configuração para ingestão generosa com dados reais
    pipeline = RealPneumoniaAnalysisPipeline(
        years=[2022, 2023],  # 2 anos para análise temporal
        ufs=["SP", "RJ", "MG"],  # 3 UFs principais
        cache_dir="./data/_pysus_cache",
        results_dir="./data/_resultados"
    )
    
    # Executa análise
    results = pipeline.run_analysis()
    
    print("\n=== RESUMO DOS RESULTADOS ===")
    for key, value in results.items():
        print(f"{key}: {value}")
    
    print("\n✅ Análise real concluída!")

if __name__ == "__main__":
    main()
