"""
Pipeline principal de análise de mortalidade por pneumonia
"""

import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict, Any
from pysus import IBGEDATASUS
from .sim_processor import SIMProcessor
from .sih_processor import SIHProcessor
from .linkage import ProbabilisticLinker
from .utils import DataUtils


class PneumoniaAnalysisPipeline:
    """Pipeline completo de análise de mortalidade por pneumonia"""
    
    def __init__(
        self, 
        years: List[int], 
        ufs: Optional[List[str]] = None,
        cache_dir: str = "./data/_pysus_cache",
        results_dir: str = "./data/_resultados"
    ):
        self.years = years
        self.ufs = ufs
        self.cache_dir = Path(cache_dir)
        self.results_dir = Path(results_dir)
        
        # Cria diretórios
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # Inicializa processadores
        self.sim_processor = SIMProcessor(str(self.cache_dir))
        self.sih_processor = SIHProcessor(str(self.cache_dir))
        self.linker = ProbabilisticLinker()
        self.utils = DataUtils()
        
        # Dados carregados
        self.sim_data = None
        self.sih_data = None
        self.population_data = None
        self.mortality_rates = None
        self.death_profiles = None
        self.linkage_result = None
    
    def run_analysis(self) -> Dict[str, Any]:
        """Executa pipeline completo de análise"""
        
        print("Iniciando análise de mortalidade por pneumonia...")
        
        # 1. Carrega dados do SIM
        print("1. Carregando dados do SIM...")
        self.sim_data = self.sim_processor.load_pneumonia_deaths(self.years, self.ufs)
        print(f"   Óbitos carregados: {len(self.sim_data)}")
        
        # 2. Carrega dados do SIH
        print("2. Carregando dados do SIH...")
        self.sih_data = self.sih_processor.load_pneumonia_deaths(self.years, self.ufs)
        print(f"   Altas por óbito carregadas: {len(self.sih_data)}")
        
        # 3. Carrega dados de população
        print("3. Carregando dados de população...")
        self.population_data = self.load_population_data()
        print(f"   Registros de população carregados: {len(self.population_data)}")
        
        # 4. Calcula taxas de mortalidade
        print("4. Calculando taxas de mortalidade...")
        self.mortality_rates = self.calculate_mortality_rates()
        
        # 5. Gera perfis dos óbitos
        print("5. Gerando perfis dos óbitos...")
        self.death_profiles = self.sim_processor.get_death_profiles(self.sim_data)
        
        # 6. Realiza vinculação probabilística
        print("6. Realizando vinculação probabilística...")
        self.linkage_result = self.linker.link_sim_sih(self.sim_data, self.sih_data)
        
        # 7. Salva resultados
        print("7. Salvando resultados...")
        self.save_results()
        
        print("Análise concluída!")
        
        return self.get_summary()
    
    def load_population_data(self) -> pd.DataFrame:
        """Carrega dados de população municipal do IBGE/SIDRA"""
        
        year_start = min(self.years)
        year_end = max(self.years)
        period = f"{year_start}-{year_end}"
        
        try:
            # Usa IBGEDATASUS para buscar dados de população
            ibge = IBGEDATASUS()
            ibge.load()
            
            frames = []
            for year in self.years:
                try:
                    # Busca dados de população por município
                    files = ibge.get_files('POP', year=year)
                    if files:
                        downloaded_files = ibge.download(files, local_dir=str(self.cache_dir))
                        for file_path in downloaded_files:
                            try:
                                # Tenta ler como parquet primeiro
                                if str(file_path).endswith('.parquet'):
                                    df = pd.read_parquet(file_path)
                                else:
                                    # Tenta ler como CSV com diferentes separadores
                                    try:
                                        df = pd.read_csv(file_path, sep=';', encoding='latin-1')
                                    except:
                                        df = pd.read_csv(file_path, sep=',', encoding='utf-8')
                                
                                if not df.empty:
                                    frames.append(df)
                            except Exception as file_error:
                                print(f"    Erro ao ler arquivo {file_path}: {file_error}")
                                continue
                except Exception as e:
                    print(f"  Erro ao carregar população {year}: {e}")
                    continue
            
            if frames:
                population = pd.concat(frames, ignore_index=True)
                # Normaliza para o formato esperado
                if 'CODMUNICIPIO' in population.columns:
                    population['mun6'] = self.utils.mun_to6(population['CODMUNICIPIO'])
                if 'ANO' in population.columns:
                    population['ano'] = pd.to_numeric(population['ANO'], errors='coerce')
                if 'POPULACAO' in population.columns:
                    population['pop'] = pd.to_numeric(population['POPULACAO'], errors='coerce')
                
                return population[['mun6', 'ano', 'pop']].dropna()
            else:
                print("Nenhum dado de população encontrado")
                return pd.DataFrame(columns=["mun6", "ano", "pop"])
            
        except Exception as e:
            print(f"Erro ao carregar dados de população: {e}")
            return pd.DataFrame(columns=["mun6", "ano", "pop"])
    
    def calculate_mortality_rates(self) -> pd.DataFrame:
        """Calcula taxas municipais de mortalidade por pneumonia"""
        
        if self.sim_data is None or self.population_data is None:
            return pd.DataFrame()
        
        # Agrupa óbitos por município e ano
        deaths = (self.sim_data
                  .groupby(["mun6", "ano"], as_index=False)
                  .size()
                  .rename(columns={"size": "obitos_pneumonia"}))
        
        # Mescla com dados de população
        rates = deaths.merge(
            self.population_data, 
            on=["mun6", "ano"], 
            how="left"
        )
        
        # Calcula taxa por 100.000 habitantes
        rates["tx_pneu_100k"] = rates.apply(
            lambda row: self.utils.calculate_mortality_rate(
                row["obitos_pneumonia"], 
                row["pop"]
            ), 
            axis=1
        )
        
        return rates.sort_values(["ano", "mun6"])
    
    def save_results(self):
        """Salva todos os resultados em arquivos"""
        
        # Taxas de mortalidade
        if self.mortality_rates is not None and not self.mortality_rates.empty:
            self.mortality_rates.to_parquet(
                self.results_dir / "taxas_pneumonia_municipio_ano.parquet", 
                index=False
            )
        
        # Perfis dos óbitos
        if self.death_profiles:
            for profile_name, profile_data in self.death_profiles.items():
                if not profile_data.empty:
                    profile_data.to_parquet(
                        self.results_dir / f"perfil_{profile_name}.parquet", 
                        index=False
                    )
        
        # Vinculação
        if self.linkage_result is not None and not self.linkage_result.empty:
            self.linkage_result.to_parquet(
                self.results_dir / "vinculacao_SIMxSIH_pneumonia.parquet", 
                index=False
            )
        
        # Dados brutos (opcional)
        if self.sim_data is not None and not self.sim_data.empty:
            self.sim_data.to_parquet(
                self.results_dir / "sim_pneumonia_raw.parquet", 
                index=False
            )
        
        if self.sih_data is not None and not self.sih_data.empty:
            self.sih_data.to_parquet(
                self.results_dir / "sih_pneumonia_raw.parquet", 
                index=False
            )
    
    def get_summary(self) -> Dict[str, Any]:
        """Retorna resumo da análise"""
        
        summary = {
            "periodo": f"{min(self.years)}-{max(self.years)}",
            "ufs": self.ufs or "Todas",
            "obitos_sim": len(self.sim_data) if self.sim_data is not None else 0,
            "altas_sih": len(self.sih_data) if self.sih_data is not None else 0,
            "municipios_com_taxas": len(self.mortality_rates) if self.mortality_rates is not None else 0,
        }
        
        if self.linkage_result is not None and not self.linkage_result.empty:
            linkage_metrics = self.linker.calculate_linkage_metrics(self.linkage_result)
            summary.update(linkage_metrics)
        
        return summary
    
    def get_mortality_rates_summary(self) -> pd.DataFrame:
        """Retorna resumo das taxas de mortalidade por ano"""
        
        if self.mortality_rates is None or self.mortality_rates.empty:
            return pd.DataFrame()
        
        return (self.mortality_rates
                .groupby("ano")
                .agg({
                    "obitos_pneumonia": "sum",
                    "pop": "sum",
                    "tx_pneu_100k": "mean"
                })
                .round(2))
    
    def get_death_profiles_summary(self) -> Dict[str, pd.DataFrame]:
        """Retorna resumo dos perfis dos óbitos"""
        
        if not self.death_profiles:
            return {}
        
        summaries = {}
        for profile_name, profile_data in self.death_profiles.items():
            if not profile_data.empty:
                summaries[profile_name] = (profile_data
                                          .groupby("ano")
                                          .agg({"obitos": "sum"})
                                          .reset_index())
        
        return summaries
