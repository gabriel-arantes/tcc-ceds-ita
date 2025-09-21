"""
Vinculação probabilística entre SIM e SIH
"""

import pandas as pd
import numpy as np
from typing import Tuple, Optional
import recordlinkage
from .utils import DataUtils


class ProbabilisticLinker:
    """Vinculação probabilística entre óbitos do SIM e altas por óbito do SIH"""
    
    def __init__(self):
        self.utils = DataUtils()
    
    def link_sim_sih(
        self, 
        sim_data: pd.DataFrame, 
        sih_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Vincula óbitos do SIM a AIHs do SIH cujo desfecho foi óbito e CID J12–J18
        
        Args:
            sim_data: DataFrame com óbitos do SIM
            sih_data: DataFrame com altas por óbito do SIH
            
        Returns:
            DataFrame com pares vinculados
        """
        
        # Prepara dados para vinculação
        A, B = self._prepare_data_for_linkage(sim_data, sih_data)
        
        if A.empty or B.empty:
            return pd.DataFrame()
        
        # Cria índice de candidatos com blocking
        candidate_pairs = self._create_candidate_pairs(A, B)
        
        if candidate_pairs.empty:
            return pd.DataFrame()
        
        # Calcula features de comparação
        features = self._calculate_comparison_features(candidate_pairs, A, B)
        
        # Classifica pares usando ECM
        matches = self._classify_pairs(features)
        
        # Constrói resultado final
        result = self._build_linkage_result(matches, A, B, features)
        
        return result
    
    def _prepare_data_for_linkage(
        self, 
        sim_data: pd.DataFrame, 
        sih_data: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Prepara dados para vinculação adicionando features necessárias"""
        
        # SIM (A)
        A = (sim_data
             .dropna(subset=["mun6", "ano"])
             .assign(
                 ano_mes=lambda d: d["data_obito"].dt.to_period("M").astype(str),
                 age_round=lambda d: d["idade_anos"].round(0),
                 ts=lambda d: d["data_obito"].view("int64") // 86_400_000_000_000  # dias desde epoch
             )
             [["mun6", "ano", "ano_mes", "sexo", "age_round", "cid3", "ts"]]
             .copy())
        
        # SIH (B)
        B = (sih_data
             .dropna(subset=["mun6", "ano"])
             .assign(
                 ano_mes=lambda d: d["data_saida"].dt.to_period("M").astype(str),
                 age_round=lambda d: d["idade_anos"].round(0),
                 ts=lambda d: d["data_saida"].view("int64") // 86_400_000_000_000
             )
             [["mun6", "ano", "ano_mes", "sexo", "age_round", "cid3", "ts"]]
             .copy())
        
        return A, B
    
    def _create_candidate_pairs(
        self, 
        A: pd.DataFrame, 
        B: pd.DataFrame
    ) -> pd.MultiIndex:
        """Cria índice de pares candidatos usando blocking"""
        
        # Configura indexador com blocking em múltiplas chaves
        indexer = recordlinkage.Index()
        indexer.block(
            left_on=["mun6", "ano_mes", "sexo"], 
            right_on=["mun6", "ano_mes", "sexo"]
        )
        
        # Gera pares candidatos
        candidate_pairs = indexer.index(A, B)
        
        return candidate_pairs
    
    def _calculate_comparison_features(
        self, 
        candidate_pairs: pd.MultiIndex, 
        A: pd.DataFrame, 
        B: pd.DataFrame
    ) -> pd.DataFrame:
        """Calcula features de comparação entre pares candidatos"""
        
        # Configura comparador
        comp = recordlinkage.Compare()
        
        # CID-3 exato
        comp.exact("cid3", "cid3", label="cid3_eq")
        
        # Idade próxima (gaussiana, escala 2 anos)
        comp.numeric(
            "age_round", "age_round", 
            method="gauss", scale=2, 
            label="age_close"
        )
        
        # Proximidade de data (gaussiana, escala 3 dias)
        comp.numeric(
            "ts", "ts", 
            method="gauss", scale=3, 
            label="date_close"
        )
        
        # Calcula features
        features = comp.compute(candidate_pairs, A, B)
        
        return features
    
    def _classify_pairs(self, features: pd.DataFrame) -> pd.Series:
        """Classifica pares usando ECM (unsupervisionado)"""
        
        # Tenta usar ECMClassifier (versão mais recente)
        try:
            clf = recordlinkage.ECMClassifier()
        except AttributeError:
            # Fallback para versões antigas
            clf = recordlinkage.BernoulliEMClassifier()
        
        # Treina e classifica
        matches = clf.fit_predict(features)
        
        return matches
    
    def _build_linkage_result(
        self, 
        matches: pd.Series, 
        A: pd.DataFrame, 
        B: pd.DataFrame, 
        features: pd.DataFrame
    ) -> pd.DataFrame:
        """Constrói resultado final da vinculação"""
        
        # Converte matches para DataFrame
        out = matches.to_frame(index=False, name="match").reset_index()
        out = out.rename(columns={"level_0": "idx_sim", "level_1": "idx_sih"})
        
        # Adiciona dados originais do SIM
        sim_data = A.reset_index().rename(columns={"index": "idx_sim"})
        out = out.join(sim_data.set_index("idx_sim"), on="idx_sim")
        
        # Adiciona dados originais do SIH
        sih_data = B.reset_index().rename(columns={"index": "idx_sih"})
        out = out.join(sih_data.set_index("idx_sih"), on="idx_sih", rsuffix="_sih")
        
        # Calcula score de similaridade
        if not features.empty and not matches.empty:
            out["score"] = features.loc[matches].sum(axis=1).values
        else:
            out["score"] = 0.0
        
        # Ordena por score
        out = out.sort_values("score", ascending=False)
        
        return out
    
    def calculate_linkage_metrics(self, linkage_result: pd.DataFrame) -> dict:
        """Calcula métricas da vinculação"""
        
        if linkage_result.empty:
            return {
                "total_pairs": 0,
                "matched_pairs": 0,
                "match_rate": 0.0,
                "avg_score": 0.0
            }
        
        total_pairs = len(linkage_result)
        matched_pairs = len(linkage_result[linkage_result["match"]])
        match_rate = matched_pairs / total_pairs if total_pairs > 0 else 0.0
        avg_score = linkage_result["score"].mean() if not linkage_result.empty else 0.0
        
        return {
            "total_pairs": total_pairs,
            "matched_pairs": matched_pairs,
            "match_rate": match_rate,
            "avg_score": avg_score
        }
