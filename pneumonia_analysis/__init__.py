"""
Análise de Mortalidade por Pneumonia

Pipeline completo para análise de mortalidade por pneumonia (CID-10 J12–J18)
utilizando dados oficiais do DATASUS com vinculação probabilística SIM-SIH.
"""

from .pipeline import PneumoniaAnalysisPipeline
from .sim_processor import SIMProcessor
from .sih_processor import SIHProcessor
from .linkage import ProbabilisticLinker
from .utils import DataUtils

__version__ = "0.1.0"
__all__ = [
    "PneumoniaAnalysisPipeline",
    "SIMProcessor", 
    "SIHProcessor",
    "ProbabilisticLinker",
    "DataUtils"
]
