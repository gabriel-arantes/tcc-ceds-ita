# 📊 Análise de Mortalidade por Pneumonia e Vinculação Probabilística

Este projeto implementa uma análise completa da mortalidade por pneumonia (CID-10 J12-J18) utilizando dados oficiais do DATASUS, incluindo vinculação probabilística entre óbitos do SIM e internações do SIH/RD.

## 🎯 Objetivos

- **Análise Exploratória** dos dados do SIM (Sistema de Informações sobre Mortalidade)
- **Cálculo de Taxas de Mortalidade** municipais por 100.000 habitantes
- **Vinculação Probabilística** entre óbitos do SIM e internações do SIH/RD
- **Análise de Qualidade** dos matches encontrados
- **Insights e Recomendações** para aplicações práticas

## 📋 Dados Utilizados

- **SIM**: Óbitos por pneumonia (CID-10 J12-J18) - 2022-2023
- **SIH/RD**: Internações com óbito por pneumonia - 2022-2024  
- **IBGE**: População municipal para cálculo de taxas
- **UFs**: São Paulo (SP), Rio de Janeiro (RJ), Distrito Federal (DF), Minas Gerais (MG)

## 📊 Resultados da Análise

### 🎯 Estatísticas Principais
- **Total de óbitos (SIM)**: 86.915
- **Total de altas (SIH)**: 91.286
- **Pares vinculados**: 340.449
- **Taxa de vinculação**: 14.64%
- **Municípios analisados**: 1.564

### 👥 Perfil Demográfico
- **Distribuição por sexo**: 50.5% feminino, 49.5% masculino
- **Faixa etária predominante**: 75+ anos (63.7% dos casos)
- **Escolaridade**: 25.6% com 4ª série completa, 23.3% com 1ª a 4ª incompleta

### 🏆 Top Municípios
1. **355030**: 19.912 casos (12.713 óbitos + 7.199 altas)
2. **330455**: 16.204 casos (9.576 óbitos + 6.628 altas)
3. **310620**: 2.879 casos (1.580 óbitos + 1.299 altas)

### 📈 Tendências Temporais
- **Óbitos 2022**: 42.741
- **Óbitos 2023**: 44.174 (+3.3%)
- **Altas 2024**: 32.754 (dados mais recentes)

## 🚀 Instalação

### Pré-requisitos

- Python 3.10+
- Poetry (gerenciador de dependências)

### Instalação das Dependências

```bash
# Clone o repositório
git clone <url-do-repositorio>
cd pneumonia-mortality-analysis

# Instale as dependências com Poetry
poetry install

# Ative o ambiente virtual
poetry shell
```

## 📁 Estrutura do Projeto

```
pneumonia-mortality-analysis/
├── notebooks/
│   └── 01_data_exploration.ipynb    # Análise completa
├── pneumonia_analysis/
│   ├── __init__.py
│   ├── pipeline.py                   # Pipeline principal
│   ├── sim_processor.py             # Processamento SIM
│   ├── sih_processor.py             # Processamento SIH
│   ├── linkage.py                   # Vinculação probabilística
│   └── utils.py                     # Utilitários
├── data/
│   ├── _resultados/                 # Dados processados (ignorados pelo git)
│   └── resultados_agregados/        # Dados agregados para análise
│       ├── resumo_analise.json      # Resumo completo em JSON
│       ├── estatisticas_principais.csv # Estatísticas principais
│       ├── top_municipios.csv       # Top municípios por casos
│       ├── relatorio_analise_final.md # Relatório completo
│       ├── metadados_dados_agregados.md # Metadados dos dados
│       ├── sim_*.csv                # Dados agregados do SIM
│       ├── sih_*.csv                # Dados agregados do SIH
│       └── linkage_*.csv            # Dados agregados de vinculação
├── pyproject.toml                   # Configuração Poetry
├── .gitignore
└── README.md
```

## 🔧 Uso

### 1. Executar o Pipeline de Ingestão

```bash
# Execute o pipeline para baixar e processar os dados
poetry run python pipeline.py
```

### 2. Executar a Análise Completa

```bash
# Abra o Jupyter Notebook
poetry run jupyter notebook notebooks/01_data_exploration.ipynb
```

## 📊 Funcionalidades Implementadas

### 🔍 Análise Exploratória
- Análise de qualidade dos dados
- Distribuições demográficas (idade, sexo, escolaridade)
- Análise temporal e sazonal
- Análise geográfica por UF

### 🔗 Vinculação Probabilística
- **Blocking Inteligente**: Múltiplas estratégias de bloqueio
- **Features Sofisticadas**: 8 features de comparação
- **Thresholds Adaptativos**: Classificação por níveis de confiança
- **Validação Cruzada**: K-Fold para robustez

### 📈 Análise de Qualidade
- Análise de duplicatas
- Taxa de sucesso real (1:1)
- Classificação por confiança
- Métricas de performance

### 🏥 Investigação de Fatores de Risco
- Fatores demográficos
- Fatores clínicos
- Fatores temporais
- Fatores geográficos

## 🛠️ Tecnologias Utilizadas

- **Python 3.10+**
- **PySUS**: Acesso aos dados do DATASUS
- **Record Linkage Toolkit**: Vinculação probabilística
- **Pandas**: Manipulação de dados
- **NumPy**: Computação numérica
- **Matplotlib/Seaborn**: Visualizações
- **Plotly**: Visualizações interativas
- **Poetry**: Gerenciamento de dependências

## 📚 Técnicas de Record Linkage

### Blocking
- **Restritivo**: Município + Ano/Mês + Sexo
- **Permissivo**: Município + Sexo
- **Múltiplas Estratégias**: Combinação de critérios

### Features de Comparação
1. **CID-10 Exato**: Comparação exata dos códigos
2. **Idade Muito Próxima**: Gaussiana com escala 1
3. **Idade Próxima**: Gaussiana com escala 3
4. **Idade Moderada**: Gaussiana com escala 5
5. **Data Muito Próxima**: Gaussiana com escala 1
6. **Data Próxima**: Gaussiana com escala 3
7. **Data Moderada**: Gaussiana com escala 7
8. **Diferença Linear de Idade**: Comparação linear

### Validação
- **K-Fold Cross-Validation**
- **Métricas de Precisão, Recall e F1-Score**
- **Validação Temporal e Geográfica**
- **Amostragem Estratificada**

## 📊 Resultados Esperados

- **Taxa de Sucesso**: ~95% (1:1)
- **Matches de Alta Confiança**: ~47%
- **Matches de Média Confiança**: ~6%
- **Análise de Duplicatas**: Implementada
- **Insights Demográficos**: Completos

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## 📞 Contato

Para dúvidas ou sugestões, abra uma issue no repositório.

## 🙏 Agradecimentos

- **DATASUS** pelos dados oficiais
- **PySUS** pela biblioteca de acesso aos dados
- **Record Linkage Toolkit** pelas ferramentas de vinculação
- **Comunidade Python** pelo suporte