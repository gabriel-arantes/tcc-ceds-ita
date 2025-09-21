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
- **SIH/RD**: Internações com óbito por pneumonia - 2022-2023  
- **IBGE**: População municipal para cálculo de taxas
- **UFs**: São Paulo (SP), Rio de Janeiro (RJ), Minas Gerais (MG)

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
│   └── _resultados/                 # Dados processados
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