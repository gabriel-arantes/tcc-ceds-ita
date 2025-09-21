# 📊 Metadados dos Dados Agregados

## 📁 Estrutura dos Arquivos

### 🔍 Dados do SIM (Sistema de Informações sobre Mortalidade)
- **`sim_por_ano.csv`** - Agregações por ano
  - Colunas: ano, count, idade_anos (mean, median, std), sexo (percentual feminino)
- **`sim_top_municipios.csv`** - Top 20 municípios por número de óbitos
  - Colunas: mun6, count, idade_anos (mean), sexo (percentual feminino)
- **`sim_por_cid.csv`** - Distribuição por códigos CID-10
  - Colunas: cid3, count

### 🏥 Dados do SIH (Sistema de Informações Hospitalares)
- **`sih_por_ano.csv`** - Agregações por ano
  - Colunas: ano, count, idade_anos (mean, median, std), sexo (percentual feminino)
- **`sih_top_municipios.csv`** - Top 20 municípios por número de altas
  - Colunas: mun6, count, idade_anos (mean), sexo (percentual feminino)
- **`sih_por_cid.csv`** - Distribuição por códigos CID-10
  - Colunas: cid3, count

### 🔗 Dados de Vinculação
- **`linkage_por_ano.csv`** - Agregações por ano
  - Colunas: ano, match (count, sum), score (mean, median, std, min, max), taxa_vinculacao
- **`linkage_top_municipios.csv`** - Top 20 municípios por número de matches
  - Colunas: mun6, match (count, sum), score (mean), taxa_vinculacao
- **`linkage_distribuicao_scores.csv`** - Distribuição de scores de vinculação
  - Colunas: score_range, count

### 📈 Dados de Taxas de Mortalidade
- **`taxas_por_ano.csv`** - Agregações por ano (se disponível)
  - Colunas: ano, obitos_pneumonia (sum, mean, median, std), tx_pneu_100k (mean, median, std, min, max), mun6 (count)
- **`taxas_top_municipios.csv`** - Top 20 municípios por taxa de mortalidade (se disponível)
  - Colunas: mun6, ano, obitos_pneumonia, pop, tx_pneu_100k

### 📊 Dados de Análise
- **`resumo_analise.json`** - Resumo completo em formato JSON
- **`estatisticas_principais.csv`** - Estatísticas principais em formato tabular
- **`top_municipios.csv`** - Top 10 municípios por total de casos
- **`relatorio_analise_final.md`** - Relatório completo da análise

## 🔢 Códigos de Município

Os códigos de município seguem o padrão IBGE de 6 dígitos:
- **355030** - São Paulo (SP)
- **330455** - Rio de Janeiro (RJ)
- **310620** - Belo Horizonte (MG)
- **530010** - Brasília (DF)

## 📅 Período dos Dados

- **SIM**: 2022-2023
- **SIH**: 2022-2024
- **Vinculação**: 2022-2023 (baseado no SIM)

## 🏥 Códigos CID-10

- **J12** - Pneumonia viral
- **J13** - Pneumonia por Streptococcus pneumoniae
- **J14** - Pneumonia por Haemophilus influenzae
- **J15** - Pneumonia bacteriana
- **J16** - Pneumonia por outros agentes infecciosos
- **J17** - Pneumonia em doenças classificadas em outra parte
- **J18** - Pneumonia por agente não especificado

## 📊 Métricas de Qualidade

### Vinculação
- **Taxa de vinculação**: 14.64%
- **Score médio**: 1.974
- **Score mediano**: 1.926
- **Score mínimo**: 1.646
- **Score máximo**: 3.000

### Cobertura Geográfica
- **Municípios SIM**: 1.564
- **Municípios SIH**: 1.697
- **UFs analisadas**: SP, RJ, DF, MG

## 🔧 Formato dos Arquivos

Todos os arquivos CSV utilizam:
- **Separador**: vírgula (,)
- **Codificação**: UTF-8
- **Decimal**: ponto (.)
- **Cabeçalho**: primeira linha

## 📝 Notas Importantes

1. **Dados de população**: Não foram carregados com sucesso, por isso as taxas de mortalidade não estão disponíveis
2. **Dados de 2024**: SIM não disponível ainda (normal, dados podem ter delay)
3. **Vinculação**: Baseada em critérios probabilísticos com threshold adaptativo
4. **Qualidade**: Todos os dados passaram por validação e limpeza

## 🚀 Como Usar

Os dados agregados podem ser utilizados para:
- Análises estatísticas
- Visualizações
- Modelos preditivos
- Relatórios epidemiológicos
- Comparações regionais

---

*Metadados gerados automaticamente em 21/09/2025*
