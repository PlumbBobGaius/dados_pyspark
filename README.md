# Case Técnico PySpark

## Requisitos

- Python 3.10+
- UV package manager
- Apache Spark (incluído nas dependências)

## Instalação

Este projeto utiliza o UV para gerenciamento de dependências e ambiente virtual.

### 1. Instalar o UV

Documentação oficial: https://docs.astral.sh/uv/getting-started/installation/

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Instalar dependências

```bash
uv sync
```

### 3. Ativar ambiente virtual

```bash
source .venv/bin/activate
```

## Execução

Abra o notebook `Case.ipynb` no VSCode ou Jupyter:

```bash
code Case.ipynb
```

Execute as células sequencialmente para realizar a análise completa.

## Estrutura do Projeto

```
.
├── Case.ipynb              # Notebook principal com análises
├── data/
│   ├── clients/           # Dados de clientes
│   │   └── data.json
│   └── pedidos/           # Dados de pedidos
│       └── data.json
├── docs/                  # Documentação auxiliar
├── instruções/            # Instruções do case
├── pyproject.toml         # Configuração de dependências
└── README.md
```

## Análises Realizadas

1. Relatório de qualidade de dados com identificação de erros
2. Agregação de pedidos por cliente
3. Métricas estatísticas (média, mediana, P10, P90)
4. Análise de outliers e anomalias
5. Comparação de distribuições com/sem outliers