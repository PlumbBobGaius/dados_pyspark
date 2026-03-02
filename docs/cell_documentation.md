# Case.ipynb - Documentação por uso de Spark

Documento alinhado ao estado atual do notebook `Case.ipynb`, descrevendo o objetivo técnico de cada célula com foco no uso de Spark.

---

## Célula 1 (Markdown)

Sem uso de Spark. Introdução do case.

---

## Célula 2 (Código): Inicialização da sessão

1. Cria `SparkSession` com `SparkSession.builder`.
2. Define parâmetros de execução local e memória (`driver` e `executor`).
3. Configura particionamento e otimizações SQL (`shuffle.partitions`, broadcast threshold, Arrow).
4. Ajusta nível de log do contexto Spark para `ERROR`.

---

## Célula 3 (Código): Leitura de dados e persistência

1. Define schemas explícitos com `StructType`/`StructField` para clientes e pedidos.
2. Lê JSON com `spark.read.schema(...).json(...)`.
3. Reparte dados opcionalmente via `repartition(min_partitions)`.
4. Persiste DataFrames com `StorageLevel.MEMORY_AND_DISK`.
5. Inspeciona o número de partições com `rdd.getNumPartitions()`.

---

## Célula 4 (Código): Inspeção inicial de clientes

1. Executa `summary('count')` para perfil básico.
2. Exibe schema efetivo com `printSchema()`.

---

## Célula 5 (Código): Inspeção inicial de pedidos

1. Executa `summary()` e `printSchema()` em pedidos.
2. Conta pedidos por cliente com `groupBy('client_id').count()`.
3. Ordena por maior volume com `orderBy(desc('count'))`.

---

## Célula 6 (Markdown)

Sem uso de Spark. Introduz o relatório de qualidade.

---

## Célula 7 (Código): Regras de qualidade de dados

1. Função `regra_falha` padroniza o output (`id`, `motivo`, `ordem_regra`).
2. Regra de duplicidade por janela (`count(id) over partitionBy(id)`), seguida de filtro `count > 1`.
3. Regra de valor nulo (`value is null`).
4. Regra de cliente inexistente com `left_anti join` contra base de clientes (`broadcast`).
5. Regras simples em uma única varredura (`id_nulo`, `client_id_nulo`, negativos e valor zero) via `when`.
6. Regra de retorno sem cobertura positiva com `groupBy(id)` e agregações condicionais usando `sum`, `when` e `abs`.
7. Regra de anomalia para `client_id == 123456`.
8. Consolidação das falhas com `unionByName(...).distinct()`.
9. Geração do relatório final por pedido (`groupBy`, `collect_list`, `sort_array`, `concat_ws`).
10. Geração da contagem por categoria (`groupBy('motivo').count()`).

---

## Célula 8 (Código): Pipeline de pedidos válidos (sem outlier)

1. Calcula IDs com retorno sem cobertura positiva (`ids_retornos_invalidos`).
2. Filtra base com regras de validade: `value > 0`, IDs e `client_id` não nulos e `>= 0`, excluindo `client_id == 123456`.
3. Identifica duplicidades apenas entre registros válidos.
4. Mantém apenas pedidos válidos com:
   - `left_anti` para remover duplicados;
   - `left_anti` para remover retornos inválidos;
   - `inner join` para garantir existência do cliente.
5. Persiste `pedidos_validos_df` para reuso nas análises.
6. Materializa contagens de total e válidos.

---

## Célula 9 (Markdown)

Explicação de negócio: diferença entre total de erros reportados e diferença entre total bruto e total válido.

---

## Célula 10 (Código): Agregação por cliente

1. Agrega `pedidos_validos_df` por `client_id` com `count` e `sum(value)`.
2. Enriquece com nome do cliente via `inner join` com `broadcast`.
3. Renomeia colunas para formato final e ordena por `valor_total`.
4. Persiste resultado em `cliente_totals_df`.

---

## Célula 11 (Código): Métricas estatísticas

1. Calcula média de `valor_total` com `agg(mean)`.
2. Calcula P10, mediana (P50) e P90 com `approxQuantile`.

---

## Célula 12 (Código): Clientes acima da média

1. Filtra `cliente_totals_df` por `valor_total > media`.
2. Ordena por `valor_total` e `nome_cliente`.

---

## Célula 13 (Código): Média truncada

1. Filtra clientes entre P10 e P90.
2. Ordena e exibe a distribuição central.

---

## Célula 14 (Markdown)

Justificativa de negócio para tratar o cliente `123456` como outlier nas análises comparativas.

---

## Célula 15 (Código): Pipeline alternativo (com outlier)

1. Repete a lógica da célula 8 para retornos inválidos (`ids_retornos_invalidos_b`).
2. Mantém a mesma validação estrutural, mas sem excluir `client_id == 123456`.
3. Remove duplicados e retornos inválidos com `left_anti`.
4. Valida cliente existente com `inner join`.
5. Persiste resultado em `pedidos_validos_df_b`.

---

## Célula 16 (Código): Diagnóstico de repetição do outlier

1. Filtra pedidos válidos do cliente `123456`.
2. Agrupa por `value` para medir repetição de valores.
3. Mantém apenas valores com repetição > 1.

---

## Célula 17 (Código): Comparação de volume com/sem outlier

1. Compara `count()` entre `pedidos_validos_df` (sem outlier) e `pedidos_validos_df_b` (com outlier).

---

## Célula 18 (Código): Comparação estatística com/sem outlier

1. Define função `calcular_metricas(df_pedidos)` para consolidar por cliente e calcular média/quantis.
2. Executa a função para os dois cenários (`com` e `sem` outlier).
3. Exibe diferenças de média, mediana, P10 e P90.

---

## Resumo dos recursos Spark usados

- Leitura e schema: `spark.read`, `StructType`, `StructField`.
- Transformações: `select`, `filter`, `withColumn`, `when`, `alias`.
- Janela: `Window.partitionBy` com `count().over(...)`.
- Agregações: `groupBy`, `agg`, `count`, `sum`, `mean`, `collect_list`.
- Estatística: `approxQuantile`.
- Joins: `inner`, `left_anti`, com `broadcast`.
- Persistência/performance: `persist(StorageLevel.MEMORY_AND_DISK)`, `repartition`.
- Ações: `show`, `count`, `collect`, `first`, `printSchema`, `summary`.
