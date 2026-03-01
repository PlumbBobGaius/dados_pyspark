# Case.ipynb - Documentação por Uso de Spark

Documentação atualizada para descrever **cada uso de Spark** no notebook, em vez de linha a linha.

---

## Célula 1 (Markdown)

Sem uso de Spark. Apenas título e descrição do case.

---

## Célula 2 (Código): Inicialização do ambiente Spark

### Usos de Spark nesta célula

1. **Criação da SparkSession (`SparkSession.builder ... .getOrCreate()`)**
   - Inicializa a sessão principal para executar operações distribuídas.

2. **Configuração de execução distribuída**
   - `.master("local[*]")`: usa todos os núcleos locais.
   - `.config("spark.driver.memory", "4g")`: memória do driver.
   - `.config("spark.executor.memory", "4g")`: memória dos executores.
   - `.config("spark.sql.shuffle.partitions", "200")`: partições padrão em shuffles.
   - `.config("spark.sql.autoBroadcastJoinThreshold", "10485760")`: limite de auto-broadcast.
   - `.config("spark.sql.execution.arrow.pyspark.enabled", "true")`: otimização Arrow.
   - `.config("spark.default.parallelism", "8")`: paralelismo padrão.

3. **Controle de logs do Spark (`spark.sparkContext.setLogLevel("ERROR")`)**
   - Reduz ruído no output mantendo apenas erros.

---

## Célula 3 (Código): Leitura dos dados e persistência

### Usos de Spark nesta célula

1. **Definição de schemas Spark (`StructType`, `StructField`)**
   - Cria schemas explícitos para `clientes` e `pedidos`.
   - Evita inferência de schema e melhora previsibilidade/performance.

2. **Leitura de JSON com Spark (`spark.read...json(path)`)**
   - Usa `.schema(schema)` para tipagem explícita.
   - Usa `.option("multiLine", "false")` para JSONL.
   - Usa `.option("mode", "PERMISSIVE")` para tolerar linhas problemáticas.

3. **Reparticionamento (`df.repartition(min_partitions)`)**
   - Aumenta paralelismo quando o número de partições estiver abaixo do mínimo definido.

4. **Persistência (`df.persist(StorageLevel.MEMORY_AND_DISK)`)**
   - Mantém DataFrames de `clientes` e `pedidos` cacheados para reuso eficiente.

5. **Inspeção de partições (`df.rdd.getNumPartitions()`)**
   - Verifica distribuição física dos dados em partições.

---

## Célula 4 (Código): Profiling inicial de clientes

### Usos de Spark nesta célula

1. **Ação de resumo (`clientes_df.summary('count').show()`)**
   - Calcula métricas resumidas e materializa no output.

2. **Inspeção de schema (`clientes_df.printSchema()`)**
   - Mostra tipos Spark efetivos das colunas.

---

## Célula 5 (Código): Profiling de pedidos e frequência por cliente

### Usos de Spark nesta célula

1. **Resumo e schema de pedidos**
   - `pedidos_df.summary().show()` e `pedidos_df.printSchema()`.

2. **Agregação distribuída (`groupBy("client_id").count()`)**
   - Conta pedidos por cliente.

3. **Ordenação distribuída (`orderBy`)**
   - Ordena clientes pelo maior volume de pedidos.

4. **Ação de exibição (`show`)**
   - Materializa o top 20 de frequência.

---

## Célula 6 (Markdown)

Sem uso de Spark. Apenas introdução da seção de qualidade de dados.

---

## Célula 7 (Código): Regras de qualidade e consolidação

### Usos de Spark nesta célula

1. **Padronização de saída com DataFrame API (`select`, `lit`, `alias`)**
   - A função `regra_falha` cria estrutura comum: `id`, `motivo`, `ordem_regra`.

2. **Regras de validação por filtros (`filter`)**
   - Detecta: valor nulo, id nulo, client_id nulo, id/client_id negativos, valor zero.

3. **Detecção de duplicidade (`groupBy("id").count().filter(count > 1)`)**
   - Identifica IDs de pedido repetidos.

4. **Validação referencial com join anti (`left_anti`) + `broadcast`**
   - Marca pedidos cujo `client_id` não encontra correspondência em clientes.

5. **Regra de retorno inválido com agregações condicionais**
   - `when`, `sum`, `abs` para comparar cobertura de valor positivo sobre negativo por `id`.

6. **Regra de anomalia para cliente 123456**
   - Filtra diretamente pedidos com `client_id == 123456` e marca como anomalia.

7. **União de regras (`reduce` + `unionByName`)**
   - Consolida todos os DataFrames de falhas em um único conjunto.

8. **Deduplicação e consolidação por pedido**
   - `dropDuplicates(["id", "motivo"])` evita repetição de motivos.
   - `groupBy("id")` + `collect_list` + `array_sort` + `concat_ws` concatena motivos.
   - `min("ordem_regra")` permite ordenar por prioridade.

9. **Ações finais (`show`, `count`)**
   - Exibe relatório principal de falhas.
   - Gera visão por categoria de erro.
   - Mostra total de registros com falha.

---

## Célula 8 (Código): Construção da base válida (sem outlier)

### Usos de Spark nesta célula

1. **Filtragem da base de pedidos válidos (`select` + `filter`)**
   - Mantém registros com `value > 0`, IDs válidos e exclui `client_id == 123456`.

2. **Detecção de IDs duplicados válidos (`groupBy` + `count`)**
   - Identifica duplicidades somente após os filtros de validade.

3. **Validação de cliente existente (`distinct` + join)**
   - Cria referência de clientes válidos e aplica `inner join`.

4. **Exclusão de duplicados com `left_anti`**
   - Remove pedidos cujo ID aparece duplicado entre os registros válidos.

5. **Otimização com `broadcast`**
   - Usa broadcast tanto no anti-join quanto no join de referência de clientes.

6. **Persistência da base limpa (`persist`)**
   - Mantém `pedidos_validos_df` em memória/disco para reuso.

7. **Ações de contagem (`count`)**
   - Calcula volume total e volume válido para comparação.

---

## Célula 9 (Código): Agregação por cliente

### Usos de Spark nesta célula

1. **Agregação principal (`groupBy("client_id").agg(...)`)**
   - Calcula `qtd_pedidos` e `valor_total` por cliente.

2. **Join enriquecedor com cadastro de clientes (`inner join` + `broadcast`)**
   - Associa nome do cliente aos totais agregados.

3. **Projeção e renomeação (`select`, `alias`)**
   - Entrega colunas finais no formato de negócio.

4. **Ordenação (`orderBy`)**
   - Ordena por maior valor total e nome.

5. **Persistência (`persist`) e ação (`show`)**
   - Persiste para etapas estatísticas seguintes e exibe resultado.

---

## Célula 10 (Código): Métricas estatísticas

### Usos de Spark nesta célula

1. **Cálculo de média (`agg(mean)`)**
   - Obtém a média de `valor_total` por cliente.

2. **Cálculo de quantis (`approxQuantile`)**
   - Calcula P10, mediana (P50) e P90 com erro relativo configurado.

3. **Ação de coleta (`collect`)**
   - Traz a média para o driver para uso em filtros posteriores.

---

## Célula 11 (Código): Filtro acima da média

### Usos de Spark nesta célula

1. **Filtro analítico (`filter(valor_total > media)`)**
   - Seleciona clientes acima da média global.

2. **Ordenação e exibição (`orderBy` + `show`)**
   - Entrega ranking dos clientes acima da média.

---

## Célula 12 (Código): Média truncada (P10 a P90)

### Usos de Spark nesta célula

1. **Filtro por intervalo estatístico (`filter` com limites P10/P90)**
   - Mantém clientes na faixa central da distribuição.

2. **Ordenação e exibição (`orderBy` + `show`)**
   - Exibe clientes da média truncada ordenados por valor.

---

## Célula 13 (Markdown)

Sem uso de Spark. É a justificativa metodológica para tratar o cliente 123456 como outlier.

---

## Célula 14 (Código): Base alternativa (com outlier)

### Usos de Spark nesta célula

1. **Repetição do pipeline de limpeza com variação de regra**
   - Igual à célula 8, mas mantendo `client_id == 123456` (filtro está comentado).

2. **Detecção e remoção de duplicidades válidas**
   - `groupBy/count` + `left_anti` para exclusão de IDs duplicados.

3. **Validação de cliente existente e otimização com broadcast**
   - `inner join` com tabela de clientes válidos.

4. **Persistência da base alternativa (`persist`)**
   - Cria `pedidos_validos_df_B` para comparar cenários.

5. **Ações de contagem (`count`)**
   - Materializa totais para diagnóstico.

---

## Célula 15 (Código): Diagnóstico de repetição de valores do outlier

### Usos de Spark nesta célula

1. **Filtro específico do cliente 123456 (`filter`)**
   - Isola os pedidos do outlier.

2. **Agregação por valor (`groupBy("value").count()`)**
   - Mede frequência de repetição de valores monetários.

3. **Filtro de repetição (`filter(qtd_repeticoes > 1)`)**
   - Mantém apenas valores repetidos.

4. **Ordenação e exibição (`orderBy` + `show`)**
   - Mostra os valores mais recorrentes.

---

## Célula 16 (Código): Comparação de volumes entre bases

### Usos de Spark nesta célula

1. **Ações de contagem (`count`) em duas bases**
   - Compara número de pedidos válidos com e sem outlier.

> Observação: o restante da célula é apenas formatação de saída (`print`) no driver.

---

## Célula 17 (Código): Comparação de métricas com e sem outlier

### Usos de Spark nesta célula

1. **Função de agregação reutilizável (`groupBy` + `sum`)**
   - Consolida valor total por cliente para qualquer DataFrame de pedidos.

2. **Conversão de tipo (`cast(DecimalType(11, 2))`)**
   - Padroniza precisão de `valor_total`.

3. **Cálculo de média (`agg(mean)`)**
   - Obtém média por cenário.

4. **Cálculo de quantis (`approxQuantile`)**
   - Obtém P10, mediana e P90 por cenário.

5. **Execução em dois cenários**
   - Com outlier: `pedidos_validos_df_B`.
   - Sem outlier: `pedidos_validos_df`.

> Observação: os `print`s finais apenas exibem no driver a comparação das métricas já calculadas pelo Spark.

---

## Resumo rápido dos principais recursos Spark usados no notebook

- **Leitura e schema**: `spark.read.json`, `StructType`, `StructField`.
- **Transformações**: `select`, `filter`, `withColumn` (indiretamente via expressões), `alias`.
- **Agregações**: `groupBy`, `agg`, `count`, `sum`, `mean`, `collect_list`.
- **Joins**: `inner`, `left_anti` com `broadcast`.
- **Qualidade de dados**: regras de validação com `when`, `abs`, filtros condicionais.
- **Persistência/performance**: `persist(StorageLevel.MEMORY_AND_DISK)`, `repartition`.
- **Ações**: `show`, `count`, `collect`, `first`, `printSchema`, `summary`.
