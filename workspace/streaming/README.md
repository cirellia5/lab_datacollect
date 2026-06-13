# Fluxo Contínuo de Dados (Streaming e CDC)

Este diretório contém a infraestrutura e os scripts necessários para a realização dos laboratórios práticos focados em **Ingestão Contínua de Dados** e **Change Data Capture (CDC)**.

O objetivo principal deste módulo é demonstrar como capturar alterações em um banco de dados transacional relacional (OLTP) em tempo real, transmitir esses eventos de forma segura através de uma plataforma de mensageria distribuída e persistir os dados em formato de micro-batches dentro de um Object Storage (Data Lake), utilizando apenas **Python Nativo** e componentes especializados de infraestrutura.

---

## Arquitetura dos Serviços Envolvidos

O ecossistema é composto por 4 serviços principais que rodam isolados em contêineres Docker, comunicando-se através de uma rede interna mapeada:

1. **PostgreSQL (`ldc_postgres`):**
   * **Papel:** Banco de dados de origem (OLTP).
   * **Configuração Técnica:** Configurado com `wal_level=logical`. O *Write-Ahead Logging* (WAL) no modo lógico instrui o Postgres a salvar um histórico detalhado e estruturado de todas as alterações de dados (`INSERT`, `UPDATE`, `DELETE`) diretamente em disco, permitindo que ferramentas externas consumam esse fluxo sem onerar a performance de consultas da aplicação.

2. **Debezium Connect (`ldc_debezium`):**
   * **Papel:** Motor de captura de mudanças (CDC).
   * **Configuração Técnica:** Conecta-se ao slot de replicação lógica do PostgreSQL usando o plugin nativo `pgoutput`. O Debezium monitora o WAL do banco continuamente e, a cada alteração detectada, encapsula o estado anterior (`before`) e o estado posterior (`after`) do registro em um payload JSON estruturado, enviando-o imediatamente para o tópico correspondente no Kafka.

3. **Apache Kafka (`ldc_kafka` - Modo KRaft):**
   * **Papel:** Plataforma distribuída de streaming de eventos e mensageria.
   * **Configuração Técnica:** Roda no modo moderno **KRaft (Kafka Raft metadata mode)**, eliminando totalmente a dependência do Apache Zookeeper. Atua como um buffer altamente resiliente, recebendo os eventos de CDC e disponibilizando-os em tópicos estruturados para consumo imediato ou assíncrono.

4. **MinIO (`ldc_minio`):**
   * **Papel:** Object Storage (Simulador de AWS S3).
   * **Configuração Técnica:** Destino final (*Sink*) da esteira de dados. Armazena os dados brutos que chegam continuamente do pipeline de streaming, simulando a camada *Bronze/Raw* de um Data Lake corporativo.

---

## Pré-requisitos: Configuração Obrigatória no PostgreSQL

> **Execute estes comandos no banco ANTES de iniciar o Debezium.** Sem essa configuração, eventos de `UPDATE` e `DELETE` serão capturados de forma incompleta e o pipeline não terá comportamento confiável.

Conecte-se ao banco via DBeaver ou qualquer cliente SQL e execute:

```sql
-- 1. Cria a Publication explícita para a tabela monitorada.
-- Sem isso, o Debezium cria automaticamente uma publication para ALL TABLES,
-- desperdiçando recursos do WAL com tabelas que não fazem parte do pipeline.
CREATE PUBLICATION dbz_publication
FOR TABLE public.transacoes_financeiras;

-- 2. Configura o REPLICA IDENTITY para captura completa.
-- Por padrão (DEFAULT), o Debezium só inclui a PK no campo "before" de eventos
-- de UPDATE e DELETE. Com FULL, o registro inteiro fica disponível para auditoria.
ALTER TABLE public.transacoes_financeiras REPLICA IDENTITY FULL;
```

Valide que foram criados corretamente:

```sql
SELECT pubname FROM pg_publication;
SELECT slot_name, plugin FROM pg_replication_slots;
```

---

## Propósito dos Arquivos do Laboratório

### `configura_cdc_debezium.ipynb`
* **Tipo:** Jupyter Notebook (Interação com API REST).
* **Propósito Técnico:** Automatiza o provisionamento do pipeline de captura via API REST do Debezium (porta `8083`).
* **O que ele faz:**
  * Estabelece a conexão do conector com as credenciais do Postgres interno.
  * Ativa filtros de isolamento (`table.include.list`) para escutar exclusivamente a tabela `public.transacoes_financeiras`.
  * Aponta para a publication criada manualmente (`publication.name: dbz_publication`) com `autocreate.mode: disabled`, garantindo que apenas a tabela configurada seja replicada no WAL.

> **Nota sobre `schemas.enable`:** O conector **não desativa** o envelope de schema (`schemas.enable`). Isso é intencional — o consumer Python espera receber o payload dentro da estrutura `envelope.payload`, que é o formato padrão do Debezium com o `JsonConverter`. Desativar o schema altera a estrutura da mensagem e causa falha silenciosa no consumer.

### `python_streaming_transacoes_financeiras.ipynb`
* **Tipo:** Jupyter Notebook (Mecanismo Consumidor e Processador).
* **Propósito Técnico:** Implementação em Python Nativo de um consumer de streaming para o ledger financeiro, sem dependência de motores como Spark Structured Streaming.
* **Componentes Críticos Implementados:**
  * **Loop de Polling Infinito:** Utiliza `consumer.poll()` para buscar blocos de dados continuamente do Kafka de forma assíncrona.
  * **Estratégia de Micro-Batch:** Mantém um buffer em memória e realiza flush no MinIO apenas quando uma das condições for atingida: **5 mensagens acumuladas** OU **10 segundos de tempo decorrido**. Isso evita o *Small File Problem* em Data Lakes.
  * **Tratamento completo de operações CDC:** O consumer processa todos os tipos de evento gerados pelo Debezium:
    * `op=r` — Snapshot inicial (leitura dos registros existentes na primeira execução)
    * `op=c` — INSERT no banco de origem
    * `op=u` — UPDATE no banco de origem
    * `op=d` — DELETE no banco de origem (recupera dados do campo `before`)
  * **Desligamento Gracioso (Graceful Shutdown):** Bloco `try-except KeyboardInterrupt` que força o flush do buffer antes de encerrar, garantindo **Zero Data Loss**.

---

## Fluxo de Execução do Laboratório Prático

### Passo 1 — Configuração do banco (única vez)

Execute os comandos SQL da seção **Pré-requisitos** no DBeaver antes de qualquer outra coisa.

### Passo 2 — Ativação do CDC

Abra e execute as células do notebook `configura_cdc_debezium.ipynb`. Valide que o retorno da API foi o código HTTP `201`.

### Passo 3 — Inicialização do Consumer

Abra o notebook `python_streaming_transacoes_financeiras.ipynb` e execute todas as células em ordem. O script entrará em modo de monitoramento contínuo exibindo:

```
Pipeline de Streaming Financeiro Ativo!
Aguardando eventos do Postgres/Debezium...
```

> **Snapshot Inicial:** Na primeira execução, o Debezium vai fotografar todos os registros já existentes na tabela e enviá-los ao Kafka com `op=r` (read). O consumer vai processar esses registros marcando-os como `SNAPSHOT` antes de entrar no modo de captura em tempo real. Esse comportamento é normal e esperado.

### Passo 4 — Simulação de Carga Transacional

Com o consumer rodando, execute comandos DML no DBeaver para testar a reatividade da esteira:

**Cenário de Inclusão (INSERT):**
```sql
INSERT INTO transacoes_financeiras
    (conta_origem, conta_destino, tipo_movimento, valor, data_transacao, hash_auditoria)
VALUES
    ('CONTA001', 'CONTA002', 'CREDITO', 150.0000, NOW(), md5(random()::text));
```
Observe o terminal exibir imediatamente o indicador `[TRANSAÇÃO]` com a tag `(INSERT)`.

**Cenário de Alteração (UPDATE):**
```sql
UPDATE transacoes_financeiras
SET valor = 200.0000
WHERE conta_origem = 'CONTA001';
```
O CDC captura a mutação em milissegundos e exibe o registro com a tag `(UPDATE)`.

**Cenário de Exclusão (DELETE):**
```sql
DELETE FROM transacoes_financeiras
WHERE conta_origem = 'CONTA001';
```
Mesmo após o registro sumir do banco, o terminal intercepta o evento exibindo `[ALERTA] Transação DELETADA na origem!` com os dados completos do `before` — possível graças ao `REPLICA IDENTITY FULL` configurado no Passo 1.

### Passo 5 — Validação do Data Lake

Acesse a interface web do MinIO em `http://localhost:9001` com as credenciais:

* **Usuário:** `root-minio`
* **Senha:** `root12345678`

Navegue até o bucket `raw` → pasta `streaming/financeiro/` e verifique os arquivos `.jsonl` gerados automaticamente pelo pipeline.

---

## Troubleshooting

**Consumer conecta mas não exibe nenhuma mensagem:**
O grupo de consumer pode ter o offset gravado no fim do tópico. Pare o kernel do Jupyter e execute:
```bash
docker exec ldc_kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group grupo-consumer \
  --topic cdc.public.transacoes_financeiras \
  --reset-offsets --to-earliest --execute
```

**Verificar se o tópico tem mensagens:**
```bash
docker exec ldc_kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic cdc.public.transacoes_financeiras \
  --from-beginning --max-messages 3
```

**Verificar status do conector Debezium:**
```bash
curl http://localhost:8083/connectors/postgres-cdc-ecommerce/status
```
