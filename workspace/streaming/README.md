# Fluxo Contínuo de Dados (Streaming e CDC)

Este diretório contém a infraestrutura e os scripts necessários para a realização dos laboratórios práticos focados em **Ingestão Contínua de Dados** e **Change Data Capture (CDC)**. 

O objetivo principal deste módulo é demonstrar como capturar alterações em um banco de dados transacional relacional (OLTP) em tempo real, transmitir esses eventos de forma segura através de uma plataforma de mensageria distribuída e persistir os dados em formato de micro-batches dentro de um Object Storage (Data Lake), utilizando apenas **Python Nativo** e componentes especializados de infraestrutura.

---

## Arquitetura dos Serviços Envolvidos

O ecossistema é composto por 4 serviços principais que rodam isolados em contêineres Docker, comunicando-se através de uma rede interna mapeada:

1. **PostgreSQL (`ldc_postgres`):**
   * **Papel:** Banco de dados de origem (OLTP).
   * **Configuração Técnica:** Configurado com `wal_level=logical`. O *Write-Ahead Logging* (WAL) no modo lógico instrui o Postgres a salvar um histórico detalhado e estruturado de todas as alterações de dados (`INSERT`, `UPDATE`, `DELETE`) diretamente em disco, permitindo que ferramentas externas consumam esse fluxo sem onerar a performance de consultas da aplicação com queries de `SELECT`.

2. **Debezium Connect (`ldc_debezium`):**
   * **Papel:** Motor de captura de mudanças (CDC).
   * **Configuração Técnica:** Conecta-se ao slot de replicação lógica do PostgreSQL usando o plugin nativo `pgoutput`. O Debezium monitora o WAL do banco continuamente e, a cada alteração detectada, encapsula o estado anterior (`before`) e o estado posterior (`after`) do registro em um payload JSON estruturado, enviando-o imediatamente para o tópico correspondente no Kafka.

3. **Apache Kafka (`ldc_kafka` - Modo KRaft):**
   * **Papel:** Plataforma distribuída de streaming de eventos e mensageria.
   * **Configuração Técnica:** Roda no modo moderno **KRaft (Kafka Raft metadata mode)**, eliminando totalmente a dependência do Apache Zookeeper. Ele atua como um buffer altamente resiliente, recebendo os eventos de CDC produzidos e disponibilizando-os em tópicos estruturados para consumo imediato ou assíncrono.

4. **MinIO (`ldc_minio`):**
   * **Papel:** Object Storage (Simulador de AWS S3).
   * **Configuração Técnica:** Destino final (*Sink*) da nossa esteira de dados. É estruturado para armazenar os dados brutos ou semi-processados que chegam continuamente da esteira de streaming, simulando a camada *Bronze/Raw* de um Data Lake corporativo.

---

## Propósito dos Arquivos do Laboratório

### `configura_cdc_debezium.ipynb`
* **Tipo:** Jupyter Notebook (Interação com API REST).
* **Propósito Técnico:** Este arquivo serve para automatizar o provisionamento do pipeline de captura. O Debezium expõe uma interface de gerenciamento via API REST na porta `8083`. Este script Python faz um disparo HTTP do tipo `POST` contendo o payload de configuração JSON. 
* **O que ele faz:**
  * Estabelece a conexão do conector com as credenciais do Postgres interno.
  * Ativa filtros de isolamento (`table.include.list`) para escutar exclusivamente a tabela de alta precisão **`public.transacoes_financeiras`**, ignorando tabelas de outros contextos.
  * Define as regras de conversão de tipos de dados e desativa metadados redundantes de esquemas (`schemas.enable: false`) para otimizar o tamanho das mensagens trafegadas no Kafka.

### `python_native_streaming_pedidos.ipynb`
* **Tipo:** Jupyter Notebook (Mecanismo Consumidor e Processador).
* **Propósito Técnico:** Substitui motores complexos de processamento distribuído (como Spark Structured Streaming) por uma implementação puramente baseada em Python Nativo para processar o ledger financeiro. Serve para demonstrar a lógica interna de um motor de processamento de fluxo contínuo.
* **Componentes Críticos Implementados:**
  * **Loop de Polling Infinito:** Utiliza o método `consumer.poll()` para buscar blocos de dados continuamente do Kafka de forma assíncrona, evitando travamentos de thread se o fluxo de mensagens diminuir.
  * **Estratégia Computacional de Micro-Batch:** Demonstra a solução para o clássico *Small File Problem* (Problema dos Arquivos Pequenos) em Data Lakes. Em vez de salvar um arquivo no MinIO para cada mensagem que chega, o script mantém uma lista em memória (*buffer*) e realiza um "Flush" (escrita em lote) apenas quando uma das duas condições limite for atingida: **5 mensagens acumuladas** OU **10 segundos de tempo decorrido**.
  * **Deserialização on-the-fly:** Realiza o parsing de bytes brutos do Kafka para estruturas nativas do Python (`dict`), extraindo o payload gerado pelo Debezium e enriquecendo-o com a flag customizada de auditoria (`tipo_operacao_cdc`: `INSERT`, `UPDATE` ou `AUDITORIA_DELETE`).
  * **Desligamento Gracioso (Graceful Shutdown):** Implementa um bloco de controle de exceção `try-except KeyboardInterrupt`. Se o usuário interromper a célula do Jupyter, o script intercepta o sinal, identifica se existem dados "órfãos" presos na memória do buffer, força a gravação final destes dados no MinIO para garantir **Zero Data Loss** (nenhuma perda de dados) e, somente após isso, fecha a conexão com o broker.

---

## Fluxo de Execução do Laboratório Prático

Para executar e validar a esteira completa durante as aulas práticas, siga os seguintes passos de maneira sequencial:

1. **Ativação do CDC:** Abra e execute as células de criação do notebook `configura_cdc_debezium.ipynb`. Valide se o retorno da API foi o código HTTP `201`, confirmando que o Debezium começou a escutar o banco.
2. **Inicialização do Consumidor:** Abra o notebook `python_native_streaming_pedidos.ipynb` e execute todas as células. O script entrará em estado de monitoramento contínuo, exibindo a mensagem: `Aguardando eventos do Postgres/Debezium...`.
3. **Simulação de Carga Transacional:** Vá até a sua IDE de banco de dados (DBeaver) e execute comandos DML para testar a reatividade da esteira em tempo real:
   * **Cenário de Inclusão (INSERT):** Crie uma nova transação (ex: movimento de `CREDITO` ou `DEBITO`). Observe o terminal do notebook capturar o evento instantaneamente, exibindo o indicador `[TRANSAÇÃO]` junto com o tipo de movimento, ID, valor e a tag `(INSERT)`.
   * **Cenário de Alteração (UPDATE):** Modifique algum valor dessa mesma transação no banco. Observe o conector de CDC capturar a mutação do dado em milissegundos, mostrando a evolução do registro com o sufixo `(UPDATE)`.
   * **Cenário de Exclusão (DELETE):** Apague a transação da tabela do banco de dados. Observe a mágica do CDC acontecer: mesmo que o registro tenha sumido fisicamente do banco de origem, o terminal do notebook interceptará o evento exibindo o alerta `[ALERTA] Transação DELETADA na origem!` e recuperará com sucesso os dados que existiam na linha antes da exclusão (lidos a partir do `before` e marcados como `AUDITORIA_DELETE`).
4. **Validação do Data Lake:** Insira mais registros para estourar o limite do buffer ou aguarde 10 segundos para acionar o timer de segurança. Acesse a interface web do MinIO (`http://localhost:9001`) e comprove a criação automática dos arquivos estruturados no padrão `.jsonl` dentro do bucket **`raw`**, sob o caminho de pastas **`streaming/financeiro/`**.