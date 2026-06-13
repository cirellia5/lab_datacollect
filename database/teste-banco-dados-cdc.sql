-- =====================================================================
-- SCRIPT DE AUDITORIA E VALIDAÇÃO DO POSTGRESQL PARA CDC
-- =====================================================================

-- 1. Verifica o nível de detalhamento do log de transações (WAL)
-- Para o Debezium/CDC funcionar, o retorno DEVE ser 'logical'. 
-- Isso instrui o Postgres a salvar no disco os dados reais que mudaram (antes/depois),
-- e não apenas referências binárias internas de páginas de disco.
SHOW wal_level;

-- 2. Verifica a quantidade de canais/processos de streaming permitidos
-- Define o número máximo de conexões simultâneas de envio de log que o banco aceita.
-- Cada conector de CDC ativo ou ferramenta de replicação consome 1 worker desse limite.
SHOW max_wal_senders;

-- 3. Verifica o limite de "grampos" (slots) de replicação que podem existir
-- O slot de replicação é o mecanismo que garante que o Postgres não vai apagar os logs
-- do disco antes que o Debezium tenha lido. Precisa ser maior que 0 (geralmente o padrão é 10).
SHOW max_replication_slots;

-- 4. Raio-X dos Slots de Replicação ativos no banco de dados
-- Essa consulta mostra se o Debezium conseguiu se registrar com sucesso no Postgres.
-- O que avaliar no resultado para os alunos:
--   - slot_name: O nome do "grampo" criado pelo conector.
--   - plugin: Deve ser 'pgoutput' (o motor de replicação lógica nativo do Postgres 15+).
--   - active: Deve ser 'true'. Se estiver 'false', o Debezium caiu ou foi desconectado.
SELECT 
    slot_name, 
    plugin, 
    slot_type, 
    active 
FROM pg_replication_slots;

-- 5. Verificação das Publicações de Replicação Lógica
-- Essa consulta mostra se a "publicação" (o mecanismo que define quais tabelas enviam dados) foi criada com sucesso.
-- O que avaliar no resultado para os alunos:
-- pubname: O nome da publicação. Deve corresponder exatamente ao nome configurado no Debezium (o padrão costuma ser 'dbz_publication'). Se o retorno vier vazio, o Debezium não chegou a criar a publicação por falta de permissão ou por falha na inicialização.
SELECT pubname FROM pg_publication;

