-- Cria a Publication explícita para a tabela monitorada.
-- Sem isso, o Debezium cria automaticamente uma publication para ALL TABLES,
-- desperdiçando recursos do WAL com tabelas que não fazem parte do pipeline.
CREATE PUBLICATION dbz_publication
FOR TABLE public.transacoes_financeiras;

-- Configura o REPLICA IDENTITY para captura completa.
-- Por padrão (DEFAULT), o Debezium só inclui a PK no campo "before" de eventos
-- de UPDATE e DELETE. Com FULL, o registro inteiro fica disponível para auditoria.
ALTER TABLE public.transacoes_financeiras REPLICA IDENTITY FULL;

-- Valida configurações do banco de dados
SELECT pubname FROM pg_publication;
SELECT slot_name, plugin FROM pg_replication_slots;
