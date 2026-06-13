CREATE PUBLICATION dbz_publication 
FOR TABLE public.transacoes_financeiras;

ALTER TABLE public.transacoes_financeiras REPLICA IDENTITY FULL;

ALTER USER postgres REPLICATION;

SELECT pubname FROM pg_publication;
SELECT slot_name, plugin FROM pg_replication_slots;
