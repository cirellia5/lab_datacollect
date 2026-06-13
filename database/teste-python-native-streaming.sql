-- =====================================================================
-- 1. [C]REATE - OPERAÇÃO: INSERT (Gera flag 'c' no Debezium)
-- =====================================================================
-- Simula a criação de uma nova transação financeira de débito.
-- O terminal do Jupyter deve capturar instantaneamente com a tag (INSERT).

INSERT INTO transacoes_financeiras (
    conta_origem, 
    conta_destino, 
    tipo_movimento, 
    valor, 
    data_transacao, 
    hash_auditoria
) VALUES (
    'conta-filipe-888', 
    'conta-destino-999', 
    'DEBITO', 
    850.50, 
    CURRENT_TIMESTAMP, 
    'sha256_hash_original_da_transacao_12345'
);


-- =====================================================================
-- 2. [U]PDATE - OPERAÇÃO: UPDATE (Gera flag 'u' no Debezium)
-- =====================================================================
-- Embora Ledger seja imutável, forçamos um UPDATE para testar a esteira.
-- Vamos simular uma atualização no hash de auditoria da transação criada acima.
-- O terminal do Jupyter deve capturar a mudança exibindo a tag (UPDATE).

UPDATE transacoes_financeiras 
SET hash_auditoria = 'sha256_hash_MODIFICADO_pelo_sistema_67890'
WHERE conta_origem = 'conta-filipe-777';


-- =====================================================================
-- 3. [D]ELETE - OPERAÇÃO: DELETE (Gera flag 'd' no Debezium)
-- =====================================================================
-- Simula uma tentativa de fraude ou erro de exclusão física no banco.
-- O dado sumirá do Postgres, mas o script Python pegará o estado anterior 
-- via 'payload.before' e disparará o print: 🚨 [ALERTA] Transação DELETADA...

DELETE FROM transacoes_financeiras 
WHERE conta_origem = 'conta-filipe-777';