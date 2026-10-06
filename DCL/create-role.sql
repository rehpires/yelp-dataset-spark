-- Depende primeiro da criação da role pelo CLI e/ou menu de settings.

CREATE OR REPLACE FUNCTION yelp_dataset.governance.mask_cpf(cpf STRING)
RETURNS STRING
COMMENT 'Mascara o CPF para roles.'
RETURN CASE
  WHEN is_account_group_member('data_analyst') OR is_member('data_analyst')
    OR is_account_group_member('business_analyst') OR is_member('business_analyst')
    THEN concat('***.***.***-', right(cpf, 2))
  ELSE cpf
END;
