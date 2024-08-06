update prod_dna_core.inditg_integration.itg_pos_offtake_fact
set key_account_name = upper(key_account_name)
where key_account_name <> upper(key_account_name)
