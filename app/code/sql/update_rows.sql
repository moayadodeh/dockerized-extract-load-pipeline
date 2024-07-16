Update {table_name} 
--SET YOUR UPDATE COLUMNS
-- SET column = {table_name_temp}.column,
-- ||
-- ||
-- ||
FROM {table_name_temp}
WHERE {table_name}.consultation_id = {table_name_temp}.consultation_id 

