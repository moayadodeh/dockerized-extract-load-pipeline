INSERT INTO {table_name}

Select t2.* from {table_name_temp} AS t2

LEFT JOIN {table_name} On  {table_name}.consultation_id = t2.consultation_id
WHERE {table_name}.consultation_id IS NULL;