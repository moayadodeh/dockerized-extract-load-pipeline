Select --specify your columns
From {table_name}
where updated_date >= {curr_date} 
Order by updated_date DESC
Limit 1