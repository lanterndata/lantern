SELECT EXISTS (
    SELECT FROM 
        pg_tables
    WHERE  
        tablename  = 'small_world'
    );
