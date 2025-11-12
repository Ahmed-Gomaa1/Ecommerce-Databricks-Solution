# etl_pipeline/writer.py

def write_delta_table(df, database_name, table_name, mode="overwrite"):
    
    full_table_name = f"{database_name}.{table_name}"
    print(f"Writing DataFrame to Delta table: {full_table_name} in mode '{mode}'...")
    
    df.write.format("delta").mode(mode).saveAsTable(full_table_name)
    
    print(f"Successfully wrote to {full_table_name}.")