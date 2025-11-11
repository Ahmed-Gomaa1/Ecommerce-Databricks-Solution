# etl_pipeline/writer.py

def write_delta_table(df, database_name, table_name, mode="overwrite"):
    """
    Writes a DataFrame to a Delta table.
    
    Args:
        df: The DataFrame to write.
        database_name: The name of the target database.
        table_name: The name of the target table.
        mode: The write mode ('overwrite', 'append', etc.).
    """
    full_table_name = f"{database_name}.{table_name}"
    print(f"Writing DataFrame to Delta table: {full_table_name} in mode '{mode}'...")
    
    df.write.format("delta").mode(mode).saveAsTable(full_table_name)
    
    print(f"Successfully wrote to {full_table_name}.")