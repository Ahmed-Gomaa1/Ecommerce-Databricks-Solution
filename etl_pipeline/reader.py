# etl_pipeline/reader.py

def read_source_data(spark, source_path):
    """
    Reads all source CSV files from the specified volume path.
    Combines the two-part item properties files into a single DataFrame.
    
    Args:
        spark: The SparkSession object.
        source_path: The base path of the volume (e.g., '/Volumes/workspace/e-commerce_data/csv_files/').
        
    Returns:
        A dictionary containing the three source DataFrames: 'events', 'properties', and 'categories'.
    """
    print(f"Reading source data from: {source_path}")

    # Define the full paths to each file
    events_path = f"{source_path}/events.csv"
    category_tree_path = f"{source_path}/category_tree.csv"
    item_properties_part1_path = f"{source_path}/item_properties_part1.csv"
    item_properties_part2_path = f"{source_path}/item_properties_part2.csv"
    
    # Read the single-file DataFrames
    events_df = spark.read.csv(events_path, header=True, inferSchema=True)
    category_df = spark.read.csv(category_tree_path, header=True, inferSchema=True)
    
    # Read both parts of the item properties data
    properties_df_part1 = spark.read.csv(item_properties_part1_path, header=True, inferSchema=True)
    properties_df_part2 = spark.read.csv(item_properties_part2_path, header=True, inferSchema=True)
    
    # Combine the two properties DataFrames into one
    properties_df = properties_df_part1.unionByName(properties_df_part2)
    
    print("Successfully read and combined source files.")
    
    return {
        "events": events_df,
        "properties": properties_df,
        "categories": category_df
    }