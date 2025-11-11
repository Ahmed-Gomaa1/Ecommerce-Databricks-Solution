# etl_pipeline/transformers.py

from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, to_date, count, countDistinct, sum as _sum, max as _max, desc, when, rank, col, first

def create_silver_layer(bronze_dfs):
    """
    Transforms raw bronze DataFrames into a single, enriched Silver DataFrame.
    
    Args:
        bronze_dfs: A dictionary of DataFrames from the reader ('events', 'properties', 'categories').
        
    Returns:
        The enriched Silver DataFrame.
    """
    print("Starting Silver layer transformation...")
    
    events_df = bronze_dfs["events"]
    properties_df = bronze_dfs["properties"]
    category_df = bronze_dfs["categories"]

    # --- Point-in-Time Join Logic ---
    print("Step A.1: Performing initial join...")
    joined_df = events_df.alias("e").join(
        properties_df.alias("p"),
        (col("e.itemid") == col("p.itemid")) & (col("e.timestamp") >= col("p.timestamp")),
        "left"
    )

    print("Step A.2: Ranking properties...")
    window_spec = Window.partitionBy("e.timestamp", "e.visitorid", "e.itemid", "e.event", "p.property").orderBy(col("p.timestamp").desc())
    ranked_properties_df = joined_df.withColumn("rank", rank().over(window_spec))
    correct_properties_df = ranked_properties_df.filter(col("rank") == 1)

    print("Step A.3: Selecting specific columns to resolve ambiguity...")
    base_events_with_properties = correct_properties_df.select(
        col("e.timestamp").alias("event_timestamp"),
        col("e.visitorid"),
        col("e.event"),
        col("e.itemid"),
        col("e.transactionid"),
        col("p.property"),
        col("p.value")
    )
    
    # --- Pivot Properties ---
    print("Step B.1: Pivoting properties...")
    pivoted_df = base_events_with_properties.groupBy("event_timestamp", "visitorid", "event", "itemid", "transactionid") \
                                            .pivot("property") \
                                            .agg(first("value")) \
                                            .withColumnRenamed("event_timestamp", "timestamp")

    if 'categoryid' in pivoted_df.columns:
        pivoted_df = pivoted_df.withColumnRenamed("categoryid", "item_category_id")

    # --- Join with Category Tree ---
    print("Step C.1: Joining with category data...")
    silver_df = pivoted_df.alias("s").join(
        category_df.alias("c"),
        col("s.item_category_id") == col("c.categoryid"),
        "left"
    ).select(
        "s.timestamp", "s.visitorid", "s.event", "s.itemid", "s.transactionid",
        "s.item_category_id", col("c.parentid").alias("parent_category_id"),
        col("s.available"), col("s.770").alias("price_property") # Alias the price column for clarity
    )
    
    print("Silver layer transformation complete.")
    return silver_df

def create_gold_layers(silver_df):
    """
    Creates various business-focused Gold DataFrames from the Silver layer.
    
    Args:
        silver_df: The enriched Silver DataFrame.
        
    Returns:
        A dictionary of Gold DataFrames, keyed by their intended table name.
    """
    print("Starting Gold layer aggregations...")
    silver_df.cache() # Cache for performance as it's used multiple times
    
    # Gold Table 1: Daily Sales by Category
    daily_sales = silver_df \
        .filter((col("event") == "transaction") & col("item_category_id").isNotNull()) \
        .withColumn("date", to_date(from_unixtime(col("timestamp") / 1000))) \
        .groupBy("date", "item_category_id", "parent_category_id") \
        .agg(count("transactionid").alias("number_of_transactions")) \
        .orderBy(desc("date"), desc("number_of_transactions"))

    # Gold Table 2: Conversion Funnel
    item_views = silver_df.filter(col("event") == "view").groupBy("itemid").agg(countDistinct("visitorid").alias("unique_views"))
    item_addtocarts = silver_df.filter(col("event") == "addtocart").groupBy("itemid").agg(countDistinct("visitorid").alias("unique_addtocarts"))
    item_transactions = silver_df.filter(col("event") == "transaction").groupBy("itemid").agg(countDistinct("visitorid").alias("unique_purchases"))
    
    conversion_funnel = item_views.join(item_addtocarts, "itemid", "left").join(item_transactions, "itemid", "left").na.fill(0)
    conversion_funnel = conversion_funnel.withColumn(
        "view_to_cart_rate",
        when(col("unique_views") > 0, col("unique_addtocarts") / col("unique_views")).otherwise(0)
    ).orderBy(desc("unique_views"))

    silver_df.unpersist() # Release the cache
    print("Gold layer aggregations complete.")

    return {
        "daily_sales_by_category_gold": daily_sales,
        "conversion_funnel_gold": conversion_funnel
    }