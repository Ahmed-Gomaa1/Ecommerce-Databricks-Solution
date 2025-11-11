# etl_pipeline/transformers.py

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    from_unixtime, to_date, count, countDistinct, 
    sum as _sum, max as _max, desc, when, rank, col, first
)

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

    # --- Join with Category Tree & Select Final Columns ---
    print("Step C.1: Joining with category data...")
    
    # Define the list of columns we want to keep to avoid errors with missing pivoted columns
    final_silver_columns = [
        "s.timestamp", "s.visitorid", "s.event", "s.itemid", "s.transactionid",
        "s.item_category_id", col("c.parentid").alias("parent_category_id")
    ]
    
    # Dynamically add pivoted columns if they exist in the DataFrame after the pivot
    if 'available' in pivoted_df.columns:
        final_silver_columns.append(col("s.available"))
    if '770' in pivoted_df.columns:
        final_silver_columns.append(col("s.770").alias("price_property"))

    silver_df = pivoted_df.alias("s").join(
        category_df.alias("c"),
        col("s.item_category_id") == col("c.categoryid"),
        "left"
    ).select(final_silver_columns)
    
    print("Silver layer transformation complete.")
    return silver_df

def create_gold_layers(silver_df):
    """
    Creates various business-focused Gold DataFrames from the Silver layer.
    This version is compatible with serverless compute (no caching).
    
    Args:
        silver_df: The enriched Silver DataFrame.
        
    Returns:
        A dictionary of Gold DataFrames, keyed by their intended table name.
    """
    print("Starting Gold layer aggregations...")
    
    # --- Gold Table 1: Daily Sales by Category ---
    daily_sales = silver_df \
        .filter((col("event") == "transaction") & col("item_category_id").isNotNull()) \
        .withColumn("date", to_date(from_unixtime(col("timestamp") / 1000))) \
        .groupBy("date", "item_category_id", "parent_category_id") \
        .agg(count("transactionid").alias("number_of_transactions")) \
        .orderBy(desc("date"), desc("number_of_transactions"))
    print("Created daily_sales_by_category_gold.")

    # --- Gold Table 2: Top 100 Most Viewed Items ---
    top_viewed_items = silver_df \
        .filter((col("event") == "view") & (col("available") == 1)) \
        .groupBy("itemid", "item_category_id") \
        .count() \
        .withColumnRenamed("count", "total_views") \
        .orderBy(desc("total_views")) \
        .limit(100)
    print("Created top_viewed_items_gold.")

    # --- Gold Table 3: Fact Table - Detailed Sales Transactions ---
    if "price_property" not in silver_df.columns:
        print("Warning: price_property column not found. Skipping fact_sales and customer_segments tables.")
        fact_sales_transactions = None
        customer_segments = None
    else:
        fact_sales_transactions = silver_df \
            .filter(col("event") == "transaction") \
            .withColumn("date", to_date(from_unixtime(col("timestamp") / 1000))) \
            .select(
                "transactionid", "date", "timestamp", "visitorid", "itemid",
                col("price_property").cast("decimal(10, 2)").alias("price"),
                "item_category_id", "parent_category_id"
            )
        print("Created fact_sales_transactions_gold.")
        
        # --- Gold Table 4: High-Value Customer Segments ---
        customer_kpis_df = fact_sales_transactions \
            .groupBy("visitorid") \
            .agg(
                countDistinct("transactionid").alias("number_of_transactions"),
                _sum("price").alias("total_spend"),
                _max("timestamp").alias("last_purchase_timestamp")
            )
        
        window_spec = Window.orderBy(col("total_spend").desc())
        
        customer_segments = customer_kpis_df \
            .withColumn("rank", rank().over(window_spec)) \
            .withColumn("segment", 
                when(col("rank") <= 500, "Top 500 Spenders")
                .when(col("rank") <= 5000, "Top 5000 Spenders")
                .otherwise("Other")
            ) \
            .orderBy(desc("total_spend"))
        print("Created customer_segments_gold.")

    # --- Gold Table 5: User Conversion Funnel ---
    item_views = silver_df.filter(col("event") == "view").groupBy("itemid").agg(countDistinct("visitorid").alias("unique_views"))
    item_addtocarts = silver_df.filter(col("event") == "addtocart").groupBy("itemid").agg(countDistinct("visitorid").alias("unique_addtocarts"))
    item_transactions = silver_df.filter(col("event") == "transaction").groupBy("itemid").agg(countDistinct("visitorid").alias("unique_purchases"))
    
    conversion_funnel = item_views.join(item_addtocarts, "itemid", "left").join(item_transactions, "itemid", "left").na.fill(0)
    conversion_funnel = conversion_funnel.withColumn(
        "view_to_cart_rate",
        when(col("unique_views") > 0, col("unique_addtocarts") / col("unique_views")).otherwise(0)
    ).withColumn(
        "cart_to_purchase_rate",
        when(col("unique_addtocarts") > 0, col("unique_purchases") / col("unique_addtocarts")).otherwise(0)
    ).orderBy(desc("unique_views"))
    print("Created conversion_funnel_gold.")

    print("Gold layer aggregations complete.")
    
    # Build the dictionary of DataFrames to return, skipping any that failed to create
    gold_dfs = {
        "daily_sales_by_category_gold": daily_sales,
        "top_viewed_items_gold": top_viewed_items,
        "conversion_funnel_gold": conversion_funnel
    }
    if fact_sales_transactions is not None:
        gold_dfs["fact_sales_transactions_gold"] = fact_sales_transactions
    if customer_segments is not None:
        gold_dfs["customer_segments_gold"] = customer_segments
        
    return gold_dfs