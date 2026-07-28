"""
scripts/benchmark_gold_queries.py

Simple before/after benchmark demonstrating the measurable impact of
partitioning and Z-ordering, run manually (not part of the pytest
suite - performance benchmarks are environment-sensitive and don't
belong in CI as pass/fail assertions, only as a documented,
reproducible measurement).
"""
import time

from src.common.spark_session import get_spark_session


def time_query(spark, description: str, query: str) -> float:
    start = time.time()
    spark.sql(query).collect()
    elapsed = time.time() - start
    print(f"{description}: {elapsed:.2f}s")
    return elapsed


def main():
    spark = get_spark_session()

    filtered_query = """
        SELECT store_sk, product_sk, SUM(stock_on_hand)
        FROM delta.`data/gold/fact_inventory_snapshot`
        WHERE snapshot_date = '2026-07-20'
        GROUP BY store_sk, product_sk
    """
    unfiltered_query = """
        SELECT store_sk, product_sk, SUM(stock_on_hand)
        FROM delta.`data/gold/fact_inventory_snapshot`
        GROUP BY store_sk, product_sk
    """

    time_query(spark, "Partition-pruned query (single date)", filtered_query)
    time_query(spark, "Full table scan (no date filter)", unfiltered_query)

    # Compare against an UNpartitioned copy to isolate the partitioning
    # effect specifically, not just "smaller result set."
    unpartitioned_path = "data/gold/fact_inventory_snapshot_unpartitioned_control"
    spark.read.format("delta").load("data/gold/fact_inventory_snapshot") \
        .write.format("delta").mode("overwrite").save(unpartitioned_path)  # no partitionBy

    control_query = f"""
        SELECT store_sk, product_sk, SUM(stock_on_hand)
        FROM delta.`{unpartitioned_path}`
        WHERE snapshot_date = '2026-07-20'
        GROUP BY store_sk, product_sk
    """
    time_query(spark, "Same filter, UNpartitioned control table", control_query)


if __name__ == "__main__":
    main()