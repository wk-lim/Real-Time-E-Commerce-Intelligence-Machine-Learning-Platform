from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as functions

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("ecommerce-phase2-smoke")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        result = (
            spark.range(1,6)
            .withColumn(
                "doubled_id",
                functions.col("id") * 2,
            )
            .collect()
        )

        actual = [
            (row["id"], row["doubled_id"])
            for row in result
        ]
        expected = [
            (1, 2),
            (2, 4),
            (3, 6),
            (4, 8),
            (5, 10),
        ]

        if actual != expected:
            raise RuntimeError(
                f"Unexpected Spark result: {actual}"
            )

        print("Phase 2 Spark smoke test passed")
        print(f"Spark version: {spark.version}")
        print(f"Rows processed: {len(actual)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()