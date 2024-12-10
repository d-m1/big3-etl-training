import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, when, count, lit

BIG3 = ["Roger Federer", "Rafael Nadal", "Novak Djokovic"]

def filter_big3_matches(df):
    return df.filter(
        (col("winner_name").isin(BIG3)) | (col("loser_name").isin(BIG3))
    )

def calculate_win_percentage(df):
    win_counts = df.groupBy("winner_name").agg(count("*").alias("wins"))
    total_matches = df.select("winner_name", "loser_name").withColumn(
        "player_name", col("winner_name")
    ).union(
        df.select("winner_name", "loser_name").withColumn(
            "player_name", col("loser_name")
        )
    ).groupBy("player_name").agg(count("*").alias("total_matches"))
    win_percentage = win_counts.join(
        total_matches, win_counts["winner_name"] == total_matches["player_name"]
    ).select(
        col("player_name").alias("name"),
        (col("wins") / col("total_matches")).alias("win_percentage")
    )
    return win_percentage

def normalize_column(df, column_name):
    min_val = df.agg(min(col(column_name))).collect()[0][0]
    max_val = df.agg(max(col(column_name))).collect()[0][0]
    return df.withColumn(
        f"{column_name}_normalized",
        (col(column_name) - min_val) / (max_val - min_val)
    )

def add_ranking_difference(df):
    return df.withColumn(
        "ranking_difference", 
        (col("winner_rank") - col("loser_rank")).cast("double")
    )

def normalize_surface_distribution(df):
    total_matches = df.groupBy("surface").count().withColumnRenamed("count", "total_count")
    total_matches = total_matches.withColumn(
        "target_ratio", lit(0.33) # normalize surfaces
    )
    df = df.join(total_matches, "surface", "left")
    df = df.withColumn(
        "adjusted_win_percentage",
        col("win_percentage") * col("target_ratio") / col("total_count")
    )
    return df

def calculate_coefficient(df, weights):
    df = normalize_column(df, "win_percentage")
    df = normalize_column(df, "ranking_difference")
    
    df = df.withColumn(
        "final_grade",
        col("win_percentage_normalized") * weights["win_percentage"] +
        col("ranking_difference_normalized") * weights["ranking_difference"]
    )
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tennis Transform").getOrCreate()

    data_path = "/opt/airflow/dags/cleaned_data/"
    df = spark.read.parquet(data_path)

    df = filter_big3_matches(df)

    df = add_ranking_difference(df)
    win_percentage = calculate_win_percentage(df)
    df = df.join(win_percentage, df["winner_name"] == win_percentage["name"], "left")
    
    df = normalize_surface_distribution(df)

    with open("/opt/airflow/dags/config/weights.json", "r") as f:
        weights = json.load(f)["weights"]
    
    df = calculate_coefficient(df, weights)

    output_path = "/opt/airflow/dags/transformed_data/"
    df.write.mode("overwrite").parquet(output_path)

    spark.stop()