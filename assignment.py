import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

spark = (
    SparkSession.builder.config("spark.executor.memory", "16g")
    .config("spark.executor.memoryOverhead", "3200m")
    # .config("spark.dynamicAllocation.enabled", "true")
    # .config("spark.dynamicAllocation.initialExecutors", "1")
    # .config("spark.dynamicAllocation.minExecutors", "1")
    # .config("spark.dynamicAllocation.maxExecutors", "3")
    # .config("spark.sql.shuffle.partitions", "20")
    # .config("spark.sparkContext.defaultParallelism", "5")
    # .config("spark.blacklist.enabled", "true") # blacklist bad machine
    # .config("spark.reducer.maxReqsInFlight", "10") # limit concurrent requests from reducer
    # .config("spark.shuffle.io.retryWait", "10s") # increase retry wait
    # .config("spark.shuffle.io.maxRetries", "10") # increase retry times
    # .config("spark.shuffle.io.backLog", "4096")
    .appName("spark_assignment")
    .getOrCreate()
)

record_schema = StructType(
    [
        StructField("userid", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("musicbrainz-artist-id", StringType(), True),
        StructField("artist-name", StringType(), True),
        StructField("musicbrainz-traick-id", StringType(), True),
        StructField("track-name", StringType(), True),
    ]
)


def _persist_data():
    user_profile_df = spark.read.csv(
        "/opt/spark/music-data/lastfm-dataset-1k/userid-profile.tsv",
        sep=r"\t",
        header=True,
    )
    play_records_df = spark.read.csv(
        "/opt/spark/music-data/lastfm-dataset-1k/userid-timestamp-artid-artname-traid-traname.tsv",
        sep=r"\t",
        schema=record_schema,
    )

    play_records_df.write.format("orc").mode("overwrite").save(
        "/opt/spark/music-data/table/play_records"
    )


def _build_user_sessions(play_records_df):
    return (
        play_records_df.alias("a")
        .join(play_records_df.alias("b"), on="userid", how="left")
        .where("b.timestamp < a.session_end_ts and b.timestamp > a.timestamp")
        .select(
            "userid",
            F.col("a.timestamp").alias("prev_track_start_ts"),
            F.col("a.session_end_ts"),
            F.col("b.timestamp").alias("curr_track_start_ts"),
            F.col("a.track-name").alias("prev_track_name"),
            F.col("b.track-name").alias("curr_track_name"),
        )
        .withColumn("session_id", F.expr("sha1(concat(userid, prev_track_start_ts))"))
    )


def _get_top_sessions(sessions_df, top_n=50):
    return (
        sessions_df.groupBy("session_id")
        .agg(F.collect_list("curr_track_name").alias("tracks_in_session"))
        .withColumn("dist", F.array_distinct("tracks_in_session"))
        .withColumn(
            "counts",
            F.expr(
                """
                transform(dist, x -> aggregate(tracks_in_session, 0, (acc, y) -> if (y=x, acc+1, acc)))
            """
            ),
        )
        .withColumn("track_count_map", F.arrays_zip("dist", "counts"))
        .drop("dist", "counts")
        .withColumn("session_tracks_count", F.size("tracks_in_session"))
        .orderBy(F.col("session_tracks_count").desc())
        .limit(top_n)
    )


def _get_top_tracks(top_n_sessions_df, top_n=10):
    return (
        top_n_sessions_df.withColumn("dict", F.explode(F.col("track_count_map")))
        .withColumn("track_name", F.col("dict.dist"))
        .withColumn("num_track_played", F.col("dict.counts"))
        .withColumn(
            "rnk",
            F.row_number().over(
                Window.partitionBy("session_id").orderBy(
                    F.col("num_track_played").desc()
                )
            ),
        )
        .where(f"rnk<={top_n}")
        .groupBy("session_id", "session_tracks_count")
        .agg(F.collect_list("track_name").alias("top_10_tracks"))
        .orderBy(F.col("session_tracks_count").desc())
    )


def _write_to_csv(results_df, path="/opt/spark/data/results/top_records.csv"):
    results_df = results_df.withColumn(
        "top_10_tracks", F.concat_ws(", ", "top_10_tracks")
    )
    results_df.coalesce(1).write.csv(path, header=True, sep=r"\t", mode="overwrite")


def run_job():
    # _persist_data()

    records_df = spark.read.format("orc").load(
        "/opt/spark/music-data/table/play_records"
    )

    play_records_df = records_df.withColumn(
        "session_end_ts", F.col("timestamp") + F.expr("INTERVAL 20 MINUTES")
    )

    sessions_df = _build_user_sessions(play_records_df).repartition(10)
    top_50_sessions_df = _get_top_sessions(sessions_df, 50)
    top_tracks_df = _get_top_tracks(top_50_sessions_df, 10)
    _write_to_csv(top_tracks_df)


run_job()
