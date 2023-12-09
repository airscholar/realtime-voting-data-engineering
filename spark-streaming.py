from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# import pyspark
#
# print(pyspark.__version__) # to check the version of pyspark

if __name__ == "__main__":
    # Initialize SparkSession
    spark = (SparkSession.builder
             .appName("ElectionAnalysis")
             .master("local[*]")  # Use local Spark execution with all available cores
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")  # Spark-Kafka integration
             .config("spark.jars",
                     "/Users/airscholar/Dev/Projects/Python/Voting/postgresql-42.7.1.jar")  # PostgreSQL driver
             .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
             .getOrCreate())

    # Define schemas for Kafka topics
    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])

    # Read data from Kafka 'votes_topic' and process it
    votes_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "votes_topic") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), vote_schema).alias("data")) \
        .select("data.*")

    # Data preprocessing: type casting and watermarking
    votes_df = votes_df.withColumn("voting_time", col("voting_time").cast(TimestampType())) \
        .withColumn('vote', col('vote').cast(IntegerType()))
    enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")

    # Aggregate votes per candidate and turnout by location
    votes_per_candidate = enriched_votes_df.groupBy("candidate_id", "candidate_name", "party_affiliation",
                                                    "photo_url").agg(_sum("vote").alias("total_votes"))
    turnout_by_location = enriched_votes_df.groupBy("address.state").count().alias("total_votes")

    # Write aggregated data to Kafka topics ('aggregated_votes_per_candidate', 'aggregated_turnout_by_location')
    votes_per_candidate_to_kafka = votes_per_candidate.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_votes_per_candidate") \
        .option("checkpointLocation", "/Users/airscholar/Dev/Projects/Python/Voting/checkpoints/checkpoint1") \
        .outputMode("update") \
        .start()

    turnout_by_location_to_kafka = turnout_by_location.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_turnout_by_location") \
        .option("checkpointLocation", "/Users/airscholar/Dev/Projects/Python/Voting/checkpoints/checkpoint2") \
        .outputMode("update") \
        .start()

    # Await termination for the streaming queries
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()

    # candidate_schema = StructType([
    # ])
    #
    # voter_schema = StructType([
    # ])

    # read candidate data from postgres
    # candidates_df = spark.read \
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql://localhost:5432/voting") \
    #     .option("dbtable", "candidates") \
    #     .option("user", "postgres") \
    #     .option("password", "postgres") \
    #     .option("driver", "org.postgresql.Driver") \
    #     .load()
    #
    # candidates_df.persist(StorageLevel.MEMORY_ONLY)
    #
    # voters_df = spark.read \
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql://localhost:5432/voting") \
    #     .option("dbtable", "voters") \
    #     .option("user", "postgres") \
    #     .option("password", "postgres") \
    #     .option("driver", "org.postgresql.Driver") \
    #     .load()

    # voters_df = spark \
    #     .readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("subscribe", "voters_topic") \
    #     .option("startingOffsets", "earliest") \
    #     .load() \
    #     .selectExpr("CAST(value AS STRING)") \
    #     .select(from_json(col("value"), voter_schema).alias("data")) \
    #     .select("data.*")
    #

    # # Perform joins
    # enriched_votes_df = votes_df.alias("vote").join(voters_df.alias("voter"),
    #                                                 expr("vote.voter_id == voter.voter_id"), "inner") \
    #     .join(candidates_df.alias("candidate"), expr("vote.candidate_id == candidate.candidate_id"), "inner") \
    #     .select(
    #     col("vote.voter_id"), col("voter.name").alias("voter_name"), col("vote.candidate_id"),
    #     col("voter.gender"), col("voting_time").cast(TimestampType()).alias("voting_time"),
    #     col("voter.address_city").alias("voter_city"), col("voter.address_state").alias("voter_state"),
    #     col("voter.address_country").alias("voter_country"),
    #     col("voter.address_postcode").alias("voter_postcode"),
    #     col("voter.registered_age").alias("voter_registered_age"),
    #     col("candidate.name").alias("candidate_name"), col("candidate.party_affiliation"))
    #
    # # Voter turnout by age
    # turnout_by_age = enriched_votes_df.groupBy("registered_age").agg(count("*").alias("total_votes"))
    #
    # # Voter turnout by gender (assuming gender data is available in voters_df)
    # turnout_by_gender = enriched_votes_df.groupBy("gender").agg(count("*").alias("total_votes"))
    #
    # # Voter turnout by location

    # party_wise_votes = enriched_votes_df.groupBy("party_affiliation").agg(count("*").alias("total_votes"))
    #
    # votes_by_region = enriched_votes_df.groupBy("address.city").agg(count("*").alias("total_votes"))

    # Write to Kafka
    # enriched_votes_to_kafka = enriched_votes_df.selectExpr("to_json(struct(*)) AS value") \
    #     .writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "enriched_votes") \
    #     .option("checkpointLocation", "/Users/airscholar/Dev/Projects/Python/Voting/checkpoints/checkpoint1") \
    #     .outputMode("update") \
    #     .start()
    # turnout_by_gender_to_kafka = turnout_by_gender.selectExpr("to_json(struct(*)) AS value") \
    #     .writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "aggregated_turnout_by_gender") \
    #     .option("checkpointLocation", "/Users/airscholar/Dev/Projects/Python/Voting/checkpoints/checkpoint4") \
    #     .outputMode("update") \
    #     .start()
    # turnout_by_age_to_kafka = turnout_by_age.selectExpr("to_json(struct(*)) AS value") \
    #     .writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "aggregated_turnout_by_age") \
    #     .option("checkpointLocation", "/Users/airscholar/Dev/Projects/Python/Voting/checkpoints/checkpoint3") \
    #     .outputMode("update") \
    #     .start()
    # party_wise_votes_to_kafka = party_wise_votes.selectExpr("to_json(struct(*)) AS value") \
    #     .writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "aggregated_party_wise_votes") \
    #     .option("checkpointLocation", "/Users/airscholar/Dev/Projects/Python/Voting/checkpoints/checkpoint6") \
    #     .outputMode("update") \
    #     .start()
    # votes_by_region_to_kafka = votes_by_region.selectExpr("to_json(struct(*)) AS value") \
    #     .writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "aggregated_votes_by_region") \
    #     .option("checkpointLocation", "/Users/airscholar/Dev/Projects/Python/Voting/checkpoints/checkpoint7") \
    #     .outputMode("update") \
    #     .start()
    # enriched_votes_to_kafka.awaitTermination()
    # turnout_by_gender_to_kafka.awaitTermination()
    # votes_by_region_to_kafka.awaitTermination()
    # turnout_by_age_to_kafka.awaitTermination()
    # party_wise_votes_to_kafka.awaitTermination()
