#!/usr/bin/env python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, first, upper
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("IOC").getOrCreate()

# Load CSV files
ath_2012 = spark.read.csv(sys.argv[1], header=True, inferSchema=True).withColumn("year",F.lit(2012))
ath_2016 = spark.read.csv(sys.argv[2], header=True, inferSchema=True).withColumn("year",F.lit(2016))
ath_2020 = spark.read.csv(sys.argv[3], header=True, inferSchema=True).withColumn("year",F.lit(2020))
coaches = spark.read.csv(sys.argv[4], header=True, inferSchema=True)
medals = spark.read.csv(sys.argv[5], header=True, inferSchema=True)

#coaches.show(100)

# Filter medals data for specific years and rename columns
medals = medals.withColumnRenamed("country", "medals_country").withColumn("sport", upper(col("sport"))).withColumn("medals_country", upper(col("medals_country"))).withColumn("medal", upper(col("medal"))).withColumnRenamed("sport", "medal_sport").withColumnRenamed("event", "medal_event").withColumn("medal_event", upper(col("medal_event"))).withColumnRenamed("year", "medal_year")
#medals.show(1000)
medals = medals.filter(col("year").isin([2012, 2016, 2020]))
#coaches = coaches.filter(col("year").isin([2012, 2016, 2020]))
# Convert relevant columns to uppercase
ath_2012 = ath_2012.withColumn("country", upper(col("country"))).withColumn("name", upper(col("name"))).withColumn("sport", upper(col("sport"))).withColumn("event", upper(col("event"))).withColumn("coach_id", upper(col("coach_id")))

ath_2016 = ath_2016.withColumn("country", upper(col("country"))).withColumn("name", upper(col("name"))).withColumn("sport", upper(col("sport"))).withColumn("event", upper(col("event"))).withColumn("coach_id", upper(col("coach_id")))

ath_2020 = ath_2020.withColumn("country", upper(col("country"))).withColumn("name", upper(col("name"))).withColumn("sport", upper(col("sport"))).withColumn("event", upper(col("event"))).withColumn("coach_id", upper(col("coach_id")))

coaches = coaches.withColumnRenamed("name", "coach_name").withColumnRenamed("country", "coach_country").withColumnRenamed("sport", "coach_sport").withColumnRenamed("id", "id")

coaches = coaches.withColumn("coach_country", upper(col("coach_country"))).withColumn("coach_name", upper(col("coach_name"))).withColumn("id", upper(col("id"))).withColumn("coach_sport", upper(col("coach_sport")))

#ath_2020.show(5)
# Union athlete data
ath = ath_2012.union(ath_2016).union(ath_2020).select("id", "name","sport","event","country","coach_id","year").dropDuplicates()
#ath.show(1000)
#ath.withColumn("event", upper(col("event")))
#ath.show(1000)

def Points(year, medal):
    points_table = {
        2012: {"GOLD": 20, "SILVER": 15, "BRONZE": 10},
        2016: {"GOLD": 12, "SILVER": 8, "BRONZE": 6},
        2020: {"GOLD": 15, "SILVER": 12, "BRONZE": 7}
    }
    return points_table.get(year, {}).get(medal, 0)

PointsUDF = udf(Points, IntegerType())

# Process medals and calculate points
medals = medals.withColumn("point", PointsUDF(medals["medal_year"], medals["medal"]))


medals = medals.withColumnRenamed("country", "medals_country")  

medal_counts = medals.groupBy("id").agg(
    F.sum("point").alias("point"),
    F.first("medal_sport").alias("medal_sport"),
    F.sum(when(col("medal") == "GOLD", 1).otherwise(0)).alias("gold_count"),
    F.sum(when(col("medal") == "SILVER", 1).otherwise(0)).alias("silver_count"),
    F.sum(when(col("medal") == "BRONZE", 1).otherwise(0)).alias("bronze_count"),
    F.first("medals_country").alias("medals_country"),
    F.first("medal_event").alias("medal_event"),
    F.first("medal_year").alias("medal_year")
)
#medal_counts.show(1000)

ath_medals = ath.join(medal_counts, 
                      (ath["id"] == medal_counts["id"]) & 
                      (ath["sport"] == medal_counts["medal_sport"]) &
                      (ath["event"] == medal_counts["medal_event"]), "inner").select("name", "sport", "point", "gold_count", "silver_count", "bronze_count", "country","coach_id")

#ath_medals.show(100)


ath_dict = {}
for row in ath_medals.collect():
    sport = row["sport"]
    if sport not in ath_dict:
        ath_dict[sport] = []
    ath_dict[sport].append((row["name"], row["point"], row["gold_count"], row["silver_count"], row["bronze_count"]))

def sort_ath(ath):
    return sorted(ath, key=lambda x: (-x[1], -x[2], -x[3], -x[4], x[0]))

best_ath = []
for sport, athletes in ath_dict.items():
    sorted_athletes = sort_ath(athletes)
    best_ath.append((sport, sorted_athletes[0][0]))

best_ath.sort()
out_best_ath = [athlete.upper() for sport, athlete in best_ath]

#print(out_best_ath)

# Task 2: 
#ath_country = ath_medals.filter(col("country").isin(["CHINA", "INDIA", "USA"]))
#ath_country.show(100)
#ath_medals.show(100)
#coaches.show(100)

'''
ath_coaches = ath.join(coaches,
			(ath["coach_id"] == coaches["id"])& (ath["sport"] == coaches["coach_sport"])) 

ath_coaches.show(100)

ath_coa_med = ath_coaches.join(medals, (ath_coaches["id"] == medals["id"] & ath_coaches["sport"] == medals["medal_sport"]& ath_coaches["event"] == medals["medal_event"] ))
ath_coa_med.show(100)
'''

ath = ath.filter(col("country").isin(["CHINA", "INDIA", "USA"]))

ath_with_points = ath.join(medals, 
                            (ath["sport"] == medals["medal_sport"]) &
                            (ath["event"] == medals["medal_event"]) &
                            (ath["id"] == medals["id"])&
                            (ath["year"] == medals["medal_year"]),
                            "inner").select(ath["id"], ath["name"], ath["sport"], ath["country"], ath["coach_id"], medals["medal"], medals["medal_year"] )
#ath_with_points.show(100)

ath_coach = ath_with_points.join(coaches,
                                  (ath_with_points["coach_id"] == coaches["id"]) & 
                                  (ath_with_points["sport"] == coaches["coach_sport"]),
                                  "inner").select(coaches["id"], coaches["coach_name"], ath_with_points["sport"], ath_with_points["country"], ath_with_points["coach_id"], coaches["coach_sport"],
    PointsUDF(ath_with_points["medal_year"], ath_with_points["medal"]).alias("point"),  
    when(ath_with_points["medal"] == "GOLD", 1).otherwise(0).alias("gold_count"),  
    when(ath_with_points["medal"] == "SILVER", 1).otherwise(0).alias("silver_count"),  
    when(ath_with_points["medal"] == "BRONZE", 1).otherwise(0).alias("bronze_count"))



ath_coach.dropDuplicates()
#ath_coach.show(100)

ath_coach_filtered = ath_coach.filter(col("country").isin(["CHINA", "INDIA", "USA"]))


ath_coach_filtered.createOrReplaceTempView("ath_coach_filtered")

query = ''' 
	SELECT
        coach_id,
        country,
        SUM(point) AS total_points,
        FIRST(coach_name) AS coach_name,
        FIRST(coach_sport) AS coach_sport,
        FIRST(country) AS grp_country,
        SUM(gold_count) AS gold_count,
        SUM(silver_count) AS silver_count,
        SUM(bronze_count) AS bronze_count
    FROM ath_coach_filtered
    GROUP BY coach_id, country  '''
 

coach_agg = spark.sql(query)

#coach_agg.show(1000)

window_spec = Window.partitionBy(coach_agg["country"]).orderBy(
    col("total_points").desc(), 
    col("gold_count").desc(),
    col("silver_count").desc(), 
    col("bronze_count").desc(),
    col("coach_name").asc()
)

coaches_ranked = coach_agg.withColumn("rank", rank().over(window_spec))

top_coaches = coaches_ranked.filter(coaches_ranked["rank"] <= 5)
#top_coaches.show(1000)

def collect_coaches(country):
    country_coa = top_coaches.filter(top_coaches["grp_country"] == country)
    country_coa_order = country_coa.select("coach_name", "total_points", "gold_count", "silver_count", "bronze_count", "rank")
    country_coa_rdd = country_coa_order.rdd.map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5]))
    country_results = country_coa_rdd.collect()
    sort_results = sort_ath(country_results)
    
    return [coach[0] for coach in sort_results]


results = {
    "china": collect_coaches("CHINA"),
    "india": collect_coaches("INDIA"),
    "usa": collect_coaches("USA")
}

final_coaches = []
for country in ["china", "india", "usa"]:
    final_coaches.extend(results[country])

#print(out_best_ath)
#print(final_coaches)

output = (out_best_ath,final_coaches)
with open(sys.argv[6], 'w') as f:
    f.write(str(output).replace("'", '"'))


# Stop the Spark session
spark.stop()

	
