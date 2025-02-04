// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.appName("SoccerQueries").getOrCreate()


// COMMAND ----------

// MAGIC %md
// MAGIC #### Create DataFrames from the Soccer data files

// COMMAND ----------

val country = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/COUNTRY-1.csv")
val disciplinaryRecord = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/DISCIPLINARY_RECORD-1.csv")
val goalScorer = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/GOAL_SCORER-1.csv")
val matchData = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/MATCH-1.csv")
val player = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/PLAYER-1.csv")
val worldCupWinner = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/WORLD_CUP_WINNER-1.csv")



// COMMAND ----------

country.printSchema()
goalScorer.printSchema()
disciplinaryRecord.printSchema()
player.printSchema()
matchData.printSchema()
worldCupWinner.printSchema()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.	Retrieve the list of country names that have won a world cup.

// COMMAND ----------

val countriesWithWins = worldCupWinner.select("Winner").distinct()
countriesWithWins.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.	Retrieve the list of country names that have won a world cup and the number of world cup each has won in descending order.

// COMMAND ----------

val worldCupWins = worldCupWinner.groupBy("Winner")
                                 .count()
                                 .orderBy(desc("count"))
                                 .withColumnRenamed("count", "WorldCupWins")
worldCupWins.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.	List the Capital of the countries in increasing order of country population for countries that have population more than 100 million.

// COMMAND ----------

val largePopulationCapitals = country.filter("Population > 100")
                                     .select("Capital", "Population")
                                     .orderBy("Population")
largePopulationCapitals.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.	List the Name of the stadium which has hosted a match where the number of goals scored by a single team was greater than 4.

// COMMAND ----------

val highScoringStadiums = matchData.filter(col("team1_score") > 4 || col("team2_score") > 4)
                                   .select("stadium")
                                   .distinct()
highScoringStadiums.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 5.	List the names of all the cities which have the name of the Stadium starting with “Estadio”.

// COMMAND ----------

val estadioCities = matchData.filter(lower(col("stadium")).contains("estadio"))
                             .select("stadium", "host_city")
                             .distinct()
estadioCities.show(50, truncate = false)


// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.	List all stadiums and the number of matches hosted by each stadium.

// COMMAND ----------

val stadiumMatchCount = matchData.groupBy("stadium")
                                 .agg(count("match_id").alias("MatchCount"))
stadiumMatchCount.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 7.	List the First Name, Last Name and Date of Birth of Players whose heights are greater than 198 cms.

// COMMAND ----------

val tallPlayers = player.filter(col("height").cast("int") > 198)
                        .select("fname", "lname", "dob")
tallPlayers.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 8.	List the Fname, Lname, Position and No of Goals scored by the Captain of a team who has more than 2 Yellow cards or 1 Red card.

// COMMAND ----------

val playerWithGoals = player
  .join(goalScorer, "player_id")
  .join(disciplinaryRecord, "player_id")
  .filter("(no_of_yellow_cards > 2 OR no_of_red_cards > 1) AND is_captain = true")
  .select("fname", "lname", "position", "goals")

playerWithGoals.show()
