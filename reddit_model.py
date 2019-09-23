from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import ArrayType, StringType
from cleantext import sanitize
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator

states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

def isStates(flair):
	if flair in states:
		return flair
	else:
		return ""
		
def cutId(commentId):
	return commentId[3:]

def isPos(probability):
	if probability[1] > 0.2:
		return 1
	else:
		return 0
	
def isNeg(probability):
	if probability[1] > 0.25:
		return 1
	else:
		return 0

def main(context):
	"""Main function takes a Spark SQL context."""
	# YOUR CODE HERE
	# YOU MAY ADD OTHER FUNCTIONS AS NEEDED
	
	# TASK 1
	# Load files as needed, files are read and automatically converted to data frames
	commentsDF = context.read.json("comments-minimal.json.bz2")
	submissionsDF = context.read.json("submissions.json.bz2")
	labeledDF = context.read.csv("labeled_data.csv")
	

	# TASKS 2, 3, 4, 5
	# This is how you create temporary views you can execute SQL queries on.
	commentsDF.createOrReplaceTempView("comments")
	labeledDF.createOrReplaceTempView("labeled")
	submissionsDF.createOrReplaceTempView("submissions")
	
	# Register our sanitize function as UDF, can use now after declaring as so.
	context.udf.register("sanitize", sanitize, ArrayType(StringType()))
	
	# Doing a SQL query like this will create a resulting, new data frame
	sqlDF = context.sql("""SELECT 
		comments.id AS id, 
		sanitize(comments.body) AS body,
		labeled._c3 AS djt
	FROM labeled
	JOIN comments ON labeled._c0 = comments.id
	""")
	
	
	# TASK 6A
	cv = CountVectorizer(inputCol="body", outputCol="vectors", minDF=10.0, binary=True)
	cv_model = cv.fit(sqlDF)
	resultDF = cv_model.transform(sqlDF)
	
	# TASK 6B
	resultDF.createOrReplaceTempView("cvtable")
	
	modelFittingDF = context.sql("""SELECT 
		id,
		body,
		djt,
		vectors,
		CASE djt WHEN 1 THEN 1 ELSE 0 END AS positive,
		CASE djt WHEN -1 THEN 1 ELSE 0 END AS negative
	FROM cvtable
	""")
	
	# TASK 7
	# These two quries are most definitely repetitive, but I only included the above modelFittingDF to comply with 
	# Task 6B of creating a new data frame with 2 new columns in it.
	pos = context.sql("""SELECT 
		id,
		body,
		djt,
		vectors AS features,
		CASE djt WHEN 1 THEN 1 ELSE 0 END AS label
	FROM cvtable
	""")
	
	neg = context.sql("""SELECT 
		id,
		body,
		djt,
		vectors AS features,
		CASE djt WHEN -1 THEN 1 ELSE 0 END AS label
	FROM cvtable
	""")
	
	# Initialize two logistic regression models.
	poslr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
	neglr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
	# This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
	posEvaluator = BinaryClassificationEvaluator()
	negEvaluator = BinaryClassificationEvaluator()
	# There are a few parameters associated with logistic regression. We do not know what they are a priori.
	# We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
	# We will assume the parameter is 1.0. Grid search takes forever.
	posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
	negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
	# We initialize a 5 fold cross-validation pipeline.
	posCrossval = CrossValidator(
		estimator=poslr,
		evaluator=posEvaluator,
		estimatorParamMaps=posParamGrid,
		numFolds=5)
	negCrossval = CrossValidator(
		estimator=neglr,
		evaluator=negEvaluator,
		estimatorParamMaps=negParamGrid,
		numFolds=5)
	# Although crossvalidation creates its own train/test sets for
	# tuning, we still need a labeled test set, because it is not
	# accessible from the crossvalidator (argh!)
	# Split the data 50/50
	posTrain, posTest = pos.randomSplit([0.5, 0.5])
	negTrain, negTest = neg.randomSplit([0.5, 0.5])
	# Train the models
	print("Training positive classifier...")
	posModel = posCrossval.fit(posTrain)
	print("Training negative classifier...")
	negModel = negCrossval.fit(negTrain)

	# Once we train the models, we don't want to do it again. We can save the models and load them again later.
	posModel.save("project2/pos.model")
	negModel.save("project2/neg.model")
	
	
	# TASK 8
	context.udf.register("isStates", isStates)
	context.udf.register("cutId", cutId)
	
	# removing the sarcasm and quotes
	noSarcasmDF = context.sql("""SELECT 
		comments.created_utc AS timestamp, 
		submissions.title AS title,
		isStates(comments.author_flair_text) AS state,
		comments.id AS id,
		sanitize(comments.body) AS body,
		comments.score AS comment_score,
		submissions.score AS story_score
	FROM comments
	JOIN submissions ON cutId(comments.link_id) = submissions.id
	WHERE comments.body NOT LIKE '&gt%' AND comments.body NOT LIKE '%/s%'
	""")
  
	# TASK 9
	#repeated names hopefully won't matter here
	resultDF = cv_model.transform(noSarcasmDF)
	
	resultDF.createOrReplaceTempView("cvtable")
	
	#new positive results
	positives = context.sql("""SELECT 
		timestamp,
		title,
		state,
		id,
		body,
		comment_score,
		story_score,
		vectors AS features
	FROM cvtable
	""")
	
	posResult = posModel.transform(positives).selectExpr("timestamp", "title", 
	"state", "id", "body", "comment_score", "story_score", "features", "probability AS pos", 
	"prediction as pos_prediction", "rawPrediction as pos_rawPred")
	posNegResult = negModel.transform(posResult)
	
	posNegResult.createOrReplaceTempView("posNegTable")
	
	context.udf.register("isPos", isPos)
	context.udf.register("isNeg", isNeg)
	
	#I didn't bother aliasing the "probability" column for the negative
	probTable = context.sql("""SELECT
		timestamp,
		title,
		state,
		id,
		body,
		comment_score,
		story_score,
		features,
		isPos(pos) AS pos,
		isNeg(probability) AS neg
	FROM posNegTable
	""")
	
	probTable.createOrReplaceTempView("finalProbs")
	
	# TASK 10
	totalPercent = context.sql("""SELECT
		title,
		AVG(pos) AS Positive,
		AVG(neg) AS Negative
	FROM finalProbs
	GROUP BY title
	""")
	
	datePercent = context.sql("""SELECT 
		FROM_UNIXTIME(timestamp, 'Y-M-d') AS date,
		AVG(pos) AS Positive,
		AVG(neg) AS Negative
	FROM finalProbs
	GROUP BY date
	""")
	
	statePercent = context.sql("""SELECT 
		state,
		AVG(pos) AS Positive,
		AVG(neg) AS Negative
	FROM finalProbs
	WHERE state <> ""
	GROUP BY state
	""")	
	
	commentPercent = context.sql("""SELECT
		comment_score,
		AVG(pos) AS Positive,
		AVG(neg) AS Negative
	FROM finalProbs
	GROUP BY comment_score
	""")
	
	storyPercent = context.sql("""SELECT
		story_score AS submission_score,
		AVG(pos) AS Positive,
		AVG(neg) AS Negative
	FROM finalProbs
	GROUP BY story_score
	""")
	
	#saving into csvs
	#the original table
	#Task 10 Part 1
	totalPercent.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("totalPercent.csv")
	#Task 10 Part 2
	datePercent.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("time_data.csv")
	#Task 10 Part 3
	statePercent.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("state_data.csv")
	#Task 10 Part 4
	commentPercent.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("comment_score.csv")
	storyPercent.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("submission_score.csv")
	
	#this is for report part 4 that is done in Spark
	totalPercent.orderBy("Positive", ascending=False).limit(10).repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("positiveTop.csv")
	totalPercent.orderBy("Negative", ascending=False).limit(10).repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("negativeTop.csv")
  

if __name__ == "__main__":
	conf = SparkConf().setAppName("CS143 Project 2B")
	conf = conf.setMaster("local[*]")
	sc   = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	sc.addPyFile("cleantext.py")
	main(sqlContext)