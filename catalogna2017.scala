// run spark shell
// bin/spark-shell --driver-memory=g5

//
// Thanks to:
// databricks/spark-corenlp
// ML-SentiCON: Cruz, Fermín L., José A. Troyano, Beatriz Pontes, F. Javier Ortega. Building layered, multilingual sentiment lexicons at synset and lemma levels, Expert Systems with Applications, 2014.
// Talend and Twitter for Talend (by Baldassarre)
//

// read twitter
val twitter = spark.read.option("delimiter",";").option("escape","\"").option("header",true).csv("/Users/mauropelucchi/Desktop/out.csv")

twitter.count

twitter.printSchema

// 
// dedup
val twitter_dedup = twitter.dropDuplicates.dropDuplicates("tweet_id").filter(x => (x.getString(x.fieldIndex("tweet_text")) != null && x.getString(x.fieldIndex("tweet_text")).length > 10))
twitter_dedup.count

// aggregate retweet
val agg_retweet = twitter_dedup.filter("tweet_retweet_count > 0").groupBy("tweet_retweeted_status_id").count.toDF("tweet_id","count_retweet")
agg_retweet.count
agg_retweet.show


val twitter_notweet =  twitter_dedup.join(agg_retweet, Seq(("tweet_id")), "left")
twitter_notweet.count
twitter_notweet.show



// count mentions and retweet
val  mentionsCount = udf((userMentions: String)  => {
  val splitCount = if (userMentions != null) userMentions.split(",").length else 0;
  val count = if (splitCount <= 0) 0 else splitCount;
  count
}: Int)
val twitter_cat = twitter_notweet.withColumn("count_mentions", mentionsCount(twitter_notweet("tweet_user_r_mentions")))
twitter_cat.show

import org.apache.spark.sql.types.IntegerType
val twitter_cat_final = twitter_cat.withColumn("my_retweet", when($"count_retweet".isNull, 0).otherwise($"count_retweet").cast(IntegerType))
twitter_cat_final.show



// tokenize
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.functions._

val regexTokenizer = new RegexTokenizer().setInputCol("tweet_text").setOutputCol("tokens").setPattern("\\W+")
val tokenized = regexTokenizer.transform(twitter_cat_final)
tokenized.show

// remove stopwords 
// 
import org.apache.spark.ml.feature.StopWordsRemover

val ca_stopwords = spark.read.option("delimiter",";").option("escape","\"").csv("/Users/mauropelucchi/Desktop/stopwords-ca.txt").map(x => x.getString(0)).collect
val es_stopwords = StopWordsRemover.loadDefaultStopWords("spanish")
val stopwords = ca_stopwords.union(es_stopwords)


val stopwordremover = new StopWordsRemover().setStopWords(stopwords).setInputCol("tokens").setOutputCol("tokens_filtered")
val tokens_filtered = stopwordremover.transform(tokenized)
tokens_filtered.count
tokens_filtered.show

// remove token with len < 2
// remove token with number
// get lemma

val lemmas = sc.textFile("/Users/mauropelucchi/Desktop/lemmatization*.*")
val lem_map = lemmas.map(x => x.split("\t")).map(x => (x(1), x(0))).collectAsMap

def my_lemmas (sentence: String): String={
      val str = lem_map.getOrElse(sentence, sentence)
      (str)
      }


val regex = """[^0-9]*""".r
val  textClean = udf((tokens: Seq[String])  => {  
  tokens.map(_.toLowerCase).filter(token => regex.pattern.matcher(token).matches()).filter(token => token.size > 2).map(token => my_lemmas(token)).toSeq
}: Seq[String])

val tokens_cleaned = tokens_filtered.withColumn("tokens_cleaned", textClean(tokens_filtered("tokens_filtered")))
// check first 10 documents
tokens_cleaned.show



// polarized tokens
val xml_ca = sc.textFile("/Users/mauropelucchi/Desktop/ML-SentiCon/senticon.ca.xml")
val ca_sent = xml_ca.filter(x => x.contains("<lemma")).map(x => (x.substring(x.indexOf(">") + 1, x.indexOf("</lemma")).trim, x.substring(x.indexOf("pol=")+5, x.indexOf("\"",x.indexOf("pol=")+5)).toFloat )).collect
val xml_es = sc.textFile("/Users/mauropelucchi/Desktop/ML-SentiCon/senticon.es.xml")
val es_sent = xml_es.filter(x => x.contains("<lemma")).map(x => (x.substring(x.indexOf(">") + 1, x.indexOf("</lemma")).trim, x.substring(x.indexOf("pol=")+5, x.indexOf("\"",x.indexOf("pol=")+5)).toFloat )).collect
val xml_en = sc.textFile("/Users/mauropelucchi/Desktop/ML-SentiCon/senticon.en.xml")
val en_sent = xml_en.filter(x => x.contains("<lemma")).map(x => (x.substring(x.indexOf(">") + 1, x.indexOf("</lemma")).trim, x.substring(x.indexOf("pol=")+5, x.indexOf("\"",x.indexOf("pol=")+5)).toFloat )).collect
val sent = en_sent.union(es_sent).union(ca_sent).toMap
sent.size


// apply to tweet
val  polarText = udf((tokens: Seq[String])  => {  
  tokens.map(token => (if (sent.contains(token)) sent(token) else 0)).sum
}: Float)
val twitter_pol = tokens_cleaned.withColumn("pol", polarText(tokens_cleaned("tokens_cleaned"))).persist
twitter_pol.show

// aggregate by user
val polarUser = udf((retweet:Int, mentions:Int, polar:Float) => {
  val c1 = if (retweet <= 0) 1 else retweet;
  val c2 = if (mentions <= 0) 1 else mentions;
  c1*c2*polar
}: Float)

// create relations
val relations = twitter_pol.groupBy("user_name","user_dest_screen_name").count.filter(col("user_name").notEqual("null")).filter(col("user_dest_screen_name").notEqual("null"))
relations.sort(desc("count")).show
relations.coalesce(1).write.option("delimiter",";").option("escape","\"").option("header",true).csv("/Users/mauropelucchi/Desktop/relations.csv")

val users = twitter_pol.groupBy("user_name").agg(count("tweet_id"),avg("pol"),sum("my_retweet"),sum("count_mentions"), avg(polarUser(col("my_retweet"), col("count_mentions"),col("pol")))).toDF("user_name","count_tweet","pol","count_retweet","count_mentions","pol_tot")
users.sort(desc("pol_tot")).show
users.coalesce(1).write.option("delimiter",";").option("escape","\"").option("header",true).csv("/Users/mauropelucchi/Desktop/users.csv")

def getDict(line: Seq[String]): Seq[String] =  { line.map(d => d.toLowerCase) }
val terms = twitter_pol.flatMap(x => getDict(x.getSeq(x.fieldIndex("tokens_cleaned"))).map(t => (t, 1))  ).toDF("term","tweet_id").groupBy("term").count
terms.sort(desc("count")).limit(100).coalesce(1).write.option("delimiter",";").option("escape","\"").option("header",true).csv("/Users/mauropelucchi/Desktop/terms.csv")







import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.IDFModel


// limit this analysis to retweet > 0
val twitter_lda = twitter_pol.filter("my_retweet > 0").persist
twitter_lda.select("tweet_id","tokens_cleaned").flatMap(x => getDict(x.getSeq(x.fieldIndex("tokens_cleaned"))).map(y => (y, x.getString(0)))).coalesce(1).write.option("delimiter",";").option("escape","\"").option("header",true).csv("/Users/mauropelucchi/Desktop/lda_analysis.csv")

val numK = 20 // create 20 cluster!
val numTopics = 3 // max number of topics
val lda = new LDA().setK(numK).setMaxIter(30).setFeaturesCol("features")


val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("tokens_cleaned").setOutputCol("ldaFeatures").setMinDF(10).fit(twitter_lda)
val cv = cvModel.transform(twitter_lda)
val vocab = cvModel.vocabulary

val idfCv = new IDF().setInputCol("ldaFeatures").setOutputCol("features")
val idfCvModel = idfCv.fit(cv)
val tfidfCv = idfCvModel.transform(cv)

val ldaModel = lda.fit(tfidfCv)

// GET TOPICS
val topicIndices = ldaModel.describeTopics(numTopics)

val getTerm = udf((s: Seq[String]) => s.map(d => vocab(d.toInt)))
val topics = topicIndices.withColumn("Terms", getTerm(topicIndices("termIndices")))
topics.show



