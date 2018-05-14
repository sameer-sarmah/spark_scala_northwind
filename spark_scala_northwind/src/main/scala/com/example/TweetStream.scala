package com.example
import twitter4j.Twitter
import twitter4j.Status
import twitter4j.TwitterFactory
import twitter4j.auth.Authorization
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext;
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.streaming.Seconds

object TweetStream extends App{
  System.setProperty("hadoop.home.dir", "C:\\Users\\I320209\\Documents\\WinUtils")
  val consumerKey = ""
  val consumerSecret = ""
  val accessToken = ""
  val accessTokenSecret = ""

  val cb =  new ConfigurationBuilder()
  cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
    .setOAuthConsumerSecret(consumerSecret)
    .setOAuthAccessToken(accessToken)
    .setOAuthAccessTokenSecret(accessTokenSecret)
  val tf = new TwitterFactory(cb.build)
  val twitter = tf.getInstance
  val auth = twitter.getAuthorization
  val filters = Seq("India")



  val appName = "TwitterPopularTags"
  val sparkMaster = "local[2]"
  val sparkConf = new SparkConf().setMaster(sparkMaster).setAppName(appName)
  val streamingContext=new StreamingContext(sparkConf, Seconds(1))
  val stream=TwitterUtils.createStream(streamingContext, Some(auth), filters)
  val words=stream.flatMap((s: Status) => {
    processStatus(s)
    s.getText().split(" ").iterator
  })
  words.print()


  streamingContext.start
  try
    streamingContext.awaitTermination
  catch {
    case e: InterruptedException =>
      // TODO Auto-generated catch block
      e.printStackTrace()
  }

  private def processStatus(status: Status): Unit = {
    System.out.println("Tweet, " + status.getText())
    System.out.println("Tweet posted by, " + status.getUser.getName)
  }
}