
package mypackage


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._


/** Listens to a stream of tweets and saves them to disk. */
object SaveTweets {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

  
    setupTwitter()
      val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(10000))
    ///by changing the seconds here you will be able to get tweets over that period
    
   // setupLogging()

   
    val tweets = TwitterUtils.createStream(ssc, None)

   //we are getting only the tweets which are in english language and the tweets which contain the keyword sports that is verified
        val verified_users = tweets.filter(_.getLang()=="en").filter(_.getText().toLowerCase().contains("sports")).filter(_.getUser().isVerified()).map(status => (status.getUser().isVerified(),"::",status.getUser().getScreenName(),"::",status.getText(),"::",status.getUser().getLocation(),"::",status.getUser().getCreatedAt()))
   verified_users.foreachRDD(t => { 
      val a= t.map(a => a)  
      a.coalesce(1,true).saveAsTextFile("verified")
    }
    )
      //we are getting only the tweets which are in english language and the tweets which contain the keyword sports that is unverified
        val unverified_users = tweets.filter(_.getLang()=="en").filter(_.getText().toLowerCase().contains("sports")).filter(!_.getUser().isVerified()).map(status => (status.getUser().isVerified(),"::",status.getUser().getScreenName(),"::",status.getText(),"::",status.getUser().getLocation(),"::",status.getUser().getCreatedAt()))
unverified_users.foreachRDD(t => { 
      val a= t.map(a => a)  
      a.coalesce(1,true).saveAsTextFile("unverified")
    }//this will ensure that the data that we are getting is saved in one file
    )
    
//setting a checkpoint 
    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}

