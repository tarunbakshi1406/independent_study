package TestingTwitter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object main {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Missing arguements. Correct Usage: unverified verified")
    } else {

      val unverified_strBuilder = new StringBuilder
      val unverified_usertweetMap = scala.collection.mutable.Map[String, ListBuffer[String]]()
      val unverified_tweetOccurrenceMap = scala.collection.mutable.Map[String, Int]()
      val unverified_locationSet = scala.collection.mutable.Set[String]()
      var unverified_array_counter = 0
      var unverified_line_break = ""
      var unver_total_tweets = 0

      val verified_strBuilder = new StringBuilder
      val verified_usertweetMap = scala.collection.mutable.Map[String, ListBuffer[String]]()
      val verified_tweetOccurrenceMap = scala.collection.mutable.Map[String, Int]()
      val verified_locationSet = scala.collection.mutable.Set[String]()
      var verified_array_counter = 0
      var verified_line_break = ""
      var ver_total_tweets = 0

      for (line <- Source.fromFile(args(0)).getLines()) {
        unverified_strBuilder.append(line)
        unverified_array_counter += line.split(",::,").length - 1
        //the reason for this array_counter is that parsing the input file contains multiple line break which would mess
        //with the ,::, ie our split so this will help us gather all the line break
        if (unverified_array_counter == 4) {
          val dataArray = unverified_strBuilder.toString().split(",::,")
          if (dataArray(3) != "null") { // checking for null locataion
            unver_total_tweets += 1
            unverified_locationSet += dataArray(3)
            //we have to do this because we want to keep the original line break from input

            // build map for statistics of user -> list of tweet from that user
            if (unverified_usertweetMap.contains(dataArray(1))) {
              val tweetList = unverified_usertweetMap(dataArray(1)) += dataArray(2)
              unverified_usertweetMap(dataArray(1)) = tweetList
            } else {
              val tweetList = new ListBuffer[String]()
              tweetList.append(dataArray(2))
              unverified_usertweetMap += (dataArray(1) -> tweetList)
            }

            // build map for statistics of tweet -> # of occurrence
            if (unverified_tweetOccurrenceMap.contains(dataArray(2))) {
              unverified_tweetOccurrenceMap(dataArray(2)) = unverified_tweetOccurrenceMap(dataArray(2)) + 1
            } else {
              unverified_tweetOccurrenceMap += (dataArray(2) -> 1)
            }
          }
          //reset all variables after we done processing one tweet
          unverified_line_break = ""
          unverified_array_counter = 0
          unverified_strBuilder.setLength(0)
        } else {
          unverified_line_break += line + "\n"
        }
      }

      //now we begin the caculation for verified tweets

      for (line <- Source.fromFile(args(1)).getLines()) {
        verified_strBuilder.append(line)
        verified_array_counter += line.split(",::,").length - 1
        //the reason for this array_counter is that parsing the input file contains multiple line break which would mess
        //with the ,::, so this will help us gather all the line break
        if (verified_array_counter == 4) {
          val dataArray = verified_strBuilder.toString().split(",::,")
          if (dataArray(3) != "null") { // check for null locataion
            ver_total_tweets += 1
            verified_locationSet += dataArray(3)
            //we have to do this because we want to keep the original line break from input

            // build map for statistics of user -> list of tweet from that user
            if (verified_usertweetMap.contains(dataArray(1))) {
              val tweetList = verified_usertweetMap(dataArray(1)) += dataArray(2)
              verified_usertweetMap(dataArray(1)) = tweetList
            } else {
              val tweetList = new ListBuffer[String]()
              tweetList.append(dataArray(2))
              verified_usertweetMap += (dataArray(1) -> tweetList)
            }

            // build map for statistics of tweet -> # of occurrence
            if (verified_tweetOccurrenceMap.contains(dataArray(2))) {
              verified_tweetOccurrenceMap(dataArray(2)) = verified_tweetOccurrenceMap(dataArray(2)) + 1
            } else {
              verified_tweetOccurrenceMap += (dataArray(2) -> 1)
            }
          }
          //reset all variables after we done processing one tweet
          verified_line_break = ""
          verified_array_counter = 0
          verified_strBuilder.setLength(0)
        } else {
          verified_line_break += line + "\n"
        }
      }

      val uv_user_count = unverified_usertweetMap.size //this will give the unique user count for unverified users
      val uv_tweet_count = unverified_tweetOccurrenceMap.size //this will give the tweet count for unverified users
      val uv_location_count = unverified_locationSet.size // this will give the location count for unverified users
      val uv_sum_rec = unverified_usertweetMap.foldLeft(0.0)((z, m1) => z + (1.0 / m1._2.distinct.size)) //this will give the summation of reciprocal of user tweet count
      val uv_diffusion_index = uv_user_count.toDouble / unver_total_tweets.toDouble //calculating the diffusion index
      val uv_geo_s_i = uv_location_count.toDouble / unver_total_tweets.toDouble // will give geographic spread index
      val uv_spam = uv_sum_rec.toDouble / unver_total_tweets.toDouble //will give spam index

      val v_user_count = verified_usertweetMap.size //this will give the unique user count for verified users
      val v_tweet_count = verified_tweetOccurrenceMap.size //this will give the tweet count for verified users
      val v_location_count = verified_locationSet.size // this will give the location count for verified users
      val v_sum_rec = verified_usertweetMap.foldLeft(0.0)((z, m1) => z + (1.0 / m1._2.distinct.size)) //this will give the summation of reciprocal of user tweet count
      val v_diffusion_index = v_user_count.toDouble / ver_total_tweets.toDouble //calculating the diffusion index
      val v_geo_s_i = v_location_count.toDouble / ver_total_tweets.toDouble // will give geographic spread index
      val v_spam = v_sum_rec.toDouble / ver_total_tweets.toDouble //will give spam index

      print("|------------------------------------------------------------------------------------|" + "\n")
      print("|content                           | unverifed                 | verified            | " + "\n")
      print("|------------------------------------------------------------------------------------|" + "\n")
      print(s"|number of unique users            |  $uv_user_count                      |    $v_user_count               | " + "\n")
      print("|------------------------------------------------------------------------------------|" + "\n")
      print(s"|number of unique tweets           | $uv_tweet_count       		       | $v_tweet_count                  |" + "\n")
      print("|------------------------------------------------------------------------------------|" + "\n")
      print(s"|number of unique locations        | $uv_location_count                       | $v_location_count                  |" + "\n")
      print("|------------------------------------------------------------------------------------|" + "\n")
      print(s"|diffusion_index                   | $uv_diffusion_index        | $v_diffusion_index  |" + "\n")
      print("|------------------------------------------------------------------------------------|" + "\n")
      print(s"|geographic spread index           | $uv_geo_s_i        | $v_geo_s_i  |" + "\n")
      print("|------------------------------------------------------------------------------------|" + "\n")
      print(s"|spam index                        | $uv_spam        | $v_spam  |" + "\n")
      print("|------------------------------------------------------------------------------------|" + "\n")

    }
  }
}

