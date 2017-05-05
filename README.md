# independent_study
veracity project
1. All the data is streamed using spark streaming and tweets are collected if it contains the keyword "sports". If you would like to check for hash tags just change "sports" to your hashtag "#hashtag"
2. You can set the time limit for how long do you want to stream the data by changing the seconds in saveTweets.scala file.
3. The main class will take two arguments which are unverified part-00000 and verified part-00000 in the respective folder of unverified and verfied
4. The result of the main will be give diffusion_index,geographic_spread_index and spam_index
	where diffusion index = #unique user /total tweets
		  geographic spread index =#unique location / total tweets
		  spam index =(summation of reciprocal of each user tweet count)/total tweets
