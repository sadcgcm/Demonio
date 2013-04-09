package com.bg.parser.twitter;

import java.util.ArrayList;
import java.util.List;

import com.bg.bd.mysql.mysql;

import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * this class is for twitter OAuth( to authenticate) the application
 * @author KRISTIAN
 *
 */
public class twitter {
	private String CONSUMER_KEY;
	private String CONSUMER_SECRET;
	private String ACCESS_TOKEN;
    private String ACCESS_TOKEN_SECRET;
    private Twitter twitter_;
    
    /**
     * the constructor of the class setting the Keys and Tokens
     */
    public twitter(){
    	CONSUMER_KEY = twitterGlobals.CONSUMER_KEY;
    	CONSUMER_SECRET = twitterGlobals.CONSUMER_SECRET;
    	ACCESS_TOKEN = twitterGlobals.ACCESS_TOKEN;
    	ACCESS_TOKEN_SECRET = twitterGlobals.ACCESS_TOKEN_SECRET;
    	Authenticate();
    }
    
    /**
     * To Authenticate the Application with the keys and tokens 
     */
    private void Authenticate(){
    	ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
            .setOAuthConsumerKey(CONSUMER_KEY)
            .setOAuthConsumerSecret(CONSUMER_SECRET)
            .setOAuthAccessToken(ACCESS_TOKEN)
            .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
        twitter_ = new TwitterFactory(cb.build()).getInstance();
    }
    
    /**
     * 
     * @return To get the instance of the twitter OAuth
     */
    public Twitter getTwitter(){
    	if (twitter_ == null) Authenticate();
    	return twitter_;
    }
    
    /**
     * 
     * @param screen_name -> To use the screen_name for getting tweets and store them
     * @param id_user -> To store the relation between tweet and user
     * @param m -> the mysql connection with the database
     */
    private void GetTweets(String screen_name, String id_user, mysql m){
        for (int i = 1; i< 25; i++){ //25
	        Paging pagina = new Paging(i,200);  //200
	        ResponseList<Status> list_tweets;
			try {
				list_tweets = twitter_.getUserTimeline(screen_name, pagina);
				if (list_tweets.size() > 0){
					for (int j = 0; j < list_tweets.size(); j++){
						Status Tweet = list_tweets.get(j);
						m.StoreTweets(Long.toString(Tweet.getId()), Tweet.getText(), Tweet.getUser().getScreenName(), Tweet.getSource(), Tweet.getCreatedAt().toString(), Tweet.toString(), Tweet.getPlace().toString(), Tweet.getRetweetCount(), Tweet.isTruncated(), Tweet.getUser().toString());
						m.StoreTweetUser(id_user, Long.toString(Tweet.getId()) );
						m.UpdateTweetDownloaded(Long.toString(Tweet.getId()) );
					}
				}
			} catch (TwitterException e) {
				System.out.println("Error when try to get tweets(CrawlerTweets). -> " + e.toString());
			}
        }
    }
    
    /**
     * To update tweets that we have stored;
     */
    public void GetTweets(){
    	List<String> User_ids = new ArrayList<String>();
    	mysql m = new mysql();
    	User_ids = m.getUsers();
    	if (User_ids.size() > 0){
    		for(int i = 0; i < twitterGlobals.GET_COUNT_USER_TWEETS; i++){
    			try {
    				String screen_mane = twitter_.showUser( Long.valueOf(User_ids.get(i)) ).getScreenName();
    				GetTweets(screen_mane, User_ids.get(i), m);
    			} catch (NumberFormatException e) {
    				System.out.println("Error on the function Update_Tweets(CrawlerTweets). NumberFormatException ->" + e.toString());
    			} catch (TwitterException e) {
    				System.out.println("Error on the function Update_Tweets(CrawlerTweets). TwitterException ->" + e.toString());
    			}
    		}
    	}
    }
    
    
}
