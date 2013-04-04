package com.bg.parser.twitter;

import java.util.ArrayList;
import java.util.List;

import com.bg.bd.mysql.mysql;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class twitter {
	private String CONSUMER_KEY;
	private String CONSUMER_SECRET;
	private String ACCESS_TOKEN;
    private String ACCESS_TOKEN_SECRET;
    private Twitter twitter_;
    
    public twitter(){
    	CONSUMER_KEY = twitterGlobals.CONSUMER_KEY;
    	CONSUMER_SECRET = twitterGlobals.CONSUMER_SECRET;
    	ACCESS_TOKEN = twitterGlobals.ACCESS_TOKEN;
    	ACCESS_TOKEN_SECRET = twitterGlobals.ACCESS_TOKEN_SECRET;
    	Authenticate();
    }
    
    private void Authenticate(){
    	ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
            .setOAuthConsumerKey(CONSUMER_KEY)
            .setOAuthConsumerSecret(CONSUMER_SECRET)
            .setOAuthAccessToken(ACCESS_TOKEN)
            .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
        twitter_ = new TwitterFactory(cb.build()).getInstance();
    }
    
    public Twitter getTwitter(){
    	if (twitter_ == null) Authenticate();
    	return twitter_;
    }
    
    public void Update_Users(){
    	List<String> User_ids = new ArrayList<String>();
    	mysql m = new mysql();
    	User_ids = m.getUsers();
    	for(int i = 0; i < twitterGlobals.USER_COUNT_UPDATE; i++){
    		try {
				twitter4j.User  user = twitter_.showUser( Long.valueOf(User_ids.get(i)) );
				m.UpdateUser(User_ids.get(i), user.getName(),  user.getLocation(), user.getDescription(), user.getFollowersCount(), user.getCreatedAt().toString(), user.toString(), user.getFriendsCount(), user.getListedCount(), user.getScreenName(), user.getStatusesCount());
			} catch (NumberFormatException e) {
				//e.printStackTrace();
			} catch (TwitterException e) {
				//e.printStackTrace();
			}
    	}
    }
    
    
}
