package com.bg.bd.mysql;

import java.sql.CallableStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.bg.parser.twitter.twitterGlobals;

/**
 * 
 * @author KRISTIAN 
 *  this class is used for inserting tweets in a database JUST TWEETS DEPENDING HOW MANY WE WANT, in other
 *  words we can decide how many tweets we want to get for each user
 */

public class mysql {
	private String DATABASE;
	private String HOST;
	private String USER;
	private String PASSWORD;
	private String PORT;
	private String URL;
	private Connection CON;
	private PreparedStatement PSTMT;
	private Statement STMT;
	private ResultSet RS;
	
/**
 * this is the constructor of the class with the parameters of mysqlGlobals
 */
	public mysql (){
		DATABASE = mysqlGlobals.DATABASE;
		HOST = mysqlGlobals.HOST;
		USER = mysqlGlobals.USER;
		PASSWORD = mysqlGlobals.PASSWORD;
		PORT = mysqlGlobals.PORT;
		URL = "jdbc:mysql://" + HOST + ":" + PORT + "/" + DATABASE; 
	}
	
	
	/**
	 * 
	 * @param id_tweet -> identidication of the tweet
	 * @param text -> the tweet's text 
	 * @param to_ -> the screen_name of the user owner
	 * @param source -> where it was posted
	 * @param create_at -> where it was posted
	 * @param json -> the complete json for the tweet 
	 * @param place -> the place where the tweet was posted it can be null
	 * @param retweet_count -> how many retweets it has
	 * @param truncate -> the text wsa truncated
	 * @param user -> the user owner
	 * @return -> true if it was store
	 */
	public boolean StoreTweets(String id_tweet, String text, String to_, String source, String create_at, String json, String place, long retweet_count, boolean truncate, String user){
		boolean is_store = false;
		int rpta = -1;
		try{
			CON = DriverManager.getConnection(URL, USER, PASSWORD);
			CallableStatement CS = CON.prepareCall(" { call get_id_tweets(?,?) } ");
			CS.setString(1,id_tweet);
			CS.registerOutParameter(2,java.sql.Types.INTEGER);
			CS.execute();
			rpta = CS.getInt(2);
			if (rpta > 0){
				CON.setAutoCommit(true);
				PSTMT = CON.prepareStatement("UPDATE tweets SET text = ?, to_ = ?, source =?, created_at = ?, json = ?, place = ?, retweet_count = ?, truncate = ?, user = ? WHERE id_tweet = " + id_tweet);
				PSTMT.setString(1, text);
				PSTMT.setString(2, to_);
				PSTMT.setString(3, source);
				PSTMT.setString(4, create_at);
				PSTMT.setString(5, json);
				PSTMT.setString(6, place);
				PSTMT.setLong(7, retweet_count);
				PSTMT.setBoolean(8, truncate);
				PSTMT.setString(9, user);
				PSTMT.executeUpdate();
				is_store = false;
			}
			if (rpta == 0){
				CON.setAutoCommit(true);
				PSTMT = CON.prepareStatement(" INSERT INTO tweets(id_tweet, text, to_, source, created_at, json, place, retweet_count, truncate, user) VALUES(?,?,?,?,?,?,?,?,?,?) ");
				PSTMT.setString(1, id_tweet);
				PSTMT.setString(2, text);
				PSTMT.setString(3, to_);
				PSTMT.setString(4, source);
				PSTMT.setString(5, create_at);
				PSTMT.setString(6, json);
				PSTMT.setString(7, place);
				PSTMT.setLong(8, retweet_count);
				PSTMT.setBoolean(9, truncate);
				PSTMT.setString(10, user);
				PSTMT.executeUpdate();
				is_store = true;
			}
			CS.close();
		}catch(Exception e){
			System.out.println("Error with sql connection(StoreTweets). SQLException -> " + e.toString());
		}finally{
			try {
				CON.close();
			} catch (SQLException e) {
				System.out.println("Error with sql connection(StoreTweets). finally - SQLException -> " + e.toString());
			}
		}
		return is_store;
	}

	/**
	 * 
	 * @param id_user -> the tweet's owner's id
	 * @param id_tweet -> the tweet's id
	 * @return
	 */
	public boolean StoreTweetUser(String id_user, String id_tweet){
		int rpta = -1;
		boolean is_insert = false;
		try{
			CON = DriverManager.getConnection(URL, USER, PASSWORD);
			CallableStatement CS = CON.prepareCall(" { call get_user_tweet(?,?,?) } ");
			CS.setString(1,id_user);
			CS.setString(2,	id_tweet);
			CS.registerOutParameter(3,java.sql.Types.INTEGER);
			CS.execute();
			rpta = CS.getInt(3);
			if (rpta == 0){
				CON.setAutoCommit(true);
				PSTMT = CON.prepareStatement(" INSERT INTO tweet_user(id_user, id_tweet) VALUES(?,?) ");
				PSTMT.setString(1, id_user);
				PSTMT.setString(2, id_tweet);
				PSTMT.executeUpdate();	
				is_insert = true;
			}
			CS.close();
		}catch(Exception e){
			System.out.println("Error with sql connection(StoreTweetUser). SQLException -> " + e.toString());
		}finally{
			try {
				CON.close();
			} catch (SQLException e) {
				System.out.println("Error with sql connection(StoreTweetUser). finally - SQLException -> " + e.toString());
			}
		}
		return is_insert;
	}
	
	/**
	 * 
	 * @param id_user -> the tweet's user's id
	 */
	public void UpdateTweetDownloaded(String id_user){
		try{
			CON = DriverManager.getConnection(URL, USER, PASSWORD);
			CON.setAutoCommit(true);
			CallableStatement CS = CON.prepareCall(" { call update_count_tweets_downloaded(?) } ");
			CS.setString(1,id_user);
			CS.execute();
			CS.close();
		}catch(Exception e){
			System.out.println("Error with sql connection(UpdateTweetDownloaded). SQLException -> " + e.toString());
		}finally{
			try {
				CON.close();
			} catch (SQLException e) {
				System.out.println("Error with sql connection(UpdateTweetDownloaded). finally - SQLException -> " + e.toString());
			}
		}
	}
	
	/**
	 * 
	 * @return -> list of users with at least the same quantity of tweets we setted in mysqlGlobals
	 */
	public List<String> getUsers(){
		int i = 0;
		List<String> List_Users = new ArrayList<String>();
		try {
			CON = DriverManager.getConnection(URL, USER, PASSWORD);
			String query = "select u.id_user from users u where u.download_tweets < " + Integer.toString(twitterGlobals.LIMIT_GET_TWEETS) + " order by u.download_tweets asc";
			STMT = CON.createStatement();
			RS = STMT.executeQuery(query);
			while(RS.next()){
				i++;
				String id_user = RS.getString(1);
				List_Users.add( id_user );
				if(i > twitterGlobals.GET_COUNT_USER_TWEETS) break;
			}	
		} catch (SQLException e) {
			System.out.println("Error with sql connection(getUsers). SQLException -> " + e.toString());
	    } finally {
	      try {
	    	  RS.close();
		      STMT.close();
		      CON.close();
	      } catch (SQLException e) {
	    	  System.out.println("Error with sql connection(getUsers). finally - SQLException -> " + e.toString());
	      }
	    }
		return List_Users;
	}
	
}
