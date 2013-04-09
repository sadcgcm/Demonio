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
 *  this class is used for updating in a database USERS INFORMATION, WHO THEY ALREADY STORED
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
	 * @param id_user -> the identification of the users
	 * @param name -> the name of the users
	 * @param location -> where users are located
	 * @param description -> the small description users write down 
	 * @param followers_count -> how many followers they have
	 * @param create_at -> when the user is created
	 * @param json -> the complete user object we got
	 * @param friends_count -> how many friend user has
	 * @param listed_count -> how many list the user has
	 * @param screen_name -> the screen name of the user
	 * @param statuses_count -> how many different statuses the user has
	 * @return
	 */
	public boolean UpdateUser(String id_user, String name, String location, String description, int followers_count, String create_at, String json, int friends_count, int listed_count, String screen_name, int statuses_count){
		boolean is_updated = false;
		int rpta = -1;
		try{
			CON = DriverManager.getConnection(URL, USER, PASSWORD);
			CON.setAutoCommit(true);
			PSTMT = CON.prepareStatement("UPDATE users SET name = ?, location = ?, description = ?, followers_count = ?, create_at = ?, json = ?, friends_count = ?, listed_count = ?, screen_name = ?, statuses_count = ?, status = ? WHERE id_user = " + id_user);
			PSTMT.setString(1, name);
			PSTMT.setString(2, location);
			PSTMT.setString(3, description);
			PSTMT.setInt(4, followers_count);
			PSTMT.setString(5, create_at);
			PSTMT.setString(6, json);
			PSTMT.setInt(7, friends_count);
			PSTMT.setInt(8, listed_count);
			PSTMT.setString(9, screen_name);
			PSTMT.setInt(10, statuses_count);
			PSTMT.setString(11,"S");
			PSTMT.executeUpdate();
			is_updated = true;
		}catch(Exception e){
			System.out.println("Error to try to connect with the database(UpdateUser). Exception -> " + e.toString());
		}finally{
			try {
				CON.close();
			} catch (SQLException e) {
				System.out.println("Error with sql connection(UpdateUser). finally - SQLException -> " + e.toString());
			}
		}
		return is_updated;
	}
	
	/*
	 * To get the list of users that don't are update
	 */
	public List<String> getUsers(){
		int i = 0;
		List<String> List_Users = new ArrayList<String>();
		try {
			CON = DriverManager.getConnection(URL, USER, PASSWORD);
			String query = "select u.id_user from users u where u.status = 'N'";
			STMT = CON.createStatement();
			RS = STMT.executeQuery(query);
			while(RS.next()){
				i++;
				String id_user = RS.getString(1);
				List_Users.add( id_user );
				if(i > twitterGlobals.USER_COUNT_UPDATE) break;
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
