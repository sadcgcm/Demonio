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
	
	public mysql (){
		DATABASE = mysqlGlobals.DATABASE;
		HOST = mysqlGlobals.HOST;
		USER = mysqlGlobals.USER;
		PASSWORD = mysqlGlobals.PASSWORD;
		PORT = mysqlGlobals.PORT;
		URL = "jdbc:mysql://" + HOST + ":" + PORT + "/" + DATABASE; 
	}
	
	public boolean UpdateUser(String id_user, String name, String location, String description, int followers_count, String create_at, String json, int friends_count, int listed_count, String screen_name, int statuses_count){
		boolean is_updated = false;
		int rpta = -1;
		try{
			CON = DriverManager.getConnection(URL, USER, PASSWORD);
			CON.setAutoCommit(true);
			PSTMT = CON.prepareStatement("UPDATE users SET name = ?, location = ?, description = ?, followers_count = ?, create_at = ?, json = ?, friends_count = ?, listed_count = ?, screen_name = ?, statuses_count = ?, status = ? WHERE id_tweet = " + id_user);
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
			System.out.println("Error al conectar con la base de datos(Actualizar). -> " + e.toString());
		}finally{
			try {
				CON.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return is_updated;
	}
	
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
			e.printStackTrace();
	    } finally {
	      try {
	    	  RS.close();
		      STMT.close();
		      CON.close();
	      } catch (SQLException e) {
	        e.printStackTrace();
	      }
	    }
		return List_Users;
	}
	
}
