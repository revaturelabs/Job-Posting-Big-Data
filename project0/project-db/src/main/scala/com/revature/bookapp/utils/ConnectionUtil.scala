package com.revature.bookapp.utils
import java.sql.DriverManager
import java.sql.Connection

// DAO (Data Access Object) - Object that deals with accessing data from the database
object ConnectionUtil {
    var conn : Connection = null; 
    /**
      * utility for retrieving connection, with hardcoded credentials
      * 
      * This should properly be a connection pool. Instead we'll use it like a connection pool with a single connection
      * that gets returned whenever any part of our application needs it
      *
      * @return
      */
    def getConnection() : Connection = {
        
        if(conn == null || conn.isClosed()) {
            classOf[org.postgresql.Driver].newInstance() // manually load the Driver
            val host = "scala-spark-usf-db.c32anyjmqflo.us-east-1.rds.amazonaws.com"
            val port = 5432
            val database = "jeff"
            conn = DriverManager.getConnection(s"jdbc:postgresql://$host:$port/$database", "jeff", "wasspord409")
        }
        
        conn
    }
}