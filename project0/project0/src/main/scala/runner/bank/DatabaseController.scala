package runner.bank

import java.sql.DriverManager
import java.sql.Connection

object DatabaseController  {
    var connection : Connection = null;
     /**
      * utility for retrieving connection, with hardcoded credentials
      * 
      * This should properly be a connection pool. Instead we'll use it like a connection pool with a single connection
      * that gets returned whenever any part of our application needs it
      *
      * @return
      */
    def getConnection() : Connection = {
        
        if(connection == null || connection.isClosed()) {
            classOf[org.postgresql.Driver].newInstance() // manually load the Driver
            val host = "scala-spark-usf-db.c32anyjmqflo.us-east-1.rds.amazonaws.com"
            val port = 5432
            val database = "jeff"
            connection = DriverManager.getConnection(s"jdbc:postgresql://$host:$port/$database", "jeff", "wasspord409")
        }
        
        connection
    }

    def disconnect() = { connection.close() }

}