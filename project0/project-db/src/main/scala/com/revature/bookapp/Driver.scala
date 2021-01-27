package com.revature.bookapp

import java.sql.DriverManager
import java.sql.ResultSet
import scala.util.Using
import com.revature.bookapp.utils.ConnectionUtil
import com.revature.bookapp.model.Book
import com.revature.bookapp.model.Track
import com.revature.bookapp.daos.BookDao
import com.revature.bookapp.cli.Cli
import java.sql.Connection

object Driver {
    def main(args: Array[String]) : Unit = {
        val cli = new Cli()
        cli.menu()
        //runDemo()
    }
    def runDemo() = {
        // missing a bit of documentation from the java code:
        // getConnection takes a url, username, password
        
        var conn : Connection = null;
        Using.Manager { use =>
        
            conn = use(ConnectionUtil.getConnection())

            // use a prepared statement to try to get tracks
            val demoStatement = use(conn.prepareStatement("SELECT * FROM track;"))
            // Execute the statement. Might take a while.
            demoStatement.execute()

            val resultSet = use(demoStatement.getResultSet())
            
            while(resultSet.next()) {
                Track.produceFromResultSet(resultSet)
            }

            demoStatement.close()
            
            // We're using a prepared statement here. A parepared statement
            // lets us use parameters in the query we sent to postgres.
            // This protects us from SQL injection
            val bookStatment = use(conn.prepareStatement("SELECT * FROM track WHERE length(name) > ? AND name LIKE ?;"))
            // our prepared statment uses ? to mark where a parameter is used
            // we then set the value of the question mark using its 1-based index
            // so the first ? is 1, and the 2nd is 2, and 3rd is 3, ... and so on
            bookStatment.setInt(1, 10)
            bookStatment.setString(2, "T%")
            bookStatment.execute()
            val rs = use(bookStatment.getResultSet())
            while(rs.next()) {
                println(Track.produceFromResultSet(rs))
            }

        }

        val insertBookStatment = conn.prepareStatement("INSERT INTO BOOK VALUES (DEFAULT, ?, ?);")
        insertBookStatment.setString(1, "New Book String")
        insertBookStatment.setString(2, "1234567890123")
        insertBookStatment.execute()
        insertBookStatment.close()
        
        
        
        conn.close() // Good practice to close resources once you're done with them
    }
}

