package com.revature.bookapp.daos

import java.sql.DriverManager
import java.sql.ResultSet
import scala.util.Using
import com.revature.bookapp.model.Book
import com.revature.bookapp.utils.ConnectionUtil
import scala.collection.mutable.ArrayBuffer
/**
  * A Book Data Access Object. BookDao has CRUD Methods for Books
  * 
  * It allows us to keep all of our database access logic in this file,
  * while still allowing the rest of our application to use Books retrieved
  * from the database.
  */
object BookDao {
    def getAll() : Seq[Book] = {
        val conn = ConnectionUtil.getConnection();
        


        Using.Manager { use => 
            val stmt = use(conn.prepareStatement("SELECT * FROM book;"))
            stmt.execute()
            val rs = use(stmt.getResultSet())
            val allBooks : ArrayBuffer[Book] = ArrayBuffer()
            while(rs.next()) {
                allBooks.addOne(Book.fromResultSet(rs))
            }
            allBooks.toList
        }.get
        // the .get retrieves the value from inside the Try[Seq[Book]] returned by Using.Manager { ... }
        // it may be better not call .get and instead return the Try[Seq[Book]]
        // that would let the calling method unpack the Try and take action in case of failure
    }

    def get(title: String) : Seq[Book] = {
        val conn = ConnectionUtil.getConnection();

        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("SELECT * FROM book WHERE title = ?;"))
            stmt.setString(1, title)
            stmt.execute()
            val rs = use(stmt.getResultSet())
            val bookWithTitle: ArrayBuffer[Book] = ArrayBuffer()
            while (rs.next()) {
                bookWithTitle.addOne(Book.fromResultSet(rs))
            }
            bookWithTitle.toList
        }.get
    }

    def saveNew(book : Book) = {
        val conn = ConnectionUtil.getConnection()
        Using.Manager { use => 
            val stmt = use(conn.prepareStatement("INSERT INTO book VALUES (DEFAULT, ?, ?);"))
            stmt.setString(1, book.title)
            stmt.setString(2, book.isbn)
            stmt.execute()

            stmt.getUpdateCount() > 0
        }.getOrElse(false)
    } 
}