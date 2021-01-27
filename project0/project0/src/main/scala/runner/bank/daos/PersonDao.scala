package runner.bank.daos

import java.sql.DriverManager
import java.sql.ResultSet
import scala.util.Using
import runner.bank.DatabaseController
import runner.bank.models.Person
import runner.bank.daos.CreditDao
import runner.bank.daos.DebitDao


/**
  * A person dao that will access the login credentials when a person logs in to the CLI
  */
object PersonDao {
    def login(username: String, password: String) : Person = {
        val conn = DatabaseController.getConnection();
        Using.Manager { use => 
            val stmt = use(conn.prepareStatement("SELECT user_id, first_name, last_name, age, username, is_admin FROM users WHERE username = ? AND password = ? LIMIT 1;"))
            stmt.setString(1, username)
            stmt.setString(2, password)
            stmt.execute()
            val rs = use(stmt.getResultSet())
            var user : Person = null

            if(rs.next()) {
                user = Person.getResult(rs, DebitDao.loadAcc(rs.getInt("user_id")), CreditDao.loadAcc(rs.getInt("user_id")))
            }
            user
        }.get
    }

    def addUser(user : Person) = {
        val conn = DatabaseController.getConnection();
        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("INSERT INTO users(first_name, last_name, age, username, password) VALUES (?, ?, ?, ?, ?);"))
            stmt.setString(1, user.first_name)
            stmt.setString(2, user.last_name)
            stmt.setInt(3, user.age)
            stmt.setString(4, user.username)
            stmt.setString(5, user.user_pass)
            stmt.execute()

            stmt.getUpdateCount() > 0
        }.getOrElse(false)
    }

    def getUsers() = {
        val conn = DatabaseController.getConnection();

        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("SELECT user_id, username, is_admin FROM users WHERE is_admin = false;"))
            stmt.execute()
            val rs = use(stmt.getResultSet())
            var rowCount = 0;

            println("___________________________________________")
            println("            List of All Users              ")
            println("___________________________________________")
            println("User ID\tUsername\tAdmin Priviledges")
            while(rs.next()) {
                println(s"${rs.getInt("user_id")}\t\t${rs.getString("username")}\t${rs.getBoolean("is_admin")}")
                rowCount = rowCount + 1
            }
            println("___________________________________________")
            rowCount > 0
        }.getOrElse(false)
    }

    def removeUser(userID: Int) = {
        val conn = DatabaseController.getConnection();
        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("DELETE FROM users WHERE user_id = ?"))
            stmt.setInt(1, userID)
            stmt.execute()
            stmt.getUpdateCount() > 0
        }.getOrElse(false)
    }

    def removeAllUser() = {
        val conn = DatabaseController.getConnection();

        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("DELETE FROM users WHERE is_admin = false;"))
            stmt.execute()
            stmt.getUpdateCount() > 0
        }.getOrElse(false)
    }

    def getAccounts(userId: Int) = {
        val conn = DatabaseController.getConnection();
        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("SELECT u.user_id AS acc_id, first_name, last_name, d.balance AS debit_bal, c.balance AS credit_bal, cash_awards AS credit_awards FROM users u INNER JOIN debit_acc d ON d.user_id = u.user_id INNER JOIN credit_acc c ON c.user_id = u.user_id WHERE u.user_id = ?;"))
            stmt.setInt(1, userId)
            stmt.execute()
            val rs = use(stmt.getResultSet())
            while(rs.next()) {
                println("____________________________________________________")
                println("               Your Account Balances                ")
                println("____________________________________________________")
                println(s"Account ID: ${rs.getInt("acc_id")}")
                println(s"First Name: ${rs.getString("first_name")}")
                println(s"Last Name: ${rs.getString("last_name")}")
                println(s"Debit Balance: ${rs.getDouble("debit_bal")}")
                println(s"Credit Balance: ${rs.getDouble("credit_bal")}")
                println(s"Credit Awards: ${rs.getDouble("credit_awards")}")
                println("____________________________________________________")
            }
        }
    }
}