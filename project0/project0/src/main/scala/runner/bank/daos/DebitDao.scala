package runner.bank.daos

import java.sql.DriverManager
import java.sql.ResultSet
import scala.util.Using
import runner.bank.DatabaseController
import runner.bank.models.Person
import runner.bank.models.Debit

object DebitDao {

    def loadAcc(userId: Int) : Debit = {
        val conn = DatabaseController.getConnection();

        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("SELECT * FROM debit_acc WHERE user_id = ?;"))
            stmt.setInt(1, userId)
            stmt.execute()
            val rs = use(stmt.getResultSet())
            var debit : Debit = null
            while(rs.next()) {
                debit = Debit.getDebitDetails(rs)
            }
            debit
        }.get
    }

    def withdraw(userId: Int, balance: Double) = {
        val conn = DatabaseController.getConnection();

        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("UPDATE debit_acc SET balance = (balance - ?) WHERE user_id = ?;"))
            stmt.setDouble(1, balance)
            stmt.setInt(2, userId)
            stmt.execute()

            stmt.getUpdateCount() > 0
        }.getOrElse(false)
    }

    def deposit(userId: Int, balance: Double) = {
        val conn = DatabaseController.getConnection();

        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("UPDATE debit_acc SET balance = (balance + ?) WHERE user_id = ?;"))
            stmt.setDouble(1, balance)
            stmt.setInt(2, userId)
            stmt.execute()

            stmt.getUpdateCount() > 0
        }.getOrElse(false)
    }

    def checkBal(userId: Int) : Double = {
        val conn = DatabaseController.getConnection();

        Using.Manager {use => 
            val stmt = use(conn.prepareStatement("SELECT balance FROM debit_acc WHERE user_id = ?;"))
            stmt.setInt(1, userId)
            stmt.execute()

            val rs = use(stmt.getResultSet())
            var balance : Double = 0.0
            while(rs.next()) {
                balance = rs.getDouble("balance")
            }
            balance
        }.get
    }
}