package runner.bank.daos

import java.sql.DriverManager
import java.sql.ResultSet
import scala.util.Using
import runner.bank.DatabaseController
import runner.bank.models.Person
import runner.bank.models.Credit


object CreditDao {

    def loadAcc(userId: Int) : Credit = {
        val conn = DatabaseController.getConnection();

        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("SELECT * FROM credit_acc WHERE user_id = ?;"))
            stmt.setInt(1, userId)
            stmt.execute()
            val rs = use(stmt.getResultSet())
            var balance : Double = 0.0
            var credit : Credit = null
            while(rs.next()) {
                credit = Credit(rs)
            }
            credit
        }.get
    }

    def withdraw(userId: Int, balance: Double) : Boolean  = {
        val conn = DatabaseController.getConnection();

        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("UPDATE credit_acc SET balance = (balance + ?) WHERE user_id = ?;"))
            stmt.setDouble(1, balance)
            stmt.setInt(2, userId)
            stmt.execute()
            stmt.getUpdateCount() > 0
        }.getOrElse(false)
    }

    def pay(userId: Int, balance: Double) = {
        val conn = DatabaseController.getConnection();

        Using.Manager { use =>
            val stmt = use(conn.prepareStatement("UPDATE credit_acc SET balance = (balance - ?) WHERE user_id = ?;"))
            stmt.setDouble(1, balance)
            stmt.setInt(2, userId)
            stmt.execute()
            val credit_upt = stmt.getUpdateCount() > 0
            stmt.close()

            val stmt2 = use(conn.prepareStatement("UPDATE credit_acc SET cash_awards = (cash_awards + (? * 0.03)) WHERE user_id = ?;"))
            stmt2.setDouble(1, userId)
            stmt2.setInt(2, userId)
            stmt2.execute()
            val cash_awrd = stmt2.getUpdateCount() > 0
            stmt2.close()

            credit_upt && cash_awrd
        }.getOrElse(false)
    }

    def checkBal(userId: Int) : Double = {
        val conn = DatabaseController.getConnection();

        Using.Manager {use => 
            val stmt = use(conn.prepareStatement("SELECT balance FROM credit_acc WHERE user_id = ?;"))
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

    def updateAwards(userId: Int, award: Double) = {
        val conn = DatabaseController.getConnection();

        Using.Manager {use => 
            val stmt = use(conn.prepareStatement("UPDATE cash_awards SET cash_awards = ?  WHERE user_id = ?;"))
            stmt.setDouble(1,award)
            stmt.setInt(2, userId)
            stmt.execute()

            stmt.getUpdateCount() > 0
        }.getOrElse(false)
    }
    
    
    def checkAwards(userId: Int) : Double = {
        val conn = DatabaseController.getConnection();

        Using.Manager {use => 
            val stmt = use(conn.prepareStatement("SELECT cash_awards FROM credit_acc WHERE user_id = ?;"))
            stmt.setInt(1, userId)
            stmt.execute()

            val rs = use(stmt.getResultSet())
            var awards : Double = 0.0
            while(rs.next()) {
                awards = rs.getDouble("cash_awards")
            }
            awards
        }.get
    }
}