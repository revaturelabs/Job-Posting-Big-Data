package runner.bank.models

import java.sql.ResultSet

class Debit(accId: Int, var balance: Double) {
  
    def addBalance(bal : Double) : Unit = {
        balance = balance + bal
    }

    def remBalance(bal : Double) : Unit = {
        balance = balance - bal
    }

    def getAccID() : Int = {
        accId
    }

    def getBalance() : Double = {
        balance
    }

}

object Debit {

    def getDebitDetails(rs : ResultSet) : Debit = {
        new Debit(
            rs.getInt("acc_id"),
            rs.getDouble("balance")
        )
    }

}
