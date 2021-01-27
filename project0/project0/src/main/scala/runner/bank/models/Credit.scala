package runner.bank.models

import java.sql.ResultSet

class Credit(accId: Int, var balance : Double, var cash_awards : Double) {

    def addBalance(bal : Double) : Unit = {
        balance = balance + bal
    }

    def remBalance(bal : Double) : Unit = {
        balance = balance - bal
    }

    def addAward(bal : Double) : Unit = {
        cash_awards = cash_awards + bal
    }

    def remAward(bal : Double) : Unit = {
        cash_awards = cash_awards - bal
    }

    def getAccID() : Int = {
        accId
    }

    def getBalance() : Double = {
        balance
    }

    def getAwards() : Double = {
        cash_awards
    }
}

object Credit {
    def apply(rs: ResultSet) : Credit = {
        new Credit(
            rs.getInt("acc_id"),
            rs.getDouble("balance"),
            rs.getDouble("cash_awards")
        )
    }
}