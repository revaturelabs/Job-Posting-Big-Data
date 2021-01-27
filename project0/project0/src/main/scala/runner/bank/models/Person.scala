package runner.bank.models

import runner.bank.models.Credit
import runner.bank.models.Debit

import java.sql.ResultSet

case class Person(userId : Int, first_name : String, last_name : String, age : Int, 
        username: String, is_admin : Boolean, user_pass : String = "", debit_acc : Debit = null, credit_acc : Credit  = null) {

    def getAccount() : Unit = {
        println("____________________________________________________")
        println("               Your Account Balances                ")
        println("____________________________________________________")
        println(s"Account ID: ${userId}")
        println(s"First Name: ${first_name}")
        println(s"Last Name: ${last_name}")
        println(s"Debit Balance: ${debit_acc.getBalance()}")
        println(s"Credit Balance: ${credit_acc.getBalance()}")
        println(s"Credit Awards: ${credit_acc.getAwards()}")
        println("____________________________________________________")
    }
}

object Person {
    def getResult(rs : ResultSet, debit : Debit, credit: Credit) : Person = {
        apply(
            rs.getInt("user_id"), 
            rs.getString("first_name"),
            rs.getString("last_name"),
            rs.getInt("age"),
            rs.getString("username"), 
            rs.getBoolean("is_admin"),
            "",
            debit, 
            credit
        )
    }
}