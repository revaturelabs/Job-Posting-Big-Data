package runner.bank

import scala.io.StdIn
import scala.util.matching.Regex
import runner.bank.FileUtil
import runner.bank.daos.PersonDao
import runner.bank.daos.CreditDao
import runner.bank.daos.DebitDao
import runner.bank.models.Person

class Cli {
    val commandArgPattern: Regex = "(\\w+)\\s*(.*)".r
    var user : Person = null

    def printWelcome() : Unit = {
        if(user == null) {
            println("       Unreliable Banking App         ")
            println("**************************")
            println("Login : Login to your bank account")
            println("Register : Register for a new bank account and credit card")
            println("exit : exit the app")
            
        } else {
            println("**************************")
            println("   Welcome to the unreliable Banking App!!     ")
            println(s"       Hello ${user.first_name}       ")
            println("  Below are your options for managing your bank acc!")
            println("**************************")
            println("account balance : check your current balance on both debit and credit")
            println("debit deposit: deposit money to your bank acc")
            println("debit withdraw : withdraw money from your bank")
            println("credit awards : check your current credit cash awards")
            println("credit collect : collect your credit cash awards")
            println("credit withdraw : withdraw money using credit card (max of $200)")
            println("credit pay : pay your current balance in your credit card")
            println(" ")

            if(user.is_admin) {
                println("**************************")
                println("      ADMIN COMMANDS      ")
                println("**************************")
                println("database upload : update csv data to the database. NOTE: Only accepts CSV files!")
                println("delete all : deletes all the users and their corresponding accounts")
                println("delete user : deletes a sepecific user")
            }
            println("**************************")
            println("Logout")
            println("exit : exit the app")
        }
    }

    def printLogin() : Unit = {
        println("   Please Login  ")
        println("**************************")
        login()
    }

    def menu() : Unit = {
        var input : String = ""
        var exit = true;
        do {
            printWelcome()
            print("What option would you like to execute: ")
            input = StdIn.readLine()
            input match {
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("login") => {
                    printLogin()
                } 
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("Register") => {
                    registerUser()
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("logout") => {
                    user = null
                } 
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("debit") && arg.equalsIgnoreCase("deposit") => {
                    try {
                        depositMoney(true)
                    }
                    catch {
                        case e : NumberFormatException => println("Invalid input format: must enter a number")
                    }  
  
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("debit") && arg.equalsIgnoreCase("withdraw") => {
                    try {
                        withdrawMoney(true)
                    }
                    catch {
                        case e : NumberFormatException => println("Invalid input format: must enter a number")
                    }
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("account") && arg.equalsIgnoreCase("balance") => {
                    //PersonDao.getAccounts(user.userId)
                    user.getAccount()
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("credit") && arg.equalsIgnoreCase("awards") => {
                    CreditDao.checkAwards(user.userId)
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("credit") && arg.equalsIgnoreCase("collect") => {
                    printAwardCollect(user.userId)
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("credit") && arg.equalsIgnoreCase("withdraw") => {
                    try {
                        withdrawMoney(false)
                    }
                    catch {
                        case e : NumberFormatException => println("Invalid input format: must enter a number")
                    }
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("credit") && arg.equalsIgnoreCase("pay") => {
                    try {
                        depositMoney(false)
                    }
                    catch {
                        case e : NumberFormatException => println("Invalid input format: must enter a number")
                    }
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("database") && arg.equalsIgnoreCase("upload") => {
                    printUploadName()
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("delete") && arg.equalsIgnoreCase("all") => {
                    removeAllConfirm()
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("delete") && arg.equalsIgnoreCase("user") => {
                    printRemoveUser()
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("exit") => {
                    user = null
                    exit = false
                }
                case _ => {
                    println("Invalid Input")
                }
            }
            spacer()
        } while (exit)
    }
    
    def printUploadName() = {
        var filename : String = ""
        print("Please enter the filename: ")
        filename = StdIn.readLine()
        FileUtil.getContents(s"./doc/${filename}", ",")
        println("Upload Successful!")
    }

    def depositMoney(isDebit : Boolean) = {
        var balance : String = ""
        if(isDebit) {
            print("Enter the amount you want to deposit: ")
            balance = StdIn.readLine()
            if(DebitDao.deposit(user.userId, balance.toDouble)) {
                user.debit_acc.addBalance(balance.toDouble)
                println("YOu have successfully deposited your money!")
            }
        }
        else {
            print("Enter the amount you want to pay: ")
            balance = StdIn.readLine()
            val bank_bal = DebitDao.checkBal(user.userId)
            if(balance.toDouble > bank_bal) {
                println("You do not have enough money to pay for your credit. You are broke F.")
            } else {
                if(CreditDao.pay(user.userId, balance.toDouble)) {
                    DebitDao.withdraw(user.userId, balance.toDouble)
                    user.credit_acc.remBalance(balance.toDouble)
                    user.credit_acc.addAward(balance.toDouble * 0.03)
                    println("You have successfully paid your credit card!")
                }
            }
        }
    }

    def withdrawMoney(isDebit : Boolean) = {
        var balance : String = ""
        print("Enter the amount you want to withdraw: ")
        balance = StdIn.readLine()
        if(isDebit) {
            val bank_bal = DebitDao.checkBal(user.userId) 
            if(balance.toDouble < bank_bal) {
                if(DebitDao.withdraw(user.userId, balance.toDouble)) {
                    println("You have successfully withdraw money to your account!")
                    user.debit_acc.remBalance(balance.toDouble)
                }
            } else {
                println("You do not have enough money in your debit account!")
            }
        }
        else {
            user.credit_acc.addBalance(balance.toDouble)
            CreditDao.withdraw(user.userId, balance.toDouble)
        }
    }

    def printRemoveUser() = {
        var user_id : String = ""
        if(PersonDao.getUsers()) {
            print("Enter the userid that you want to delete: ")
            user_id = StdIn.readLine()
            if(PersonDao.removeUser(user_id.toInt)) {
                println("Successfully removed the user.")
            }
        } else {
            println("There are no users right now in the database!")
        }
    }

    def removeAllConfirm() = {
        var answer : String = ""
        print("Would you really want to delete all the users (Yes or No)?: ")
        answer = StdIn.readLine()
        if(answer.equalsIgnoreCase("Yes")) {
            if(PersonDao.removeAllUser()) {
                println("Users removed. Complaints will come soon Good Luck on that!")
            }
        }
    }
 
    def printAwardCollect(userID : Int) = {
        var award : String = ""
        try {
            print("Enter the amount you want to collect: ")
            award = StdIn.readLine()
            val currAward = CreditDao.checkAwards(userID)
            if(award.toDouble <= currAward) {
                var userAward = user.credit_acc.getAwards()
                user.credit_acc.remAward(award.toDouble)
                user.debit_acc.addBalance(award.toDouble)
                CreditDao.updateAwards(userID, userAward - award.toDouble)
                DebitDao.deposit(userID, award.toDouble)
            } else {
                println("You do not have enough awards to collect this much.")
            }
        }
        catch {
            case e : NumberFormatException => println("Invalid input format: must enter a number")
        }
    }

    def registerUser() = {
        try {
            println("       Registration       ")
            println("**************************")
            println("First Name: ")
            val first_name = StdIn.readLine()
            println("Last Name: ")
            val last_name = StdIn.readLine()
            println("Age: ")
            val age = StdIn.readLine()
            println("Username: ")
            val username = StdIn.readLine()
            println("Password: ")
            val password = StdIn.readLine()

            PersonDao.addUser(new Person(-1, first_name, last_name, age.toInt, username, false, password))
            printLogin()
        }
        catch {
            case e : Exception => println("Invalid input format")
        }
    }

    def admMenu() : Unit = {
        println("        Admin Menu        ")
        println("**************************")
        println("1 - Add User")
        println("2 - Add question and answer")

        var input : String = ""
        input = StdIn.readLine()
    }

    def login() : Unit = {
        var userName : String = ""
        var pass : String = ""
        print("Username: ")
        userName = StdIn.readLine()
        print("Password: ")
        pass = StdIn.readLine()

        user = PersonDao.login(userName, pass)
        if(user == null) {
            println("Invalid username and password!")
        }
    }

    def close() = {
        DatabaseController.disconnect()
    }


    def spacer() : Unit = {
        println(" ")
        println(" ")
        println(" ")
        println(" ")
        println(" ")
        println(" ")
        println(" ")
        println(" ")
        println(" ")
        println(" ")
        println(" ")
        println(" ")
    }
}