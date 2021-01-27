package com.revature.bookapp.cli

import scala.io.StdIn
import scala.util.matching.Regex

import com.revature.bookapp.model.Book
import com.revature.bookapp.daos.BookDao
/**
  * A CLI that allows the user to interact with our application
  * 
  * This CLI is a class because in the future we might provide customization options that can be
  * set when creating a new CLI instance.
  * 
  * This Cli class will contain all the logic involving interacting with the user.
  * We don't want all of our classes to be able to receive input from the command line or
  * write to the command line. Instead, we'll have (almost) all that happen here.
  * 
  */

class Cli {

    /**
      * commandArgPattern is a regular expression (regex) that will help us
      * extract the command and arginement from user input on the command line
      * 
      * Regex is a tool used for pattern matching strings. Lots of languages and other tools
      * support regex. It's good to learn atleast the basic, but you just use this code for your project
      * if you like.
      */
    val commandArgPattern : Regex = "(\\w+)\\s*(.*)".r

    /**
     * Prints a greeting to the user
     */
    def printWelcome() : Unit = {
        println("Welcome to Word Count CLI!")
    }

    /**
      * Prints the commands available to the User
      */
    def printOptions() : Unit = {
        println("Commands Available:")
        println("list books : Display all the books")
        println("find [title] : Finds all the books with the title")
        println("add book : Add another book to the database")
        println("exit : exit the CLI")
    }

    /**
      * This runs the menu, this is the entrypoint to the Cli class
      * 
      * The menu will interact with the user on a loop and call other methods/classes
      * in order to achieve results of the user's commands
      */
    def menu() : Unit = {
        
        // this is the first version, not a loop
        // take user input using StdIn.readLine
        // readLine is "blocking" which means that it pauses the program execution while it waits for input
        // this is fine for us, but  we do want to take notee.
        var exit = true;
        var input : String = ""
        // Here's an example using our regex above, feel free to just follow along with similar commands and args
        do {
            printWelcome()
            printOptions()
            input = StdIn.readLine()
            input match {
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("exit") => {
                    exit = false;
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("list") && arg.equalsIgnoreCase("books") => {
                    printAllBooks()
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("find") => {
                    printBooks(arg)
                }
                case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("add") && cmd.equalsIgnoreCase("book") => {
                    runAddBookSubMenu()
                }
                case _ => {
                    println(s"""Please add enter a input command """)
                }

            }
        } while (exit)
        println("Thank you for using Word Count CLIm, goodbye!")
    }

    def printAllBooks() : Unit = {
        BookDao.getAll().foreach(println)
    }

    def printBooks(arg : String) : Unit = {
        BookDao.get(arg).foreach(println)
    }

    /**
      * runs an add book sub menu, we're skipping some QoL features present in the main menu
      */
    def runAddBookSubMenu() : Unit = {
        println("title?")
        val titleInput = StdIn.readLine()
        println("ISBN? (Must be 13 digits long)")
        val isbnInput = StdIn.readLine()


        try {
            if(BookDao.saveNew(Book(0, titleInput, isbnInput))) {
                println("Added New Book")
            }
        } catch {
            case e : Exception => {
                println("Failed to add book.")
            }
        }
    }
}