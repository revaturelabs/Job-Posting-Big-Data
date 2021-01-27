package runner.bank

import scala.io.BufferedSource
import scala.io.Source
import scala.collection.Iterator
import scala.collection.mutable.Map
import java.io.File
import runner.bank.daos.PersonDao
import runner.bank.models.Person
import runner.bank.daos.CreditDao
import runner.bank.daos.DebitDao

object FileUtil {

    def getContents(filename: String, sep: String = " ") = {

        var openedFile : BufferedSource = null
        var lines : Iterator[String] = null

        try {
            openedFile = Source.fromFile(filename)
            lines = openedFile.getLines()
            
            for(line <- lines) {
                val col = line.split(",").map(_.trim)
                PersonDao.addUser(Person(-1, col(0), col(1), col(2).toInt, col(3), false, col(4), null, null))
            }

        } catch {
            case e : Exception => {
                println(s"${e.getMessage()}")
            }
        }finally {
            if(openedFile != null) openedFile.close()
        }

    }


}