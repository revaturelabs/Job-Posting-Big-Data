package com.revature.proj1

import scala.io.BufferedSource
import scala.io.Source
import java.io.File
import scala.collection.Iterator


object FileUtil {


    def getMaxReceivedCounts(filename: String, sep: String = " ") = {
        var openedFile : BufferedSource = null
        var lines : Iterator[String] = null

        var highestViewed : String = "" 
        var maxViews : Double = 0.0

        try {
            openedFile = Source.fromFile(filename)
            lines = openedFile.getLines()
            while(lines.hasNext) {
                var line = lines.next().split("\\t")

                if(maxViews < line(1).toDouble && line(0).toString != "Main_Page" && line(0).toString != "Special:Search") {
                    highestViewed = line(0)
                    maxViews = line(1).toDouble
                }
            }

            highestViewed + " " + maxViews.toString
        }
        catch {
            case e : Exception => {
                println(s"${e.getMessage()}")
            }
        } finally {
            if(openedFile != null) openedFile.close()
        }

    } 
}