package edu.berkeley.nlp.assignments.assign3.student

import scala.io._
import scala.collection.mutable.HashMap

import java.io.FileWriter


/**
 * Converts the corpus to integers. This saves us many hash map lookups
 * during alignment.
 */
object DataPreprocessor extends Application {

  override def main(args: Array[String]) {

    val converter = new Converter

    args.foreach( file => {
        println("Converting " + file)
        converter.convert(file)
        println("# of vocab: " + converter.index.size)
    })
  }
}


class Converter {
  
  val index = new HashMap[String, Int]

  def convert(file: String) {
    val s = Source.fromFile(file)

    val out = new FileWriter(file + ".out")

    val newFileData = s.getLines.foreach( line => {
      out.write(
        line.split(" ").map( word => {
          if (index.contains(word)) {
            index(word)
          } else {
            index += word -> (index.size + 1)
            index(word)
          }
        }).mkString(" ") + "\n"
      )
    })

    out.close()
  }

}

