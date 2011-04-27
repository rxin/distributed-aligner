package edu.berkeley.cs.amplab.aligner

import scala.collection.mutable.Buffer

import edu.berkeley.nlp.mt.SentencePair


class SimpleSentencePair (
  val englishWords: Buffer[Int],
  val frenchWords: Buffer[Int]) {
}


object SimpleSentencePair {

  def lineToSimpleSentencePair(line: String): SimpleSentencePair = {
    val pair = line.trim.split('|')
    val englishWords: Array[Int] = pair(0).trim.split("\\s").map{ _.toInt }
    val frenchWords: Array[Int] = pair(1).trim.split("\\s").map{ _.toInt }
    return new SimpleSentencePair(englishWords.toBuffer, frenchWords.toBuffer)
  }
}


