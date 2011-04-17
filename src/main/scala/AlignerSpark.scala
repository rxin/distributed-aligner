/*****************************************************************************
 * A machine translation IBM Model 1 aligner in Scala. The first version that
 * runs on Spark using a naive MapReduce model.
 *
 * Author: Reynold Xin
 * Email: rxin@cs.berkeley.edu
 *****************************************************************************/

package edu.berkeley.cs.amplab.aligner

import scala.collection.JavaConversions._

import java.io.File
import java.lang.{Iterable => JavaIterable}

import edu.berkeley.nlp.mt.{Alignment, SentencePair}
import spark.{Broadcast, RDD, SparkContext}


/**
 * A scala driver for the aligner.
 *
 * @author rxin
 */
object AlignerSpark extends Application {

  override def main(args: Array[String]) {
    // args(0) = spark master
    // args(1) = num of training sentence pairs
    // args(2) = data path
    run(args(0), args(1).toInt, args(2))
  }

  def run(master: String, maxTrain: Int, path:String = "./data/",
    printAlign: Boolean = true) {

    val trainingSentencePairs: JavaIterable[SentencePair] =
      SentencePair.readSentencePairs(
        new File(path, "training").getPath(), maxTrain)

    val testSentencePairs: JavaIterable[SentencePair] =
      SentencePair.readSentencePairs(path + "/test_aligns_big",
                                     Integer.MAX_VALUE)
    val testAlignments = Alignment.readAlignments(
      path + "/test_aligns_big/test.wa")

    val concatSentencePairs: JavaIterable[SentencePair] = trainingSentencePairs

    // Init aligner.
    val sc = new SparkContext(master, "aligner")
    val wordAligner = new Model1AlignerSpark(sc)
    wordAligner.init(trainingSentencePairs)

    // Run the distributed aligner.
    val trainingSentencePairsRdd = sc.parallelize(trainingSentencePairs.toSeq)
    wordAligner.train(trainingSentencePairsRdd)

    // Test alignment.
    var proposedSureCount = 0;
    var proposedPossibleCount = 0;
    var sureCount = 0;
    var proposedCount = 0;

    // Align the sentences.
    testSentencePairs.foreach { sentencePair =>
      val proposedAlignment: Alignment = wordAligner.alignSentencePair(
        sentencePair);
      val referenceAlignment: Alignment = testAlignments.get(
        sentencePair.getSentenceID());
      
      println("Alignment:\n" +
        Alignment.render(referenceAlignment, proposedAlignment, sentencePair))

      sentencePair.getFrenchWords.zipWithIndex.foreach { case(fw, fi) =>
        sentencePair.getEnglishWords.zipWithIndex.foreach { case(ew, ei) =>
          val proposed = proposedAlignment.containsSureAlignment(ei, fi)
          val sure = referenceAlignment.containsSureAlignment(ei, fi)
          val possible = referenceAlignment.containsPossibleAlignment(ei, fi)
          if (proposed && sure) proposedSureCount += 1
          if (proposed && possible) proposedPossibleCount += 1
          if (proposed) proposedCount += 1
          if (sure) sureCount += 1
        }
      }
    }

    // Print precision, recall, and AER.
    println("Precision: " +
      proposedPossibleCount / proposedCount.asInstanceOf[Double])
    println("Recall: " + proposedSureCount / sureCount.asInstanceOf[Double])
    println("AER: " +  (1.0 - (proposedSureCount + proposedPossibleCount)
      / (sureCount + proposedCount).asInstanceOf[Double]))

  }
}


/**
 * IBM Model 1 Aligner using soft EM.
 *
 * @author rxin
 */
@serializable
class Model1AlignerSpark(val sc: SparkContext) {

  val NUM_EM_ITERATIONS = 20

  val NULL_LIKELIHOOD = 0.20

  val NON_NULL_LIKELIHOOD = (1 - NULL_LIKELIHOOD)

  var alignProb = new CounterMap

  /**
   * Generate the initial word pair counts (translation probability). This
   * function sets the initial translation probability to
   * c(e, f) / (c(e)*c(f)).
   * 
   * The reason we don't use a simple uniform distribution for initial
   * probability is because if we do that, most words will be aligned to the
   * first word in the sentence, which happen to be "the", and converge at
   * that local optimum (for non-convex) or make convergence slower (for
   * convex).
   */  
  def init(trainingData: JavaIterable[SentencePair]) {

    trainingData.zipWithIndex.foreach { case(sentencePair, sentenceIndex) => {
      // Run the init alignment.
      // Append 0 for NULL alignment.
      sentencePair.englishWords.append("0")
      sentencePair.englishWords.foreach( e => {
        sentencePair.frenchWords.foreach( f => {
          // TODO toInt is slow.
          alignProb.incrementCount(e.toInt, f.toInt, 1)
        })
      })
    }}

    alignProb.normalize()
  }

  /**
   * Train the aligner. This must be called before using alignSentencePair().
   */
  def train(trainingData: RDD[SentencePair]) {
    // EM iterations.
    for (emIteration <- 1 to NUM_EM_ITERATIONS) {
      println("EM iteration # " + emIteration + " / " + NUM_EM_ITERATIONS)

      val alignProbBroadcast = sc.broadcast[CounterMap](alignProb)

      // E step: align words using alignProb.
      val counterMaps = trainingData.map { sentencePair => {

        val counterMap = new CounterMap

        // Append 0 for NULL alignment.
        sentencePair.englishWords.append("0")
        
        sentencePair.frenchWords.foreach { f => {

          // The likelihood that this French word (f) should be aligned to
          // each of the English words.
          val alignDist: Seq[Double] = sentencePair.englishWords.map { e =>
            if (e.toInt == 0) {
              (alignProbBroadcast.value.getCount(e.toInt, f.toInt)
                * NULL_LIKELIHOOD)
            } else {
              (alignProbBroadcast.value.getCount(e.toInt, f.toInt)
                * NON_NULL_LIKELIHOOD / (sentencePair.englishWords.size + 1))
            }
          }

          val alignDistSum = alignDist.sum

          (sentencePair.englishWords zip alignDist).foreach { case(e, p) =>
            counterMap.incrementCount(e.toInt, f.toInt, p / alignDistSum)
          }
        }}

        counterMap
      }}

      // M step: update alignProb.
      alignProb = counterMaps.reduce(CounterMap.merge)
      alignProb.normalize()
    }
  }

  def alignSentencePair(sentencePair: SentencePair): Alignment = {
    val alignment = new Alignment

    // For each French word, find the most likely alignment.
    sentencePair.frenchWords.zipWithIndex.foreach { case(f, fi) => {
      
      // First align the word to null (0).
      var bestProb = alignProb.getCount(0, f.toInt) * NULL_LIKELIHOOD
      var alignToEi = -1

      // Find the alignment of highest likehihood.
      sentencePair.englishWords.zipWithIndex.foreach { case(e, ei) => {
        val prob = (alignProb.getCount(e.toInt, f.toInt) * NON_NULL_LIKELIHOOD
            / (sentencePair.englishWords.size + 1))

        if (prob > bestProb) {
          bestProb = prob
          alignToEi = ei
        }
      }}

      // Specify the alignment.
      if (alignToEi != -1) {
        alignment.addAlignment(alignToEi, fi, true)
      }
    }}

    return alignment
  }
}

