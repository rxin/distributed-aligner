/*****************************************************************************
 * A machine translation IBM Model 1 aligner in Scala. This one runs on single
 * node, single threaded.
 *
 * Author: Reynold Xin
 * Email: rxin@cs.berkeley.edu
 *****************************************************************************/

package edu.berkeley.cs.amplab.aligner

import scala.collection.JavaConversions._

import java.io.File
import java.lang.{Iterable => JavaIterable}

import edu.berkeley.nlp.assignments.assign3.AlignmentTester
import edu.berkeley.nlp.mt.{Alignment, SentencePair, WordAligner, WordAlignerFactory}
import edu.berkeley.nlp.util.CollectionUtils
import edu.berkeley.nlp.util.CounterMap


/**
 * A scala driver for the aligner.
 *
 * @author rxin
 */
object AlignerSingleThread extends Application {

  override def main(args: Array[String]) {
    //AlignmentTester.main(args)
    run(args(0).toInt)
  }

  def run(maxTrain: Int, printAlign: Boolean = true, path:String = "./data/") {
    val trainingSentencePairs: JavaIterable[SentencePair] =
      SentencePair.readSentencePairs(
        new File(path, "training").getPath(), maxTrain)

    val testSentencePairs: JavaIterable[SentencePair] =
      SentencePair.readSentencePairs(path + "/test_aligns_big",
                                     Integer.MAX_VALUE)
    val testAlignments = Alignment.readAlignments(
      path + "/test_aligns_big/test.wa")

    val concatSentencePairs: JavaIterable[SentencePair] = trainingSentencePairs
    //  CollectionUtils.concat(trainingSentencePairs, testSentencePairs)

    // Init aligner.
    val wordAligner = (new Model1AlignerFactory).newAligner(concatSentencePairs)

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
 * Aligner factory.
 *
 * @author rxin
 */
class Model1AlignerFactory extends WordAlignerFactory {

  def newAligner(trainingData: JavaIterable[SentencePair]) : WordAligner = {
    val aligner = new Model1SoftEmWeirdAligner1()
    aligner.train(trainingData)
    return aligner
  }

}


/**
 * IBM Model 1 Aligner using soft EM.
 * The caller must call train() to train the model before using it.
 *
 * @author rxin
 */
class Model1SoftEmWeirdAligner1 extends WordAligner {

  val NUM_EM_ITERATIONS = 20

  val NULL_LIKELIHOOD = 0.20

  val NON_NULL_LIKELIHOOD = (1 - NULL_LIKELIHOOD)

  var alignProb = new CounterMap[Int, Int]

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
  def train(trainingData: JavaIterable[SentencePair]) {
    init(trainingData)

    // EM iterations.
    for (emIteration <- 1 to NUM_EM_ITERATIONS) {
      println("EM iteration # " + emIteration + " / " + NUM_EM_ITERATIONS)

      val newAlignProb = new CounterMap[Int, Int]

      // E step: align words using alignProb.
      trainingData.foreach { sentencePair => {

        // Append 0 for NULL alignment.
        sentencePair.englishWords.append("0")
        
        sentencePair.frenchWords.foreach { f => {

          // The likelihood that this French word (f) should be aligned to
          // each of the English words.
          // TODO this can be optimized to avoid constant allocation of
          // the alignDist array.
          val alignDist = sentencePair.englishWords.map { e =>
            if (e.toInt == 0) {
              (alignProb.getCount(e.toInt, f.toInt) * NULL_LIKELIHOOD)
            } else {
              (alignProb.getCount(e.toInt, f.toInt) * NON_NULL_LIKELIHOOD
                  / (sentencePair.englishWords.size + 1))
            }
          }

          val alignDistSum = alignDist.sum

          // Increment the normalized alignment count.
          (sentencePair.englishWords zip alignDist).foreach { case(e, p) =>
            newAlignProb.incrementCount(e.toInt, f.toInt, p / alignDistSum)
          }
        }}
      }}

      // M step: update alignProb based on the alignment.
      newAlignProb.normalize()
      alignProb = newAlignProb
    }
  }

  /* (non-Javadoc)
   * @see edu.berkeley.nlp.mt.WordAligner#alignSentencePair(edu.berkeley.nlp.mt.SentencePair)
   */
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

