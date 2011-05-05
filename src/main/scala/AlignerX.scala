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
import spark._
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import mesos._
//import spark.{RDD, SparkContext}

/*
 * Some custom RDDs for this application
 */

class IndexedRDD[T: ClassManifest](prev: RDD[T])
extends RDD[(Int, T)](prev.sparkContext) {

  @transient val splits_ = prev.splits.zip(1 to prev.splits.size).map(p => new SeededSplit(p._1, p._2))

  override def splits = splits_.asInstanceOf[Array[Split]]

  override def preferredLocations(split: Split) = prev.preferredLocations(split.asInstanceOf[SeededSplit].prev)

  override def iterator(splitIn: Split) = {
    val split = splitIn.asInstanceOf[SeededSplit]
    // add the seed number in front
    prev.iterator(split.prev).map(x => (split.seed, x))
  }

  override def taskStarted(split: Split, slot: SlaveOffer) = prev.taskStarted(split.asInstanceOf[SeededSplit].prev, slot)
}

@serializable class ZipSplit(val left: Split, val right: Split) extends Split {
  override def getId () =
    "ZipSplit(" + left.getId() + ", " + right.getId() + ")"
}

class ZippedRDD[L: ClassManifest, R: ClassManifest](
  left: RDD[(Int, L)], right: RDD[(Int, R)]
)
extends RDD[(L, R)](left.sparkContext) {
  @transient val splits_ = { 
    // create tmp hashmaps
    val lMap = new HashMap[Int,Split]()
    left.splits.foreach(s => {
      val seed = left.iterator(s).toSeq.take(1)(0)._1 
      lMap(seed) = s
    })
    val rMap = new HashMap[Int,Split]()
    right.splits.foreach(s => {
      val seed = right.iterator(s).toSeq.take(1)(0)._1 
      rMap(seed) = s
    })
    left.splits.map(p => {
      val seed = left.iterator(p).toSeq.take(1)(0)._1 
      new ZipSplit(lMap(seed), rMap(seed))
    })
  }
  override def splits = splits_.asInstanceOf[Array[Split]]
  override def preferredLocations(split: Split) = left.preferredLocations(split.asInstanceOf[ZipSplit].left)
  override def iterator(splitIn: Split) = {
    val split = splitIn.asInstanceOf[ZipSplit]
    left.iterator(split.left).map(_._2).zip(right.iterator(split.right).map(_._2))
  }
  override def taskStarted(split: Split, slot: SlaveOffer) = left.taskStarted(split.asInstanceOf[ZipSplit].left, slot)
}

/**
 * A scala driver for the aligner.
 *
 * @author rxin
 */
object AlignerX extends Application {

  override def main(args: Array[String]) {
    // args(0) = spark master
    // args(1) = num of training sentence pairs per node
    // args(2) = test data path
    // args(3) = training data path (HDFS)
    run(args(0), args(1).toInt, args(2), args(3))
  }

  def run(master: String, maxTrain: Int, testDataPath:String,
    trainingDataPath:String, printAlign: Boolean = false) {

    val testSentencePairs: JavaIterable[SentencePair] =
      SentencePair.readSentencePairs(testDataPath + "/test_aligns_big",
                                     Integer.MAX_VALUE)
    val testAlignments = Alignment.readAlignments(
      testDataPath+ "/test_aligns_big/test.wa")

    // Init aligner and load training data from HDFS.
    val sc = new SparkContext(master, "aligner")
    val trainingLines = sc.textFile(trainingDataPath).splitRdd.flatMap {
      _.take(maxTrain)
    }
    val trainingSentencePairsRdd = trainingLines.map {
      SimpleSentencePair.lineToSimpleSentencePair(_)
    }.cache()
    val wordAligner = new Model1AlignerX(sc)

    // Run the distributed aligner.
    var model = wordAligner.init(trainingSentencePairsRdd)
    model = wordAligner.train(trainingSentencePairsRdd, model)
    var finalModel:CounterMap = model.map(_._1).reduce(CounterMap.merge)

    // Test alignment.
    var proposedSureCount = 0;
    var proposedPossibleCount = 0;
    var sureCount = 0;
    var proposedCount = 0;

    // Align the sentences.
    testSentencePairs.foreach { sentencePair =>
      val proposedAlignment: Alignment = wordAligner.alignSentencePair(
        sentencePair, finalModel);
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
class Model1AlignerX(val sc: SparkContext) {

  val NUM_EM_ITERATIONS = 20

  val NULL_LIKELIHOOD = 0.20

  val NON_NULL_LIKELIHOOD = (1 - NULL_LIKELIHOOD)

  //var alignProb:RDD[CounterMap] = new CounterMap

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
  def init(trainingData: RDD[SimpleSentencePair]): RDD[(CounterMap, HashSet[Int])] = {

    val counterMaps = trainingData.map { sentencePair => {
      val counterMap = new CounterMap
      sentencePair.englishWords += 0
      sentencePair.englishWords.foreach { e => {
        sentencePair.frenchWords.foreach { f => {
          counterMap.incrementCount(e, f, 1)
        }}
      }}

      counterMap
    }}

    val flattenedCounterMaps: RDD[(Int, CounterMap)] = counterMaps.flatMap((m: CounterMap) => m.map((p: (Int,Counter)) => { val eWord = p._1 ; (eWord, CounterMap(p)) }))
    val unnormAlignProb0 = new IndexedRDD(flattenedCounterMaps)
    val unnormAlignProb1: RDD[(Int, (CounterMap, HashSet[Int]))] = unnormAlignProb0.map(p =>
      (p._2._1, // eWord (english word)
       (p._2._2, HashSet(p._1)) // (CounterMap, seed/partition number)
      )
    )
    val unnormAlignProb = new PairRDDExtras(unnormAlignProb1).reduceByKey((x, y) =>
      (CounterMap.merge(x._1, y._1), (x._2 ++ y._2)) // (CounterMap, seed/partition number)
    )
    val alignProb = unnormAlignProb.map(p => {
      val cm = p._2._1
      val seeds = p._2._2
      cm.normalize
      (cm, seeds)
    })

    alignProb
  }

  /**
   * Train the aligner. This must be called before using alignSentencePair().
   */
  def train(trainingData: RDD[SimpleSentencePair], alignProbIn: RDD[(CounterMap, HashSet[Int])]): RDD[(CounterMap, HashSet[Int])] = {
    // EM iterations.
    var alignProb = alignProbIn

    for (emIteration <- 1 to NUM_EM_ITERATIONS) {
      println("EM iteration # " + emIteration + " / " + NUM_EM_ITERATIONS)

      //val alignProbBroadcast = sc.broadcast[CounterMap](alignProb)

      // E step: align words using alignProb.
      val distParam: RDD[(Int, CounterMap)] = new PairRDDExtras(alignProb.flatMap(p => { val cm = p._1 ; val seeds = p._2 ; seeds.map((_, cm)) })).reduceByKey(CounterMap.merge)
      val dataPlusParameters: RDD[(Array[SimpleSentencePair],CounterMap)] = new ZippedRDD(new IndexedRDD(trainingData.splitRdd), distParam)
      val counterMaps = dataPlusParameters.flatMap { p => {
        val data: Array[SimpleSentencePair] = p._1 // data
        val prob: CounterMap = p._2 // parameters

        data.map { sentencePair => 

          val counterMap = new CounterMap

          // Append 0 for NULL alignment.
          sentencePair.englishWords += 0
          
          sentencePair.frenchWords.foreach { f => {

            // The likelihood that this French word (f) should be aligned to
            // each of the English words.
            val alignDist: Seq[Double] = sentencePair.englishWords.map { e =>
              if (e == 0) {
                (prob.getCount(e, f)
                  * NULL_LIKELIHOOD)
              } else {
                (prob.getCount(e, f)
                  * NON_NULL_LIKELIHOOD / (sentencePair.englishWords.size + 1))
              }
            }

            val alignDistSum = alignDist.sum

            (sentencePair.englishWords zip alignDist).foreach { case(e, p) =>
              counterMap.incrementCount(e, f, p / alignDistSum)
            }
          }}

          counterMap
        }

      }}

      // M step: update alignProb.
      val flattenedCounterMaps: RDD[(Int, CounterMap)] = counterMaps.flatMap((m: CounterMap) => m.map((p: (Int,Counter)) => { val eWord = p._1 ; (eWord, CounterMap(p)) }))
      val unnormAlignProb0 = new IndexedRDD(flattenedCounterMaps)
      val unnormAlignProb1: RDD[(Int, (CounterMap, HashSet[Int]))] = unnormAlignProb0.map(p =>
        (p._2._1, // eWord (english word)
         (p._2._2, HashSet(p._1)) // (CounterMap, seed/partition number)
        )
      )
      val unnormAlignProb = new PairRDDExtras(unnormAlignProb1).reduceByKey((x, y) =>
        (CounterMap.merge(x._1, y._1), (x._2 ++ y._2)) // (CounterMap, seed/partition number)
      )
      alignProb = unnormAlignProb.map(p => {
        val cm = p._2._1
        val seeds = p._2._2
        cm.normalize
        (cm, seeds)
      })
    }

    alignProb
  }

  def alignSentencePair(
    sentencePair: SentencePair,
    alignProb: CounterMap
  ): Alignment = {
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

  //def alignSentencePair(
  //  sentencePair: SentencePair,
  //  model: RDD[(CounterMap, HashSet[Int])]
  //): Alignment = {
  //  val alignment = new Alignment

  //  // For each French word, find the most likely alignment.
  //  sentencePair.frenchWords.zipWithIndex.foreach { case(f, fi) => {
  //    
  //    // First align the word to null (0).
  //    var bestProb = alignProb.getCount(0, f.toInt) * NULL_LIKELIHOOD
  //    var alignToEi = -1

  //    // Find the alignment of highest likehihood.
  //    sentencePair.englishWords.zipWithIndex.foreach { case(e, ei) => {
  //      val prob = (alignProb.getCount(e.toInt, f.toInt) * NON_NULL_LIKELIHOOD
  //          / (sentencePair.englishWords.size + 1))

  //      if (prob > bestProb) {
  //        bestProb = prob
  //        alignToEi = ei
  //      }
  //    }}

  //    // Specify the alignment.
  //    if (alignToEi != -1) {
  //      alignment.addAlignment(alignToEi, fi, true)
  //    }
  //  }}

  //  return alignment
  //}
}

