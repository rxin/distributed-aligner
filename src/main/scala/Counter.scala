package edu.berkeley.cs.amplab.aligner

import scala.collection.mutable.HashMap


class CounterMap extends HashMap[Int, Counter] {
  
  /**
   * Gets the count of the given (key, value) entry, or zero if that entry is
	 * not present. Does not create any objects.
   */
  def getCount(key: Int, value: Int): Double = {
    get(key) match {
      case Some(counter) => counter.getCount(value)
      case None => 0
    }
  }

  /**
   * Increments the count for a particular (key, value) pair.
   */
  def incrementCount(key: Int, value:Int, amount: Double) {
    ensureCounter(key).incrementCount(value, amount)
  }

  /**
   * Sets the count for a particular (key, value) pair.
   */
  def setCount(key: Int, value: Int, count: Double) {
    ensureCounter(key).setCount(value, count)
  }

  /**
   * Normalizes the maps inside this CounterMap -- not the CounterMap itself.
   */
  def normalize() {
    foreach { case(k, v) => v.normalize() }
  }

  /**
   * Merge the current CounterMap with another CounterMap, and return the
   * result. The merge is in place, i.e. affects the current CounterMap.
   */
  def mergeWith(anotherCounterMap: CounterMap): CounterMap = {
    anotherCounterMap.foreach { case(k, anotherCounter) => {
      ensureCounter(k).mergeWith(anotherCounter)
    }}
    this
  }

  def ensureCounter(key: Int): Counter = {
    get(key) match {
      case Some(counter) => counter
      case None => {
        val counter = new Counter
        put(key, counter)
        counter
      }
    }
  }

}


object CounterMap {
  def apply(p: (Int, Counter)) = { val cm = new CounterMap ; cm.put(p._1, p._2) ; cm }
  def merge(first: CounterMap, second: CounterMap): CounterMap = {
    first.mergeWith(second)
  }
}


class Counter extends HashMap[Int, Double] {

  /**
   * Get the count of the element, or zero if the element is not in the counter.
   */
  def getCount(key: Int): Double = {
    getOrElse(key, 0)
  }

  /**
   * Increment a key's count by the given amount.
   */
  def incrementCount(key: Int, amount: Double) {
    setCount(key, getCount(key) + amount)
  }

  /**
   * Set the count for the given key, clobbering any previous count.
   */
  def setCount(key: Int, count: Double) {
    put(key, count)
  }

  /**
   * Destructively normalize this Counter in place.
   */
  def normalize() {
    val normSum = values.sum
    foreach { case(k, v) =>
      setCount(k, v / normSum)
    }
  }

  /**
   * Merge the current Counter with another Counter, and return the result.
   * The merge is in place, i.e. affects the current Counter.
   */
  def mergeWith(anotherCounter: Counter): Counter = {
    anotherCounter.foreach { case(k, v) => {
      incrementCount(k, v)
    }}
    this
  }
}

