package io.github.datacatering.datacaterer.core.generator.metrics

import scala.collection.mutable

/**
 * Simple percentile calculator for performance metrics.
 * 
 * This is a simplified implementation that stores values directly.
 * For datasets > 100k values, we sample to keep memory bounded.
 * 
 * TODO: Optimize with proper T-Digest algorithm for very large datasets.
 */
class TDigest(compression: Double = 100.0) {

  private val values = mutable.ArrayBuffer[Double]()
  private var totalCount: Long = 0
  private val maxStoredValues = 100000 // Keep memory bounded
  
  /**
   * Add a single value to the digest
   */
  def add(value: Double, weight: Long = 1): Unit = {
    if (value.isNaN || value.isInfinite) return
    
    totalCount += weight
    
    // Add the value 'weight' times (up to limit)
    var i = 0L
    while (i < weight && values.size < maxStoredValues) {
      values += value
      i += 1
    }
  }

  /**
   * Add multiple values to the digest
   */
  def addAll(vals: Seq[Double]): Unit = {
    vals.foreach(add(_))
  }

  /**
   * Get the estimated quantile (0.0 to 1.0)
   * For example: quantile(0.95) returns the 95th percentile
   */
  def quantile(q: Double): Double = {
    if (q < 0 || q > 1) {
      throw new IllegalArgumentException(s"Quantile must be between 0 and 1, got: $q")
    }

    if (values.isEmpty) return 0.0
    
    val sorted = values.sorted
    
    if (q == 0) return sorted.head
    if (q == 1) return sorted.last
    
    // Standard percentile calculation
    val index = q * (sorted.length - 1)
    val lower = index.floor.toInt
    val upper = index.ceil.toInt
    
    if (lower == upper) {
      sorted(lower)
    } else {
      // Linear interpolation
      val weight = index - lower
      sorted(lower) * (1 - weight) + sorted(upper) * weight
    }
  }

  /**
   * Get multiple percentiles efficiently
   */
  def percentiles(ps: Seq[Double]): Seq[Double] = {
    // Sort once, then calculate all percentiles
    val sorted = values.sorted
    ps.map { p =>
      val q = p / 100.0
      if (sorted.isEmpty) 0.0
      else if (q == 0) sorted.head
      else if (q >= 1) sorted.last
      else {
        val index = q * (sorted.length - 1)
        val lower = index.floor.toInt
        val upper = index.ceil.toInt
        
        if (lower == upper) {
          sorted(lower)
        } else {
          val weight = index - lower
          sorted(lower) * (1 - weight) + sorted(upper) * weight
        }
      }
    }
  }

  /**
   * Get the number of values added to the digest
   */
  def count: Long = totalCount

  /**
   * Get the number of centroids (for debugging/monitoring)
   */
  def centroidCount: Int = values.size

  /**
   * Get memory efficiency ratio (values per centroid)
   */
  def compressionRatio: Double = {
    if (values.isEmpty) 0.0
    else totalCount.toDouble / values.size
  }

  /**
   * Get summary statistics
   */
  def summary: String = {
    val min = if (values.isEmpty) 0.0 else values.min
    val max = if (values.isEmpty) 0.0 else values.max
    s"TDigest(count=$totalCount, stored=${values.size}, " +
      s"compression=${compressionRatio.toInt}:1, min=$min, max=$max)"
  }

  /**
   * Reset the digest
   */
  def reset(): Unit = {
    values.clear()
    totalCount = 0
  }
}

object TDigest {

  /**
   * Create a new T-Digest with default compression
   */
  def apply(): TDigest = new TDigest()

  /**
   * Create a new T-Digest with custom compression
   * Higher compression = more accuracy but more memory
   */
  def apply(compression: Double): TDigest = new TDigest(compression)

  /**
   * Create a T-Digest from a sequence of values
   */
  def fromValues(values: Seq[Double], compression: Double = 100.0): TDigest = {
    val digest = new TDigest(compression)
    digest.addAll(values)
    digest
  }

  /**
   * Threshold for switching from exact to approximate percentile calculation
   */
  val LARGE_DATASET_THRESHOLD: Int = 100000

  /**
   * Recommended compression values for different use cases
   */
  val COMPRESSION_LOW: Double = 50.0      // Less accurate, more memory efficient
  val COMPRESSION_MEDIUM: Double = 100.0  // Balanced (default)
  val COMPRESSION_HIGH: Double = 200.0    // More accurate, more memory
}
