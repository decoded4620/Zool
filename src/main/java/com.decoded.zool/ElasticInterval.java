package com.decoded.zool;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


/**
 * This can be used to start an interval aggressive and expand it until its max value
 */
public class ElasticInterval extends AtomicLong {
  private final Function<Long, Long> updateAlgorithm;
  private final long minValue;
  private final long maxValue;
  private final long initialValue;

  /**
   * Elastic ramp from min to max by growing at a rate equal to growth rate
   * @param growthRate the rate of growth for the ramp
   * @param min the min value
   * @param max the max value
   * @return the interval
   */
  public static ElasticInterval elasticRamp(double growthRate, long min, long max) {
    return new ElasticInterval(min, min, max,
        input -> input = Math.round(input * (1 + Math.max(0, Math.min(1, growthRate)))));
  }

  /**
   * Elastic sunset from max to min by shrinking at a rate equal to the sunset rate
   * @param sunsetRate the shrink rate in percentage terms (e.g. 0.0 -&gt; 1.0)
   * @param min the minimum value for the ramp e.g. 0
   * @param max the maximum value e.g. 1000
   * @return the interval
   */
  public static ElasticInterval elasticSunset(double sunsetRate, long min, long max) {
    return new ElasticInterval(max, min, max,
        input -> input = Math.round(input * (1 - Math.max(0, Math.min(1, sunsetRate)))));
  }
  /**
   * Constructor
   *
   * @param initialValue the starting value
   * @param minValue the minimum value
   * @param maxValue the maximum value
   * @param updateAlgorithm the function to apply to the current value to get the next value.
   */
  public ElasticInterval(long initialValue, long minValue, long maxValue, Function<Long, Long> updateAlgorithm) {
    super(initialValue);
    this.initialValue = initialValue;
    this.updateAlgorithm = updateAlgorithm;
    this.minValue = minValue;
    this.maxValue = maxValue;
  }

  /**
   * Reset the interval to the initial value.
   * @return this interval object
   */
  public ElasticInterval reset() {
    set(initialValue);
    return this;
  }

  /**
   * Returns the next elastic value after applying the update algorithm
   *
   * @return the updated value.
   */
  public synchronized Long getAndUpdate() {
    // keep the new value within the range of min and max
    long newValue = Math.min(this.maxValue, Math.max(this.minValue, this.updateAlgorithm.apply(get())));
    set(newValue);
    return newValue;
  }
}
