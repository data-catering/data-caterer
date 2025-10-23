package io.github.datacatering.datacaterer.javaapi.api;

/**
 * A simple container for value-weight pairs used in Java API for oneOfWeighted field method.
 */
public class WeightedValue {
    private final Object value;
    private final double weight;

    /**
     * Constructs a new WeightedValue.
     * 
     * @param value  The value
     * @param weight The weight associated with the value
     */
    public WeightedValue(Object value, double weight) {
        this.value = value;
        this.weight = weight;
    }

    /**
     * Gets the value.
     * 
     * @return The value
     */
    public Object value() {
        return value;
    }

    /**
     * Gets the weight.
     * 
     * @return The weight
     */
    public double weight() {
        return weight;
    }

    /**
     * Static factory method for easier creation.
     * 
     * @param value  The value
     * @param weight The weight associated with the value
     * @return A new WeightedValue instance
     */
    public static WeightedValue of(Object value, double weight) {
        return new WeightedValue(value, weight);
    }
}