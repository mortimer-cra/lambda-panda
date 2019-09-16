package com.ljg.panda.common.math;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.stat.descriptive.AbstractStorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;

import java.io.Serializable;

public final class DoubleWeightedMean extends AbstractStorelessUnivariateStatistic implements Serializable{
    private long count;
    private double totalWeight;
    private double mean;

    public DoubleWeightedMean() {
        this(0,0,Double.NaN);
    }

    public DoubleWeightedMean(long count, double totalWeight, double mean) {
        this.count = count;
        this.totalWeight = totalWeight;
        this.mean = mean;
    }

    @Override
    public StorelessUnivariateStatistic copy() {
        return new DoubleWeightedMean(count, totalWeight, mean);
    }

    @Override
    public void clear() {
        count = 0;
        totalWeight = 0.0;
        mean = Double.NaN;
    }

    @Override
    public double getResult() {
        return mean;
    }

    @Override
    public long getN() {
        return count;
    }

    @Override
    public void increment(double datum) {
        increment(datum, 1.0);
    }

    /**
     *  将新来的数据按照一定的权重累加到平均值上
     * @param datum 新来的数据
     * @param weight    权重
     */
    public void increment(double datum, double weight) {
        Preconditions.checkArgument(weight >= 0.0);
        if (count == 0) {
            count = 1;
            mean = datum;
            totalWeight = weight;
        } else {
            count++;
            totalWeight += weight;
            mean += (weight - totalWeight) * (datum - mean); // 加权平均数
        }
    }

    @Override
    public String toString() {
        return Double.toString(mean);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(count) ^ Double.hashCode(totalWeight) ^ Double.hashCode(mean);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DoubleWeightedMean)) {
            return false;
        }
        DoubleWeightedMean other = (DoubleWeightedMean) o;
        return count == other.count &&
                Double.compare(totalWeight, other.totalWeight) == 0 &&
                Double.compare(mean, other.mean) == 0;
    }
}
