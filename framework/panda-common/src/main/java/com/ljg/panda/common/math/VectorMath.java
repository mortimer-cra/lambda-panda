package com.ljg.panda.common.math;

import com.github.fommil.netlib.BLAS;
import org.apache.commons.math3.random.RandomGenerator;

import java.util.Collection;

/**
 * 向量的一些数学方法
 */
public final class VectorMath {
    private static final BLAS blas = BLAS.getInstance();

    private VectorMath() {}

    /**
     * 两个向量的点积
     * @return dot product of the two given arrays
     * @param x one array
     * @param y the other array
     */
    public static double dot(float[] x, float[] y) {
        int length = x.length;
        double dot = 0.0;
        for (int i = 0; i < length; i++) {
            dot += (double) x[i] * y[i];
        }
        return dot;
    }

    /**
     * 向量的L2范数
     * @param x vector for whom norm to be calculated
     * @return the L2 norm of vector x
     */
    public static double norm(float[] x) {
        double total = 0.0;
        for (float f : x) {
            total += (double) f * f;
        }
        return Math.sqrt(total);
    }

    /**
     *  向量的L2范数
     * @param x vector for whom norm to be calculated
     * @return the L2 norm of vector x
     */
    public static double norm(double[] x) {
        double total = 0.0;
        for (double d : x) {
            total += d * d;
        }
        return Math.sqrt(total);
    }

    /**
     * 计算两个给定的数组的 cosine 相似度
     *
     * @param x one array
     * @param y the other array
     * @param normY norm of y 向量y的范数
     * @return cosine similarity = dot(x,y) / (norm(x) * norm(y))
     */
    public static double cosineSimilarity(float[] x, float[] y, double normY) {
        int length = x.length;
        double dot = 0.0;
        double totalXSq = 0.0;
        for (int i = 0; i < length; i++) {
            double xi = x[i];
            totalXSq += xi * xi;
            dot += xi * y[i];
        }
        return dot / (Math.sqrt(totalXSq) * normY);
    }

    /**
     * 将又高又瘦的矩阵M转换成MT * M的packed row-major行式
     * @param M tall, skinny matrix
     * @return MT * M as a dense lower-triangular matrix, represented in packed row-major form.
     */
    public static double[] transposeTimesSelf(Collection<float[]> M) {
        if (M == null || M.isEmpty()) {
            return null;
        }
        int features = M.iterator().next().length;
        double[] result = new double[features * (features + 1) / 2];
        double[] vectorD = new double[features];
        for (float[] vector : M) {
            // Unfortunately need to copy into a double[]
            for (int i = 0; i < vector.length; i++) {
                vectorD[i] = vector[i];
            }
            blas.dspr("L", features, 1.0, vectorD, 1, result);
        }
        return result;
    }

    /**
     * 将String类型的向量转成double类型的向量
     * @param values numeric values as {@link String}s
     * @return values parsed as {@code double[]}
     */
    public static double[] parseVector(String[] values) {
        double[] doubles = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            doubles[i] = Double.parseDouble(values[i]);
        }
        return doubles;
    }

    /**
     * 随机生成指定维度的向量
     * @param features dimension of vector
     * @param random random number generator
     * @return vector whose direction from the origin is chosen uniformly at random, but which is not normalized
     */
    public static float[] randomVectorF(int features, RandomGenerator random) {
        float[] vector = new float[features];
        for (int i = 0; i < features; i++) {
            vector[i] = (float) random.nextGaussian();
        }
        return vector;
    }


}
