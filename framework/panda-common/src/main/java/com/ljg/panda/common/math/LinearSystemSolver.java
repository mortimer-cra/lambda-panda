package com.ljg.panda.common.math;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.RRQRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 线性方程组求解器(利用RRQR 可以求出矩阵的秩)
 * 求解Ax=b
 */
public final class LinearSystemSolver {
    private static final Logger log = LoggerFactory.getLogger(LinearSystemSolver.class);
    private static final double SINGULARITY_THRESHOLD_RATIO = 1.0e-5;

    private LinearSystemSolver() {}

    /**
     * 先将packed的矩阵转换成unpacked的矩阵(这个矩阵是对称的矩阵)
     * 然后根据unpacked的对阵矩阵拿到对应的RRQRDecomposition的求解器
     * @param packed packed后的对称的矩阵
     * @return
     */
    public static Solver getSolver(double[] packed) {
        if (packed == null) {
            return null;
        }
        int dim = (int) Math.round((Math.sqrt(8.0 * packed.length + 1.0) - 1.0) / 2.0);
        double[][] unpacked = new double[dim][dim];
        int offset = 0;
        for (int col = 0; col < dim; col++) {
            double[] unpackedCol = unpacked[col];
            for (int row = col; row < dim; row++) {
                unpacked[row][col] = unpackedCol[row] = packed[offset++];
            }
        }
        return getSolver(unpacked);
    }

    /**
     * 返回求解Ax=b的求解器
     * 其中A就是data
     * @param data dense matrix represented in row-major form
     * @return solver for the system Ax = b
     */
    static Solver getSolver(double[][] data) {
        if (data == null) {
            return null;
        }
        RealMatrix M = new Array2DRowRealMatrix(data, false);
        double infNorm = M.getNorm();
        double singularityThreshold = infNorm * SINGULARITY_THRESHOLD_RATIO;
        RRQRDecomposition decomposition = new RRQRDecomposition(M, singularityThreshold);
        DecompositionSolver solver = decomposition.getSolver();
        if (solver.isNonSingular()) {
            return new Solver(solver);
        }
        // Otherwise try to report apparent rank
        int apparentRank = decomposition.getRank(0.01); // Better value?
        log.warn("{} x {} matrix is near-singular (threshold {}). Add more data or decrease the " +
                        "number of features, to <= about {}",
                M.getRowDimension(),
                M.getColumnDimension(),
                singularityThreshold,
                apparentRank);
        throw new SingularMatrixSolverException(apparentRank, "Apparent rank: " + apparentRank);
    }
}
