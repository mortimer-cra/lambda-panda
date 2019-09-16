package com.ljg.panda.common.math;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.RealVector;

/**
 * Ax=b的求解器
 * 底层使用Commons Math3框架中矩阵分解求解器DecompositionSolver
 */
public final class Solver {

    private final DecompositionSolver solver;

    Solver(DecompositionSolver solver) {
        this.solver = solver;
    }

    /**
     * 求解Ax=b中的x
     * 因为求解器中的b是double型的，所以需要从float在double之间需要转换下
     * @param b
     * @return
     */
    public float[] solveFToF(float[] b) {
        RealVector bVec = new ArrayRealVector(b.length);
        for (int i = 0; i < b.length; i++) {
            bVec.setEntry(i, b[i]);
        }
        RealVector resultVec = solver.solve(bVec);
        float[] result = new float[resultVec.getDimension()];
        for (int i = 0; i < result.length; i++) {
            result[i] = (float)resultVec.getEntry(i);
        }
        return result;
    }

    /**
     *  求解Ax=b中的x
     * @param b
     * @return
     */
    public double[] solveDToD(double[] b) {
        RealVector bVec = new ArrayRealVector(b, false);
        return solver.solve(bVec).toArray();
    }

    @Override
    public String toString() {
        return "Solver[" + solver + "]";
    }

}
