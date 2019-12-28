/*
 * Copyright (C) 2016 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.da.util;

import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.IntPair;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Throwables;
import com.landawn.abacus.util.function.IntFunction;
import com.landawn.abacus.util.stream.IntStream;
import com.landawn.abacus.util.stream.Stream;

// TODO: Auto-generated Javadoc
/**
 * <li>
 * {@code R} = Row, {@code C} = Column, {@code H} = Horizontal, {@code V} = Vertical.
 * </li>
 *
 * @author Haiyang Li
 * @param <A>
 * @param <PL>
 * @param <ES> element stream
 * @param <RS> row/column stream.
 * @param <X>
 * @since 0.8
 */
public abstract class AbstractMatrix<A, PL, ES, RS, X extends AbstractMatrix<A, PL, ES, RS, X>> {

    /** The Constant CHAR_0. */
    static final char CHAR_0 = (char) 0;

    /** The Constant BYTE_0. */
    static final byte BYTE_0 = (byte) 0;

    /** The Constant BYTE_1. */
    static final byte BYTE_1 = (byte) 1;

    /** The Constant SHORT_0. */
    static final short SHORT_0 = (short) 0;

    /** The Constant isParallelStreamSupported. */
    static final boolean isParallelStreamSupported;
    static {
        boolean tmp = false;

        try {
            if (ClassUtil.forClass("com.landawn.abacus.util.stream.ParallelArrayIntStream") != null
                    && ClassUtil.forClass("com.landawn.abacus.util.stream.ParallelIteratorIntStream") != null) {
                tmp = true;
            }
        } catch (Exception e) {
            // ignore.
        }

        isParallelStreamSupported = tmp;
    }

    /**
     * Row length.
     */
    public final int rows;

    /**
     * Column length.
     */
    public final int cols;

    /** The count. */
    public final long count;

    /** The a. */
    final A[] a;

    /**
     * Instantiates a new abstract matrix.
     *
     * @param a
     */
    protected AbstractMatrix(A[] a) {
        this.a = a;
        this.rows = a.length;
        this.cols = a.length == 0 ? 0 : length(a[0]);

        if (a.length > 1) {
            for (int i = 1, len = a.length; i < len; i++) {
                if (length(a[i]) != this.cols) {
                    throw new IllegalArgumentException("The length of sub arrays must be same");
                }
            }
        }

        this.count = this.cols * this.rows * 1L;
    }

    /**
     *
     * @return
     */
    public A[] array() {
        return a;
    }

    /**
     * Println.
     */
    public abstract void println();

    /**
     * Checks if is empty.
     *
     * @return true, if is empty
     */
    public boolean isEmpty() {
        return count == 0;
    }

    // Replaced by stream and stream2.
    //    public abstract PL row(int i);
    //
    //    public abstract PL column(int j);

    /**
     *
     * @return a new Matrix
     */
    public abstract X copy();

    /**
     *
     * @param fromRowIndex
     * @param toRowIndex
     * @return a new Matrix
     */
    public abstract X copy(int fromRowIndex, int toRowIndex);

    /**
     *
     * @param fromRowIndex
     * @param toRowIndex
     * @param fromColumnIndex
     * @param toColumnIndex
     * @return a new Matrix
     */
    public abstract X copy(int fromRowIndex, int toRowIndex, int fromColumnIndex, int toColumnIndex);

    /**
     * Rotate this matrix clockwise 90.
     *
     * @return a new Matrix
     */
    public abstract X rotate90();

    /**
     * Rotate this matrix clockwise 180.
     *
     * @return a new Matrix
     */
    public abstract X rotate180();

    /**
     * Rotate this matrix clockwise 270.
     *
     * @return a new Matrix
     */
    public abstract X rotate270();

    /**
     *
     * @return
     */
    public abstract X transpose();

    /**
     *
     * @param newCols
     * @return
     */
    public X reshape(int newCols) {
        return reshape((int) (count % newCols == 0 ? count / newCols : count / newCols + 1), newCols);
    }

    /**
     *
     * @param newRows
     * @param newCols
     * @return
     */
    public abstract X reshape(int newRows, int newCols);

    /**
     * Checks if is same shape.
     *
     * @param x
     * @return true, if is same shape
     */
    public boolean isSameShape(X x) {
        return this.rows == x.rows && this.cols == x.cols;
    }

    /**
     * Repeat elements <code>rowRepeats</code> times in row direction and <code>colRepeats</code> times in column direction.
     *
     * @param rowRepeats
     * @param colRepeats
     * @return a new matrix
     * @see <a href="https://www.mathworks.com/help/matlab/ref/repelem.html">https://www.mathworks.com/help/matlab/ref/repelem.html</a>
     */
    public abstract X repelem(int rowRepeats, int colRepeats);

    /**
     * Repeat this matrix <code>rowRepeats</code> times in row direction and <code>colRepeats</code> times in column direction.
     *
     * @param rowRepeats
     * @param colRepeats
     * @return a new matrix
     * @see <a href="https://www.mathworks.com/help/matlab/ref/repmat.html">https://www.mathworks.com/help/matlab/ref/repmat.html</a>
     */
    public abstract X repmat(int rowRepeats, int colRepeats);

    /**
     *
     * @return
     */
    public abstract PL flatten();

    /**
     * flatten -> execute {@code op} -> set values back.
     * <pre>
     * <code>
     * matrix.flatOp(a -> N.sort(a));
     * </code>
     * </pre>
     *
     * @param <E>
     * @param op
     * @throws E the e
     */
    public abstract <E extends Exception> void flatOp(Throwables.Consumer<A, E> op) throws E;

    /**
     * <pre>
     * <code>
     * for (int i = 0; i < rows; i++) {
     *      for (int j = 0; j < cols; j++) {
     *          action.accept(i, j);
     *      }
     *  }
     * </code>
     * </pre>
     *
     * @param <E>
     * @param action
     * @throws E the e
     */
    public <E extends Exception> void forEach(Throwables.IntBiConsumer<E> action) throws E {
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                action.accept(i, j);
            }
        }
    }

    /**
     * Points LU 2 RD.
     *
     * @return
     */
    public Stream<IntPair> pointsLU2RD() {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        return IntStream.range(0, rows).mapToObj(new IntFunction<IntPair>() {
            @Override
            public IntPair apply(int i) {
                return IntPair.of(i, i);
            }
        });
    }

    /**
     * Points RU 2 LD.
     *
     * @return
     */
    public Stream<IntPair> pointsRU2LD() {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        return IntStream.range(0, rows).mapToObj(new IntFunction<IntPair>() {
            @Override
            public IntPair apply(int i) {
                return IntPair.of(i, cols - i - 1);
            }
        });
    }

    /**
     *
     * @return
     */
    public Stream<IntPair> pointsH() {
        return pointsH(0, rows);
    }

    /**
     *
     * @param rowIndex
     * @return
     */
    public Stream<IntPair> pointsH(int rowIndex) {
        return pointsH(rowIndex, rowIndex + 1);
    }

    /**
     *
     * @param fromRowIndex
     * @param toRowIndex
     * @return
     */
    public Stream<IntPair> pointsH(int fromRowIndex, int toRowIndex) {
        N.checkFromToIndex(fromRowIndex, toRowIndex, rows);

        return IntStream.range(fromRowIndex, toRowIndex).flatMapToObj(new IntFunction<Stream<IntPair>>() {
            @Override
            public Stream<IntPair> apply(final int rowIndex) {
                return IntStream.range(0, cols).mapToObj(new IntFunction<IntPair>() {
                    @Override
                    public IntPair apply(final int columnIndex) {
                        return IntPair.of(rowIndex, columnIndex);
                    }
                });
            }
        });
    }

    /**
     *
     * @return
     */
    public Stream<IntPair> pointsV() {
        return pointsV(0, cols);
    }

    /**
     *
     * @param columnIndex
     * @return
     */
    public Stream<IntPair> pointsV(int columnIndex) {
        return pointsV(columnIndex, columnIndex + 1);
    }

    /**
     *
     * @param fromColumnIndex
     * @param toColumnIndex
     * @return
     */
    public Stream<IntPair> pointsV(int fromColumnIndex, int toColumnIndex) {
        N.checkFromToIndex(fromColumnIndex, toColumnIndex, cols);

        return IntStream.range(fromColumnIndex, toColumnIndex).flatMapToObj(new IntFunction<Stream<IntPair>>() {
            @Override
            public Stream<IntPair> apply(final int columnIndex) {
                return IntStream.range(0, rows).mapToObj(new IntFunction<IntPair>() {
                    @Override
                    public IntPair apply(final int rowIndex) {
                        return IntPair.of(rowIndex, columnIndex);
                    }
                });
            }
        });
    }

    /**
     *
     * @return
     */
    public Stream<Stream<IntPair>> pointsR() {
        return pointsR(0, rows);
    }

    /**
     *
     * @param fromRowIndex
     * @param toRowIndex
     * @return
     */
    public Stream<Stream<IntPair>> pointsR(int fromRowIndex, int toRowIndex) {
        N.checkFromToIndex(fromRowIndex, toRowIndex, rows);

        return IntStream.range(fromRowIndex, toRowIndex).mapToObj(new IntFunction<Stream<IntPair>>() {
            @Override
            public Stream<IntPair> apply(final int rowIndex) {
                return IntStream.range(0, cols).mapToObj(new IntFunction<IntPair>() {
                    @Override
                    public IntPair apply(final int columnIndex) {
                        return IntPair.of(rowIndex, columnIndex);
                    }
                });
            }
        });
    }

    /**
     *
     * @return
     */
    public Stream<Stream<IntPair>> pointsC() {
        return pointsR(0, cols);
    }

    /**
     *
     * @param fromColumnIndex
     * @param toColumnIndex
     * @return
     */
    public Stream<Stream<IntPair>> pointsC(int fromColumnIndex, int toColumnIndex) {
        N.checkFromToIndex(fromColumnIndex, toColumnIndex, cols);

        return IntStream.range(fromColumnIndex, toColumnIndex).mapToObj(new IntFunction<Stream<IntPair>>() {
            @Override
            public Stream<IntPair> apply(final int columnIndex) {
                return IntStream.range(0, rows).mapToObj(new IntFunction<IntPair>() {
                    @Override
                    public IntPair apply(final int rowIndex) {
                        return IntPair.of(rowIndex, columnIndex);
                    }
                });
            }
        });
    }

    /**
     * Stream LU 2 RD.
     *
     * @return
     */
    public abstract ES streamLU2RD();

    /**
     * Stream RU 2 LD.
     *
     * @return
     */
    public abstract ES streamRU2LD();

    /**
     *
     * @return
     */
    public abstract ES streamH();

    /**
     *
     * @param rowIndex
     * @return
     */
    public abstract ES streamH(final int rowIndex);

    /**
     *
     * @param fromRowIndex
     * @param toRowIndex
     * @return
     */
    public abstract ES streamH(final int fromRowIndex, final int toRowIndex);

    /**
     *
     * @return
     */
    public abstract ES streamV();

    /**
     *
     * @param columnIndex
     * @return
     */
    public abstract ES streamV(final int columnIndex);

    /**
     *
     * @param fromColumnIndex
     * @param toColumnIndex
     * @return
     */
    public abstract ES streamV(final int fromColumnIndex, final int toColumnIndex);

    /**
     *
     * @return
     */
    public abstract RS streamR();

    /**
     *
     * @param fromRowIndex
     * @param toRowIndex
     * @return
     */
    public abstract RS streamR(final int fromRowIndex, final int toRowIndex);

    /**
     *
     * @return
     */
    public abstract RS streamC();

    /**
     *
     * @param fromColumnIndex
     * @param toColumnIndex
     * @return
     */
    public abstract RS streamC(final int fromColumnIndex, final int toColumnIndex);

    /**
     *
     * @param a
     * @return
     */
    protected abstract int length(A a);

    /**
     * Checks if is parallelable.
     *
     * @return true, if is parallelable
     */
    boolean isParallelable() {
        return isParallelStreamSupported && count > 8192;
    }

    /**
     * Checks if is parallelable.
     *
     * @param bm
     * @return true, if is parallelable
     */
    boolean isParallelable(final int bm) {
        return isParallelStreamSupported && count * bm > 8192;
    }

    /**
     * Check same shape.
     *
     * @param x
     */
    void checkSameShape(X x) {
        N.checkArgument(this.isSameShape(x), "Must be same shape");
    }
}
