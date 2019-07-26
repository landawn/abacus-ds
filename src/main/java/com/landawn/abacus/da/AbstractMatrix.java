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

package com.landawn.abacus.da;

import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.IntPair;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Try;
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
 * @param <A> the generic type
 * @param <PL> the generic type
 * @param <ES> element stream
 * @param <RS> row/column stream.
 * @param <X> the generic type
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
     * @param a the a
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
     * Array.
     *
     * @return the a[]
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
     * Copy.
     *
     * @return a new Matrix
     */
    public abstract X copy();

    /**
     * Copy.
     *
     * @param fromRowIndex the from row index
     * @param toRowIndex the to row index
     * @return a new Matrix
     */
    public abstract X copy(int fromRowIndex, int toRowIndex);

    /**
     * Copy.
     *
     * @param fromRowIndex the from row index
     * @param toRowIndex the to row index
     * @param fromColumnIndex the from column index
     * @param toColumnIndex the to column index
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
     * Transpose.
     *
     * @return the x
     */
    public abstract X transpose();

    /**
     * Reshape.
     *
     * @param newCols the new cols
     * @return the x
     */
    public X reshape(int newCols) {
        return reshape((int) (count % newCols == 0 ? count / newCols : count / newCols + 1), newCols);
    }

    /**
     * Reshape.
     *
     * @param newRows the new rows
     * @param newCols the new cols
     * @return the x
     */
    public abstract X reshape(int newRows, int newCols);

    /**
     * Checks if is same shape.
     *
     * @param x the x
     * @return true, if is same shape
     */
    public boolean isSameShape(X x) {
        return this.rows == x.rows && this.cols == x.cols;
    }

    /**
     * Repeat elements <code>rowRepeats</code> times in row direction and <code>colRepeats</code> times in column direction.
     *
     * @param rowRepeats the row repeats
     * @param colRepeats the col repeats
     * @return a new matrix
     * @see <a href="https://www.mathworks.com/help/matlab/ref/repelem.html">https://www.mathworks.com/help/matlab/ref/repelem.html</a>
     */
    public abstract X repelem(int rowRepeats, int colRepeats);

    /**
     * Repeat this matrix <code>rowRepeats</code> times in row direction and <code>colRepeats</code> times in column direction.
     *
     * @param rowRepeats the row repeats
     * @param colRepeats the col repeats
     * @return a new matrix
     * @see <a href="https://www.mathworks.com/help/matlab/ref/repmat.html">https://www.mathworks.com/help/matlab/ref/repmat.html</a>
     */
    public abstract X repmat(int rowRepeats, int colRepeats);

    /**
     * Flatten.
     *
     * @return the pl
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
     * @param <E> the element type
     * @param op the op
     * @throws E the e
     */
    public abstract <E extends Exception> void flatOp(Try.Consumer<A, E> op) throws E;

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
     * @param <E> the element type
     * @param action the action
     * @throws E the e
     */
    public <E extends Exception> void forEach(Try.IntBiConsumer<E> action) throws E {
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                action.accept(i, j);
            }
        }
    }

    /**
     * Points LU 2 RD.
     *
     * @return the stream
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
     * @return the stream
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
     * Points H.
     *
     * @return the stream
     */
    public Stream<IntPair> pointsH() {
        return pointsH(0, rows);
    }

    /**
     * Points H.
     *
     * @param rowIndex the row index
     * @return the stream
     */
    public Stream<IntPair> pointsH(int rowIndex) {
        return pointsH(rowIndex, rowIndex + 1);
    }

    /**
     * Points H.
     *
     * @param fromRowIndex the from row index
     * @param toRowIndex the to row index
     * @return the stream
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
     * Points V.
     *
     * @return the stream
     */
    public Stream<IntPair> pointsV() {
        return pointsV(0, cols);
    }

    /**
     * Points V.
     *
     * @param columnIndex the column index
     * @return the stream
     */
    public Stream<IntPair> pointsV(int columnIndex) {
        return pointsV(columnIndex, columnIndex + 1);
    }

    /**
     * Points V.
     *
     * @param fromColumnIndex the from column index
     * @param toColumnIndex the to column index
     * @return the stream
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
     * Points R.
     *
     * @return the stream
     */
    public Stream<Stream<IntPair>> pointsR() {
        return pointsR(0, rows);
    }

    /**
     * Points R.
     *
     * @param fromRowIndex the from row index
     * @param toRowIndex the to row index
     * @return the stream
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
     * Points C.
     *
     * @return the stream
     */
    public Stream<Stream<IntPair>> pointsC() {
        return pointsR(0, cols);
    }

    /**
     * Points C.
     *
     * @param fromColumnIndex the from column index
     * @param toColumnIndex the to column index
     * @return the stream
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
     * @return the es
     */
    public abstract ES streamLU2RD();

    /**
     * Stream RU 2 LD.
     *
     * @return the es
     */
    public abstract ES streamRU2LD();

    /**
     * Stream H.
     *
     * @return the es
     */
    public abstract ES streamH();

    /**
     * Stream H.
     *
     * @param rowIndex the row index
     * @return the es
     */
    public abstract ES streamH(final int rowIndex);

    /**
     * Stream H.
     *
     * @param fromRowIndex the from row index
     * @param toRowIndex the to row index
     * @return the es
     */
    public abstract ES streamH(final int fromRowIndex, final int toRowIndex);

    /**
     * Stream V.
     *
     * @return the es
     */
    public abstract ES streamV();

    /**
     * Stream V.
     *
     * @param columnIndex the column index
     * @return the es
     */
    public abstract ES streamV(final int columnIndex);

    /**
     * Stream V.
     *
     * @param fromColumnIndex the from column index
     * @param toColumnIndex the to column index
     * @return the es
     */
    public abstract ES streamV(final int fromColumnIndex, final int toColumnIndex);

    /**
     * Stream R.
     *
     * @return the rs
     */
    public abstract RS streamR();

    /**
     * Stream R.
     *
     * @param fromRowIndex the from row index
     * @param toRowIndex the to row index
     * @return the rs
     */
    public abstract RS streamR(final int fromRowIndex, final int toRowIndex);

    /**
     * Stream C.
     *
     * @return the rs
     */
    public abstract RS streamC();

    /**
     * Stream C.
     *
     * @param fromColumnIndex the from column index
     * @param toColumnIndex the to column index
     * @return the rs
     */
    public abstract RS streamC(final int fromColumnIndex, final int toColumnIndex);

    /**
     * Length.
     *
     * @param a the a
     * @return the int
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
     * @param bm the bm
     * @return true, if is parallelable
     */
    boolean isParallelable(final int bm) {
        return isParallelStreamSupported && count * bm > 8192;
    }

    /**
     * Check same shape.
     *
     * @param x the x
     */
    void checkSameShape(X x) {
        N.checkArgument(this.isSameShape(x), "Must be same shape");
    }
}
