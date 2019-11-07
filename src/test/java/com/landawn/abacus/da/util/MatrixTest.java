/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.util;

import org.junit.Test;

import com.landawn.abacus.util.Array;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Profiler;
import com.landawn.abacus.util.StringUtil;
import com.landawn.abacus.util.Try;
import com.landawn.abacus.util.function.CharConsumer;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.function.ToCharFunction;
import com.landawn.abacus.util.stream.CharStream;

import junit.framework.TestCase;

/**
 *
 * @since 0.8
 *
 * @author Haiyang Li
 */
public class MatrixTest extends TestCase {

    @Test
    public void test_extend() {

        Matrix.repeat(null, 10).println();

        Matrix.repeat("a", 10).println();

        Matrix.repeat(1, 10).println();

        ByteMatrix matrix = ByteMatrix.repeat((byte) 3, 6).reshape(2, 3);
        matrix.println();

        matrix.extend(5, 5).println();

        matrix.extend(5, 5, (byte) -1).println();

        matrix.extend(1, 1, 2, 2).println();

        matrix.extend(1, 1, 2, 2, (byte) -1).println();

        matrix.extend(0, 1, 0, 2).println();

        matrix.extend(1, 0, 2, 0, (byte) -1).println();
    }

    @Test
    public void test_extend_2() {
        ByteMatrix matrix = ByteMatrix.repeat((byte) 3, 6).reshape(2, 3);
        matrix.println();

        matrix.boxed().extend(5, 5).println();

        matrix.boxed().extend(5, 5, (byte) -1).println();

        matrix.boxed().extend(1, 1, 2, 2).println();

        matrix.boxed().extend(1, 1, 2, 2, (byte) -1).println();

        matrix.boxed().extend(0, 1, 0, 2).println();

        matrix.boxed().extend(1, 0, 2, 0, (byte) -1).println();
    }

    @Test
    public void test_repelem() {
        DoubleMatrix matrix = DoubleMatrix.of(Array.of(1d, 2, 3, 4)).reshape(2);
        matrix.println();

        N.println(StringUtil.repeat('=', 80));
        matrix.repelem(1, 1).println();

        N.println(StringUtil.repeat('=', 80));
        matrix.repelem(1, 2).println();

        N.println(StringUtil.repeat('=', 80));
        matrix.repelem(2, 1).println();

        N.println(StringUtil.repeat('=', 80));
        matrix.repelem(2, 2).println();

        N.println(StringUtil.repeat('=', 80));
        matrix.boxed().repelem(1, 1).println();

        N.println(StringUtil.repeat('=', 80));
        matrix.boxed().repelem(1, 2).println();

        N.println(StringUtil.repeat('=', 80));
        matrix.boxed().repelem(2, 1).println();

        N.println(StringUtil.repeat('=', 80));
        matrix.boxed().repelem(2, 2).println();
    }

    @Test
    public void test_repmat() {
        IntMatrix matrix = IntMatrix.range(0, 4).reshape(2);

        matrix.boxed().repmat(2, 2).println();
    }

    public void test_perf3() {
        final IntMatrix m = IntMatrix.rangeClosed(0, 10_000).reshape(100, 100);

        Profiler.run(8, 100, 3, new Try.Runnable() {
            @Override
            public void run() {
                assertTrue(m.streamV().toArray().length == 10_000);
            }
        }).printResult();
    }

    public void test_perf2() {
        final IntMatrix m = IntMatrix.rangeClosed(0, 10_000_000).reshape(10_000, 1_000);

        Profiler.run(1, 1, 3, new Try.Runnable() {
            @Override
            public void run() {
                assertEquals(10_000_000, m.rotate90().count);
            }
        }).printResult();
    }

    public void test_perf() {
        final IntMatrix m = IntMatrix.rangeClosed(0, 999_999).reshape(10_000, 100);
        final IntMatrix m2 = IntMatrix.rangeClosed(1, 1_000_000).reshape(100, 10_000);

        Profiler.run(1, 1, 3, new Try.Runnable() {
            @Override
            public void run() {
                assertEquals(100_000_000, m.multiply(m2).count);
            }
        }).printResult();
    }

    public void test_add_subtract_multiply() {
        IntMatrix m = IntMatrix.rangeClosed(0, 5).reshape(2, 3);
        m.println();
        N.println("----------------------");

        IntMatrix m2 = IntMatrix.rangeClosed(3, 8).reshape(2, 3);
        m2.println();
        N.println("----------------------");

        m.add(m2).println();
        N.println("----------------------");

        m.subtract(m2).println();
        N.println("----------------------");

        N.println("========================================");

        m.println();
        N.println("----------------------");
        m2.rotate90().println();
        N.println("----------------------");

        m.multiply(m2.rotate90()).println();
        N.println("----------------------");
    }

    public void test_reshape() {
        IntMatrix m = IntMatrix.rangeClosed(1, 9).reshape(3, 3);
        m.println();
        N.println("----------------------");
        m.reshape(3, 5).println();
        N.println("----------------------");
        m.reshape(2, 4).println();
        N.println("----------------------");
        m.reshape(3, 5).reshape(2, 4).println();

        N.println("----------------------");
        N.println(m.flatten());
    }

    public void test_01() {
        IntMatrix.rangeClosed(3, 9).reshape(3, 5).println();
        IntMatrix.random(15).reshape(3, 5).println();
        IntMatrix.repeat(13, 15).reshape(5, 3).println();

        IntMatrix.range(1, 100).reshape(3, 5).streamH().println();
        IntMatrix.range(1, 100).reshape(3, 5).streamV().println();

        IntMatrix.range(1, 100).reshape(3, 5).streamR().println();
        IntMatrix.range(1, 100).reshape(3, 5).streamC().println();

        IntMatrix.range(1, 100).reshape(3, 5).add(IntMatrix.range(1, 100).reshape(3, 5)).println();

        IntMatrix.range(1, 100).reshape(3, 5).subtract(IntMatrix.range(1, 100).reshape(3, 5)).println();

        IntMatrix.range(1, 100).reshape(3, 5).multiply(IntMatrix.range(1, 100).reshape(5, 3)).println();

        N.println(IntMatrix.range(1, 100).reshape(3, 5).flatten());

        IntMatrix.range(1, 16).reshape(3, 5).boxed().println();
    }

    public void test_05() {
        IntMatrix a = IntMatrix.repeat(0, 25).reshape(5);
        a.fill(IntMatrix.repeat(1, 9).reshape(2, 4).a);
        a.println();

        N.println(StringUtil.repeat('=', 80));

        a = IntMatrix.repeat(0, 25).reshape(5);
        a.fill(3, 1, IntMatrix.repeat(2, 9).reshape(2, 4).a);
        a.println();

        N.println(StringUtil.repeat('=', 80));

        a = IntMatrix.repeat(0, 25).reshape(5);
        a.fill(3, 3, IntMatrix.repeat(2, 9).reshape(2, 4).a);
        a.println();

        N.println(StringUtil.repeat('=', 80));

        a = IntMatrix.repeat(0, 25).reshape(5);
        a.fill(1, 1, IntMatrix.repeat(3, 36).reshape(6, 6).a);
        a.println();
    }

    public void test_04() {
        IntMatrix a = IntMatrix.of(new int[][] { { 1, 2, 3 }, { 4, 5, 6 } });
        IntMatrix b = IntMatrix.of(new int[][] { { 7, 8, 9 }, { 10, 11, 12 } });
        a.vstack(b).println();

        N.println(StringUtil.repeat('-', 80));
        a.hstack(b).println();
    }

    public void test_03() {
        BooleanMatrix matrix = BooleanMatrix.random(9).reshape(3);
        matrix.println();

        matrix.streamH().println();

        matrix.streamH(0, 0).println();

        matrix.streamH(0, 2).println();

        matrix.streamV().println();

        matrix.streamV(0, 0).println();

        matrix.streamV(0, 2).println();
    }

    public void test_02() {
        IntMatrix matrix = IntMatrix.range(0, 9).reshape(3);
        matrix.println();

        matrix.streamH().println();

        matrix.streamH(0, 0).println();

        matrix.streamH(0, 2).println();

        matrix.streamV().println();

        matrix.streamV(0, 0).println();

        matrix.streamV(0, 2).println();

    }

    public void test_rotate() {
        IntMatrix m = IntMatrix.rangeClosed(1, 6);
        m.println();
        N.println("----------------------");
        m.rotate90().println();
        N.println("----------------------");
        m.rotate180().println();
        N.println("----------------------");
        m.rotate270().println();
        N.println("----------------------");
        m.transpose().println();
        N.println("----------------------");

        N.println("========================================");

        m = IntMatrix.rangeClosed(1, 6).reshape(2, 3);
        m.println();
        N.println("----------------------");
        m.rotate90().println();
        N.println("----------------------");
        m.rotate180().println();
        N.println("----------------------");
        m.rotate270().println();
        N.println("----------------------");
        m.transpose().println();
        N.println("----------------------");

        N.println("========================================");

        m = IntMatrix.rangeClosed(1, 9).reshape(3, 3);
        m.println();
        N.println("----------------------");
        m.rotate90().println();
        N.println("----------------------");
        m.rotate180().println();
        N.println("----------------------");
        m.rotate270().println();
        N.println("----------------------");
        m.transpose().println();
        N.println("----------------------");

        assertEquals(m, m.rotate180().rotate180());
        assertEquals(m, m.rotate90().rotate270());
        assertEquals(m, m.rotate270().rotate90());
    }

    public void test_map() {

        N.asList("a", "b", "c").stream().toArray(s -> new String[0]);

        Matrix<String> m = Matrix.of(CharStream.range('a', 'z').mapToObj(String::valueOf).<String> toArray(s -> new String[0]));

        m = m.reshape(2, 3);

        m.println();

        CharMatrix m2 = m.mapToChar(new ToCharFunction<String>() {
            @Override
            public char applyAsChar(String value) {
                return value.charAt(0);
            }
        });

        N.println("----------------");

        m2.println();

        m2.streamH().min().ifPresent(new CharConsumer() {
            @Override
            public void accept(char t) {
                N.println(t);
            }
        });

        Matrix<Character> m3 = m.map(Character.class, new Function<String, Character>() {
            @Override
            public Character apply(String t) {
                return Character.valueOf(t.charAt(0));
            }
        });

        N.println("----------------");

        m3.println();
    }

    public void test_transpose() {
        IntMatrix m = IntMatrix.rangeClosed(1, 9).reshape(3, 3);
        m.println();
        N.println("----------------------");

        m.transpose().println();
        N.println("----------------------");

        m.transpose().transpose().println();
        N.println("----------------------");

        assertEquals(m, m.transpose().transpose());

        m = m.reshape(3, 5);

        m.println();
        N.println("----------------------");

        m.transpose().println();
        N.println("----------------------");

        m.transpose().transpose().println();
        N.println("----------------------");

        assertEquals(m, m.transpose().transpose());

        m = m.reshape(2, 4);

        m.println();
        N.println("----------------------");

        m.transpose().println();
        N.println("----------------------");

        m.transpose().transpose().println();
        N.println("----------------------");

        assertEquals(m, m.transpose().transpose());
    }

    public void test_streamLU2RD() {
        N.println("---------------");
        IntMatrix.diagonal(null, null).println();
        N.println("---------------");
        IntMatrix.diagonalLU2RD(Array.of(1, 1, 1)).println();
        N.println("---------------");
        IntMatrix.diagonal(null, Array.of(2, 2, 2)).println();
        N.println("---------------");
        IntMatrix.diagonal(Array.of(1, 1, 1), Array.of(2, 2, 2)).println();

        N.println("---------------");
        IntMatrix.diagonalLU2RD(Array.of(1, 1, 1)).streamLU2RD().println();
        IntMatrix.diagonalLU2RD(Array.of(1, 1, 1)).streamRU2LD().println();
        IntMatrix.diagonal(null, Array.of(2, 2, 2)).streamLU2RD().println();
        IntMatrix.diagonal(null, Array.of(2, 2, 2)).streamRU2LD().println();
        IntMatrix.diagonal(Array.of(1, 1, 1), Array.of(2, 2, 2)).streamLU2RD().println();
        IntMatrix.diagonal(Array.of(1, 1, 1), Array.of(2, 2, 2)).streamRU2LD().println();

        N.println("---------------");
        Matrix.diagonalLU2RD(new String[0]).println();
        N.println("---------------");
        Matrix.diagonalLU2RD(N.asArray("1", "1", "1")).println();
        N.println("---------------");
        Matrix.diagonal(null, N.asArray("2", "2", "2")).println();
        N.println("---------------");
        Matrix<String> m = Matrix.diagonal(N.asArray("1", "1", "1"), N.asArray("2", "2", "2"));
        m.println();

        N.println("---------------");
        Matrix.diagonalLU2RD(N.asArray("1", "1", "1")).streamLU2RD().println();
        Matrix.diagonalLU2RD(N.asArray("1", "1", "1")).streamRU2LD().println();
        Matrix.diagonal(null, N.asArray("2", "2", "2")).streamLU2RD().println();
        Matrix.diagonal(null, N.asArray("2", "2", "2")).streamRU2LD().println();
        Matrix.diagonal(N.asArray("1", "1", "1"), N.asArray("2", "2", "2")).streamLU2RD().println();
        Matrix.diagonal(N.asArray("1", "1", "1"), N.asArray("2", "2", "2")).streamRU2LD().println();
    }

}
