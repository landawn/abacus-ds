package com.landawn.abacus.da.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.landawn.abacus.da.util.f.ff;
import com.landawn.abacus.da.util.f.fff;
import com.landawn.abacus.util.Array;
import com.landawn.abacus.util.ByteList;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.function.ByteBiFunction;
import com.landawn.abacus.util.function.Consumer;
 
public class FTest {

    @Test
    public void test_flatOp() {
        int[][] a = { { 1, 2, 3 }, { 1, 2 } };
        f.println(a);

        f.flatOp(a, new Consumer<int[]>() {
            @Override
            public void accept(int[] a) {
                N.sort(a);
            }
        });

        f.println(a);
    }

    @Test
    public void test_np() {
        int[][] a = { { 1, 2, 3 }, { 1, 2 } };
        f.println(a);
        a = new int[][] { { 1, 2, 3 } };
        f.println(a);
        N.println(f.flatten(a));

    }

    @Test
    public void test_np_2() {
        int[][][] a = { { { 1, 2, 3 } }, { { 1 } }, { {}, { 1, 2 } } };
        f.println(a);
        a = new int[][][] { { { 1, 2, 3 } } };
        f.println(a);
        N.println(f.flatten(a));
    }

    @Test
    public void test_np_3() {
        double[][][] a = { { { 1, 2, 3 } }, { { 1 } }, { {}, { 1, 2 } } };
        f.println(a);
        a = new double[][][] { { { 1, 2, 3 } } };
        f.println(a);
        N.println(f.flatten(a));
    }

    @Test
    public void test_np_4() {
        String[][][] a = { { { "a", "b" } }, { { "c" } }, { {}, { "d", "e" } } };
        f.println(a);
        a = new String[][][] { { { "a", "b" } } };
        f.println(a);
        N.println(ff.flatten(a));
    }

    @Test
    public void test_01() {
        N.println(f.reshape(Array.range(0, 5), 8));

        byte[][][] a = f.reshape(ByteList.range((byte) 0, (byte) 12).array(), 2, 3);
        byte[][][] b = f.reshape(ByteList.range((byte) 0, (byte) 15).array(), 3, 2);

        N.println("==================");
        f.println(a);
        N.println("==================");
        f.println(b);

        byte[][][] c = f.zip(a, b, new ByteBiFunction<Byte>() {
            @Override
            public Byte apply(byte t, byte u) {
                return (byte) (t + u);
            }
        });
        N.println("==================");

        f.println(c);

        c = f.zip(a, b, (byte) 0, (byte) 0, new ByteBiFunction<Byte>() {
            @Override
            public Byte apply(byte t, byte u) {
                return (byte) (t + u);
            }
        });
        N.println("==================");
        f.println(c);

        c = f.zip(b, a, (byte) 0, (byte) 0, new ByteBiFunction<Byte>() {
            @Override
            public Byte apply(byte t, byte u) {
                return (byte) (t + u);
            }
        });
        N.println("==================");
        f.println(c);
    }

    @Test
    public void test_02() {

        {
            long lng = (long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE + 0L;
            N.println(lng);
            assertEquals(Integer.MAX_VALUE, lng / 2);
        }

        {
            int[] a = Array.range(0, 5);
            f.plus(a, 1);
            N.println(a);

            a = Array.range(0, 5);
            f.minus(a, 1);
            N.println(a);

            a = Array.range(0, 5);
            f.multipliedBy(a, 2);
            N.println(a);

            a = Array.range(0, 5);
            f.dividedBy(a, 2);
            N.println(a);

            N.println("======================");

            a = Array.range(0, 5);
            int[][] b = f.reshape(a, 1);
            N.println(b);
            assertTrue(N.equals(a, f.flatten(b)));
            b = f.reshape(a, 2);
            N.println("---------------------");
            N.println(b);
            assertTrue(N.equals(a, f.flatten(b)));
            b = f.reshape(a, 3);
            N.println("---------------------");
            N.println(b);
            assertTrue(N.equals(a, f.flatten(b)));
            b = f.reshape(a, 5);
            N.println("---------------------");
            N.println(b);
            assertTrue(N.equals(a, f.flatten(b)));
            b = f.reshape(a, 7);
            N.println("---------------------");
            N.println(b);
            assertTrue(N.equals(a, f.flatten(b)));

            N.println("======================");

            a = Array.range(0, 5);
            int[][][] c = f.reshape(a, 1, 1);
            N.println(c);
            assertTrue(N.equals(a, f.flatten(b)));
            c = f.reshape(a, 2, 2);
            N.println("---------------------");
            N.println(c);
            assertTrue(N.equals(a, f.flatten(b)));
            c = f.reshape(a, 3, 2);
            N.println("---------------------");
            N.println(c);
            assertTrue(N.equals(a, f.flatten(b)));
            c = f.reshape(a, 5, 2);
            N.println("---------------------");
            N.println(c);
            assertTrue(N.equals(a, f.flatten(b)));
            c = f.reshape(a, 1, 5);
            N.println("---------------------");
            N.println(c);
            assertTrue(N.equals(a, f.flatten(b)));
        }

        {
            N.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");

            String[] a = { "a", "b", "c", "d", "e" };
            String[][] b = ff.reshape(a, 1);
            N.println(b);
            assertTrue(N.equals(a, ff.flatten(b)));
            b = ff.reshape(a, 2);
            N.println("---------------------");
            N.println(b);
            assertTrue(N.equals(a, ff.flatten(b)));
            b = ff.reshape(a, 3);
            N.println("---------------------");
            N.println(b);
            assertTrue(N.equals(a, ff.flatten(b)));
            b = ff.reshape(a, 5);
            N.println("---------------------");
            N.println(b);
            assertTrue(N.equals(a, ff.flatten(b)));
            b = ff.reshape(a, 7);
            N.println("---------------------");
            N.println(b);
            assertTrue(N.equals(a, ff.flatten(b)));

            N.println("======================");

            a = new String[] { "a", "b", "c", "d", "e" };
            String[][][] c = fff.reshape(a, 1, 1);
            N.println(c);
            assertTrue(N.equals(a, ff.flatten(b)));
            c = fff.reshape(a, 2, 2);
            N.println("---------------------");
            N.println(c);
            assertTrue(N.equals(a, ff.flatten(b)));
            c = fff.reshape(a, 3, 2);
            N.println("---------------------");
            N.println(c);
            assertTrue(N.equals(a, ff.flatten(b)));
            c = fff.reshape(a, 5, 2);
            N.println("---------------------");
            N.println(c);
            assertTrue(N.equals(a, ff.flatten(b)));
            c = fff.reshape(a, 1, 5);
            N.println("---------------------");
            N.println(c);
            assertTrue(N.equals(a, ff.flatten(b)));
        }
    }
}
