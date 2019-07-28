/*
 * Copyright (c) 2019, Haiyang Li.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.landawn.abacus.da.util;

import com.landawn.abacus.util.N;

/**
 * 
 * @author haiyangl
 *
 */
public final class Points {
    private Points() {
        // singleton.
    }

    public static final class xy {
        private xy() {
            // singleton.
        }

        public static final class ByteBytePoint {
            public byte x;
            public byte y;
            public byte v; // value

            public ByteBytePoint() {
            }

            public ByteBytePoint(byte x, byte y, byte v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static ByteBytePoint of(byte x, byte y, byte v) {
                return new ByteBytePoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (x * 31 + y) * 31 + v;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof ByteBytePoint) {
                    ByteBytePoint other = (ByteBytePoint) obj;
                    return this.x == other.x && this.y == other.y && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class ByteIntPoint {
            public byte x;
            public byte y;
            public int v; // value

            public ByteIntPoint() {
            }

            public ByteIntPoint(byte x, byte y, int v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static ByteIntPoint of(byte x, byte y, int v) {
                return new ByteIntPoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (x * 31 + y) * 31 + v;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof ByteIntPoint) {
                    ByteIntPoint other = (ByteIntPoint) obj;
                    return this.x == other.x && this.y == other.y && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class ByteLongPoint {
            public byte x;
            public byte y;
            public long v; // value

            public ByteLongPoint() {
            }

            public ByteLongPoint(byte x, byte y, long v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static ByteLongPoint of(byte x, byte y, long v) {
                return new ByteLongPoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof ByteLongPoint) {
                    ByteLongPoint other = (ByteLongPoint) obj;
                    return this.x == other.x && this.y == other.y && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class ByteDoublePoint {
            public byte x;
            public byte y;
            public double v; // value

            public ByteDoublePoint() {
            }

            public ByteDoublePoint(byte x, byte y, double v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static ByteDoublePoint of(byte x, byte y, double v) {
                return new ByteDoublePoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof ByteDoublePoint) {
                    ByteDoublePoint other = (ByteDoublePoint) obj;
                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class ByteObjPoint<T> {
            public byte x;
            public byte y;
            public T v; // value

            public ByteObjPoint() {
            }

            public ByteObjPoint(byte x, byte y, T v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static <T> ByteObjPoint<T> of(byte x, byte y, T v) {
                return new ByteObjPoint<>(x, y, v);
            }

            @Override
            public int hashCode() {
                return (x * 31 + y) * 31 + N.hashCode(v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof ByteObjPoint) {
                    ByteObjPoint<?> other = (ByteObjPoint<?>) obj;
                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + N.toString(v) + "]";
            }
        }

        public static final class IntBytePoint {
            public int x;
            public int y;
            public byte v; // value

            public IntBytePoint() {
            }

            public IntBytePoint(int x, int y, byte v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static IntBytePoint of(int x, int y, byte v) {
                return new IntBytePoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (x * 31 + y) * 31 + v;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof IntBytePoint) {
                    IntBytePoint other = (IntBytePoint) obj;
                    return this.x == other.x && this.y == other.y && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class IntIntPoint {
            public int x;
            public int y;
            public int v; // value

            public IntIntPoint() {
            }

            public IntIntPoint(int x, int y, int v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static IntIntPoint of(int x, int y, int v) {
                return new IntIntPoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (x * 31 + y) * 31 + v;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof IntIntPoint) {
                    IntIntPoint other = (IntIntPoint) obj;
                    return this.x == other.x && this.y == other.y && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class IntLongPoint {
            public int x;
            public int y;
            public long v; // value

            public IntLongPoint() {
            }

            public IntLongPoint(int x, int y, long v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static IntLongPoint of(int x, int y, long v) {
                return new IntLongPoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof IntLongPoint) {
                    IntLongPoint other = (IntLongPoint) obj;
                    return this.x == other.x && this.y == other.y && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class IntDoublePoint {
            public int x;
            public int y;
            public double v; // value

            public IntDoublePoint() {
            }

            public IntDoublePoint(int x, int y, double v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static IntDoublePoint of(int x, int y, double v) {
                return new IntDoublePoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof IntDoublePoint) {
                    IntDoublePoint other = (IntDoublePoint) obj;
                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class IntObjPoint<T> {
            public int x;
            public int y;
            public T v; // value

            public IntObjPoint() {
            }

            public IntObjPoint(int x, int y, T v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static <T> IntObjPoint<T> of(int x, int y, T v) {
                return new IntObjPoint<>(x, y, v);
            }

            @Override
            public int hashCode() {
                return (x * 31 + y) * 31 + N.hashCode(v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof IntObjPoint) {
                    IntObjPoint<?> other = (IntObjPoint<?>) obj;
                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + N.toString(v) + "]";
            }
        }

        public static final class LongBytePoint {
            public long x;
            public long y;
            public byte v; // value

            public LongBytePoint() {
            }

            public LongBytePoint(long x, long y, byte v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static LongBytePoint of(long x, long y, byte v) {
                return new LongBytePoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof LongBytePoint) {
                    LongBytePoint other = (LongBytePoint) obj;
                    return this.x == other.x && this.y == other.y && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class LongIntPoint {
            public long x;
            public long y;
            public int v; // value

            public LongIntPoint() {
            }

            public LongIntPoint(long x, long y, int v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static LongIntPoint of(long x, long y, int v) {
                return new LongIntPoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof LongIntPoint) {
                    LongIntPoint other = (LongIntPoint) obj;
                    return this.x == other.x && this.y == other.y && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class LongLongPoint {
            public long x;
            public long y;
            public long v; // value

            public LongLongPoint() {
            }

            public LongLongPoint(long x, long y, long v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static LongLongPoint of(long x, long y, long v) {
                return new LongLongPoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof LongLongPoint) {
                    LongLongPoint other = (LongLongPoint) obj;
                    return this.x == other.x && this.y == other.y && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class LongDoublePoint {
            public long x;
            public long y;
            public double v; // value

            public LongDoublePoint() {
            }

            public LongDoublePoint(long x, long y, double v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static LongDoublePoint of(long x, long y, double v) {
                return new LongDoublePoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof LongDoublePoint) {
                    LongDoublePoint other = (LongDoublePoint) obj;
                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class LongObjPoint<T> {
            public long x;
            public long y;
            public T v; // value

            public LongObjPoint() {
            }

            public LongObjPoint(long x, long y, T v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static <T> LongObjPoint<T> of(long x, long y, T v) {
                return new LongObjPoint<>(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + N.hashCode(v));
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof LongObjPoint) {
                    LongObjPoint<?> other = (LongObjPoint<?>) obj;
                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + N.toString(v) + "]";
            }
        }

        public static final class DoubleBytePoint {
            public double x;
            public double y;
            public byte v; // value

            public DoubleBytePoint() {
            }

            public DoubleBytePoint(double x, double y, byte v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static DoubleBytePoint of(double x, double y, byte v) {
                return new DoubleBytePoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof DoubleBytePoint) {
                    DoubleBytePoint other = (DoubleBytePoint) obj;
                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class DoubleIntPoint {
            public double x;
            public double y;
            public int v; // value

            public DoubleIntPoint() {
            }

            public DoubleIntPoint(double x, double y, int v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static DoubleIntPoint of(double x, double y, int v) {
                return new DoubleIntPoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof DoubleIntPoint) {
                    DoubleIntPoint other = (DoubleIntPoint) obj;
                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class DoubleLongPoint {
            public double x;
            public double y;
            public long v; // value

            public DoubleLongPoint() {
            }

            public DoubleLongPoint(double x, double y, long v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static DoubleLongPoint of(double x, double y, long v) {
                return new DoubleLongPoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof DoubleLongPoint) {
                    DoubleLongPoint other = (DoubleLongPoint) obj;
                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class DoubleDoublePoint {
            public double x;
            public double y;
            public double v; // value

            public DoubleDoublePoint() {
            }

            public DoubleDoublePoint(double x, double y, double v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static DoubleDoublePoint of(double x, double y, double v) {
                return new DoubleDoublePoint(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof DoubleDoublePoint) {
                    DoubleDoublePoint other = (DoubleDoublePoint) obj;
                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + v + "]";
            }
        }

        public static final class DoubleObjPoint<T> {
            public double x;
            public double y;
            public T v; // value

            public DoubleObjPoint() {
            }

            public DoubleObjPoint(double x, double y, T v) {
                this.x = x;
                this.y = y;
                this.v = v;
            }

            public static <T> DoubleObjPoint<T> of(double x, double y, T v) {
                return new DoubleObjPoint<>(x, y, v);
            }

            @Override
            public int hashCode() {
                return (int) ((x * 31 + y) * 31 + N.hashCode(v));
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof DoubleObjPoint) {
                    DoubleObjPoint<?> other = (DoubleObjPoint<?>) obj;
                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + N.toString(v) + "]";
            }
        }

        //        public static final class ByteByteTimePoint {
        //            public byte x;
        //            public byte y;
        //            public long t; // timestamp
        //            public byte v; // value
        //
        //            public ByteByteTimePoint() {
        //            }
        //
        //            public ByteByteTimePoint(byte x, byte y, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static ByteByteTimePoint of(byte x, byte y, long t, byte v) {
        //                return new ByteByteTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ByteByteTimePoint) {
        //                    ByteByteTimePoint other = (ByteByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ByteIntTimePoint {
        //            public byte x;
        //            public byte y;
        //            public long t; // timestamp
        //            public int v; // value
        //
        //            public ByteIntTimePoint() {
        //            }
        //
        //            public ByteIntTimePoint(byte x, byte y, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static ByteIntTimePoint of(byte x, byte y, long t, int v) {
        //                return new ByteIntTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ByteIntTimePoint) {
        //                    ByteIntTimePoint other = (ByteIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ByteLongTimePoint {
        //            public byte x;
        //            public byte y;
        //            public long t; // timestamp
        //            public long v; // value
        //
        //            public ByteLongTimePoint() {
        //            }
        //
        //            public ByteLongTimePoint(byte x, byte y, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static ByteLongTimePoint of(byte x, byte y, long t, long v) {
        //                return new ByteLongTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ByteLongTimePoint) {
        //                    ByteLongTimePoint other = (ByteLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ByteDoubleTimePoint {
        //            public byte x;
        //            public byte y;
        //            public long t; // timestamp
        //            public double v; // value
        //
        //            public ByteDoubleTimePoint() {
        //            }
        //
        //            public ByteDoubleTimePoint(byte x, byte y, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static ByteDoubleTimePoint of(byte x, byte y, long t, double v) {
        //                return new ByteDoubleTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ByteDoubleTimePoint) {
        //                    ByteDoubleTimePoint other = (ByteDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ByteObjTimePoint<T> {
        //            public byte x;
        //            public byte y;
        //            public long t; // timestamp
        //            public T v; // value
        //
        //            public ByteObjTimePoint() {
        //            }
        //
        //            public ByteObjTimePoint(byte x, byte y, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static <T> ByteObjTimePoint<T> of(byte x, byte y, long t, T v) {
        //                return new ByteObjTimePoint<>(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + t) * 31 + N.hashCode(v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ByteObjTimePoint) {
        //                    ByteObjTimePoint<?> other = (ByteObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class IntByteTimePoint {
        //            public int x;
        //            public int y;
        //            public long t; // timestamp
        //            public byte v; // value
        //
        //            public IntByteTimePoint() {
        //            }
        //
        //            public IntByteTimePoint(int x, int y, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static IntByteTimePoint of(int x, int y, long t, byte v) {
        //                return new IntByteTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof IntByteTimePoint) {
        //                    IntByteTimePoint other = (IntByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class IntIntTimePoint {
        //            public int x;
        //            public int y;
        //            public long t; // timestamp
        //            public int v; // value
        //
        //            public IntIntTimePoint() {
        //            }
        //
        //            public IntIntTimePoint(int x, int y, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static IntIntTimePoint of(int x, int y, long t, int v) {
        //                return new IntIntTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof IntIntTimePoint) {
        //                    IntIntTimePoint other = (IntIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class IntLongTimePoint {
        //            public int x;
        //            public int y;
        //            public long t; // timestamp
        //            public long v; // value
        //
        //            public IntLongTimePoint() {
        //            }
        //
        //            public IntLongTimePoint(int x, int y, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static IntLongTimePoint of(int x, int y, long t, long v) {
        //                return new IntLongTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof IntLongTimePoint) {
        //                    IntLongTimePoint other = (IntLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class IntDoubleTimePoint {
        //            public int x;
        //            public int y;
        //            public long t; // timestamp
        //            public double v; // value
        //
        //            public IntDoubleTimePoint() {
        //            }
        //
        //            public IntDoubleTimePoint(int x, int y, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static IntDoubleTimePoint of(int x, int y, long t, double v) {
        //                return new IntDoubleTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof IntDoubleTimePoint) {
        //                    IntDoubleTimePoint other = (IntDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class IntObjTimePoint<T> {
        //            public int x;
        //            public int y;
        //            public long t; // timestamp
        //            public T v; // value
        //
        //            public IntObjTimePoint() {
        //            }
        //
        //            public IntObjTimePoint(int x, int y, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static <T> IntObjTimePoint<T> of(int x, int y, long t, T v) {
        //                return new IntObjTimePoint<>(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + t) * 31 + N.hashCode(v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof IntObjTimePoint) {
        //                    IntObjTimePoint<?> other = (IntObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class LongByteTimePoint {
        //            public long x;
        //            public long y;
        //            public long t; // timestamp
        //            public byte v; // value
        //
        //            public LongByteTimePoint() {
        //            }
        //
        //            public LongByteTimePoint(long x, long y, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static LongByteTimePoint of(long x, long y, long t, byte v) {
        //                return new LongByteTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof LongByteTimePoint) {
        //                    LongByteTimePoint other = (LongByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class LongIntTimePoint {
        //            public long x;
        //            public long y;
        //            public long t; // timestamp
        //            public int v; // value
        //
        //            public LongIntTimePoint() {
        //            }
        //
        //            public LongIntTimePoint(long x, long y, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static LongIntTimePoint of(long x, long y, long t, int v) {
        //                return new LongIntTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof LongIntTimePoint) {
        //                    LongIntTimePoint other = (LongIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class LongLongTimePoint {
        //            public long x;
        //            public long y;
        //            public long t; // timestamp
        //            public long v; // value
        //
        //            public LongLongTimePoint() {
        //            }
        //
        //            public LongLongTimePoint(long x, long y, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static LongLongTimePoint of(long x, long y, long t, long v) {
        //                return new LongLongTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof LongLongTimePoint) {
        //                    LongLongTimePoint other = (LongLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class LongDoubleTimePoint {
        //            public long x;
        //            public long y;
        //            public long t; // timestamp
        //            public double v; // value
        //
        //            public LongDoubleTimePoint() {
        //            }
        //
        //            public LongDoubleTimePoint(long x, long y, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static LongDoubleTimePoint of(long x, long y, long t, double v) {
        //                return new LongDoubleTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof LongDoubleTimePoint) {
        //                    LongDoubleTimePoint other = (LongDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class LongObjTimePoint<T> {
        //            public long x;
        //            public long y;
        //            public long t; // timestamp
        //            public T v; // value
        //
        //            public LongObjTimePoint() {
        //            }
        //
        //            public LongObjTimePoint(long x, long y, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static <T> LongObjTimePoint<T> of(long x, long y, long t, T v) {
        //                return new LongObjTimePoint<>(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof LongObjTimePoint) {
        //                    LongObjTimePoint<?> other = (LongObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class DoubleByteTimePoint {
        //            public double x;
        //            public double y;
        //            public long t; // timestamp
        //            public byte v; // value
        //
        //            public DoubleByteTimePoint() {
        //            }
        //
        //            public DoubleByteTimePoint(double x, double y, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static DoubleByteTimePoint of(double x, double y, long t, byte v) {
        //                return new DoubleByteTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof DoubleByteTimePoint) {
        //                    DoubleByteTimePoint other = (DoubleByteTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class DoubleIntTimePoint {
        //            public double x;
        //            public double y;
        //            public long t; // timestamp
        //            public int v; // value
        //
        //            public DoubleIntTimePoint() {
        //            }
        //
        //            public DoubleIntTimePoint(double x, double y, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static DoubleIntTimePoint of(double x, double y, long t, int v) {
        //                return new DoubleIntTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof DoubleIntTimePoint) {
        //                    DoubleIntTimePoint other = (DoubleIntTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class DoubleLongTimePoint {
        //            public double x;
        //            public double y;
        //            public long t; // timestamp
        //            public long v; // value
        //
        //            public DoubleLongTimePoint() {
        //            }
        //
        //            public DoubleLongTimePoint(double x, double y, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static DoubleLongTimePoint of(double x, double y, long t, long v) {
        //                return new DoubleLongTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof DoubleLongTimePoint) {
        //                    DoubleLongTimePoint other = (DoubleLongTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class DoubleDoubleTimePoint {
        //            public double x;
        //            public double y;
        //            public long t; // timestamp
        //            public double v; // value
        //
        //            public DoubleDoubleTimePoint() {
        //            }
        //
        //            public DoubleDoubleTimePoint(double x, double y, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static DoubleDoubleTimePoint of(double x, double y, long t, double v) {
        //                return new DoubleDoubleTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof DoubleDoubleTimePoint) {
        //                    DoubleDoubleTimePoint other = (DoubleDoubleTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class DoubleObjTimePoint<T> {
        //            public double x;
        //            public double y;
        //            public long t; // timestamp
        //            public T v; // value
        //
        //            public DoubleObjTimePoint() {
        //            }
        //
        //            public DoubleObjTimePoint(double x, double y, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static <T> DoubleObjTimePoint<T> of(double x, double y, long t, T v) {
        //                return new DoubleObjTimePoint<>(x, y, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof DoubleObjTimePoint) {
        //                    DoubleObjTimePoint<?> other = (DoubleObjTimePoint<?>) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteBytePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final byte v; // value
        //
        //            public ImmutableByteBytePoint(byte x, byte y, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteBytePoint of(byte x, byte y, byte v) {
        //                return new ImmutableByteBytePoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (x * 31 + y) * 31 + v;
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteBytePoint) {
        //                    ImmutableByteBytePoint other = (ImmutableByteBytePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteIntPoint {
        //            public final byte x;
        //            public final byte y;
        //            public final int v; // value
        //
        //            public ImmutableByteIntPoint(byte x, byte y, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteIntPoint of(byte x, byte y, int v) {
        //                return new ImmutableByteIntPoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (x * 31 + y) * 31 + v;
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteIntPoint) {
        //                    ImmutableByteIntPoint other = (ImmutableByteIntPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteLongPoint {
        //            public final byte x;
        //            public final byte y;
        //            public final long v; // value
        //
        //            public ImmutableByteLongPoint(byte x, byte y, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteLongPoint of(byte x, byte y, long v) {
        //                return new ImmutableByteLongPoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteLongPoint) {
        //                    ImmutableByteLongPoint other = (ImmutableByteLongPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteDoublePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final double v; // value
        //
        //            public ImmutableByteDoublePoint(byte x, byte y, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteDoublePoint of(byte x, byte y, double v) {
        //                return new ImmutableByteDoublePoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteDoublePoint) {
        //                    ImmutableByteDoublePoint other = (ImmutableByteDoublePoint) obj;
        //                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteObjPoint<T> {
        //            public final byte x;
        //            public final byte y;
        //            public final T v; // value
        //
        //            public ImmutableByteObjPoint(byte x, byte y, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableByteObjPoint<T> of(byte x, byte y, T v) {
        //                return new ImmutableByteObjPoint<>(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (x * 31 + y) * 31 + N.hashCode(v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteObjPoint) {
        //                    ImmutableByteObjPoint<?> other = (ImmutableByteObjPoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntBytePoint {
        //            public final int x;
        //            public final int y;
        //            public final byte v; // value
        //
        //            public ImmutableIntBytePoint(int x, int y, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntBytePoint of(int x, int y, byte v) {
        //                return new ImmutableIntBytePoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (x * 31 + y) * 31 + v;
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntBytePoint) {
        //                    ImmutableIntBytePoint other = (ImmutableIntBytePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntIntPoint {
        //            public final int x;
        //            public final int y;
        //            public final int v; // value
        //
        //            public ImmutableIntIntPoint(int x, int y, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntIntPoint of(int x, int y, int v) {
        //                return new ImmutableIntIntPoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (x * 31 + y) * 31 + v;
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntIntPoint) {
        //                    ImmutableIntIntPoint other = (ImmutableIntIntPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntLongPoint {
        //            public final int x;
        //            public final int y;
        //            public final long v; // value
        //
        //            public ImmutableIntLongPoint(int x, int y, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntLongPoint of(int x, int y, long v) {
        //                return new ImmutableIntLongPoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntLongPoint) {
        //                    ImmutableIntLongPoint other = (ImmutableIntLongPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntDoublePoint {
        //            public final int x;
        //            public final int y;
        //            public final double v; // value
        //
        //            public ImmutableIntDoublePoint(int x, int y, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntDoublePoint of(int x, int y, double v) {
        //                return new ImmutableIntDoublePoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntDoublePoint) {
        //                    ImmutableIntDoublePoint other = (ImmutableIntDoublePoint) obj;
        //                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntObjPoint<T> {
        //            public final int x;
        //            public final int y;
        //            public final T v; // value
        //
        //            public ImmutableIntObjPoint(int x, int y, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableIntObjPoint<T> of(int x, int y, T v) {
        //                return new ImmutableIntObjPoint<>(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (x * 31 + y) * 31 + N.hashCode(v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntObjPoint) {
        //                    ImmutableIntObjPoint<?> other = (ImmutableIntObjPoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongBytePoint {
        //            public final long x;
        //            public final long y;
        //            public final byte v; // value
        //
        //            public ImmutableLongBytePoint(long x, long y, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongBytePoint of(long x, long y, byte v) {
        //                return new ImmutableLongBytePoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongBytePoint) {
        //                    ImmutableLongBytePoint other = (ImmutableLongBytePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongIntPoint {
        //            public final long x;
        //            public final long y;
        //            public final int v; // value
        //
        //            public ImmutableLongIntPoint(long x, long y, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongIntPoint of(long x, long y, int v) {
        //                return new ImmutableLongIntPoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongIntPoint) {
        //                    ImmutableLongIntPoint other = (ImmutableLongIntPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongLongPoint {
        //            public final long x;
        //            public final long y;
        //            public final long v; // value
        //
        //            public ImmutableLongLongPoint(long x, long y, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongLongPoint of(long x, long y, long v) {
        //                return new ImmutableLongLongPoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongLongPoint) {
        //                    ImmutableLongLongPoint other = (ImmutableLongLongPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongDoublePoint {
        //            public final long x;
        //            public final long y;
        //            public final double v; // value
        //
        //            public ImmutableLongDoublePoint(long x, long y, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongDoublePoint of(long x, long y, double v) {
        //                return new ImmutableLongDoublePoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongDoublePoint) {
        //                    ImmutableLongDoublePoint other = (ImmutableLongDoublePoint) obj;
        //                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongObjPoint<T> {
        //            public final long x;
        //            public final long y;
        //            public final T v; // value
        //
        //            public ImmutableLongObjPoint(long x, long y, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableLongObjPoint<T> of(long x, long y, T v) {
        //                return new ImmutableLongObjPoint<>(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongObjPoint) {
        //                    ImmutableLongObjPoint<?> other = (ImmutableLongObjPoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleBytePoint {
        //            public final double x;
        //            public final double y;
        //            public final byte v; // value
        //
        //            public ImmutableDoubleBytePoint(double x, double y, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleBytePoint of(double x, double y, byte v) {
        //                return new ImmutableDoubleBytePoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleBytePoint) {
        //                    ImmutableDoubleBytePoint other = (ImmutableDoubleBytePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleIntPoint {
        //            public final double x;
        //            public final double y;
        //            public final int v; // value
        //
        //            public ImmutableDoubleIntPoint(double x, double y, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleIntPoint of(double x, double y, int v) {
        //                return new ImmutableDoubleIntPoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleIntPoint) {
        //                    ImmutableDoubleIntPoint other = (ImmutableDoubleIntPoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleLongPoint {
        //            public final double x;
        //            public final double y;
        //            public final long v; // value
        //
        //            public ImmutableDoubleLongPoint(double x, double y, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleLongPoint of(double x, double y, long v) {
        //                return new ImmutableDoubleLongPoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleLongPoint) {
        //                    ImmutableDoubleLongPoint other = (ImmutableDoubleLongPoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleDoublePoint {
        //            public final double x;
        //            public final double y;
        //            public final double v; // value
        //
        //            public ImmutableDoubleDoublePoint(double x, double y, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleDoublePoint of(double x, double y, double v) {
        //                return new ImmutableDoubleDoublePoint(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleDoublePoint) {
        //                    ImmutableDoubleDoublePoint other = (ImmutableDoubleDoublePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleObjPoint<T> {
        //            public final double x;
        //            public final double y;
        //            public final T v; // value
        //
        //            public ImmutableDoubleObjPoint(double x, double y, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableDoubleObjPoint<T> of(double x, double y, T v) {
        //                return new ImmutableDoubleObjPoint<>(x, y, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleObjPoint) {
        //                    ImmutableDoubleObjPoint<?> other = (ImmutableDoubleObjPoint<?>) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteByteTimePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final long t; // timestamp
        //            public final byte v; // value
        //
        //            public ImmutableByteByteTimePoint(byte x, byte y, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteByteTimePoint of(byte x, byte y, long t, byte v) {
        //                return new ImmutableByteByteTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteByteTimePoint) {
        //                    ImmutableByteByteTimePoint other = (ImmutableByteByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteIntTimePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final long t; // timestamp
        //            public final int v; // value
        //
        //            public ImmutableByteIntTimePoint(byte x, byte y, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteIntTimePoint of(byte x, byte y, long t, int v) {
        //                return new ImmutableByteIntTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteIntTimePoint) {
        //                    ImmutableByteIntTimePoint other = (ImmutableByteIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteLongTimePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final long t; // timestamp
        //            public final long v; // value
        //
        //            public ImmutableByteLongTimePoint(byte x, byte y, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteLongTimePoint of(byte x, byte y, long t, long v) {
        //                return new ImmutableByteLongTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteLongTimePoint) {
        //                    ImmutableByteLongTimePoint other = (ImmutableByteLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteDoubleTimePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final long t; // timestamp
        //            public final double v; // value
        //
        //            public ImmutableByteDoubleTimePoint(byte x, byte y, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteDoubleTimePoint of(byte x, byte y, long t, double v) {
        //                return new ImmutableByteDoubleTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteDoubleTimePoint) {
        //                    ImmutableByteDoubleTimePoint other = (ImmutableByteDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteObjTimePoint<T> {
        //            public final byte x;
        //            public final byte y;
        //            public final long t; // timestamp
        //            public final T v; // value
        //
        //            public ImmutableByteObjTimePoint(byte x, byte y, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableByteObjTimePoint<T> of(byte x, byte y, long t, T v) {
        //                return new ImmutableByteObjTimePoint<>(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + t) * 31 + N.hashCode(v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteObjTimePoint) {
        //                    ImmutableByteObjTimePoint<?> other = (ImmutableByteObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntByteTimePoint {
        //            public final int x;
        //            public final int y;
        //            public final long t; // timestamp
        //            public final byte v; // value
        //
        //            public ImmutableIntByteTimePoint(int x, int y, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntByteTimePoint of(int x, int y, long t, byte v) {
        //                return new ImmutableIntByteTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntByteTimePoint) {
        //                    ImmutableIntByteTimePoint other = (ImmutableIntByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntIntTimePoint {
        //            public final int x;
        //            public final int y;
        //            public final long t; // timestamp
        //            public final int v; // value
        //
        //            public ImmutableIntIntTimePoint(int x, int y, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntIntTimePoint of(int x, int y, long t, int v) {
        //                return new ImmutableIntIntTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntIntTimePoint) {
        //                    ImmutableIntIntTimePoint other = (ImmutableIntIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntLongTimePoint {
        //            public final int x;
        //            public final int y;
        //            public final long t; // timestamp
        //            public final long v; // value
        //
        //            public ImmutableIntLongTimePoint(int x, int y, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntLongTimePoint of(int x, int y, long t, long v) {
        //                return new ImmutableIntLongTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntLongTimePoint) {
        //                    ImmutableIntLongTimePoint other = (ImmutableIntLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntDoubleTimePoint {
        //            public final int x;
        //            public final int y;
        //            public final long t; // timestamp
        //            public final double v; // value
        //
        //            public ImmutableIntDoubleTimePoint(int x, int y, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntDoubleTimePoint of(int x, int y, long t, double v) {
        //                return new ImmutableIntDoubleTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntDoubleTimePoint) {
        //                    ImmutableIntDoubleTimePoint other = (ImmutableIntDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntObjTimePoint<T> {
        //            public final int x;
        //            public final int y;
        //            public final long t; // timestamp
        //            public final T v; // value
        //
        //            public ImmutableIntObjTimePoint(int x, int y, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableIntObjTimePoint<T> of(int x, int y, long t, T v) {
        //                return new ImmutableIntObjTimePoint<>(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((x * 31 + y) * 31 + t) * 31 + N.hashCode(v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntObjTimePoint) {
        //                    ImmutableIntObjTimePoint<?> other = (ImmutableIntObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongByteTimePoint {
        //            public final long x;
        //            public final long y;
        //            public final long t; // timestamp
        //            public final byte v; // value
        //
        //            public ImmutableLongByteTimePoint(long x, long y, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongByteTimePoint of(long x, long y, long t, byte v) {
        //                return new ImmutableLongByteTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongByteTimePoint) {
        //                    ImmutableLongByteTimePoint other = (ImmutableLongByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongIntTimePoint {
        //            public final long x;
        //            public final long y;
        //            public final long t; // timestamp
        //            public final int v; // value
        //
        //            public ImmutableLongIntTimePoint(long x, long y, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongIntTimePoint of(long x, long y, long t, int v) {
        //                return new ImmutableLongIntTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongIntTimePoint) {
        //                    ImmutableLongIntTimePoint other = (ImmutableLongIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongLongTimePoint {
        //            public final long x;
        //            public final long y;
        //            public final long t; // timestamp
        //            public final long v; // value
        //
        //            public ImmutableLongLongTimePoint(long x, long y, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongLongTimePoint of(long x, long y, long t, long v) {
        //                return new ImmutableLongLongTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongLongTimePoint) {
        //                    ImmutableLongLongTimePoint other = (ImmutableLongLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongDoubleTimePoint {
        //            public final long x;
        //            public final long y;
        //            public final long t; // timestamp
        //            public final double v; // value
        //
        //            public ImmutableLongDoubleTimePoint(long x, long y, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongDoubleTimePoint of(long x, long y, long t, double v) {
        //                return new ImmutableLongDoubleTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongDoubleTimePoint) {
        //                    ImmutableLongDoubleTimePoint other = (ImmutableLongDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongObjTimePoint<T> {
        //            public final long x;
        //            public final long y;
        //            public final long t; // timestamp
        //            public final T v; // value
        //
        //            public ImmutableLongObjTimePoint(long x, long y, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableLongObjTimePoint<T> of(long x, long y, long t, T v) {
        //                return new ImmutableLongObjTimePoint<>(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongObjTimePoint) {
        //                    ImmutableLongObjTimePoint<?> other = (ImmutableLongObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleByteTimePoint {
        //            public final double x;
        //            public final double y;
        //            public final long t; // timestamp
        //            public final byte v; // value
        //
        //            public ImmutableDoubleByteTimePoint(double x, double y, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleByteTimePoint of(double x, double y, long t, byte v) {
        //                return new ImmutableDoubleByteTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleByteTimePoint) {
        //                    ImmutableDoubleByteTimePoint other = (ImmutableDoubleByteTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleIntTimePoint {
        //            public final double x;
        //            public final double y;
        //            public final long t; // timestamp
        //            public final int v; // value
        //
        //            public ImmutableDoubleIntTimePoint(double x, double y, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleIntTimePoint of(double x, double y, long t, int v) {
        //                return new ImmutableDoubleIntTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleIntTimePoint) {
        //                    ImmutableDoubleIntTimePoint other = (ImmutableDoubleIntTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleLongTimePoint {
        //            public final double x;
        //            public final double y;
        //            public final long t; // timestamp
        //            public final long v; // value
        //
        //            public ImmutableDoubleLongTimePoint(double x, double y, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleLongTimePoint of(double x, double y, long t, long v) {
        //                return new ImmutableDoubleLongTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleLongTimePoint) {
        //                    ImmutableDoubleLongTimePoint other = (ImmutableDoubleLongTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleDoubleTimePoint {
        //            public final double x;
        //            public final double y;
        //            public final long t; // timestamp
        //            public final double v; // value
        //
        //            public ImmutableDoubleDoubleTimePoint(double x, double y, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleDoubleTimePoint of(double x, double y, long t, double v) {
        //                return new ImmutableDoubleDoubleTimePoint(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + t) * 31 + v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleDoubleTimePoint) {
        //                    ImmutableDoubleDoubleTimePoint other = (ImmutableDoubleDoubleTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleObjTimePoint<T> {
        //            public final double x;
        //            public final double y;
        //            public final long t; // timestamp
        //            public final T v; // value
        //
        //            public ImmutableDoubleObjTimePoint(double x, double y, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableDoubleObjTimePoint<T> of(double x, double y, long t, T v) {
        //                return new ImmutableDoubleObjTimePoint<>(x, y, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleObjTimePoint) {
        //                    ImmutableDoubleObjTimePoint<?> other = (ImmutableDoubleObjTimePoint<?>) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + t + "," + N.toString(v) + "]";
        //            }
        //        }
    }

    public static final class xyz {
        private xyz() {
            // singleton.
        }

        public static final class ByteBytePoint {
            public byte x;
            public byte y;
            public byte z;
            public byte v; // value

            public ByteBytePoint() {
            }

            public ByteBytePoint(byte x, byte y, byte z, byte v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static ByteBytePoint of(byte x, byte y, byte z, byte v) {
                return new ByteBytePoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return ((x * 31 + y) * 31 + z) * 31 + v;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof ByteBytePoint) {
                    ByteBytePoint other = (ByteBytePoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class ByteIntPoint {
            public byte x;
            public byte y;
            public byte z;
            public int v; // value

            public ByteIntPoint() {
            }

            public ByteIntPoint(byte x, byte y, byte z, int v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static ByteIntPoint of(byte x, byte y, byte z, int v) {
                return new ByteIntPoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return ((x * 31 + y) * 31 + z) * 31 + v;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof ByteIntPoint) {
                    ByteIntPoint other = (ByteIntPoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class ByteLongPoint {
            public byte x;
            public byte y;
            public byte z;
            public long v; // value

            public ByteLongPoint() {
            }

            public ByteLongPoint(byte x, byte y, byte z, long v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static ByteLongPoint of(byte x, byte y, byte z, long v) {
                return new ByteLongPoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof ByteLongPoint) {
                    ByteLongPoint other = (ByteLongPoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class ByteDoublePoint {
            public byte x;
            public byte y;
            public byte z;
            public double v; // value

            public ByteDoublePoint() {
            }

            public ByteDoublePoint(byte x, byte y, byte z, double v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static ByteDoublePoint of(byte x, byte y, byte z, double v) {
                return new ByteDoublePoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof ByteDoublePoint) {
                    ByteDoublePoint other = (ByteDoublePoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class ByteObjPoint<T> {
            public byte x;
            public byte y;
            public byte z;
            public T v; // value

            public ByteObjPoint() {
            }

            public ByteObjPoint(byte x, byte y, byte z, T v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static <T> ByteObjPoint<T> of(byte x, byte y, byte z, T v) {
                return new ByteObjPoint<>(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return ((x * 31 + y) * 31 + z) * 31 + N.hashCode(v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof ByteObjPoint) {
                    ByteObjPoint<?> other = (ByteObjPoint<?>) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + N.toString(v) + "]";
            }
        }

        public static final class IntBytePoint {
            public int x;
            public int y;
            public int z;
            public byte v; // value

            public IntBytePoint() {
            }

            public IntBytePoint(int x, int y, int z, byte v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static IntBytePoint of(int x, int y, int z, byte v) {
                return new IntBytePoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return ((x * 31 + y) * 31 + z) * 31 + v;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof IntBytePoint) {
                    IntBytePoint other = (IntBytePoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class IntIntPoint {
            public int x;
            public int y;
            public int z;
            public int v; // value

            public IntIntPoint() {
            }

            public IntIntPoint(int x, int y, int z, int v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static IntIntPoint of(int x, int y, int z, int v) {
                return new IntIntPoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return ((x * 31 + y) * 31 + z) * 31 + v;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof IntIntPoint) {
                    IntIntPoint other = (IntIntPoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class IntLongPoint {
            public int x;
            public int y;
            public int z;
            public long v; // value

            public IntLongPoint() {
            }

            public IntLongPoint(int x, int y, int z, long v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static IntLongPoint of(int x, int y, int z, long v) {
                return new IntLongPoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof IntLongPoint) {
                    IntLongPoint other = (IntLongPoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class IntDoublePoint {
            public int x;
            public int y;
            public int z;
            public double v; // value

            public IntDoublePoint() {
            }

            public IntDoublePoint(int x, int y, int z, double v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static IntDoublePoint of(int x, int y, int z, double v) {
                return new IntDoublePoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof IntDoublePoint) {
                    IntDoublePoint other = (IntDoublePoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class IntObjPoint<T> {
            public int x;
            public int y;
            public int z;
            public T v; // value

            public IntObjPoint() {
            }

            public IntObjPoint(int x, int y, int z, T v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static <T> IntObjPoint<T> of(int x, int y, int z, T v) {
                return new IntObjPoint<>(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return ((x * 31 + y) * 31 + z) * 31 + N.hashCode(v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof IntObjPoint) {
                    IntObjPoint<?> other = (IntObjPoint<?>) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + N.toString(v) + "]";
            }
        }

        public static final class LongBytePoint {
            public long x;
            public long y;
            public long z;
            public byte v; // value

            public LongBytePoint() {
            }

            public LongBytePoint(long x, long y, long z, byte v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static LongBytePoint of(long x, long y, long z, byte v) {
                return new LongBytePoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof LongBytePoint) {
                    LongBytePoint other = (LongBytePoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class LongIntPoint {
            public long x;
            public long y;
            public long z;
            public int v; // value

            public LongIntPoint() {
            }

            public LongIntPoint(long x, long y, long z, int v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static LongIntPoint of(long x, long y, long z, int v) {
                return new LongIntPoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof LongIntPoint) {
                    LongIntPoint other = (LongIntPoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class LongLongPoint {
            public long x;
            public long y;
            public long z;
            public long v; // value

            public LongLongPoint() {
            }

            public LongLongPoint(long x, long y, long z, long v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static LongLongPoint of(long x, long y, long z, long v) {
                return new LongLongPoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof LongLongPoint) {
                    LongLongPoint other = (LongLongPoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class LongDoublePoint {
            public long x;
            public long y;
            public long z;
            public double v; // value

            public LongDoublePoint() {
            }

            public LongDoublePoint(long x, long y, long z, double v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static LongDoublePoint of(long x, long y, long z, double v) {
                return new LongDoublePoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof LongDoublePoint) {
                    LongDoublePoint other = (LongDoublePoint) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class LongObjPoint<T> {
            public long x;
            public long y;
            public long z;
            public T v; // value

            public LongObjPoint() {
            }

            public LongObjPoint(long x, long y, long z, T v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static <T> LongObjPoint<T> of(long x, long y, long z, T v) {
                return new LongObjPoint<>(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + N.hashCode(v));
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof LongObjPoint) {
                    LongObjPoint<?> other = (LongObjPoint<?>) obj;
                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + N.toString(v) + "]";
            }
        }

        public static final class DoubleBytePoint {
            public double x;
            public double y;
            public double z;
            public byte v; // value

            public DoubleBytePoint() {
            }

            public DoubleBytePoint(double x, double y, double z, byte v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static DoubleBytePoint of(double x, double y, double z, byte v) {
                return new DoubleBytePoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof DoubleBytePoint) {
                    DoubleBytePoint other = (DoubleBytePoint) obj;
                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class DoubleIntPoint {
            public double x;
            public double y;
            public double z;
            public int v; // value

            public DoubleIntPoint() {
            }

            public DoubleIntPoint(double x, double y, double z, int v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static DoubleIntPoint of(double x, double y, double z, int v) {
                return new DoubleIntPoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof DoubleIntPoint) {
                    DoubleIntPoint other = (DoubleIntPoint) obj;
                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class DoubleLongPoint {
            public double x;
            public double y;
            public double z;
            public long v; // value

            public DoubleLongPoint() {
            }

            public DoubleLongPoint(double x, double y, double z, long v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static DoubleLongPoint of(double x, double y, double z, long v) {
                return new DoubleLongPoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof DoubleLongPoint) {
                    DoubleLongPoint other = (DoubleLongPoint) obj;
                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.v == other.v;
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class DoubleDoublePoint {
            public double x;
            public double y;
            public double z;
            public double v; // value

            public DoubleDoublePoint() {
            }

            public DoubleDoublePoint(double x, double y, double z, double v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static DoubleDoublePoint of(double x, double y, double z, double v) {
                return new DoubleDoublePoint(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof DoubleDoublePoint) {
                    DoubleDoublePoint other = (DoubleDoublePoint) obj;
                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
            }
        }

        public static final class DoubleObjPoint<T> {
            public double x;
            public double y;
            public double z;
            public T v; // value

            public DoubleObjPoint() {
            }

            public DoubleObjPoint(double x, double y, double z, T v) {
                this.x = x;
                this.y = y;
                this.z = z;
                this.v = v;
            }

            public static <T> DoubleObjPoint<T> of(double x, double y, double z, T v) {
                return new DoubleObjPoint<>(x, y, z, v);
            }

            @Override
            public int hashCode() {
                return (int) (((x * 31 + y) * 31 + z) * 31 + N.hashCode(v));
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj instanceof DoubleObjPoint) {
                    DoubleObjPoint<?> other = (DoubleObjPoint<?>) obj;
                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && N.equals(this.v, other.v);
                }

                return false;
            }

            @Override
            public String toString() {
                return "[" + x + ", " + y + ", " + z + ", " + N.toString(v) + "]";
            }
        }

        //        public static final class ByteByteTimePoint {
        //            public byte x;
        //            public byte y;
        //            public byte z;
        //            public long t; // timestamp
        //            public byte v; // value
        //
        //            public ByteByteTimePoint() {
        //            }
        //
        //            public ByteByteTimePoint(byte x, byte y, byte z, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static ByteByteTimePoint of(byte x, byte y, byte z, long t, byte v) {
        //                return new ByteByteTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ByteByteTimePoint) {
        //                    ByteByteTimePoint other = (ByteByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ByteIntTimePoint {
        //            public byte x;
        //            public byte y;
        //            public byte z;
        //            public long t; // timestamp
        //            public int v; // value
        //
        //            public ByteIntTimePoint() {
        //            }
        //
        //            public ByteIntTimePoint(byte x, byte y, byte z, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static ByteIntTimePoint of(byte x, byte y, byte z, long t, int v) {
        //                return new ByteIntTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ByteIntTimePoint) {
        //                    ByteIntTimePoint other = (ByteIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ByteLongTimePoint {
        //            public byte x;
        //            public byte y;
        //            public byte z;
        //            public long t; // timestamp
        //            public long v; // value
        //
        //            public ByteLongTimePoint() {
        //            }
        //
        //            public ByteLongTimePoint(byte x, byte y, byte z, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static ByteLongTimePoint of(byte x, byte y, byte z, long t, long v) {
        //                return new ByteLongTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ByteLongTimePoint) {
        //                    ByteLongTimePoint other = (ByteLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ByteDoubleTimePoint {
        //            public byte x;
        //            public byte y;
        //            public byte z;
        //            public long t; // timestamp
        //            public double v; // value
        //
        //            public ByteDoubleTimePoint() {
        //            }
        //
        //            public ByteDoubleTimePoint(byte x, byte y, byte z, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static ByteDoubleTimePoint of(byte x, byte y, byte z, long t, double v) {
        //                return new ByteDoubleTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ByteDoubleTimePoint) {
        //                    ByteDoubleTimePoint other = (ByteDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ByteObjTimePoint<T> {
        //            public byte x;
        //            public byte y;
        //            public byte z;
        //            public long t; // timestamp
        //            public T v; // value
        //
        //            public ByteObjTimePoint() {
        //            }
        //
        //            public ByteObjTimePoint(byte x, byte y, byte z, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static <T> ByteObjTimePoint<T> of(byte x, byte y, byte z, long t, T v) {
        //                return new ByteObjTimePoint<>(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ByteObjTimePoint) {
        //                    ByteObjTimePoint<?> other = (ByteObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class IntByteTimePoint {
        //            public int x;
        //            public int y;
        //            public int z;
        //            public long t; // timestamp
        //            public byte v; // value
        //
        //            public IntByteTimePoint() {
        //            }
        //
        //            public IntByteTimePoint(int x, int y, int z, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static IntByteTimePoint of(int x, int y, int z, long t, byte v) {
        //                return new IntByteTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof IntByteTimePoint) {
        //                    IntByteTimePoint other = (IntByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class IntIntTimePoint {
        //            public int x;
        //            public int y;
        //            public int z;
        //            public long t; // timestamp
        //            public int v; // value
        //
        //            public IntIntTimePoint() {
        //            }
        //
        //            public IntIntTimePoint(int x, int y, int z, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static IntIntTimePoint of(int x, int y, int z, long t, int v) {
        //                return new IntIntTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof IntIntTimePoint) {
        //                    IntIntTimePoint other = (IntIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class IntLongTimePoint {
        //            public int x;
        //            public int y;
        //            public int z;
        //            public long t; // timestamp
        //            public long v; // value
        //
        //            public IntLongTimePoint() {
        //            }
        //
        //            public IntLongTimePoint(int x, int y, int z, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static IntLongTimePoint of(int x, int y, int z, long t, long v) {
        //                return new IntLongTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof IntLongTimePoint) {
        //                    IntLongTimePoint other = (IntLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class IntDoubleTimePoint {
        //            public int x;
        //            public int y;
        //            public int z;
        //            public long t; // timestamp
        //            public double v; // value
        //
        //            public IntDoubleTimePoint() {
        //            }
        //
        //            public IntDoubleTimePoint(int x, int y, int z, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static IntDoubleTimePoint of(int x, int y, int z, long t, double v) {
        //                return new IntDoubleTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof IntDoubleTimePoint) {
        //                    IntDoubleTimePoint other = (IntDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class IntObjTimePoint<T> {
        //            public int x;
        //            public int y;
        //            public int z;
        //            public long t; // timestamp
        //            public T v; // value
        //
        //            public IntObjTimePoint() {
        //            }
        //
        //            public IntObjTimePoint(int x, int y, int z, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static <T> IntObjTimePoint<T> of(int x, int y, int z, long t, T v) {
        //                return new IntObjTimePoint<>(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof IntObjTimePoint) {
        //                    IntObjTimePoint<?> other = (IntObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class LongByteTimePoint {
        //            public long x;
        //            public long y;
        //            public long z;
        //            public long t; // timestamp
        //            public byte v; // value
        //
        //            public LongByteTimePoint() {
        //            }
        //
        //            public LongByteTimePoint(long x, long y, long z, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static LongByteTimePoint of(long x, long y, long z, long t, byte v) {
        //                return new LongByteTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof LongByteTimePoint) {
        //                    LongByteTimePoint other = (LongByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class LongIntTimePoint {
        //            public long x;
        //            public long y;
        //            public long z;
        //            public long t; // timestamp
        //            public int v; // value
        //
        //            public LongIntTimePoint() {
        //            }
        //
        //            public LongIntTimePoint(long x, long y, long z, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static LongIntTimePoint of(long x, long y, long z, long t, int v) {
        //                return new LongIntTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof LongIntTimePoint) {
        //                    LongIntTimePoint other = (LongIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class LongLongTimePoint {
        //            public long x;
        //            public long y;
        //            public long z;
        //            public long t; // timestamp
        //            public long v; // value
        //
        //            public LongLongTimePoint() {
        //            }
        //
        //            public LongLongTimePoint(long x, long y, long z, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static LongLongTimePoint of(long x, long y, long z, long t, long v) {
        //                return new LongLongTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof LongLongTimePoint) {
        //                    LongLongTimePoint other = (LongLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class LongDoubleTimePoint {
        //            public long x;
        //            public long y;
        //            public long z;
        //            public long t; // timestamp
        //            public double v; // value
        //
        //            public LongDoubleTimePoint() {
        //            }
        //
        //            public LongDoubleTimePoint(long x, long y, long z, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static LongDoubleTimePoint of(long x, long y, long z, long t, double v) {
        //                return new LongDoubleTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof LongDoubleTimePoint) {
        //                    LongDoubleTimePoint other = (LongDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class LongObjTimePoint<T> {
        //            public long x;
        //            public long y;
        //            public long z;
        //            public long t; // timestamp
        //            public T v; // value
        //
        //            public LongObjTimePoint() {
        //            }
        //
        //            public LongObjTimePoint(long x, long y, long z, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static <T> LongObjTimePoint<T> of(long x, long y, long z, long t, T v) {
        //                return new LongObjTimePoint<>(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof LongObjTimePoint) {
        //                    LongObjTimePoint<?> other = (LongObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class DoubleByteTimePoint {
        //            public double x;
        //            public double y;
        //            public double z;
        //            public long t; // timestamp
        //            public byte v; // value
        //
        //            public DoubleByteTimePoint() {
        //            }
        //
        //            public DoubleByteTimePoint(double x, double y, double z, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static DoubleByteTimePoint of(double x, double y, double z, long t, byte v) {
        //                return new DoubleByteTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof DoubleByteTimePoint) {
        //                    DoubleByteTimePoint other = (DoubleByteTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class DoubleIntTimePoint {
        //            public double x;
        //            public double y;
        //            public double z;
        //            public long t; // timestamp
        //            public int v; // value
        //
        //            public DoubleIntTimePoint() {
        //            }
        //
        //            public DoubleIntTimePoint(double x, double y, double z, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static DoubleIntTimePoint of(double x, double y, double z, long t, int v) {
        //                return new DoubleIntTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof DoubleIntTimePoint) {
        //                    DoubleIntTimePoint other = (DoubleIntTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class DoubleLongTimePoint {
        //            public double x;
        //            public double y;
        //            public double z;
        //            public long t; // timestamp
        //            public long v; // value
        //
        //            public DoubleLongTimePoint() {
        //            }
        //
        //            public DoubleLongTimePoint(double x, double y, double z, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static DoubleLongTimePoint of(double x, double y, double z, long t, long v) {
        //                return new DoubleLongTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof DoubleLongTimePoint) {
        //                    DoubleLongTimePoint other = (DoubleLongTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class DoubleDoubleTimePoint {
        //            public double x;
        //            public double y;
        //            public double z;
        //            public long t; // timestamp
        //            public double v; // value
        //
        //            public DoubleDoubleTimePoint() {
        //            }
        //
        //            public DoubleDoubleTimePoint(double x, double y, double z, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static DoubleDoubleTimePoint of(double x, double y, double z, long t, double v) {
        //                return new DoubleDoubleTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof DoubleDoubleTimePoint) {
        //                    DoubleDoubleTimePoint other = (DoubleDoubleTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.t == other.t
        //                            && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class DoubleObjTimePoint<T> {
        //            public double x;
        //            public double y;
        //            public double z;
        //            public long t; // timestamp
        //            public T v; // value
        //
        //            public DoubleObjTimePoint() {
        //            }
        //
        //            public DoubleObjTimePoint(double x, double y, double z, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public static <T> DoubleObjTimePoint<T> of(double x, double y, double z, long t, T v) {
        //                return new DoubleObjTimePoint<>(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof DoubleObjTimePoint) {
        //                    DoubleObjTimePoint<?> other = (DoubleObjTimePoint<?>) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.t == other.t
        //                            && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteBytePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final byte z;
        //            public final byte v; // value
        //
        //            public ImmutableByteBytePoint(byte x, byte y, byte z, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteBytePoint of(byte x, byte y, byte z, byte v) {
        //                return new ImmutableByteBytePoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return ((x * 31 + y) * 31 + z) * 31 + v;
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteBytePoint) {
        //                    ImmutableByteBytePoint other = (ImmutableByteBytePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteIntPoint {
        //            public final byte x;
        //            public final byte y;
        //            public final byte z;
        //            public final int v; // value
        //
        //            public ImmutableByteIntPoint(byte x, byte y, byte z, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteIntPoint of(byte x, byte y, byte z, int v) {
        //                return new ImmutableByteIntPoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return ((x * 31 + y) * 31 + z) * 31 + v;
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteIntPoint) {
        //                    ImmutableByteIntPoint other = (ImmutableByteIntPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteLongPoint {
        //            public final byte x;
        //            public final byte y;
        //            public final byte z;
        //            public final long v; // value
        //
        //            public ImmutableByteLongPoint(byte x, byte y, byte z, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteLongPoint of(byte x, byte y, byte z, long v) {
        //                return new ImmutableByteLongPoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteLongPoint) {
        //                    ImmutableByteLongPoint other = (ImmutableByteLongPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteDoublePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final byte z;
        //            public final double v; // value
        //
        //            public ImmutableByteDoublePoint(byte x, byte y, byte z, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteDoublePoint of(byte x, byte y, byte z, double v) {
        //                return new ImmutableByteDoublePoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteDoublePoint) {
        //                    ImmutableByteDoublePoint other = (ImmutableByteDoublePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteObjPoint<T> {
        //            public final byte x;
        //            public final byte y;
        //            public final byte z;
        //            public final T v; // value
        //
        //            public ImmutableByteObjPoint(byte x, byte y, byte z, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableByteObjPoint<T> of(byte x, byte y, byte z, T v) {
        //                return new ImmutableByteObjPoint<>(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return ((x * 31 + y) * 31 + z) * 31 + N.hashCode(v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteObjPoint) {
        //                    ImmutableByteObjPoint<?> other = (ImmutableByteObjPoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntBytePoint {
        //            public final int x;
        //            public final int y;
        //            public final int z;
        //            public final byte v; // value
        //
        //            public ImmutableIntBytePoint(int x, int y, int z, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntBytePoint of(int x, int y, int z, byte v) {
        //                return new ImmutableIntBytePoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return ((x * 31 + y) * 31 + z) * 31 + v;
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntBytePoint) {
        //                    ImmutableIntBytePoint other = (ImmutableIntBytePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntIntPoint {
        //            public final int x;
        //            public final int y;
        //            public final int z;
        //            public final int v; // value
        //
        //            public ImmutableIntIntPoint(int x, int y, int z, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntIntPoint of(int x, int y, int z, int v) {
        //                return new ImmutableIntIntPoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return ((x * 31 + y) * 31 + z) * 31 + v;
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntIntPoint) {
        //                    ImmutableIntIntPoint other = (ImmutableIntIntPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntLongPoint {
        //            public final int x;
        //            public final int y;
        //            public final int z;
        //            public final long v; // value
        //
        //            public ImmutableIntLongPoint(int x, int y, int z, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntLongPoint of(int x, int y, int z, long v) {
        //                return new ImmutableIntLongPoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntLongPoint) {
        //                    ImmutableIntLongPoint other = (ImmutableIntLongPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntDoublePoint {
        //            public final int x;
        //            public final int y;
        //            public final int z;
        //            public final double v; // value
        //
        //            public ImmutableIntDoublePoint(int x, int y, int z, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntDoublePoint of(int x, int y, int z, double v) {
        //                return new ImmutableIntDoublePoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntDoublePoint) {
        //                    ImmutableIntDoublePoint other = (ImmutableIntDoublePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntObjPoint<T> {
        //            public final int x;
        //            public final int y;
        //            public final int z;
        //            public final T v; // value
        //
        //            public ImmutableIntObjPoint(int x, int y, int z, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableIntObjPoint<T> of(int x, int y, int z, T v) {
        //                return new ImmutableIntObjPoint<>(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return ((x * 31 + y) * 31 + z) * 31 + N.hashCode(v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntObjPoint) {
        //                    ImmutableIntObjPoint<?> other = (ImmutableIntObjPoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongBytePoint {
        //            public final long x;
        //            public final long y;
        //            public final long z;
        //            public final byte v; // value
        //
        //            public ImmutableLongBytePoint(long x, long y, long z, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongBytePoint of(long x, long y, long z, byte v) {
        //                return new ImmutableLongBytePoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongBytePoint) {
        //                    ImmutableLongBytePoint other = (ImmutableLongBytePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongIntPoint {
        //            public final long x;
        //            public final long y;
        //            public final long z;
        //            public final int v; // value
        //
        //            public ImmutableLongIntPoint(long x, long y, long z, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongIntPoint of(long x, long y, long z, int v) {
        //                return new ImmutableLongIntPoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongIntPoint) {
        //                    ImmutableLongIntPoint other = (ImmutableLongIntPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongLongPoint {
        //            public final long x;
        //            public final long y;
        //            public final long z;
        //            public final long v; // value
        //
        //            public ImmutableLongLongPoint(long x, long y, long z, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongLongPoint of(long x, long y, long z, long v) {
        //                return new ImmutableLongLongPoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongLongPoint) {
        //                    ImmutableLongLongPoint other = (ImmutableLongLongPoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongDoublePoint {
        //            public final long x;
        //            public final long y;
        //            public final long z;
        //            public final double v; // value
        //
        //            public ImmutableLongDoublePoint(long x, long y, long z, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongDoublePoint of(long x, long y, long z, double v) {
        //                return new ImmutableLongDoublePoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongDoublePoint) {
        //                    ImmutableLongDoublePoint other = (ImmutableLongDoublePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongObjPoint<T> {
        //            public final long x;
        //            public final long y;
        //            public final long z;
        //            public final T v; // value
        //
        //            public ImmutableLongObjPoint(long x, long y, long z, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableLongObjPoint<T> of(long x, long y, long z, T v) {
        //                return new ImmutableLongObjPoint<>(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongObjPoint) {
        //                    ImmutableLongObjPoint<?> other = (ImmutableLongObjPoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleBytePoint {
        //            public final double x;
        //            public final double y;
        //            public final double z;
        //            public final byte v; // value
        //
        //            public ImmutableDoubleBytePoint(double x, double y, double z, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleBytePoint of(double x, double y, double z, byte v) {
        //                return new ImmutableDoubleBytePoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleBytePoint) {
        //                    ImmutableDoubleBytePoint other = (ImmutableDoubleBytePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleIntPoint {
        //            public final double x;
        //            public final double y;
        //            public final double z;
        //            public final int v; // value
        //
        //            public ImmutableDoubleIntPoint(double x, double y, double z, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleIntPoint of(double x, double y, double z, int v) {
        //                return new ImmutableDoubleIntPoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleIntPoint) {
        //                    ImmutableDoubleIntPoint other = (ImmutableDoubleIntPoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleLongPoint {
        //            public final double x;
        //            public final double y;
        //            public final double z;
        //            public final long v; // value
        //
        //            public ImmutableDoubleLongPoint(double x, double y, double z, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleLongPoint of(double x, double y, double z, long v) {
        //                return new ImmutableDoubleLongPoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleLongPoint) {
        //                    ImmutableDoubleLongPoint other = (ImmutableDoubleLongPoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleDoublePoint {
        //            public final double x;
        //            public final double y;
        //            public final double z;
        //            public final double v; // value
        //
        //            public ImmutableDoubleDoublePoint(double x, double y, double z, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleDoublePoint of(double x, double y, double z, double v) {
        //                return new ImmutableDoubleDoublePoint(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleDoublePoint) {
        //                    ImmutableDoubleDoublePoint other = (ImmutableDoubleDoublePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleObjPoint<T> {
        //            public final double x;
        //            public final double y;
        //            public final double z;
        //            public final T v; // value
        //
        //            public ImmutableDoubleObjPoint(double x, double y, double z, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableDoubleObjPoint<T> of(double x, double y, double z, T v) {
        //                return new ImmutableDoubleObjPoint<>(x, y, z, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) (((x * 31 + y) * 31 + z) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleObjPoint) {
        //                    ImmutableDoubleObjPoint<?> other = (ImmutableDoubleObjPoint<?>) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteByteTimePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final byte z;
        //            public final long t; // timestamp
        //            public final byte v; // value
        //
        //            public ImmutableByteByteTimePoint(byte x, byte y, byte z, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteByteTimePoint of(byte x, byte y, byte z, long t, byte v) {
        //                return new ImmutableByteByteTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteByteTimePoint) {
        //                    ImmutableByteByteTimePoint other = (ImmutableByteByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteIntTimePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final byte z;
        //            public final long t; // timestamp
        //            public final int v; // value
        //
        //            public ImmutableByteIntTimePoint(byte x, byte y, byte z, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteIntTimePoint of(byte x, byte y, byte z, long t, int v) {
        //                return new ImmutableByteIntTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteIntTimePoint) {
        //                    ImmutableByteIntTimePoint other = (ImmutableByteIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteLongTimePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final byte z;
        //            public final long t; // timestamp
        //            public final long v; // value
        //
        //            public ImmutableByteLongTimePoint(byte x, byte y, byte z, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteLongTimePoint of(byte x, byte y, byte z, long t, long v) {
        //                return new ImmutableByteLongTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteLongTimePoint) {
        //                    ImmutableByteLongTimePoint other = (ImmutableByteLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteDoubleTimePoint {
        //            public final byte x;
        //            public final byte y;
        //            public final byte z;
        //            public final long t; // timestamp
        //            public final double v; // value
        //
        //            public ImmutableByteDoubleTimePoint(byte x, byte y, byte z, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableByteDoubleTimePoint of(byte x, byte y, byte z, long t, double v) {
        //                return new ImmutableByteDoubleTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteDoubleTimePoint) {
        //                    ImmutableByteDoubleTimePoint other = (ImmutableByteDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableByteObjTimePoint<T> {
        //            public final byte x;
        //            public final byte y;
        //            public final byte z;
        //            public final long t; // timestamp
        //            public final T v; // value
        //
        //            public ImmutableByteObjTimePoint(byte x, byte y, byte z, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableByteObjTimePoint<T> of(byte x, byte y, byte z, long t, T v) {
        //                return new ImmutableByteObjTimePoint<>(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableByteObjTimePoint) {
        //                    ImmutableByteObjTimePoint<?> other = (ImmutableByteObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntByteTimePoint {
        //            public final int x;
        //            public final int y;
        //            public final int z;
        //            public final long t; // timestamp
        //            public final byte v; // value
        //
        //            public ImmutableIntByteTimePoint(int x, int y, int z, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntByteTimePoint of(int x, int y, int z, long t, byte v) {
        //                return new ImmutableIntByteTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntByteTimePoint) {
        //                    ImmutableIntByteTimePoint other = (ImmutableIntByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntIntTimePoint {
        //            public final int x;
        //            public final int y;
        //            public final int z;
        //            public final long t; // timestamp
        //            public final int v; // value
        //
        //            public ImmutableIntIntTimePoint(int x, int y, int z, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntIntTimePoint of(int x, int y, int z, long t, int v) {
        //                return new ImmutableIntIntTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntIntTimePoint) {
        //                    ImmutableIntIntTimePoint other = (ImmutableIntIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntLongTimePoint {
        //            public final int x;
        //            public final int y;
        //            public final int z;
        //            public final long t; // timestamp
        //            public final long v; // value
        //
        //            public ImmutableIntLongTimePoint(int x, int y, int z, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntLongTimePoint of(int x, int y, int z, long t, long v) {
        //                return new ImmutableIntLongTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntLongTimePoint) {
        //                    ImmutableIntLongTimePoint other = (ImmutableIntLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntDoubleTimePoint {
        //            public final int x;
        //            public final int y;
        //            public final int z;
        //            public final long t; // timestamp
        //            public final double v; // value
        //
        //            public ImmutableIntDoubleTimePoint(int x, int y, int z, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableIntDoubleTimePoint of(int x, int y, int z, long t, double v) {
        //                return new ImmutableIntDoubleTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntDoubleTimePoint) {
        //                    ImmutableIntDoubleTimePoint other = (ImmutableIntDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableIntObjTimePoint<T> {
        //            public final int x;
        //            public final int y;
        //            public final int z;
        //            public final long t; // timestamp
        //            public final T v; // value
        //
        //            public ImmutableIntObjTimePoint(int x, int y, int z, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableIntObjTimePoint<T> of(int x, int y, int z, long t, T v) {
        //                return new ImmutableIntObjTimePoint<>(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableIntObjTimePoint) {
        //                    ImmutableIntObjTimePoint<?> other = (ImmutableIntObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongByteTimePoint {
        //            public final long x;
        //            public final long y;
        //            public final long z;
        //            public final long t; // timestamp
        //            public final byte v; // value
        //
        //            public ImmutableLongByteTimePoint(long x, long y, long z, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongByteTimePoint of(long x, long y, long z, long t, byte v) {
        //                return new ImmutableLongByteTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongByteTimePoint) {
        //                    ImmutableLongByteTimePoint other = (ImmutableLongByteTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongIntTimePoint {
        //            public final long x;
        //            public final long y;
        //            public final long z;
        //            public final long t; // timestamp
        //            public final int v; // value
        //
        //            public ImmutableLongIntTimePoint(long x, long y, long z, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongIntTimePoint of(long x, long y, long z, long t, int v) {
        //                return new ImmutableLongIntTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongIntTimePoint) {
        //                    ImmutableLongIntTimePoint other = (ImmutableLongIntTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongLongTimePoint {
        //            public final long x;
        //            public final long y;
        //            public final long z;
        //            public final long t; // timestamp
        //            public final long v; // value
        //
        //            public ImmutableLongLongTimePoint(long x, long y, long z, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongLongTimePoint of(long x, long y, long z, long t, long v) {
        //                return new ImmutableLongLongTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongLongTimePoint) {
        //                    ImmutableLongLongTimePoint other = (ImmutableLongLongTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongDoubleTimePoint {
        //            public final long x;
        //            public final long y;
        //            public final long z;
        //            public final long t; // timestamp
        //            public final double v; // value
        //
        //            public ImmutableLongDoubleTimePoint(long x, long y, long z, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableLongDoubleTimePoint of(long x, long y, long z, long t, double v) {
        //                return new ImmutableLongDoubleTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongDoubleTimePoint) {
        //                    ImmutableLongDoubleTimePoint other = (ImmutableLongDoubleTimePoint) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableLongObjTimePoint<T> {
        //            public final long x;
        //            public final long y;
        //            public final long z;
        //            public final long t; // timestamp
        //            public final T v; // value
        //
        //            public ImmutableLongObjTimePoint(long x, long y, long z, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableLongObjTimePoint<T> of(long x, long y, long z, long t, T v) {
        //                return new ImmutableLongObjTimePoint<>(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableLongObjTimePoint) {
        //                    ImmutableLongObjTimePoint<?> other = (ImmutableLongObjTimePoint<?>) obj;
        //                    return this.x == other.x && this.y == other.y && this.z == other.z && this.t == other.t && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + N.toString(v) + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleByteTimePoint {
        //            public final double x;
        //            public final double y;
        //            public final double z;
        //            public final long t; // timestamp
        //            public final byte v; // value
        //
        //            public ImmutableDoubleByteTimePoint(double x, double y, double z, long t, byte v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleByteTimePoint of(double x, double y, double z, long t, byte v) {
        //                return new ImmutableDoubleByteTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleByteTimePoint) {
        //                    ImmutableDoubleByteTimePoint other = (ImmutableDoubleByteTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleIntTimePoint {
        //            public final double x;
        //            public final double y;
        //            public final double z;
        //            public final long t; // timestamp
        //            public final int v; // value
        //
        //            public ImmutableDoubleIntTimePoint(double x, double y, double z, long t, int v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleIntTimePoint of(double x, double y, double z, long t, int v) {
        //                return new ImmutableDoubleIntTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleIntTimePoint) {
        //                    ImmutableDoubleIntTimePoint other = (ImmutableDoubleIntTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleLongTimePoint {
        //            public final double x;
        //            public final double y;
        //            public final double z;
        //            public final long t; // timestamp
        //            public final long v; // value
        //
        //            public ImmutableDoubleLongTimePoint(double x, double y, double z, long t, long v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleLongTimePoint of(double x, double y, double z, long t, long v) {
        //                return new ImmutableDoubleLongTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleLongTimePoint) {
        //                    ImmutableDoubleLongTimePoint other = (ImmutableDoubleLongTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.t == other.t && this.v == other.v;
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleDoubleTimePoint {
        //            public final double x;
        //            public final double y;
        //            public final double z;
        //            public final long t; // timestamp
        //            public final double v; // value
        //
        //            public ImmutableDoubleDoubleTimePoint(double x, double y, double z, long t, double v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static ImmutableDoubleDoubleTimePoint of(double x, double y, double z, long t, double v) {
        //                return new ImmutableDoubleDoubleTimePoint(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + v);
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleDoubleTimePoint) {
        //                    ImmutableDoubleDoubleTimePoint other = (ImmutableDoubleDoubleTimePoint) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.t == other.t
        //                            && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + v + "]";
        //            }
        //        }
        //
        //        public static final class ImmutableDoubleObjTimePoint<T> {
        //            public final double x;
        //            public final double y;
        //            public final double z;
        //            public final long t; // timestamp
        //            public final T v; // value
        //
        //            public ImmutableDoubleObjTimePoint(double x, double y, double z, long t, T v) {
        //                this.x = x;
        //                this.y = y;
        //                this.z = z;
        //                this.t = t;
        //                this.v = v;
        //            }
        //
        //            public final static <T> ImmutableDoubleObjTimePoint<T> of(double x, double y, double z, long t, T v) {
        //                return new ImmutableDoubleObjTimePoint<>(x, y, z, t, v);
        //            }
        //
        //            @Override
        //            public final int hashCode() {
        //                return (int) ((((x * 31 + y) * 31 + z) * 31 + t) * 31 + N.hashCode(v));
        //            }
        //
        //            @Override
        //            public final boolean equals(Object obj) {
        //                if (obj == this) {
        //                    return true;
        //                }
        //
        //                if (obj instanceof ImmutableDoubleObjTimePoint) {
        //                    ImmutableDoubleObjTimePoint<?> other = (ImmutableDoubleObjTimePoint<?>) obj;
        //                    return N.equals(this.x, other.x) && N.equals(this.y, other.y) && N.equals(this.z, other.z) && this.t == other.t
        //                            && N.equals(this.v, other.v);
        //                }
        //
        //                return false;
        //            }
        //
        //            @Override
        //            public final String toString() {
        //                return "[" + x + ", " + y + ", " + z + ", " + t + ", " + N.toString(v) + "]";
        //            }
        //        }
    }
}
