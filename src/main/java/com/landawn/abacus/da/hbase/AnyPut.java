/*
 * Copyright (C) 2015 HaiYang Li
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

package com.landawn.abacus.da.hbase;

import static com.landawn.abacus.da.hbase.HBaseExecutor.toFamilyQualifierBytes;
import static com.landawn.abacus.da.hbase.HBaseExecutor.toRowBytes;
import static com.landawn.abacus.da.hbase.HBaseExecutor.toValueBytes;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;

// TODO: Auto-generated Javadoc
/**
 * It's a wrapper of <code>Put</code> in HBase to reduce the manual conversion between bytes and String/Object.
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.Put
 * @since 0.8
 */
public final class AnyPut extends AnyMutation<AnyPut> {

    /** The put. */
    private final Put put;

    /**
     * Instantiates a new any put.
     *
     * @param rowKey
     */
      AnyPut(final Object rowKey) {
        super(new Put(toRowBytes(rowKey)));
        this.put = (Put) mutation;
    }

    /**
     * Instantiates a new any put.
     *
     * @param rowKey
     * @param timestamp
     */
      AnyPut(final Object rowKey, final long timestamp) {
        super(new Put(toRowBytes(rowKey), timestamp));
        this.put = (Put) mutation;
    }

    /**
     * Instantiates a new any put.
     *
     * @param rowKey
     * @param rowOffset
     * @param rowLength
     */
      AnyPut(final Object rowKey, final int rowOffset, final int rowLength) {
        super(new Put(toRowBytes(rowKey), rowOffset, rowLength));
        this.put = (Put) mutation;
    }

    /**
     * Instantiates a new any put.
     *
     * @param rowKey
     * @param rowOffset
     * @param rowLength
     * @param timestamp
     */
      AnyPut(final Object rowKey, int rowOffset, int rowLength, final long timestamp) {
        super(new Put(toRowBytes(rowKey), rowOffset, rowLength, timestamp));
        this.put = (Put) mutation;
    }

    /**
     * Instantiates a new any put.
     *
     * @param rowKey
     * @param rowIsImmutable
     */
      AnyPut(final Object rowKey, final boolean rowIsImmutable) {
        super(new Put(toRowBytes(rowKey), rowIsImmutable));
        this.put = (Put) mutation;
    }

    /**
     * Instantiates a new any put.
     *
     * @param rowKey
     * @param timestamp
     * @param rowIsImmutable
     */
      AnyPut(final Object rowKey, final long timestamp, final boolean rowIsImmutable) {
        super(new Put(toRowBytes(rowKey), timestamp, rowIsImmutable));
        this.put = (Put) mutation;
    }

    /**
     * Instantiates a new any put.
     *
     * @param rowKey
     */
      AnyPut(final ByteBuffer rowKey) {
        super(new Put(rowKey));
        this.put = (Put) mutation;
    }

    /**
     * Instantiates a new any put.
     *
     * @param rowKey
     * @param timestamp
     */
      AnyPut(final ByteBuffer rowKey, final long timestamp) {
        super(new Put(rowKey, timestamp));
        this.put = (Put) mutation;
    }

    /**
     * Instantiates a new any put.
     *
     * @param putToCopy
     */
      AnyPut(final Put putToCopy) {
        super(new Put(putToCopy));
        this.put = (Put) mutation;
    }

    /**
     *
     * @param rowKey
     * @return
     */
    public static AnyPut of(final Object rowKey) {
        return new AnyPut(rowKey);
    }

    /**
     *
     * @param rowKey
     * @param timestamp
     * @return
     */
    public static AnyPut of(final Object rowKey, final long timestamp) {
        return new AnyPut(rowKey, timestamp);
    }

    /**
     *
     * @param rowKey
     * @param rowOffset
     * @param rowLength
     * @return
     */
    public static AnyPut of(final Object rowKey, final int rowOffset, final int rowLength) {
        return new AnyPut(rowKey, rowOffset, rowLength);
    }

    /**
     *
     * @param rowKey
     * @param rowOffset
     * @param rowLength
     * @param timestamp
     * @return
     */
    public static AnyPut of(final Object rowKey, final int rowOffset, final int rowLength, final long timestamp) {
        return new AnyPut(rowKey, rowOffset, rowLength, timestamp);
    }

    /**
     *
     * @param rowKey
     * @param rowIsImmutable
     * @return
     */
    public static AnyPut of(final Object rowKey, final boolean rowIsImmutable) {
        return new AnyPut(rowKey, rowIsImmutable);
    }

    /**
     *
     * @param rowKey
     * @param timestamp
     * @param rowIsImmutable
     * @return
     */
    public static AnyPut of(final Object rowKey, final long timestamp, final boolean rowIsImmutable) {
        return new AnyPut(rowKey, timestamp, rowIsImmutable);
    }

    /**
     *
     * @param rowKey
     * @return
     */
    public static AnyPut of(final ByteBuffer rowKey) {
        return new AnyPut(rowKey);
    }

    /**
     *
     * @param rowKey
     * @param timestamp
     * @return
     */
    public static AnyPut of(final ByteBuffer rowKey, final long timestamp) {
        return new AnyPut(rowKey, timestamp);
    }

    /**
     *
     * @param putToCopy
     * @return
     */
    public static AnyPut of(final Put putToCopy) {
        return new AnyPut(putToCopy);
    }

    /**
     *
     * @return
     */
    public Put val() {
        return put;
    }

    /**
     * Adds the column.
     *
     * @param family
     * @param qualifier
     * @param value
     * @return
     */
    public AnyPut addColumn(String family, String qualifier, Object value) {
        put.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), toValueBytes(value));

        return this;
    }

    /**
     * Adds the column.
     *
     * @param family
     * @param qualifier
     * @param ts
     * @param value
     * @return
     */
    public AnyPut addColumn(String family, String qualifier, long ts, Object value) {
        put.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), ts, toValueBytes(value));

        return this;
    }

    /**
     * Adds the column.
     *
     * @param family
     * @param qualifier
     * @param value
     * @return
     */
    public AnyPut addColumn(byte[] family, byte[] qualifier, byte[] value) {
        put.addColumn(family, qualifier, value);

        return this;
    }

    /**
     * Adds the column.
     *
     * @param family
     * @param qualifier
     * @param ts
     * @param value
     * @return
     */
    public AnyPut addColumn(byte[] family, byte[] qualifier, long ts, byte[] value) {
        put.addColumn(family, qualifier, ts, value);

        return this;
    }

    /**
     * Adds the column.
     *
     * @param family
     * @param qualifier
     * @param ts
     * @param value
     * @return
     */
    public AnyPut addColumn(byte[] family, ByteBuffer qualifier, long ts, ByteBuffer value) {
        put.addColumn(family, qualifier, ts, value);

        return this;
    }

    /**
     * See {@code addColumn(byte[], byte[], byte[])}. This version expects
     * that the underlying arrays won't change. It's intended
     * for usage internal HBase to and for advanced client applications.
     *
     * @param family
     * @param qualifier
     * @param value
     * @return
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     *             Use {@code add(Cell)} and {@link org.apache.hadoop.hbase.CellBuilder} instead
     */
    @Deprecated
    public AnyPut addImmutable(String family, String qualifier, Object value) {
        put.addImmutable(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), toValueBytes(value));

        return this;
    }

    /**
     * See {@code addColumn(byte[], byte[], long, byte[])}. This version expects
     * that the underlying arrays won't change. It's intended
     * for usage internal HBase to and for advanced client applications.
     *
     * @param family
     * @param qualifier
     * @param ts
     * @param value
     * @return
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     *             Use {@code add(Cell)} and {@link org.apache.hadoop.hbase.CellBuilder} instead
     */
    @Deprecated
    public AnyPut addImmutable(String family, String qualifier, long ts, Object value) {
        put.addImmutable(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), ts, toValueBytes(value));

        return this;
    }

    /**
     * See {@code addColumn(byte[], byte[], byte[])}. This version expects
     * that the underlying arrays won't change. It's intended
     * for usage internal HBase to and for advanced client applications.
     *
     * @param family
     * @param qualifier
     * @param value
     * @return
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     *             Use {@code add(Cell)} and {@link org.apache.hadoop.hbase.CellBuilder} instead
     */
    @Deprecated
    public AnyPut addImmutable(byte[] family, byte[] qualifier, byte[] value) {
        put.addImmutable(family, qualifier, value);

        return this;
    }

    /**
     * See {@code addColumn(byte[], byte[], long, byte[])}. This version expects
     * that the underlying arrays won't change. It's intended
     * for usage internal HBase to and for advanced client applications.
     *
     * @param family
     * @param qualifier
     * @param ts
     * @param value
     * @return
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     *             Use {@code add(Cell)} and {@link org.apache.hadoop.hbase.CellBuilder} instead
     */
    @Deprecated
    public AnyPut addImmutable(byte[] family, byte[] qualifier, long ts, byte[] value) {
        put.addImmutable(family, qualifier, ts, value);

        return this;
    }

    /**
     * See {@code addColumn(byte[], byte[], long, byte[])}. This version expects
     * that the underlying arrays won't change. It's intended
     * for usage internal HBase to and for advanced client applications.
     *
     * @param family
     * @param qualifier
     * @param ts
     * @param value
     * @return
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     *             Use {@code add(Cell)} and {@link org.apache.hadoop.hbase.CellBuilder} instead
     */
    @Deprecated
    public AnyPut addImmutable(byte[] family, ByteBuffer qualifier, long ts, ByteBuffer value) {
        put.addImmutable(family, qualifier, ts, value);

        return this;
    }

    /**
     *
     * @param kv
     * @return
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public AnyPut add(Cell kv) throws IOException {
        put.add(kv);

        return this;
    }

    /**
     *
     * @return
     */
    @Override
    public int hashCode() {
        return put.hashCode();
    }

    /**
     *
     * @param obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof AnyPut) {
            AnyPut other = (AnyPut) obj;

            return this.put.equals(other.put);
        }

        return false;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return put.toString();
    }
}
