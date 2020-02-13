/*
 * Copyright (C) 200 HaiYang Li
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

import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.io.TimeRange;

// TODO: Auto-generated Javadoc
/**
 * It's a wrapper of <code>Append</code> in HBase to reduce the manual conversion between bytes and String/Object.
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.Put
 * @since 0.8
 */
public final class AnyAppend extends AnyMutation<AnyAppend> {

    private final Append append;

    AnyAppend(final Object rowKey) {
        super(new Append(toRowBytes(rowKey)));
        this.append = (Append) mutation;
    }

    AnyAppend(final byte[] rowKey) {
        super(new Append(rowKey));
        this.append = (Append) mutation;
    }

    AnyAppend(final byte[] rowKey, final int rowOffset, final int rowLength) {
        super(new Append(rowKey, rowOffset, rowLength));
        this.append = (Append) mutation;
    }

    AnyAppend(byte[] rowKey, long timestamp, NavigableMap<byte[], List<Cell>> familyMap) {
        super(new Append(rowKey, timestamp, familyMap));
        this.append = (Append) mutation;
    }

    AnyAppend(final Append appendToCopy) {
        super(new Append(appendToCopy));
        this.append = (Append) mutation;
    }

    public static AnyAppend of(final Object rowKey) {
        return new AnyAppend(rowKey);
    }

    public static AnyAppend of(byte[] rowKey) {
        return new AnyAppend(rowKey);
    }

    public static AnyAppend of(final byte[] rowKey, final int offset, final int length) {
        return new AnyAppend(rowKey, offset, length);
    }

    public static AnyAppend of(byte[] rowKey, long timestamp, NavigableMap<byte[], List<Cell>> familyMap) {
        return new AnyAppend(rowKey);
    }

    public static AnyAppend of(final Append appendToCopy) {
        return new AnyAppend(appendToCopy);
    }

    /**
     *
     * @return
     */
    public Append val() {
        return append;
    }

    /**
     * Sets the TimeRange to be used on the Get for this append.
     * <p>
     * This is useful for when you have counters that only last for specific
     * periods of time (ie. counters that are partitioned by time).  By setting
     * the range of valid times for this append, you can potentially gain
     * some performance with a more optimal Get operation.
     * Be careful adding the time range to this class as you will update the old cell if the
     * time range doesn't include the latest cells.
     * <p>
     * This range is used as [minStamp, maxStamp).
     * @param minStamp minimum timestamp value, inclusive
     * @param maxStamp maximum timestamp value, exclusive
     * @return this
     */
    public AnyAppend setTimeRange(long minStamp, long maxStamp) {
        append.setTimeRange(minStamp, maxStamp);

        return this;
    }

    /**
     * Gets the TimeRange used for this append.
     * @return TimeRange
     */
    public TimeRange getTimeRange() {
        return append.getTimeRange();
    }

    /**
     * @param returnResults
     *          True (default) if the append operation should return the results.
     *          A client that is not interested in the result can save network
     *          bandwidth setting this to false.
     */
    public AnyAppend setReturnResults(boolean returnResults) {
        append.setReturnResults(returnResults);

        return this;
    }

    /**
     * @return current setting for returnResults
     */
    // This method makes public the superclasses's protected method. 
    public boolean isReturnResults() {
        return append.isReturnResults();
    }

    /**
     * Add the specified column and value to this Append operation.
     * @param family family name
     * @param qualifier column qualifier
     * @param value value to append to specified column
     * @return this
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     *             Use {@link #addColumn(byte[], byte[], byte[])} instead
     */
    @Deprecated
    public AnyAppend add(byte[] family, byte[] qualifier, byte[] value) {
        append.add(family, qualifier, value);

        return this;
    }

    /**
     * Add the specified column and value to this Append operation.
     * @param family family name
     * @param qualifier column qualifier
     * @param value value to append to specified column
     * @return this
     */
    public AnyAppend addColumn(byte[] family, byte[] qualifier, byte[] value) {
        append.addColumn(family, qualifier, value);

        return this;
    }

    public AnyAppend addColumn(String family, String qualifier, Object value) {
        append.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), toValueBytes(value));

        return this;
    }

    /**
     * Add column and value to this Append operation.
     * @param cell
     * @return This instance
     */
    @SuppressWarnings("unchecked")
    public AnyAppend add(final Cell cell) {
        append.add(cell);

        return this;
    }

    public AnyAppend setAttribute(String name, byte[] value) {
        append.setAttribute(name, value);

        return this;
    }

    /**
     *
     * @return
     */
    @Override
    public int hashCode() {
        return append.hashCode();
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

        if (obj instanceof AnyAppend) {
            AnyAppend other = (AnyAppend) obj;

            return this.append.equals(other.append);
        }

        return false;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return append.toString();
    }
}
