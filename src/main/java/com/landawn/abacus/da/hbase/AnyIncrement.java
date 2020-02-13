/*
 * Copyright (C) 2020 HaiYang Li
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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.io.TimeRange;

// TODO: Auto-generated Javadoc
/**
 * It's a wrapper of <code>Increment</code> in HBase to reduce the manual conversion between bytes and String/Object.
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.Put
 * @since 0.8
 */
public final class AnyIncrement extends AnyMutation<AnyIncrement> {

    private final Increment increment;

    AnyIncrement(final Object rowKey) {
        super(new Increment(toRowBytes(rowKey)));
        this.increment = (Increment) mutation;
    }

    AnyIncrement(byte[] rowKey) {
        super(new Increment(rowKey));
        this.increment = (Increment) mutation;
    }

    AnyIncrement(final byte[] rowKey, final int offset, final int length) {
        super(new Increment(rowKey, offset, length));
        this.increment = (Increment) mutation;
    }

    AnyIncrement(byte[] rowKey, long timestamp, NavigableMap<byte[], List<Cell>> familyMap) {
        super(new Increment(rowKey, timestamp, familyMap));
        this.increment = (Increment) mutation;
    }

    AnyIncrement(Increment incrementToCopy) {
        super(new Increment(incrementToCopy));
        this.increment = (Increment) mutation;
    }

    public static AnyIncrement of(final Object rowKey) {
        return new AnyIncrement(rowKey);
    }

    public static AnyIncrement of(byte[] rowKey) {
        return new AnyIncrement(rowKey);
    }

    public static AnyIncrement of(final byte[] rowKey, final int offset, final int length) {
        return new AnyIncrement(rowKey, offset, length);
    }

    public static AnyIncrement of(byte[] rowKey, long timestamp, NavigableMap<byte[], List<Cell>> familyMap) {
        return new AnyIncrement(rowKey);
    }

    public static AnyIncrement of(Increment incrementToCopy) {
        return new AnyIncrement(incrementToCopy);
    }

    /**
     *
     * @return
     */
    public Increment val() {
        return increment;
    }

    /**
     * Add the specified KeyValue to this operation.
     * @param cell individual Cell
     * @return this
     * @throws java.io.IOException e
     */
    public AnyIncrement add(Cell cell) throws IOException {
        increment.add(cell);

        return this;
    }

    /**
     * Increment the column from the specific family with the specified qualifier
     * by the specified amount.
     * <p>
     * Overrides previous calls to addColumn for this family and qualifier.
     * @param family family name
     * @param qualifier column qualifier
     * @param amount amount to increment by
     * @return the Increment object
     */
    public AnyIncrement addColumn(byte[] family, byte[] qualifier, long amount) {
        increment.addColumn(family, qualifier, amount);

        return this;
    }

    /**
     * 
     * @param family
     * @param qualifier
     * @param amount
     * @return
     */
    public AnyIncrement addColumn(String family, String qualifier, long amount) {
        increment.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), amount);

        return this;
    }

    /**
     * Gets the TimeRange used for this increment.
     * @return TimeRange
     */
    public TimeRange getTimeRange() {
        return increment.getTimeRange();
    }

    /**
     * Sets the TimeRange to be used on the Get for this increment.
     * <p>
     * This is useful for when you have counters that only last for specific
     * periods of time (ie. counters that are partitioned by time).  By setting
     * the range of valid times for this increment, you can potentially gain
     * some performance with a more optimal Get operation.
     * Be careful adding the time range to this class as you will update the old cell if the
     * time range doesn't include the latest cells.
     * <p>
     * This range is used as [minStamp, maxStamp).
     * @param minStamp minimum timestamp value, inclusive
     * @param maxStamp maximum timestamp value, exclusive
     * @throws IOException if invalid time range
     * @return this
     */
    public AnyIncrement setTimeRange(long minStamp, long maxStamp) throws IOException {
        increment.setTimeRange(minStamp, maxStamp);

        return this;
    }

    /**
     * @param returnResults True (default) if the increment operation should return the results. A
     *          client that is not interested in the result can save network bandwidth setting this
     *          to false.
     */
    public AnyIncrement setReturnResults(boolean returnResults) {
        increment.setReturnResults(returnResults);

        return this;
    }

    /**
     * @return current setting for returnResults
     */
    // This method makes public the superclasses's protected method. 
    public boolean isReturnResults() {

        return increment.isReturnResults();
    }

    /**
     * Method for checking if any families have been inserted into this Increment
     * @return true if familyMap is non empty false otherwise
     */
    public boolean hasFamilies() {
        return increment.hasFamilies();
    }

    /**
     * Before 0.95, when you called Increment#getFamilyMap(), you got back
     * a map of families to a list of Longs.  Now, {@link #getFamilyCellMap()} returns
     * families by list of Cells.  This method has been added so you can have the
     * old behavior.
     * @return Map of families to a Map of qualifiers and their Long increments.
     * @since 0.95.0
     */
    public Map<byte[], NavigableMap<byte[], Long>> getFamilyMapOfLongs() {
        return increment.getFamilyMapOfLongs();
    }

    public AnyIncrement setAttribute(String name, byte[] value) {
        increment.setAttribute(name, value);

        return this;
    }

    /**
     *
     * @return
     */
    @SuppressWarnings("deprecation")
    @Override
    public int hashCode() {
        return increment.hashCode();
    }

    /**
     *
     * @param obj
     * @return true, if successful
     */
    @SuppressWarnings("deprecation")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof AnyIncrement) {
            AnyIncrement other = (AnyIncrement) obj;

            return this.increment.equals(other.increment);
        }

        return false;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return increment.toString();
    }
}
