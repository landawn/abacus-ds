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
import static com.landawn.abacus.da.hbase.HBaseExecutor.toRowKeyBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.TimeRange;

// TODO: Auto-generated Javadoc
/**
 * It's a wrapper of <code>Get</code> in HBase to reduce the manual conversion between bytes and String/Object.
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.Get
 * @since 0.8
 */
public final class AnyGet extends AnyQuery<AnyGet> implements Comparable<Row> {

    /** The get. */
    private final Get get;

    /**
     * Instantiates a new any get.
     *
     * @param rowKey
     */
    public AnyGet(Object rowKey) {
        super(new Get(toRowKeyBytes(rowKey)));
        this.get = (Get) query;
    }

    /**
     * Instantiates a new any get.
     *
     * @param rowKey
     * @param rowOffset
     * @param rowLength
     */
    public AnyGet(Object rowKey, int rowOffset, int rowLength) {
        super(new Get(toRowKeyBytes(rowKey), rowOffset, rowLength));
        this.get = (Get) query;
    }

    /**
     * Instantiates a new any get.
     *
     * @param row
     */
    public AnyGet(ByteBuffer row) {
        super(new Get(row));
        this.get = (Get) query;
    }

    /**
     * Instantiates a new any get.
     *
     * @param get
     */
    public AnyGet(Get get) {
        super(get);
        this.get = (Get) query;
    }

    /**
     * Of.
     *
     * @param rowKey
     * @return
     */
    public static AnyGet of(Object rowKey) {
        return new AnyGet(rowKey);
    }

    /**
     * Of.
     *
     * @param rowKey
     * @param rowOffset
     * @param rowLength
     * @return
     */
    public static AnyGet of(Object rowKey, int rowOffset, int rowLength) {
        return new AnyGet(rowKey, rowOffset, rowLength);
    }

    /**
     * Of.
     *
     * @param row
     * @return
     */
    public static AnyGet of(ByteBuffer row) {
        return new AnyGet(row);
    }

    /**
     * Of.
     *
     * @param get
     * @return
     */
    public static AnyGet of(Get get) {
        return new AnyGet(get);
    }

    /**
     * Val.
     *
     * @return
     */
    public Get val() {
        return get;
    }

    /**
     * Adds the family.
     *
     * @param family
     * @return
     */
    public AnyGet addFamily(String family) {
        get.addFamily(toFamilyQualifierBytes(family));

        return this;
    }

    /**
     * Adds the family.
     *
     * @param family
     * @return
     */
    public AnyGet addFamily(byte[] family) {
        get.addFamily(family);

        return this;
    }

    /**
     * Adds the column.
     *
     * @param family
     * @param qualifier
     * @return
     */
    public AnyGet addColumn(String family, String qualifier) {
        get.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));

        return this;
    }

    /**
     * Adds the column.
     *
     * @param family
     * @param qualifier
     * @return
     */
    public AnyGet addColumn(byte[] family, byte[] qualifier) {
        get.addColumn(family, qualifier);

        return this;
    }

    /**
     * Gets the family map.
     *
     * @return
     */
    public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
        return get.getFamilyMap();
    }

    /**
     * Checks if is check existence only.
     *
     * @return true, if is check existence only
     */
    public boolean isCheckExistenceOnly() {
        return get.isCheckExistenceOnly();
    }

    /**
     * Sets the check existence only.
     *
     * @param checkExistenceOnly
     * @return
     */
    public AnyGet setCheckExistenceOnly(boolean checkExistenceOnly) {
        get.setCheckExistenceOnly(checkExistenceOnly);

        return this;
    }

    /**
     * This will always return the default value which is false as client cannot set the value to this
     * property any more.
     *
     * @return true, if is closest row before
     * @deprecated since 2.0.0 and will be removed in 3.0.0
     */
    @Deprecated
    public boolean isClosestRowBefore() {
        return get.isClosestRowBefore();
    }

    /**
     * This is not used any more and does nothing. Use reverse scan instead.
     *
     * @param closestRowBefore
     * @return
     * @deprecated since 2.0.0 and will be removed in 3.0.0
     */
    @Deprecated
    public AnyGet setClosestRowBefore(boolean closestRowBefore) {
        get.setClosestRowBefore(closestRowBefore);

        return this;
    }

    /**
     * Gets the time range.
     *
     * @return
     */
    public TimeRange getTimeRange() {
        return get.getTimeRange();
    }

    /**
     * Sets the time range.
     *
     * @param minStamp
     * @param maxStamp
     * @return
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public AnyGet setTimeRange(long minStamp, long maxStamp) throws IOException {
        get.setTimeRange(minStamp, maxStamp);

        return this;
    }

    /**
     * Sets the timestamp.
     *
     * @param timestamp
     * @return
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public AnyGet setTimestamp(long timestamp) throws IOException {
        get.setTimestamp(timestamp);

        return this;
    }

    /**
     * Get versions of columns with the specified timestamp.
     *
     * @param timestamp version timestamp
     * @return this for invocation chaining
     * @throws IOException Signals that an I/O exception has occurred.
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     *             Use {@code setTimestamp(long)} instead
     */
    @Deprecated
    public AnyGet setTimeStamp(long timestamp) throws IOException {
        get.setTimeStamp(timestamp);

        return this;
    }

    /**
     * Gets the max versions.
     *
     * @return
     */
    public int getMaxVersions() {
        return get.getMaxVersions();
    }

    /**
     * Get up to the specified number of versions of each column.
     *
     * @param maxVersions maximum versions for each column
     * @return this for invocation chaining
     * @throws IOException if invalid number of versions
     * @deprecated It is easy to misunderstand with column family's max versions, so use
     *             {@code readVersions(int)} instead.
     */
    @Deprecated
    public AnyGet setMaxVersions(int maxVersions) throws IOException {
        get.setMaxVersions(maxVersions);

        return this;
    }

    /**
     * Get all available versions.
     * @return this for invocation chaining
     * @deprecated It is easy to misunderstand with column family's max versions, so use
     *             {@code readAllVersions()} instead.
     */
    @Deprecated
    public AnyGet setMaxVersions() {
        get.setMaxVersions();

        return this;
    }

    /**
     * Get up to the specified number of versions of each column.
     *
     * @param versions specified number of versions for each column
     * @return this for invocation chaining
     * @throws IOException if invalid number of versions
     */
    public AnyGet readVersions(int versions) throws IOException {
        get.readVersions(versions);

        return this;
    }

    /**
     * Get all available versions.
     * @return this for invocation chaining
     */
    public AnyGet readAllVersions() {
        get.readAllVersions();

        return this;
    }

    /**
     * Gets the max results per column family.
     *
     * @return
     */
    public int getMaxResultsPerColumnFamily() {
        return get.getMaxResultsPerColumnFamily();
    }

    /**
     * Sets the max results per column family.
     *
     * @param limit
     * @return
     */
    public AnyGet setMaxResultsPerColumnFamily(int limit) {
        get.setMaxResultsPerColumnFamily(limit);

        return this;
    }

    /**
     * Gets the row offset per column family.
     *
     * @return
     */
    public int getRowOffsetPerColumnFamily() {
        return get.getRowOffsetPerColumnFamily();
    }

    /**
     * Sets the row offset per column family.
     *
     * @param offset
     * @return
     */
    public AnyGet setRowOffsetPerColumnFamily(int offset) {
        get.setRowOffsetPerColumnFamily(offset);

        return this;
    }

    /**
     * Gets the cache blocks.
     *
     * @return
     */
    public boolean getCacheBlocks() {
        return get.getCacheBlocks();
    }

    /**
     * Sets the cache blocks.
     *
     * @param cacheBlocks
     * @return
     */
    public AnyGet setCacheBlocks(boolean cacheBlocks) {
        get.setCacheBlocks(cacheBlocks);

        return this;
    }

    /**
     * To Keep it simple, there should be no methods for the properties if it's not set by this class
     * The properties not set by this should be get by the methods in <code>Get</code>.
     *
     * @return
     */
    public byte[] getRow() {
        return get.getRow();
    }

    /**
     * Checks for families.
     *
     * @return true, if successful
     */
    public boolean hasFamilies() {
        return get.hasFamilies();
    }

    /**
     * Num families.
     *
     * @return
     */
    public int numFamilies() {
        return get.numFamilies();
    }

    /**
     * Family set.
     *
     * @return
     */
    public Set<byte[]> familySet() {
        return get.familySet();
    }

    /**
     * Compare to.
     *
     * @param other
     * @return
     */
    @Override
    public int compareTo(Row other) {
        return get.compareTo(other);
    }

    /**
     * Hash code.
     *
     * @return
     */
    @Override
    public int hashCode() {
        return get.hashCode();
    }

    /**
     * Equals.
     *
     * @param obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof AnyGet) {
            AnyGet other = (AnyGet) obj;

            return this.get.equals(other.get);
        }

        return false;
    }

    /**
     * To string.
     *
     * @return
     */
    @Override
    public String toString() {
        return get.toString();
    }
}
