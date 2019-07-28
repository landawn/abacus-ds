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
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Cursor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;

// TODO: Auto-generated Javadoc
/**
 * It's a wrapper of <code>Scan</code> in HBase to reduce the manual conversion between bytes and String/Object.
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.Scan
 * @since 0.8
 */
public final class AnyScan extends AnyQuery<AnyScan> {

    /** The scan. */
    private final Scan scan;

    /**
     * Instantiates a new any scan.
     */
    public AnyScan() {
        super(new Scan());
        this.scan = (Scan) query;
    }

    /**
     * Create a Scan operation starting at the specified row.
     * <p>
     * If the specified row does not exist, the Scanner will start from the next closest row after the
     * specified row.
     * @param startRow row to start scanner at or after
     * @deprecated use {@code new Scan().withStartRow(startRow)} instead.
     */
    @Deprecated
    public AnyScan(final Object startRow) {
        super(new Scan(toRowKeyBytes(startRow)));
        this.scan = (Scan) query;
    }

    /**
     * Create a Scan operation for the range of rows specified.
     * @param startRow row to start scanner at or after (inclusive)
     * @param stopRow row to stop scanner before (exclusive)
     * @deprecated use {@code new Scan().withStartRow(startRow).withStopRow(stopRow)} instead.
     */
    @Deprecated
    public AnyScan(final Object startRow, final Object stopRow) {
        super(new Scan(toRowKeyBytes(startRow), toRowKeyBytes(stopRow)));
        this.scan = (Scan) query;
    }

    /**
     * Instantiates a new any scan.
     *
     * @param startRow the start row
     * @param filter the filter
     * @deprecated use {@code new Scan().withStartRow(startRow).setFilter(filter)} instead.
     */
    @Deprecated
    public AnyScan(final Object startRow, final Filter filter) {
        super(new Scan(toRowKeyBytes(startRow), filter));
        this.scan = (Scan) query;
    }

    /**
     * Instantiates a new any scan.
     *
     * @param scan the scan
     */
    public AnyScan(final Scan scan) {
        super(scan);
        this.scan = (Scan) query;
    }

    /**
     * Instantiates a new any scan.
     *
     * @param get the get
     */
    public AnyScan(final Get get) {
        super(get);
        this.scan = (Scan) query;
    }

    /**
     * Creates the.
     *
     * @return the any scan
     */
    public static AnyScan create() {
        return new AnyScan();
    }

    /**
     * Creates the scan from cursor.
     *
     * @param cursor the cursor
     * @return the any scan
     */
    public static AnyScan createScanFromCursor(Cursor cursor) {
        return new AnyScan(Scan.createScanFromCursor(cursor));
    }

    /**
     * Create a Scan operation starting at the specified row.
     * <p>
     * If the specified row does not exist, the Scanner will start from the next closest row after the
     * specified row.
     *
     * @param startRow row to start scanner at or after
     * @return the any scan
     * @deprecated use {@code new Scan().withStartRow(startRow)} instead.
     */
    @Deprecated
    public static AnyScan of(final Object startRow) {
        return new AnyScan(startRow);
    }

    /**
     * Create a Scan operation for the range of rows specified.
     *
     * @param startRow row to start scanner at or after (inclusive)
     * @param stopRow row to stop scanner before (exclusive)
     * @return the any scan
     * @deprecated use {@code new Scan().withStartRow(startRow).withStopRow(stopRow)} instead.
     */
    @Deprecated
    public static AnyScan of(final Object startRow, final Object stopRow) {
        return new AnyScan(startRow, stopRow);
    }

    /**
     * Of.
     *
     * @param startRow the start row
     * @param filter the filter
     * @return the any scan
     * @deprecated use {@code new Scan().withStartRow(startRow).setFilter(filter)} instead.
     */
    @Deprecated
    public static AnyScan of(final Object startRow, final Filter filter) {
        return new AnyScan(startRow, filter);
    }

    /**
     * Of.
     *
     * @param scan the scan
     * @return the any scan
     */
    public static AnyScan of(final Scan scan) {
        return new AnyScan(scan);
    }

    /**
     * Of.
     *
     * @param get the get
     * @return the any scan
     */
    public static AnyScan of(final Get get) {
        return new AnyScan(get);
    }

    /**
     * Val.
     *
     * @return the scan
     */
    public Scan val() {
        return scan;
    }

    /**
     * Checks if is gets the scan.
     *
     * @return true, if is gets the scan
     */
    public boolean isGetScan() {
        return scan.isGetScan();
    }

    /**
     * Checks for families.
     *
     * @return true, if successful
     */
    public boolean hasFamilies() {
        return scan.hasFamilies();
    }

    /**
     * Num families.
     *
     * @return the int
     */
    public int numFamilies() {
        return scan.numFamilies();
    }

    /**
     * Gets the families.
     *
     * @return the families
     */
    public byte[][] getFamilies() {
        return scan.getFamilies();
    }

    /**
     * Adds the family.
     *
     * @param family the family
     * @return the any scan
     */
    public AnyScan addFamily(String family) {
        scan.addFamily(toFamilyQualifierBytes(family));

        return this;
    }

    /**
     * Adds the family.
     *
     * @param family the family
     * @return the any scan
     */
    public AnyScan addFamily(byte[] family) {
        scan.addFamily(family);

        return this;
    }

    /**
     * Gets the family map.
     *
     * @return the family map
     */
    public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
        return scan.getFamilyMap();
    }

    /**
     * Sets the family map.
     *
     * @param familyMap the family map
     * @return the any scan
     */
    public AnyScan setFamilyMap(Map<byte[], NavigableSet<byte[]>> familyMap) {
        scan.setFamilyMap(familyMap);

        return this;
    }

    /**
     * Sets the column family time range.
     *
     * @param family the family
     * @param minTimestamp the min timestamp
     * @param maxTimestamp the max timestamp
     * @return the any scan
     */
    @Override
    public AnyScan setColumnFamilyTimeRange(String family, long minTimestamp, long maxTimestamp) {
        scan.setColumnFamilyTimeRange(toFamilyQualifierBytes(family), minTimestamp, maxTimestamp);

        return this;
    }

    /**
     * Sets the column family time range.
     *
     * @param family the family
     * @param minTimestamp the min timestamp
     * @param maxTimestamp the max timestamp
     * @return the any scan
     */
    @Override
    public AnyScan setColumnFamilyTimeRange(byte[] family, long minTimestamp, long maxTimestamp) {
        scan.setColumnFamilyTimeRange(family, minTimestamp, maxTimestamp);

        return this;
    }

    /**
     * Adds the column.
     *
     * @param family the family
     * @param qualifier the qualifier
     * @return the any scan
     */
    public AnyScan addColumn(String family, String qualifier) {
        scan.addColumn(toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier));

        return this;
    }

    /**
     * Adds the column.
     *
     * @param family the family
     * @param qualifier the qualifier
     * @return the any scan
     */
    public AnyScan addColumn(byte[] family, byte[] qualifier) {
        scan.addColumn(family, qualifier);

        return this;
    }

    /**
     * Gets the time range.
     *
     * @return the time range
     */
    public TimeRange getTimeRange() {
        return scan.getTimeRange();
    }

    /**
     * Sets the time range.
     *
     * @param minStamp the min stamp
     * @param maxStamp the max stamp
     * @return the any scan
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public AnyScan setTimeRange(long minStamp, long maxStamp) throws IOException {
        scan.setTimeRange(minStamp, maxStamp);

        return this;
    }

    /**
     * Sets the timestamp.
     *
     * @param timestamp the timestamp
     * @return the any scan
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public AnyScan setTimestamp(long timestamp) throws IOException {
        scan.setTimestamp(timestamp);

        return this;
    }

    /**
     * Get versions of columns with the specified timestamp. Note, default maximum
     * versions to return is 1.  If your time range spans more than one version
     * and you want all versions returned, up the number of versions beyond the
     * defaut.
     *
     * @param timestamp version timestamp
     * @return this
     * @throws IOException Signals that an I/O exception has occurred.
     * @see Scan#setMaxVersions()
     * @see Scan#setMaxVersions(int)
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     *             Use {@code setTimestamp(long)} instead
     */
    @Deprecated
    public AnyScan setTimeStamp(long timestamp) throws IOException {
        scan.setTimeStamp(timestamp);

        return this;
    }

    /**
     * Include start row.
     *
     * @return true, if successful
     */
    public boolean includeStartRow() {
        return scan.includeStartRow();
    }

    /**
     * Gets the start row.
     *
     * @return the start row
     */
    public byte[] getStartRow() {
        return scan.getStartRow();
    }

    /**
     * Set the start row of the scan.
     * <p>
     * If the specified row does not exist, the Scanner will start from the next closest row after the
     * specified row.
     * @param startRow row to start scanner at or after
     * @return this
     * @throws IllegalArgumentException if startRow does not meet criteria for a row key (when length
     *           exceeds {@link HConstants#MAX_ROW_LENGTH})
     * @deprecated use {@code withStartRow(byte[])} instead. This method may change the inclusive of
     *             the stop row to keep compatible with the old behavior.
     */
    @Deprecated
    public AnyScan setStartRow(final Object startRow) {
        scan.setStartRow(toRowKeyBytes(startRow));

        return this;
    }

    /**
     * With start row.
     *
     * @param startRow the start row
     * @return the any scan
     */
    public AnyScan withStartRow(final Object startRow) {
        scan.withStartRow(toRowKeyBytes(startRow));

        return this;
    }

    /**
     * With start row.
     *
     * @param startRow the start row
     * @param inclusive the inclusive
     * @return the any scan
     */
    public AnyScan withStartRow(final Object startRow, final boolean inclusive) {
        scan.withStartRow(toRowKeyBytes(startRow), inclusive);

        return this;
    }

    /**
     * Include stop row.
     *
     * @return true, if successful
     */
    public boolean includeStopRow() {
        return scan.includeStopRow();
    }

    /**
     * Gets the stop row.
     *
     * @return the stop row
     */
    public byte[] getStopRow() {
        return scan.getStopRow();
    }

    /**
     * Set the stop row of the scan.
     * <p>
     * The scan will include rows that are lexicographically less than the provided stopRow.
     * <p>
     * <b>Note:</b> When doing a filter for a rowKey <u>Prefix</u> use
     * {@code setRowPrefixFilter(byte[])}. The 'trailing 0' will not yield the desired result.
     * </p>
     * @param stopRow row to end at (exclusive)
     * @return this
     * @throws IllegalArgumentException if stopRow does not meet criteria for a row key (when length
     *           exceeds {@link HConstants#MAX_ROW_LENGTH})
     * @deprecated use {@code withStopRow(byte[])} instead. This method may change the inclusive of
     *             the stop row to keep compatible with the old behavior.
     */
    @Deprecated
    public AnyScan setStopRow(final Object stopRow) {
        scan.setStopRow(toRowKeyBytes(stopRow));

        return this;
    }

    /**
     * With stop row.
     *
     * @param stopRow the stop row
     * @return the any scan
     */
    public AnyScan withStopRow(final Object stopRow) {
        scan.withStopRow(toRowKeyBytes(stopRow));

        return this;
    }

    /**
     * With stop row.
     *
     * @param stopRow the stop row
     * @param inclusive the inclusive
     * @return the any scan
     */
    public AnyScan withStopRow(final Object stopRow, final boolean inclusive) {
        scan.withStopRow(toRowKeyBytes(stopRow), inclusive);

        return this;
    }

    /**
     * Sets the row prefix filter.
     *
     * @param rowPrefix the row prefix
     * @return the any scan
     */
    public AnyScan setRowPrefixFilter(final Object rowPrefix) {
        scan.setRowPrefixFilter(toRowKeyBytes(rowPrefix));

        return this;
    }

    /**
     * Gets the max versions.
     *
     * @return the max versions
     */
    public int getMaxVersions() {
        return scan.getMaxVersions();
    }

    /**
     * Get all available versions.
     *
     * @param maxVersions the max versions
     * @return this
     * @throws IOException Signals that an I/O exception has occurred.
     * @deprecated It is easy to misunderstand with column family's max versions, so use
     *             {@code readAllVersions()} instead.
     */
    @Deprecated
    public AnyScan setMaxVersions(int maxVersions) throws IOException {
        scan.setMaxVersions(maxVersions);

        return this;
    }

    /**
     * Get all available versions.
     * @return this
     * @deprecated It is easy to misunderstand with column family's max versions, so use
     *             {@code readAllVersions()} instead.
     */
    @Deprecated
    public AnyScan setMaxVersions() {
        scan.setMaxVersions();

        return this;
    }

    /**
     * Read versions.
     *
     * @param maxVersions the max versions
     * @return the any scan
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public AnyScan readVersions(int maxVersions) throws IOException {
        scan.readVersions(maxVersions);

        return this;
    }

    /**
     * Read all versions.
     *
     * @return the any scan
     */
    public AnyScan readAllVersions() {
        scan.readAllVersions();

        return this;
    }

    /**
     * Gets the batch.
     *
     * @return the batch
     */
    public int getBatch() {
        return scan.getBatch();
    }

    /**
     * Sets the batch.
     *
     * @param batch the batch
     * @return the any scan
     */
    public AnyScan setBatch(int batch) {
        scan.setBatch(batch);

        return this;
    }

    /**
     * Gets the max results per column family.
     *
     * @return the max results per column family
     */
    public int getMaxResultsPerColumnFamily() {
        return scan.getMaxResultsPerColumnFamily();
    }

    /**
     * Sets the max results per column family.
     *
     * @param limit the limit
     * @return the any scan
     */
    public AnyScan setMaxResultsPerColumnFamily(int limit) {
        scan.setMaxResultsPerColumnFamily(limit);

        return this;
    }

    /**
     * Gets the row offset per column family.
     *
     * @return the row offset per column family
     */
    public int getRowOffsetPerColumnFamily() {
        return scan.getRowOffsetPerColumnFamily();
    }

    /**
     * Sets the row offset per column family.
     *
     * @param offset the offset
     * @return the any scan
     */
    public AnyScan setRowOffsetPerColumnFamily(int offset) {
        scan.setRowOffsetPerColumnFamily(offset);

        return this;
    }

    /**
     * Gets the caching.
     *
     * @return the caching
     */
    public int getCaching() {
        return scan.getCaching();
    }

    /**
     * Sets the caching.
     *
     * @param caching the caching
     * @return the any scan
     */
    public AnyScan setCaching(int caching) {
        scan.setCaching(caching);

        return this;
    }

    /**
     * Gets the cache blocks.
     *
     * @return the cache blocks
     */
    public boolean getCacheBlocks() {
        return scan.getCacheBlocks();
    }

    /**
     * Sets the cache blocks.
     *
     * @param cacheBlocks the cache blocks
     * @return the any scan
     */
    public AnyScan setCacheBlocks(boolean cacheBlocks) {
        scan.setCacheBlocks(cacheBlocks);

        return this;
    }

    /**
     * Gets the max result size.
     *
     * @return the max result size
     */
    public long getMaxResultSize() {
        return scan.getMaxResultSize();
    }

    /**
     * Sets the max result size.
     *
     * @param maxResultSize the max result size
     * @return the any scan
     */
    public AnyScan setMaxResultSize(long maxResultSize) {
        scan.setMaxResultSize(maxResultSize);

        return this;
    }

    /**
     * Gets the limit.
     *
     * @return the limit
     */
    public int getLimit() {
        return scan.getLimit();
    }

    /**
     * Sets the limit.
     *
     * @param limit the limit
     * @return the any scan
     */
    public AnyScan setLimit(int limit) {
        scan.setLimit(limit);

        return this;
    }

    /**
     * Sets the one row limit.
     *
     * @return the any scan
     */
    public AnyScan setOneRowLimit() {
        scan.setOneRowLimit();

        return this;
    }

    /**
     * Checks for filter.
     *
     * @return true, if successful
     */
    public boolean hasFilter() {
        return scan.hasFilter();
    }

    /**
     * Checks if is reversed.
     *
     * @return true, if is reversed
     */
    public boolean isReversed() {
        return scan.isReversed();
    }

    /**
     * Sets the reversed.
     *
     * @param reversed the reversed
     * @return the any scan
     */
    public AnyScan setReversed(boolean reversed) {
        scan.setReversed(reversed);

        return this;
    }

    /**
     * Gets the allow partial results.
     *
     * @return the allow partial results
     */
    public boolean getAllowPartialResults() {
        return scan.getAllowPartialResults();
    }

    /**
     * Sets the allow partial results.
     *
     * @param allowPartialResults the allow partial results
     * @return the any scan
     */
    public AnyScan setAllowPartialResults(boolean allowPartialResults) {
        scan.setAllowPartialResults(allowPartialResults);

        return this;
    }

    /**
     * Checks if is raw.
     *
     * @return true, if is raw
     */
    public boolean isRaw() {
        return scan.isRaw();
    }

    /**
     * Sets the raw.
     *
     * @param raw the raw
     * @return the any scan
     */
    public AnyScan setRaw(boolean raw) {
        scan.setRaw(raw);

        return this;
    }

    /**
     * Get whether this scan is a small scan.
     *
     * @return true if small scan
     * @deprecated since 2.0.0. See the comment of {@code setSmall(boolean)}
     */
    @Deprecated
    public boolean isSmall() {
        return scan.isSmall();
    }

    /**
     * Set whether this scan is a small scan
     * <p>
     * Small scan should use pread and big scan can use seek + read seek + read is fast but can cause
     * two problem (1) resource contention (2) cause too much network io [89-fb] Using pread for
     * non-compaction read request https://issues.apache.org/jira/browse/HBASE-7266 On the other hand,
     * if setting it true, we would do openScanner,next,closeScanner in one RPC call. It means the
     * better performance for small scan. [HBASE-9488]. Generally, if the scan range is within one
     * data block(64KB), it could be considered as a small scan.
     *
     * @param small the small
     * @return the any scan
     * @see Scan#setLimit(int)
     * @see Scan#setReadType(ReadType)
     * @deprecated since 2.0.0. Use {@code setLimit(int)} and {@code setReadType(ReadType)} instead.
     *             And for the one rpc optimization, now we will also fetch data when openScanner, and
     *             if the number of rows reaches the limit then we will close the scanner
     *             automatically which means we will fall back to one rpc.
     */
    @Deprecated
    public AnyScan setSmall(boolean small) {
        scan.setSmall(small);

        return this;
    }

    /**
     * Checks if is scan metrics enabled.
     *
     * @return true, if is scan metrics enabled
     */
    public boolean isScanMetricsEnabled() {
        return scan.isScanMetricsEnabled();
    }

    /**
     * Sets the scan metrics enabled.
     *
     * @param enabled the enabled
     * @return the any scan
     */
    public AnyScan setScanMetricsEnabled(final boolean enabled) {
        scan.setScanMetricsEnabled(enabled);

        return this;
    }

    /**
     * Gets the scan metrics.
     *
     * @return Metrics on this Scan, if metrics were enabled.
     * @see Scan#setScanMetricsEnabled(boolean)
     * @deprecated Use {@link ResultScanner#getScanMetrics()} instead. And notice that, please do not
     *             use this method and {@link ResultScanner#getScanMetrics()} together, the metrics
     *             will be messed up.
     */
    @Deprecated
    public ScanMetrics getScanMetrics() {
        return scan.getScanMetrics();
    }

    /**
     * Checks if is async prefetch.
     *
     * @return the boolean
     */
    public Boolean isAsyncPrefetch() {
        return scan.isAsyncPrefetch();
    }

    /**
     * Sets the async prefetch.
     *
     * @param asyncPrefetch the async prefetch
     * @return the any scan
     */
    public AnyScan setAsyncPrefetch(boolean asyncPrefetch) {
        scan.setAsyncPrefetch(asyncPrefetch);

        return this;
    }

    /**
     * Gets the read type.
     *
     * @return the read type for this scan
     */
    public ReadType getReadType() {
        return scan.getReadType();
    }

    /**
     * Set the read type for this scan.
     * <p>
     * Notice that we may choose to use pread even if you specific {@link ReadType#STREAM} here. For
     * example, we will always use pread if this is a get scan.
     *
     * @param readType the read type
     * @return this
     */
    public AnyScan setReadType(ReadType readType) {
        scan.setReadType(readType);

        return this;
    }

    /**
     * Checks if is need cursor result.
     *
     * @return true, if is need cursor result
     */
    public boolean isNeedCursorResult() {
        return scan.isNeedCursorResult();
    }

    /**
     * Sets the need cursor result.
     *
     * @param needCursorResult the need cursor result
     * @return the any scan
     */
    public AnyScan setNeedCursorResult(boolean needCursorResult) {
        scan.setAllowPartialResults(needCursorResult);

        return this;
    }

    /**
     * Hash code.
     *
     * @return the int
     */
    @Override
    public int hashCode() {
        return scan.hashCode();
    }

    /**
     * Equals.
     *
     * @param obj the obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof AnyScan) {
            AnyScan other = (AnyScan) obj;

            return this.scan.equals(other.scan);
        }

        return false;
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return scan.toString();
    }
}
