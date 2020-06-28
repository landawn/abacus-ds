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

import static com.landawn.abacus.da.hbase.HBaseExecutor.toRowBytes;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;

/**
 * It's a wrapper of <code>RowMutations</code> in HBase to reduce the manual conversion between bytes and String/Object.
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.Put
 * @since 1.9.4
 */
public final class AnyRowMutations implements Row {

    private final RowMutations rowMutations;

    AnyRowMutations(final Object rowKey) {
        this.rowMutations = new RowMutations(toRowBytes(rowKey));
    }

    AnyRowMutations(final Object rowKey, final int initialCapacity) {
        this.rowMutations = new RowMutations(toRowBytes(rowKey), initialCapacity);
    }

    AnyRowMutations(final byte[] rowKey) {
        this.rowMutations = new RowMutations(rowKey);
    }

    AnyRowMutations(final byte[] rowKey, final int initialCapacity) {
        this.rowMutations = new RowMutations(rowKey);
    }

    public static AnyRowMutations of(final Object rowKey) {
        return new AnyRowMutations(rowKey);
    }

    public static AnyRowMutations of(final Object rowKey, final int initialCapacity) {
        return new AnyRowMutations(rowKey, initialCapacity);
    }

    public static AnyRowMutations of(byte[] rowKey) {
        return new AnyRowMutations(rowKey);
    }

    public static AnyRowMutations of(final byte[] rowKey, final int initialCapacity) {
        return new AnyRowMutations(rowKey, initialCapacity);
    }

    public RowMutations val() {
        return rowMutations;
    }

    /**
     * Currently only supports {@link Put} and {@link Delete} mutations.
     *
     * @param mutation The data to send.
     * @throws IOException if the row of added mutation doesn't match the original row
     */
    public AnyRowMutations add(Mutation mutation) throws IOException {
        rowMutations.add(mutation);

        return this;
    }

    /**
     * Currently only supports {@link Put} and {@link Delete} mutations.
     *
     * @param mutations The data to send.
     * @throws IOException if the row of added mutation doesn't match the original row
     */
    public AnyRowMutations add(List<? extends Mutation> mutations) throws IOException {
        rowMutations.add(mutations);

        return this;
    }

    @Override
    public byte[] getRow() {
        return rowMutations.getRow();
    }

    @Override
    @SuppressWarnings("deprecation")
    public int compareTo(Row row) {
        return rowMutations.compareTo(row);
    }

    @Override
    @SuppressWarnings("deprecation")
    public int hashCode() {
        return rowMutations.hashCode();
    }

    /**
     *
     * @param obj
     * @return true, if successful
     */
    @Override
    @SuppressWarnings("deprecation")
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof AnyRowMutations) {
            AnyRowMutations other = (AnyRowMutations) obj;

            return this.rowMutations.equals(other.rowMutations);
        }

        return false;
    }

    @Override
    public String toString() {
        return rowMutations.toString();
    }
}
