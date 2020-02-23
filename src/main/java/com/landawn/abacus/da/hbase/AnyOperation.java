/*
 * Copyright (C) 2019 HaiYang Li
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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Operation;

import com.landawn.abacus.exception.UncheckedIOException;

/**
 * It's a wrapper of <code>Operation</code> in HBase to reduce the manual conversion between bytes and String/Object.
 *
 * @param <AO>
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.Operation
 * @since 1.7.13
 */
abstract class AnyOperation<AO extends AnyOperation<AO>> {

    /** The op. */
    protected final Operation op;

    /**
     * Instantiates a new any operation.
     *
     * @param op
     */
    protected AnyOperation(final Operation op) {
        this.op = op;
    }

    /**
     * Gets the fingerprint.
     *
     * @return
     */
    public Map<String, Object> getFingerprint() {
        return op.getFingerprint();
    }

    /**
     *
     * @return
     */
    public Map<String, Object> toMap() {
        return op.toMap();
    }

    /**
     *
     * @param maxCols
     * @return
     */
    public Map<String, Object> toMap(final int maxCols) {
        return op.toMap(maxCols);
    }

    /**
     *
     * @return
     */
    public String toJSON()  {
        try {
            return op.toJSON();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     *
     * @param maxCols
     * @return
     */
    public String toJSON(final int maxCols)  {
        try {
            return op.toJSON(maxCols);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return op.toString();
    }

    /**
     *
     * @param maxCols
     * @return
     */
    public String toString(final int maxCols) {
        return op.toString(maxCols);
    }
}
