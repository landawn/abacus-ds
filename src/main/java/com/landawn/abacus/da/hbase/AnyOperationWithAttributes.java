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

import java.util.Map;

import org.apache.hadoop.hbase.client.OperationWithAttributes;

// TODO: Auto-generated Javadoc
/**
 * It's a wrapper of <code>OperationWithAttributes</code> in HBase to reduce the manual conversion between bytes and String/Object.
 *
 * @param <AP> the generic type
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.OperationWithAttributes
 * @since 1.7.13
 */
abstract class AnyOperationWithAttributes<AP extends AnyOperationWithAttributes<?>> extends AnyOperation<AP> {

    /** The ap. */
    protected final OperationWithAttributes ap;

    /**
     * Instantiates a new any operation with attributes.
     *
     * @param ap the ap
     */
    protected AnyOperationWithAttributes(final OperationWithAttributes ap) {
        super(ap);
        this.ap = ap;
    }

    /**
     * Gets the attribute.
     *
     * @param name the name
     * @return the attribute
     */
    public byte[] getAttribute(String name) {
        return ap.getAttribute(name);
    }

    /**
     * Gets the attributes map.
     *
     * @return the attributes map
     */
    public Map<String, byte[]> getAttributesMap() {
        return ap.getAttributesMap();
    }

    /**
     * Sets the attribute.
     *
     * @param name the name
     * @param value the value
     * @return the ap
     */
    public AP setAttribute(final String name, final Object value) {
        ap.setAttribute(name, HBaseExecutor.toValueBytes(value));

        return (AP) this;
    }

    /**
     * This method allows you to retrieve the identifier for the operation if one
     * was set.
     * @return the id or null if not set
     */
    public String getId() {
        return ap.getId();
    }

    /**
     * This method allows you to set an identifier on an operation. The original
     * motivation for this was to allow the identifier to be used in slow query
     * logging, but this could obviously be useful in other places. One use of
     * this could be to put a class.method identifier in here to see where the
     * slow query is coming from.
     *
     * @param id          id to set for the scan
     * @return the ap
     */
    public AP setId(final String id) {
        ap.setId(id);

        return (AP) this;
    }

    /**
     * Gets the priority.
     *
     * @return the priority
     */
    public int getPriority() {
        return ap.getPriority();
    }

    /**
     * Sets the priority.
     *
     * @param priority the priority
     * @return the ap
     */
    public AP setPriority(final int priority) {
        ap.setPriority(priority);

        return (AP) this;
    }
}
