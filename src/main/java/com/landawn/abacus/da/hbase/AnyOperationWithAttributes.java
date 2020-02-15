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

/**
 * It's a wrapper of <code>OperationWithAttributes</code> in HBase to reduce the manual conversion between bytes and String/Object.
 *
 * @param <AOWA>
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.OperationWithAttributes
 * @since 1.7.13
 */
abstract class AnyOperationWithAttributes<AOWA extends AnyOperationWithAttributes<AOWA>> extends AnyOperation<AOWA> {

    /** The ap. */
    protected final OperationWithAttributes owa;

    /**
     * Instantiates a new any operation with attributes.
     *
     * @param owa
     */
    protected AnyOperationWithAttributes(final OperationWithAttributes owa) {
        super(owa);
        this.owa = owa;
    }

    /**
     * Gets the attribute.
     *
     * @param name
     * @return
     */
    public byte[] getAttribute(String name) {
        return owa.getAttribute(name);
    }

    /**
     * Gets the attributes map.
     *
     * @return
     */
    public Map<String, byte[]> getAttributesMap() {
        return owa.getAttributesMap();
    }

    /**
     * Sets the attribute.
     *
     * @param name
     * @param value
     * @return
     */
    public AOWA setAttribute(final String name, final Object value) {
        owa.setAttribute(name, HBaseExecutor.toValueBytes(value));

        return (AOWA) this;
    }

    /**
     * This method allows you to retrieve the identifier for the operation if one
     * was set.
     * @return
     */
    public String getId() {
        return owa.getId();
    }

    /**
     * This method allows you to set an identifier on an operation. The original
     * motivation for this was to allow the identifier to be used in slow query
     * logging, but this could obviously be useful in other places. One use of
     * this could be to put a class.method identifier in here to see where the
     * slow query is coming from.
     *
     * @param id id to set for the scan
     * @return
     */
    public AOWA setId(final String id) {
        owa.setId(id);

        return (AOWA) this;
    }

    /**
     * Gets the priority.
     *
     * @return
     */
    public int getPriority() {
        return owa.getPriority();
    }

    /**
     * Sets the priority.
     *
     * @param priority
     * @return
     */
    public AOWA setPriority(final int priority) {
        owa.setPriority(priority);

        return (AOWA) this;
    }
}
