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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;

import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.EntityInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.Tuple.Tuple3;

/**
 * It's a wrapper of <code>Put</code> in HBase to reduce the manual conversion between bytes and String/Object.
 *
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">http://hbase.apache.org/devapidocs/index.html</a>
 * @see org.apache.hadoop.hbase.client.Put
 * @since 0.8
 */
public final class AnyPut extends AnyMutation<AnyPut> {

    private final Put put;

    AnyPut(final Object rowKey) {
        super(new Put(toRowBytes(rowKey)));
        this.put = (Put) mutation;
    }

    AnyPut(final Object rowKey, final long timestamp) {
        super(new Put(toRowBytes(rowKey), timestamp));
        this.put = (Put) mutation;
    }

    AnyPut(final Object rowKey, final int rowOffset, final int rowLength) {
        super(new Put(toRowBytes(rowKey), rowOffset, rowLength));
        this.put = (Put) mutation;
    }

    AnyPut(final Object rowKey, int rowOffset, int rowLength, final long timestamp) {
        super(new Put(toRowBytes(rowKey), rowOffset, rowLength, timestamp));
        this.put = (Put) mutation;
    }

    AnyPut(final Object rowKey, final boolean rowIsImmutable) {
        super(new Put(toRowBytes(rowKey), rowIsImmutable));
        this.put = (Put) mutation;
    }

    AnyPut(final Object rowKey, final long timestamp, final boolean rowIsImmutable) {
        super(new Put(toRowBytes(rowKey), timestamp, rowIsImmutable));
        this.put = (Put) mutation;
    }

    AnyPut(final ByteBuffer rowKey) {
        super(new Put(rowKey));
        this.put = (Put) mutation;
    }

    AnyPut(final ByteBuffer rowKey, final long timestamp) {
        super(new Put(rowKey, timestamp));
        this.put = (Put) mutation;
    }

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
     * @param entity
     * @return
     */
    public static AnyPut from(final Object entity) {
        return from(entity, NamingPolicy.LOWER_CAMEL_CASE);
    }

    /**
     * 
     * @param entity
     * @param namingPolicy
     * @return
     */
    public static AnyPut from(final Object entity, final NamingPolicy namingPolicy) {
        N.checkArgNotNull(entity, "entity");

        final EntityInfo entityInfo = ParserUtil.getEntityInfo(entity.getClass());

        return from(entity, namingPolicy, entityInfo, entityInfo.propInfoList);
    }

    /**
     * 
     * @param entities
     * @return
     */
    public static List<AnyPut> from(final Collection<?> entities) {
        final List<AnyPut> anyPuts = new ArrayList<>(entities.size());

        for (Object entity : entities) {
            anyPuts.add(entity instanceof AnyPut ? (AnyPut) entity : from(entity));
        }

        return anyPuts;
    }

    /**
     * 
     * @param entities
     * @param namingPolicy
     * @return
     */
    public static List<AnyPut> from(final Collection<?> entities, final NamingPolicy namingPolicy) {
        final List<AnyPut> anyPuts = new ArrayList<>(entities.size());

        for (Object entity : entities) {
            anyPuts.add(entity instanceof AnyPut ? (AnyPut) entity : from(entity, namingPolicy));
        }

        return anyPuts;
    }

    /**
     * 
     * @param entity
     * @param selectPropNames
     * @return
     */
    public static AnyPut from(final Object entity, final Collection<String> selectPropNames) {
        return from(entity, selectPropNames, NamingPolicy.LOWER_CAMEL_CASE);
    }

    /**
     * 
     * @param entity
     * @param selectPropNames
     * @param namingPolicy
     * @return
     */
    public static AnyPut from(final Object entity, final Collection<String> selectPropNames, final NamingPolicy namingPolicy) {
        N.checkArgNotNull(entity, "entity");

        if (selectPropNames == null) {
            return from(entity, namingPolicy);
        }

        final EntityInfo entityInfo = ParserUtil.getEntityInfo(entity.getClass());

        return from(entity, namingPolicy, entityInfo, N.map(selectPropNames, propName -> N.checkArgNotNull(entityInfo.getPropInfo(propName))));
    }

    /**
     * 
     * @param entities
     * @param selectPropNames
     * @return
     */
    public static List<AnyPut> from(final Collection<?> entities, final Collection<String> selectPropNames) {
        final List<AnyPut> anyPuts = new ArrayList<>(entities.size());

        for (Object entity : entities) {
            anyPuts.add(entity instanceof AnyPut ? (AnyPut) entity : from(entity, selectPropNames));
        }

        return anyPuts;
    }

    /**
     * 
     * @param entities
     * @param selectPropNames
     * @param namingPolicy
     * @return
     */
    public static List<AnyPut> from(final Collection<?> entities, final Collection<String> selectPropNames, final NamingPolicy namingPolicy) {
        final List<AnyPut> anyPuts = new ArrayList<>(entities.size());

        for (Object entity : entities) {
            anyPuts.add(entity instanceof AnyPut ? (AnyPut) entity : from(entity, selectPropNames, namingPolicy));
        }

        return anyPuts;
    }

    private static AnyPut from(final Object entity, final NamingPolicy namingPolicy, final EntityInfo entityInfo, final Collection<PropInfo> selectPropInfos) {
        final Class<?> cls = entity.getClass();

        HBaseExecutor.checkEntityClass(cls);

        final Map<String, Tuple3<String, String, Boolean>> classFamilyColumnNameMap = HBaseExecutor.getClassFamilyColumnNameMap(cls, namingPolicy);
        final Method rowKeySetMethod = HBaseExecutor.getRowKeySetMethod(cls);
        final Method rowKeyGetMethod = rowKeySetMethod == null ? null : ClassUtil.getPropGetMethod(cls, ClassUtil.getPropNameByMethod(rowKeySetMethod));

        if (rowKeySetMethod == null) {
            throw new IllegalArgumentException(
                    "Row key property is required to create AnyPut instance. But no row key property found in class: " + ClassUtil.getCanonicalClassName(cls));
        }

        final AnyPut anyPut = new AnyPut(ClassUtil.<Object> getPropValue(entity, rowKeyGetMethod));
        final boolean annotatedByDefaultColumnFamily = entityInfo.isAnnotationPresent(ColumnFamily.class);

        PropInfo columnPropInfo = null;
        Collection<HBaseColumn<?>> columnColl = null;
        Map<Long, HBaseColumn<?>> columnMap = null;
        HBaseColumn<?> column = null;
        Object propValue = null;
        Tuple3<String, String, Boolean> tp = null;
        String columnName = null;

        for (PropInfo propInfo : selectPropInfos) {
            if (rowKeyGetMethod != null && propInfo.getMethod.equals(rowKeyGetMethod)) {
                continue;
            }

            propValue = propInfo.getPropValue(entity);

            if (propValue == null) {
                continue;
            }

            tp = classFamilyColumnNameMap.get(propInfo.name);
            columnName = tp._3 || annotatedByDefaultColumnFamily || propInfo.isAnnotationPresent(ColumnFamily.class) ? tp._2 : HBaseExecutor.EMPTY_QULIFIER;

            if (propInfo.jsonXmlType.isEntity() && tp._3 == false) {
                final Map<String, Tuple3<String, String, Boolean>> propEntityFamilyColumnNameMap = HBaseExecutor.getClassFamilyColumnNameMap(propInfo.clazz,
                        namingPolicy);
                final Class<?> propEntityClass = propInfo.jsonXmlType.clazz();
                final EntityInfo propEntityInfo = ParserUtil.getEntityInfo(propEntityClass);
                final Object propEntity = propValue;
                Tuple3<String, String, Boolean> propEntityTP = null;

                final Map<String, Method> columnGetMethodMap = ClassUtil.getPropGetMethods(propEntityClass);

                for (Map.Entry<String, Method> columnGetMethodEntry : columnGetMethodMap.entrySet()) {
                    columnPropInfo = propEntityInfo.getPropInfo(columnGetMethodEntry.getKey());

                    propValue = columnPropInfo.getPropValue(propEntity);

                    if (propValue == null) {
                        continue;
                    }

                    propEntityTP = propEntityFamilyColumnNameMap.get(columnPropInfo.name);

                    if (columnPropInfo.jsonXmlType.isMap() && columnPropInfo.jsonXmlType.getParameterTypes()[1].clazz().equals(HBaseColumn.class)) {
                        columnMap = (Map<Long, HBaseColumn<?>>) propValue;

                        for (HBaseColumn<?> e : columnMap.values()) {
                            anyPut.addColumn(tp._1, propEntityTP._2, e.version(), e.value());

                        }
                    } else if (columnPropInfo.jsonXmlType.isCollection()
                            && columnPropInfo.jsonXmlType.getParameterTypes()[0].clazz().equals(HBaseColumn.class)) {
                        columnColl = (Collection<HBaseColumn<?>>) propValue;

                        for (HBaseColumn<?> e : columnColl) {
                            anyPut.addColumn(tp._1, propEntityTP._2, e.version(), e.value());

                        }
                    } else if (columnPropInfo.jsonXmlType.clazz().equals(HBaseColumn.class)) {
                        column = (HBaseColumn<?>) propValue;
                        anyPut.addColumn(tp._1, propEntityTP._2, column.version(), column.value());
                    } else {
                        anyPut.addColumn(tp._1, propEntityTP._2, propValue);
                    }
                }
            } else if (propInfo.jsonXmlType.isMap() && propInfo.jsonXmlType.getParameterTypes()[1].clazz().equals(HBaseColumn.class)) {
                columnMap = (Map<Long, HBaseColumn<?>>) propValue;

                for (HBaseColumn<?> e : columnMap.values()) {
                    anyPut.addColumn(tp._1, columnName, e.version(), e.value());

                }
            } else if (propInfo.jsonXmlType.isCollection() && propInfo.jsonXmlType.getParameterTypes()[0].clazz().equals(HBaseColumn.class)) {
                columnColl = (Collection<HBaseColumn<?>>) propValue;

                for (HBaseColumn<?> e : columnColl) {
                    anyPut.addColumn(tp._1, columnName, e.version(), e.value());

                }
            } else if (propInfo.jsonXmlType.clazz().equals(HBaseColumn.class)) {
                column = (HBaseColumn<?>) propValue;
                anyPut.addColumn(tp._1, columnName, column.version(), column.value());
            } else {
                anyPut.addColumn(tp._1, columnName, propValue);
            }
        }

        return anyPut;
    }

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

    @Override
    public String toString() {
        return put.toString();
    }
}
