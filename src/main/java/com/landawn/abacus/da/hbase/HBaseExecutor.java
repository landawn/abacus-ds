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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.landawn.abacus.DirtyMarker;
import com.landawn.abacus.core.DirtyMarkerUtil;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.EntityInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.BooleanList;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.Tuple;
import com.landawn.abacus.util.Tuple.Tuple2;
import com.landawn.abacus.util.Tuple.Tuple3;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.function.Supplier;
import com.landawn.abacus.util.stream.ObjIteratorEx;
import com.landawn.abacus.util.stream.Stream;

/**
 * It's a simple wrapper of HBase Java client.
 * 
 * <br />
 * <br />
 * 
 * By default, the field name in the class is mapped to {@code Column Family} in HBase table and {@code Column} name will be empty String {@code ""} if there is no annotation {@code Column/ColumnFamily} added to the class/field and the field type is not an entity with getter/setter methods.
 * <br />
 * For example:
 * <pre> 
    public static class Account {
        {@literal @}Id
        private String id; // columnFamily/Column in HBase will be: "id:"
        private String gui; // columnFamily/Column in HBase will be: "gui:"
        private Name name;  // columnFamily/Column in HBase will be: "name:firstName" and "name:lastName" 
        private String emailAddress; // columnFamily/Column in HBase will be: "emailAddress:"
    }

    public static class Name {
        private String firstName; // columnFamily/Column in HBase will be: "name:firstName" 
        private String lastName; // columnFamily/Column in HBase will be: "name:lastName" 
    }
 * </pre>
 * 
 * But if the class is annotated by {@literal @}ColumnFamily, the field name in the class will be mapped to {@code Column} in HBase table.
 * 
 * <pre> 
    {@literal @}ColumnFamily("columnFamily2B");
    public static class Account {
        {@literal @}Id
        private String id; // columnFamily/Column in HBase will be: "columnFamily2B:id"
        {@literal @}Column("guid") 
        private String gui; // columnFamily/Column in HBase will be: "columnFamily2B:guid"
        {@literal @}ColumnFamily("fullName")
        private Name name;  // columnFamily/Column in HBase will be: "fullName:givenName" and "fullName:lastName"
        {@literal @}ColumnFamily("email")
        private String emailAddress; // columnFamily/Column in HBase will be: "email:emailAddress"
    }
    
    public static class Name {
        {@literal @}Column("givenName")
        private String firstName;
        private String lastName;
    }
 * </pre>
 *
 * @author Haiyang Li
 * @see com.landawn.abacus.util.HBaseColumn
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">org.apache.hadoop.hbase.client.Table</a>
 * @since 0.8
 */
public final class HBaseExecutor implements Closeable {

    static final String EMPTY_QULIFIER = N.EMPTY_STRING;

    static final AsyncExecutor DEFAULT_ASYNC_EXECUTOR = new AsyncExecutor(Math.max(64, Math.min(IOUtil.CPU_CORES * 8, IOUtil.MAX_MEMORY_IN_MB / 1024) * 32),
            Math.max(256, (IOUtil.MAX_MEMORY_IN_MB / 1024) * 64), 180L, TimeUnit.SECONDS);

    private static final Map<String, byte[]> familyQualifierBytesPool = new ConcurrentHashMap<>();

    private static final Map<Class<?>, Method> classRowkeySetMethodPool = new ConcurrentHashMap<>();

    private static final Map<Class<?>, Map<NamingPolicy, Map<String, Tuple3<String, String, Boolean>>>> classFamilyColumnNamePool = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Tuple2<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>>> classFamilyColumnFieldNamePool = new ConcurrentHashMap<>();

    private final Admin admin;

    private final Connection conn;

    private final AsyncHBaseExecutor asyncHBaseExecutor;

    public HBaseExecutor(final Connection conn) {
        this(conn, DEFAULT_ASYNC_EXECUTOR);
    }

    public HBaseExecutor(final Connection conn, final AsyncExecutor asyncExecutor) {
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        this.conn = conn;

        this.asyncHBaseExecutor = new AsyncHBaseExecutor(this, asyncExecutor);
    }

    public Admin admin() {
        return admin;
    }

    public Connection connection() {
        return conn;
    }

    public AsyncHBaseExecutor async() {
        return asyncHBaseExecutor;
    }

    /**
     * The row key property will be read from/write to the specified property.
     *
     * @param cls entity classes with getter/setter methods
     * @param rowKeyPropertyName
     * @see com.landawn.abacus.annotation.Id
     * @see javax.persistence.Id
     * 
     * @deprecated please defined or annotated the key/id field by {@code @Id}
     */
    @Deprecated
    public static void registerRowKeyProperty(final Class<?> cls, final String rowKeyPropertyName) {
        if (ClassUtil.getPropGetMethod(cls, rowKeyPropertyName) == null || ClassUtil.getPropSetMethod(cls, rowKeyPropertyName) == null) {
            throw new IllegalArgumentException("The specified class: " + ClassUtil.getCanonicalClassName(cls)
                    + " doesn't have getter or setter method for the specified row key propery: " + rowKeyPropertyName);
        }

        final EntityInfo entityInfo = ParserUtil.getEntityInfo(cls);
        final PropInfo rowKeyPropInfo = entityInfo.getPropInfo(rowKeyPropertyName);
        final Method setMethod = rowKeyPropInfo.setMethod;

        if (HBaseColumn.class.equals(rowKeyPropInfo.clazz)
                || (rowKeyPropInfo.type.getParameterTypes().length == 1 && rowKeyPropInfo.type.getParameterTypes()[0].clazz().equals(HBaseColumn.class))
                || (rowKeyPropInfo.type.getParameterTypes().length == 2 && rowKeyPropInfo.type.getParameterTypes()[1].clazz().equals(HBaseColumn.class))) {
            throw new IllegalArgumentException(
                    "Unsupported row key property type: " + setMethod.toGenericString() + ". The row key property type can't be be HBaseColumn");
        }

        classRowkeySetMethodPool.put(cls, setMethod);

        classFamilyColumnNamePool.remove(cls);
        classFamilyColumnFieldNamePool.remove(cls);
    }

    /**
     * Gets the row key set method.
     *
     * @param <T>
     * @param targetClass
     * @return
     */
    @SuppressWarnings("deprecation")
    static <T> Method getRowKeySetMethod(final Class<T> targetClass) {
        Method rowKeySetMethod = classRowkeySetMethodPool.get(targetClass);

        if (rowKeySetMethod == null) {
            final List<String> ids = ClassUtil.getIdFieldNames(targetClass);

            if (ids.size() > 1) {
                throw new IllegalArgumentException("Multiple ids: " + ids + " defined/annotated in class: " + ClassUtil.getCanonicalClassName(targetClass));
            } else {
                registerRowKeyProperty(targetClass, ids.get(0));

                rowKeySetMethod = classRowkeySetMethodPool.get(targetClass);
            }

            if (rowKeySetMethod == null) {
                rowKeySetMethod = ClassUtil.METHOD_MASK;
                classRowkeySetMethodPool.put(targetClass, rowKeySetMethod);
            }
        }

        return rowKeySetMethod == ClassUtil.METHOD_MASK ? null : rowKeySetMethod;
    }

    static Map<String, Tuple3<String, String, Boolean>> getClassFamilyColumnNameMap(Class<?> entityClass, NamingPolicy namingPolicy) {
        Map<NamingPolicy, Map<String, Tuple3<String, String, Boolean>>> namingPolicyFamilyColumnNameMap = classFamilyColumnNamePool.get(entityClass);

        if (namingPolicyFamilyColumnNameMap == null) {
            namingPolicyFamilyColumnNameMap = new ConcurrentHashMap<>();
            classFamilyColumnNamePool.put(entityClass, namingPolicyFamilyColumnNameMap);
        }

        Map<String, Tuple3<String, String, Boolean>> classFamilyColumnNameMap = namingPolicyFamilyColumnNameMap.get(namingPolicy);

        if (classFamilyColumnNameMap == null) {
            classFamilyColumnNameMap = new HashMap<>();

            final EntityInfo entityInfo = ParserUtil.getEntityInfo(entityClass);
            final ColumnFamily defaultColumnFamilyAnno = entityInfo.getAnnotation(ColumnFamily.class);
            final String defaultColumnFamilyName = defaultColumnFamilyAnno == null ? null : getAnnotatedColumnFamily(defaultColumnFamilyAnno);

            for (PropInfo propInfo : entityInfo.propInfoList) {
                String columnFamilyName = null;
                String columnName = null;
                boolean hasColumnAnnotation = false;

                if (propInfo.isAnnotationPresent(ColumnFamily.class)) {
                    columnFamilyName = getAnnotatedColumnFamily(propInfo.getAnnotation(ColumnFamily.class));
                } else if (N.notNullOrEmpty(defaultColumnFamilyName)) {
                    columnFamilyName = defaultColumnFamilyName;
                } else {
                    columnFamilyName = formatName(propInfo.name, namingPolicy);
                }

                if (propInfo.columnName.isPresent()) {
                    columnName = propInfo.columnName.get();
                    hasColumnAnnotation = true;
                } else {
                    columnName = formatName(propInfo.name, namingPolicy);
                }

                classFamilyColumnNameMap.put(propInfo.name, Tuple.of(columnFamilyName, columnName, hasColumnAnnotation));
            }

            namingPolicyFamilyColumnNameMap.put(namingPolicy, classFamilyColumnNameMap);
        }

        return classFamilyColumnNameMap;
    }

    private static Tuple2<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>> getFamilyColumnFieldNameMap(final Class<?> entityClass) {
        Tuple2<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>> familyColumnFieldNameMapTP = classFamilyColumnFieldNamePool
                .get(entityClass);

        if (familyColumnFieldNameMapTP == null) {
            final EntityInfo entityInfo = ParserUtil.getEntityInfo(entityClass);
            final ColumnFamily defaultColumnFamily = entityInfo.getAnnotation(ColumnFamily.class);
            final String defaultColumnFamilyName = defaultColumnFamily == null ? null : getAnnotatedColumnFamily(defaultColumnFamily);

            familyColumnFieldNameMapTP = Tuple.of(new HashMap<>(), new HashMap<>(entityInfo.propInfoList.size()));

            final Tuple2<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>> finalFamilyColumnFieldNameMapTP = familyColumnFieldNameMapTP;

            for (PropInfo propInfo : entityInfo.propInfoList) {
                List<String> columnFamilyNames = null;
                List<String> columnNames = null;
                boolean hasColumnAnnotation = false;

                if (propInfo.isAnnotationPresent(ColumnFamily.class)) {
                    columnFamilyNames = N.asList(getAnnotatedColumnFamily(propInfo.getAnnotation(ColumnFamily.class)));
                } else if (N.notNullOrEmpty(defaultColumnFamilyName)) {
                    columnFamilyNames = N.asList(defaultColumnFamilyName);
                } else {
                    columnFamilyNames = Stream.of(NamingPolicy.values())
                            .map(it -> formatName(propInfo.name, it))
                            .filter(it -> !finalFamilyColumnFieldNameMapTP._1.containsKey(it))
                            .toList();
                }

                if (propInfo.columnName.isPresent()) {
                    columnNames = N.asList(propInfo.columnName.get());
                    hasColumnAnnotation = true;
                } else {
                    columnNames = Stream.of(NamingPolicy.values())
                            .map(it -> formatName(propInfo.name, it))
                            .filter(it -> !finalFamilyColumnFieldNameMapTP._2.containsKey(it))
                            .__(s -> propInfo.type.isEntity() || (N.isNullOrEmpty(defaultColumnFamilyName) && !propInfo.isAnnotationPresent(ColumnFamily.class))
                                    ? s.append(EMPTY_QULIFIER)
                                    : s)
                            .toList();
                }

                for (String columnFamilyName : columnFamilyNames) {
                    Map<String, Tuple2<String, Boolean>> columnFieldMap = familyColumnFieldNameMapTP._1.get(columnFamilyName);

                    if (columnFieldMap == null) {
                        columnFieldMap = new HashMap<>(columnNames.size());
                        familyColumnFieldNameMapTP._1.put(columnFamilyName, columnFieldMap);
                    }

                    for (String columnName : columnNames) {
                        columnFieldMap.put(columnName, Tuple.of(propInfo.name, hasColumnAnnotation));
                    }
                }

                for (String columnName : columnNames) {
                    familyColumnFieldNameMapTP._2.put(columnName, propInfo.name);
                }
            }

            classFamilyColumnFieldNamePool.put(entityClass, familyColumnFieldNameMapTP);
        }

        return familyColumnFieldNameMapTP;
    }

    private static String getAnnotatedColumnFamily(final ColumnFamily defaultColumnFamilyAnno) {
        return N.checkArgNotNullOrEmpty(defaultColumnFamilyAnno.value(), "Column Family can't be null or empty");
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param resultScanner
     * @return
     */
    public static <T> List<T> toList(final Class<T> targetClass, final ResultScanner resultScanner) {
        return toList(targetClass, resultScanner, 0, Integer.MAX_VALUE);
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param resultScanner
     * @param offset
     * @param count
     * @return
     */
    public static <T> List<T> toList(final Class<T> targetClass, final ResultScanner resultScanner, int offset, int count) {
        if (offset < 0 || count < 0) {
            throw new IllegalArgumentException("Offset and count can't be negative");
        }

        final Type<T> targetType = N.typeOf(targetClass);

        final EntityInfo entityInfo = targetType.isEntity() ? ParserUtil.getEntityInfo(targetClass) : null;
        final Method rowKeySetMethod = targetType.isEntity() ? getRowKeySetMethod(targetClass) : null;
        final Type<?> rowKeyType = rowKeySetMethod == null ? null : N.typeOf(rowKeySetMethod.getParameterTypes()[0]);
        final Map<String, Map<String, Tuple2<String, Boolean>>> familyFieldNameMap = targetType.isEntity() ? getFamilyColumnFieldNameMap(targetClass)._1 : null;

        final List<T> resultList = new ArrayList<>();

        try {
            while (offset-- > 0 && resultScanner.next() != null) {
            }

            Result result = null;

            while (count-- > 0 && (result = resultScanner.next()) != null) {
                resultList.add(toValue(targetType, targetClass, entityInfo, rowKeySetMethod, rowKeyType, familyFieldNameMap, result));
            }

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return resultList;
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param results
     * @return
     */
    static <T> List<T> toList(final Class<T> targetClass, final List<Result> results) {
        final Type<T> targetType = N.typeOf(targetClass);

        final EntityInfo entityInfo = targetType.isEntity() ? ParserUtil.getEntityInfo(targetClass) : null;
        final Method rowKeySetMethod = targetType.isEntity() ? getRowKeySetMethod(targetClass) : null;
        final Type<?> rowKeyType = rowKeySetMethod == null ? null : N.typeOf(rowKeySetMethod.getParameterTypes()[0]);
        final Map<String, Map<String, Tuple2<String, Boolean>>> familyFieldNameMap = targetType.isEntity() ? getFamilyColumnFieldNameMap(targetClass)._1 : null;

        final List<T> resultList = new ArrayList<>(results.size());

        try {
            for (Result result : results) {
                if (result.isEmpty()) {
                    continue;
                }

                resultList.add(toValue(targetType, targetClass, entityInfo, rowKeySetMethod, rowKeyType, familyFieldNameMap, result));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return resultList;
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param result
     * @return
     */
    public static <T> T toEntity(final Class<T> targetClass, final Result result) {
        final Type<T> targetType = N.typeOf(targetClass);

        try {
            return toValue(targetType, targetClass, result);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static <T> T toValue(final Type<T> type, final Class<T> targetClass, final Result result) throws IOException {
        if (type.isMap()) {
            throw new IllegalArgumentException("Map is not supported");
        }

        if (result.isEmpty() || result.advance() == false) {
            return type.defaultValue();
        }

        if (type.isEntity()) {
            final EntityInfo entityInfo = ParserUtil.getEntityInfo(targetClass);
            final Method rowKeySetMethod = getRowKeySetMethod(targetClass);
            final Type<?> rowKeyType = rowKeySetMethod == null ? null : N.typeOf(rowKeySetMethod.getParameterTypes()[0]);
            final Map<String, Map<String, Tuple2<String, Boolean>>> familyFieldNameMap = getFamilyColumnFieldNameMap(targetClass)._1;

            return toValue(type, targetClass, entityInfo, rowKeySetMethod, rowKeyType, familyFieldNameMap, result);
        } else {
            final CellScanner cellScanner = result.cellScanner();

            if (cellScanner.advance() == false) {
                return type.defaultValue();
            }

            final Cell cell = cellScanner.current();

            T value = type.valueOf(getValueString(cell));

            if (cellScanner.advance()) {
                throw new IllegalArgumentException("Can't covert result with columns: " + getFamilyString(cell) + ":" + getQualifierString(cell) + " to class: "
                        + ClassUtil.getCanonicalClassName(type.clazz()));
            }

            return value;
        }
    }

    @SuppressWarnings("null")
    private static <T> T toValue(final Type<T> type, final Class<T> targetClass, final EntityInfo entityInfo, final Method rowKeySetMethod,
            final Type<?> rowKeyType, final Map<String, Map<String, Tuple2<String, Boolean>>> familyFieldNameMap, final Result result) throws IOException {
        if (type.isMap()) {
            throw new IllegalArgumentException("Map is not supported");
        }

        if (result.isEmpty() || result.advance() == false) {
            return type.defaultValue();
        }

        if (type.isEntity()) {
            final T entity = N.newInstance(targetClass);
            final CellScanner cellScanner = result.cellScanner();

            Map<String, Map<String, Type<?>>> familyColumnValueTypeMap = null;
            Map<String, Map<String, Collection<HBaseColumn<?>>>> familyColumnCollectionMap = null;
            Map<String, Map<String, Map<Long, HBaseColumn<?>>>> familyColumnMapMap = null;

            Object rowKey = null;
            String family = null;
            String qualifier = null;
            String fieldName = null;
            PropInfo familyPropInfo = null;
            PropInfo columnPropInfo = null;
            Type<?> columnValueType = null;
            Map<String, Tuple2<String, Boolean>> familyTPMap = null;
            Tuple2<String, Boolean> familyTP = null;

            Map<String, Type<?>> columnValueTypeMap = null;
            Collection<HBaseColumn<?>> columnColl = null;
            Map<String, Collection<HBaseColumn<?>>> columnCollectionMap = null;
            Map<Long, HBaseColumn<?>> columnMap = null;
            Map<String, Map<Long, HBaseColumn<?>>> columnMapMap = null;
            HBaseColumn<?> column = null;

            while (cellScanner.advance()) {
                final Cell cell = cellScanner.current();

                if (rowKeyType != null && rowKey == null) {
                    rowKey = rowKeyType.valueOf(getRowKeyString(cell));
                    ClassUtil.setPropValue(entity, rowKeySetMethod, rowKey);
                }

                family = getFamilyString(cell);
                qualifier = getQualifierString(cell);

                // .....................................................................................
                columnMapMap = familyColumnMapMap == null ? null : familyColumnMapMap.get(family);

                if (N.notNullOrEmpty(columnMapMap)) {
                    columnMap = columnMapMap.get(qualifier);

                    if (N.notNullOrEmpty(columnMap)) {
                        columnValueType = familyColumnValueTypeMap.get(family).get(qualifier);
                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                        columnMapMap.put(qualifier, columnMap);

                        continue;
                    }
                }

                // .....................................................................................
                columnCollectionMap = familyColumnCollectionMap == null ? null : familyColumnCollectionMap.get(family);

                if (N.notNullOrEmpty(columnCollectionMap)) {
                    columnColl = columnCollectionMap.get(qualifier);

                    if (N.notNullOrEmpty(columnColl)) {
                        columnValueType = familyColumnValueTypeMap.get(family).get(qualifier);
                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                        columnColl.add(column);

                        continue;
                    }
                }

                // .....................................................................................
                familyTPMap = familyFieldNameMap.get(family);

                // ignore unknown column family.
                if (familyTPMap == null) {
                    continue;
                }

                familyTP = familyTPMap.get(qualifier);

                if (familyTP == null) {
                    familyTP = familyTPMap.get(EMPTY_QULIFIER);
                }

                // ignore the unknown column:
                if (familyTP == null) {
                    continue;
                }

                fieldName = familyTP._1;
                familyPropInfo = entityInfo.getPropInfo(fieldName);

                // ignore the unknown field/property:
                if (familyPropInfo == null) {
                    continue;
                }

                if (familyPropInfo.jsonXmlType.isEntity() && (familyTPMap == null || familyTP._2 == false)) {
                    final Class<?> propEntityClass = familyPropInfo.jsonXmlType.clazz();
                    final Map<String, String> propEntityColumnFieldNameMap = getFamilyColumnFieldNameMap(propEntityClass)._2;
                    final EntityInfo propEntityInfo = ParserUtil.getEntityInfo(propEntityClass);
                    Object propEntity = familyPropInfo.getPropValue(entity);

                    if (propEntity == null) {
                        propEntity = N.newInstance(propEntityClass);

                        familyPropInfo.setPropValue(entity, propEntity);
                    }

                    columnPropInfo = propEntityInfo.getPropInfo(propEntityColumnFieldNameMap.getOrDefault(qualifier, qualifier));

                    // ignore the unknown property.
                    if (columnPropInfo == null) {
                        continue;
                    }

                    if (columnPropInfo.jsonXmlType.isMap() && columnPropInfo.jsonXmlType.getParameterTypes()[1].clazz().equals(HBaseColumn.class)) {
                        columnValueType = columnPropInfo.jsonXmlType.getParameterTypes()[1].getElementType();

                        if (familyColumnValueTypeMap == null) {
                            familyColumnValueTypeMap = new HashMap<>();
                        } else {
                            columnValueTypeMap = familyColumnValueTypeMap.get(family);
                        }

                        if (columnValueTypeMap == null) {
                            columnValueTypeMap = new HashMap<>();
                            familyColumnValueTypeMap.put(family, columnValueTypeMap);
                        }

                        columnValueTypeMap.put(qualifier, columnValueType);
                        columnMap = (Map<Long, HBaseColumn<?>>) N.newInstance(columnPropInfo.jsonXmlType.clazz());
                        columnPropInfo.setPropValue(propEntity, columnMap);

                        if (columnMapMap == null) {
                            if (familyColumnMapMap == null) {
                                familyColumnMapMap = new HashMap<>();
                            }

                            columnMapMap = new HashMap<>();
                            familyColumnMapMap.put(family, columnMapMap);
                        }

                        columnMapMap.put(qualifier, columnMap);

                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                        columnMap.put(column.version(), column);
                    } else if (columnPropInfo.jsonXmlType.isCollection()
                            && columnPropInfo.jsonXmlType.getParameterTypes()[0].clazz().equals(HBaseColumn.class)) {
                        columnValueType = columnPropInfo.jsonXmlType.getParameterTypes()[0].getElementType();

                        if (familyColumnValueTypeMap == null) {
                            familyColumnValueTypeMap = new HashMap<>();
                        } else {
                            columnValueTypeMap = familyColumnValueTypeMap.get(family);
                        }

                        if (columnValueTypeMap == null) {
                            columnValueTypeMap = new HashMap<>();
                            familyColumnValueTypeMap.put(family, columnValueTypeMap);
                        }

                        columnValueTypeMap.put(qualifier, columnValueType);
                        columnColl = (Collection<HBaseColumn<?>>) N.newInstance(columnPropInfo.jsonXmlType.clazz());
                        columnPropInfo.setPropValue(propEntity, columnColl);

                        if (columnCollectionMap == null) {
                            if (familyColumnCollectionMap == null) {
                                familyColumnCollectionMap = new HashMap<>();
                            }

                            columnCollectionMap = new HashMap<>();
                            familyColumnCollectionMap.put(family, columnCollectionMap);
                        }

                        columnCollectionMap.put(qualifier, columnColl);

                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                        columnColl.add(column);
                    } else if (columnPropInfo.jsonXmlType.clazz().equals(HBaseColumn.class)) {
                        if (familyColumnValueTypeMap == null) {
                            familyColumnValueTypeMap = new HashMap<>();
                        } else {
                            columnValueTypeMap = familyColumnValueTypeMap.get(family);
                        }

                        if (columnValueTypeMap == null) {
                            columnValueTypeMap = new HashMap<>();
                            familyColumnValueTypeMap.put(family, columnValueTypeMap);
                        }

                        columnValueType = columnValueTypeMap.get(qualifier);

                        if (columnValueType == null) {
                            columnValueType = columnPropInfo.jsonXmlType.getParameterTypes()[0];
                            columnValueTypeMap.put(qualifier, columnValueType);
                        }

                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());

                        columnPropInfo.setPropValue(propEntity, column);
                    } else {
                        columnPropInfo.setPropValue(propEntity, columnPropInfo.jsonXmlType.valueOf(getValueString(cell)));
                    }

                } else if (familyPropInfo.jsonXmlType.isMap() && familyPropInfo.jsonXmlType.getParameterTypes()[1].clazz().equals(HBaseColumn.class)) {
                    columnValueType = familyPropInfo.jsonXmlType.getParameterTypes()[1].getElementType();

                    if (familyColumnValueTypeMap == null) {
                        familyColumnValueTypeMap = new HashMap<>();
                    } else {
                        columnValueTypeMap = familyColumnValueTypeMap.get(family);
                    }

                    if (columnValueTypeMap == null) {
                        columnValueTypeMap = new HashMap<>();
                        familyColumnValueTypeMap.put(family, columnValueTypeMap);
                    }

                    columnValueTypeMap.put(qualifier, columnValueType);
                    columnMap = (Map<Long, HBaseColumn<?>>) N.newInstance(familyPropInfo.jsonXmlType.clazz());
                    familyPropInfo.setPropValue(entity, columnMap);

                    if (columnMapMap == null) {
                        if (familyColumnMapMap == null) {
                            familyColumnMapMap = new HashMap<>();
                        }

                        columnMapMap = new HashMap<>();
                        familyColumnMapMap.put(family, columnMapMap);
                    }

                    columnMapMap.put(qualifier, columnMap);

                    column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                    columnMap.put(column.version(), column);
                } else if (familyPropInfo.jsonXmlType.isCollection() && familyPropInfo.jsonXmlType.getParameterTypes()[0].clazz().equals(HBaseColumn.class)) {
                    columnValueType = familyPropInfo.jsonXmlType.getParameterTypes()[0].getElementType();

                    if (familyColumnValueTypeMap == null) {
                        familyColumnValueTypeMap = new HashMap<>();
                    } else {
                        columnValueTypeMap = familyColumnValueTypeMap.get(family);
                    }

                    if (columnValueTypeMap == null) {
                        columnValueTypeMap = new HashMap<>();
                        familyColumnValueTypeMap.put(family, columnValueTypeMap);
                    }

                    columnValueTypeMap.put(qualifier, columnValueType);
                    columnColl = (Collection<HBaseColumn<?>>) N.newInstance(familyPropInfo.jsonXmlType.clazz());
                    familyPropInfo.setPropValue(entity, columnColl);

                    if (columnCollectionMap == null) {
                        if (familyColumnCollectionMap == null) {
                            familyColumnCollectionMap = new HashMap<>();
                        }

                        columnCollectionMap = new HashMap<>();
                        familyColumnCollectionMap.put(family, columnCollectionMap);
                    }

                    columnCollectionMap.put(qualifier, columnColl);

                    column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                    columnColl.add(column);
                } else if (familyPropInfo.jsonXmlType.clazz().equals(HBaseColumn.class)) {
                    if (familyColumnValueTypeMap == null) {
                        familyColumnValueTypeMap = new HashMap<>();
                    } else {
                        columnValueTypeMap = familyColumnValueTypeMap.get(family);
                    }

                    if (columnValueTypeMap == null) {
                        columnValueTypeMap = new HashMap<>();
                        familyColumnValueTypeMap.put(family, columnValueTypeMap);
                    }

                    columnValueType = columnValueTypeMap.get(qualifier);

                    if (columnValueType == null) {
                        columnValueType = familyPropInfo.jsonXmlType.getParameterTypes()[0];
                        columnValueTypeMap.put(qualifier, columnValueType);
                    }

                    column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());

                    familyPropInfo.setPropValue(entity, column);
                } else {
                    familyPropInfo.setPropValue(entity, familyPropInfo.jsonXmlType.valueOf(getValueString(cell)));
                }
            }

            if (DirtyMarkerUtil.isDirtyMarker(entity.getClass())) {
                DirtyMarkerUtil.markDirty((DirtyMarker) entity, false);
            }

            return entity;
        } else {
            final CellScanner cellScanner = result.cellScanner();

            if (cellScanner.advance() == false) {
                return type.defaultValue();
            }

            final Cell cell = cellScanner.current();

            T value = type.valueOf(getValueString(cell));

            if (cellScanner.advance()) {
                throw new IllegalArgumentException("Can't covert result with columns: " + getFamilyString(cell) + ":" + getQualifierString(cell) + " to class: "
                        + ClassUtil.getCanonicalClassName(type.clazz()));
            }

            return value;
        }
    }

    /**
     * Check entity class.
     *
     * @param <T>
     * @param targetClass
     */
    static <T> void checkEntityClass(final Class<T> targetClass) {
        if (!ClassUtil.isEntity(targetClass)) {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(targetClass)
                    + ". Only Entity class generated by CodeGenerator with getter/setter methods are supported");
        }
    }

    /**
     *
     *
     * @param entity entity with getter/setter methods
     * @return
     * @deprecated replaced by {@code AnyPut.from(Object)}
     */
    @Deprecated
    public static AnyPut toAnyPut(final Object entity) {
        return toAnyPut(entity, NamingPolicy.LOWER_CAMEL_CASE);
    }

    /**
     * To any put.
     *
     * @param entity entity with getter/setter methods
     * @param namingPolicy
     * @return
     * @deprecated replaced by {@code AnyPut.from(Object, NamingPolicy)}
     */
    @Deprecated
    public static AnyPut toAnyPut(final Object entity, final NamingPolicy namingPolicy) {
        return AnyPut.from(entity, namingPolicy);
    }

    /**
     *
     * @param name
     * @param namingPolicy
     * @return
     */
    private static String formatName(String name, final NamingPolicy namingPolicy) {
        return namingPolicy == NamingPolicy.LOWER_CAMEL_CASE ? name : namingPolicy.convert(name);
    }

    /**
     * To any put.
     *
     * @param entities <code>AnyPut</code> or entity with getter/setter methods
     * @return
     * @deprecated replaced by {@code AnyPut.from(Collection)}
     */
    @Deprecated
    public static List<AnyPut> toAnyPut(final Collection<?> entities) {
        return AnyPut.from(entities);
    }

    /**
     * To any put.
     *
     * @param entities <code>AnyPut</code> or entity with getter/setter methods
     * @param namingPolicy
     * @return
     * @deprecated replaced by {@code AnyPut.from(Collection, NamingPolicy)}
     */
    @Deprecated
    public static List<AnyPut> toAnyPut(final Collection<?> entities, final NamingPolicy namingPolicy) {
        return AnyPut.from(entities, namingPolicy);
    }

    /**
     *
     * @param entities <code>AnyPut</code> or entity with getter/setter methods
     * @return
     * @deprecated replaced by {@code AnyPut.from(Collection)}
     */
    @Deprecated
    public static List<Put> toPut(final Collection<?> entities) {
        final List<Put> puts = new ArrayList<>(entities.size());

        for (Object entity : entities) {
            puts.add(entity instanceof AnyPut ? ((AnyPut) entity).val() : toAnyPut(entity).val());
        }

        return puts;
    }

    /**
     *
     * @param entities <code>AnyPut</code> or entity with getter/setter methods
     * @param namingPolicy
     * @return
     * @deprecated replaced by {@code AnyPut.from(Collection, NamingPolicy)}
     */
    @Deprecated
    public static List<Put> toPut(final Collection<?> entities, final NamingPolicy namingPolicy) {
        final List<Put> puts = new ArrayList<>(entities.size());

        for (Object entity : entities) {
            puts.add(entity instanceof AnyPut ? ((AnyPut) entity).val() : toAnyPut(entity, namingPolicy).val());
        }

        return puts;
    }

    /**
     *
     * @param anyGets
     * @return
     */
    public static List<Get> toGet(final Collection<AnyGet> anyGets) {
        final List<Get> gets = new ArrayList<>(anyGets.size());

        for (AnyGet anyGet : anyGets) {
            gets.add(anyGet.val());
        }

        return gets;
    }

    /**
     *
     * @param anyDeletes
     * @return
     */
    public static List<Delete> toDelete(final Collection<AnyDelete> anyDeletes) {
        final List<Delete> deletes = new ArrayList<>(anyDeletes.size());

        for (AnyDelete anyDelete : anyDeletes) {
            deletes.add(anyDelete.val());
        }

        return deletes;
    }

    /**
     * Gets the table.
     *
     * @param tableName
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Table getTable(final String tableName) throws UncheckedIOException {
        try {
            return conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    private final Map<Class<?>, HBaseMapper> mapperPool = new ConcurrentHashMap<>();

    /**
     * 
     * @param <T>
     * @param <K>
     * @param targetEntityClass
     * @return
     */
    public <T, K> HBaseMapper<T, K> mapper(final Class<T> targetEntityClass) {
        @SuppressWarnings("rawtypes")
        HBaseMapper mapper = mapperPool.get(targetEntityClass);

        if (mapper == null) {
            final EntityInfo entityInfo = ParserUtil.getEntityInfo(targetEntityClass);

            if (entityInfo.tableName.isEmpty()) {
                throw new IllegalArgumentException("The target entity class: " + targetEntityClass
                        + " must be annotated with com.landawn.abacus.annotation.Table or javax.persistence.Table. Otherwise call  HBaseExecutor.mapper(final String tableName, final Class<T> targetEntityClass) instead");
            }

            mapper = mapper(targetEntityClass, entityInfo.tableName.get(), NamingPolicy.LOWER_CAMEL_CASE);

            mapperPool.put(targetEntityClass, mapper);
        }

        return mapper;
    }

    /**
     * 
     * @param <T>
     * @param <K>
     * @param targetEntityClass
     * @param tableName
     * @param namingPolicy
     * @return
     */
    public <T, K> HBaseMapper<T, K> mapper(final Class<T> targetEntityClass, final String tableName, final NamingPolicy namingPolicy) {
        return new HBaseMapper<>(targetEntityClass, this, tableName, namingPolicy);
    }

    /**
     *
     * @param table
     */
    private static void closeQuietly(final Table table) {
        IOUtil.closeQuietly(table);
    }

    // There is no too much benefit to add method for "Object rowKey"
    /**
     *
     * @param tableName
     * @param rowKey
     * @return true, if successful
     * @throws UncheckedIOException the unchecked IO exception
     */
    // And it may cause error because the "Object" is ambiguous to any type.
    boolean exists(final String tableName, final Object rowKey) throws UncheckedIOException {
        return exists(tableName, AnyGet.of(rowKey));
    }

    /**
     *
     * @param tableName
     * @param get
     * @return true, if successful
     * @throws UncheckedIOException the unchecked IO exception
     */
    public boolean exists(final String tableName, final Get get) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.exists(get);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param tableName
     * @param gets
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public List<Boolean> exists(final String tableName, final List<Get> gets) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return BooleanList.of(table.exists(gets)).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Test for the existence of columns in the table, as specified by the Gets.
     * This will return an array of booleans. Each value will be true if the related Get matches
     * one or more keys, false if not.
     * This is a server-side call so it prevents any data from being transferred to
     * the client.
     *
     * @param tableName
     * @param gets
     * @return Array of boolean.  True if the specified Get matches one or more keys, false if not.
     * @throws UncheckedIOException the unchecked IO exception
     * @deprecated since 2.0 version and will be removed in 3.0 version.
     *             use {@code exists(List)}
     */
    @Deprecated
    public List<Boolean> existsAll(final String tableName, final List<Get> gets) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return BooleanList.of(table.existsAll(gets)).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param tableName
     * @param anyGet
     * @return true, if successful
     * @throws UncheckedIOException the unchecked IO exception
     */
    public boolean exists(final String tableName, final AnyGet anyGet) throws UncheckedIOException {
        return exists(tableName, anyGet.val());
    }

    /**
     *
     * @param tableName
     * @param anyGets
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public List<Boolean> exists(final String tableName, final Collection<AnyGet> anyGets) throws UncheckedIOException {
        return existsAll(tableName, toGet(anyGets));
    }

    /**
     *
     * @param tableName
     * @param anyGets
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     * @deprecated  use {@code exists(String, Collection)}
     */
    @Deprecated
    public List<Boolean> existsAll(final String tableName, final Collection<AnyGet> anyGets) throws UncheckedIOException {
        return existsAll(tableName, toGet(anyGets));
    }

    // There is no too much benefit to add method for "Object rowKey"
    /**
     *
     * @param tableName
     * @param rowKey
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    // And it may cause error because the "Object" is ambiguous to any type.
    Result get(final String tableName, final Object rowKey) throws UncheckedIOException {
        return get(tableName, AnyGet.of(rowKey));
    }

    /**
     *
     * @param tableName
     * @param get
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Result get(final String tableName, final Get get) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.get(get);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param tableName
     * @param gets
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public List<Result> get(final String tableName, final List<Get> gets) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return N.asList(table.get(gets));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param tableName
     * @param anyGet
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Result get(final String tableName, final AnyGet anyGet) throws UncheckedIOException {
        return get(tableName, anyGet.val());
    }

    /**
     *
     * @param tableName
     * @param anyGets
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public List<Result> get(final String tableName, final Collection<AnyGet> anyGets) throws UncheckedIOException {
        return get(tableName, toGet(anyGets));
    }

    // There is no too much benefit to add method for "Object rowKey"
    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param rowKey
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    // And it may cause error because the "Object" is ambiguous to any type.
    <T> T get(final Class<T> targetClass, final String tableName, final Object rowKey) throws UncheckedIOException {
        return get(targetClass, tableName, AnyGet.of(rowKey));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param get
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public <T> T get(final Class<T> targetClass, final String tableName, final Get get) throws UncheckedIOException {
        return toEntity(targetClass, get(tableName, get));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param gets
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public <T> List<T> get(final Class<T> targetClass, final String tableName, final List<Get> gets) throws UncheckedIOException {
        return toList(targetClass, get(tableName, gets));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param anyGet
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public <T> T get(final Class<T> targetClass, final String tableName, final AnyGet anyGet) throws UncheckedIOException {
        return toEntity(targetClass, get(tableName, anyGet));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param anyGets
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public <T> List<T> get(final Class<T> targetClass, final String tableName, final Collection<AnyGet> anyGets) throws UncheckedIOException {
        return toList(targetClass, get(tableName, anyGets));
    }

    /**
     *
     * @param tableName
     * @param family
     * @return
     */
    public Stream<Result> scan(final String tableName, final String family) {
        return scan(tableName, AnyScan.create().addFamily(family));
    }

    /**
     *
     * @param tableName
     * @param family
     * @param qualifier
     * @return
     */
    public Stream<Result> scan(final String tableName, final String family, final String qualifier) {
        return scan(tableName, AnyScan.create().addColumn(family, qualifier));
    }

    /**
     *
     * @param tableName
     * @param family
     * @return
     */
    public Stream<Result> scan(final String tableName, final byte[] family) {
        return scan(tableName, AnyScan.create().addFamily(family));
    }

    /**
     *
     * @param tableName
     * @param family
     * @param qualifier
     * @return
     */
    public Stream<Result> scan(final String tableName, final byte[] family, final byte[] qualifier) {
        return scan(tableName, AnyScan.create().addColumn(family, qualifier));
    }

    /**
     *
     * @param tableName
     * @param anyScan
     * @return
     */
    public Stream<Result> scan(final String tableName, final AnyScan anyScan) {
        return scan(tableName, anyScan.val());
    }

    /**
     *
     * @param tableName
     * @param scan
     * @return
     */
    public Stream<Result> scan(final String tableName, final Scan scan) {
        N.checkArgNotNull(tableName, "tableName");
        N.checkArgNotNull(scan, "scan");

        final ObjIteratorEx<Result> lazyIter = ObjIteratorEx.of(new Supplier<ObjIteratorEx<Result>>() {
            private ObjIteratorEx<Result> internalIter = null;

            @Override
            public ObjIteratorEx<Result> get() {
                if (internalIter == null) {
                    final Table table = getTable(tableName);

                    try {
                        final ResultScanner resultScanner = table.getScanner(scan);
                        final Iterator<Result> iter = resultScanner.iterator();

                        internalIter = new ObjIteratorEx<Result>() {
                            @Override
                            public boolean hasNext() {
                                return iter.hasNext();
                            }

                            @Override
                            public Result next() {
                                return iter.next();
                            }

                            @Override
                            public void close() {
                                try {
                                    IOUtil.closeQuietly(resultScanner);
                                } finally {
                                    IOUtil.closeQuietly(table);
                                }
                            }
                        };
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } finally {
                        if (internalIter == null) {
                            IOUtil.closeQuietly(table);
                        }
                    }
                }

                return internalIter;
            }
        });

        return Stream.of(lazyIter).onClose(new Runnable() {
            @Override
            public void run() {
                lazyIter.close();
            }
        });
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param family
     * @return
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final String family) {
        return scan(tableName, family).map(toEntity(targetClass));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param family
     * @param qualifier
     * @return
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final String family, final String qualifier) {
        return scan(tableName, family, qualifier).map(toEntity(targetClass));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param family
     * @return
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final byte[] family) {
        return scan(tableName, family).map(toEntity(targetClass));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param family
     * @param qualifier
     * @return
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final byte[] family, final byte[] qualifier) {
        return scan(tableName, family, qualifier).map(toEntity(targetClass));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param anyScan
     * @return
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final AnyScan anyScan) {
        return scan(tableName, anyScan).map(toEntity(targetClass));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param scan
     * @return
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final Scan scan) {
        return scan(tableName, scan).map(toEntity(targetClass));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @return
     */
    private static <T> Function<Result, T> toEntity(final Class<T> targetClass) {
        return new Function<Result, T>() {
            @Override
            public T apply(Result t) {
                return toEntity(targetClass, t);
            }
        };
    }

    /**
     *
     * @param tableName
     * @param put
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void put(final String tableName, final Put put) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.put(put);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param tableName
     * @param puts
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void put(final String tableName, final List<Put> puts) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.put(puts);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param tableName
     * @param anyPut
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void put(final String tableName, final AnyPut anyPut) throws UncheckedIOException {
        put(tableName, anyPut.val());
    }

    /**
     *
     * @param tableName
     * @param anyPuts
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void put(final String tableName, final Collection<AnyPut> anyPuts) throws UncheckedIOException {
        put(tableName, toPut(anyPuts));
    }

    // There is no too much benefit to add method for "Object rowKey"
    /**
     *
     * @param tableName
     * @param rowKey
     * @throws UncheckedIOException the unchecked IO exception
     */
    // And it may cause error because the "Object" is ambiguous to any type.
    void delete(final String tableName, final Object rowKey) throws UncheckedIOException {
        delete(tableName, AnyDelete.of(rowKey));
    }

    /**
     *
     * @param tableName
     * @param delete
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void delete(final String tableName, final Delete delete) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param tableName
     * @param deletes
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void delete(final String tableName, final List<Delete> deletes) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.delete(deletes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param tableName
     * @param anyDelete
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void delete(final String tableName, final AnyDelete anyDelete) throws UncheckedIOException {
        delete(tableName, anyDelete.val());
    }

    /**
     *
     * @param tableName
     * @param anyDeletes
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void delete(final String tableName, final Collection<AnyDelete> anyDeletes) throws UncheckedIOException {
        delete(tableName, toDelete(anyDeletes));
    }

    /**
     *
     * @param tableName
     * @param rm
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void mutateRow(final String tableName, final AnyRowMutations rm) throws UncheckedIOException {
        mutateRow(tableName, rm.val());
    }

    /**
     *
     * @param tableName
     * @param rm
     * @throws UncheckedIOException the unchecked IO exception
     */
    public void mutateRow(final String tableName, final RowMutations rm) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.mutateRow(rm);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param tableName
     * @param append
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Result append(final String tableName, final AnyAppend append) throws UncheckedIOException {
        return append(tableName, append.val());
    }

    /**
     *
     * @param tableName
     * @param append
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Result append(final String tableName, final Append append) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.append(append);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param tableName
     * @param increment
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Result increment(final String tableName, final AnyIncrement increment) throws UncheckedIOException {
        return increment(tableName, increment.val());
    }

    /**
     *
     * @param tableName
     * @param increment
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public Result increment(final String tableName, final Increment increment) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.increment(increment);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Increment column value.
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @param amount
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier, final long amount)
            throws UncheckedIOException {
        return incrementColumnValue(tableName, rowKey, toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), amount);
    }

    /**
     * Increment column value.
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @param amount
     * @param durability
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier, final long amount,
            final Durability durability) throws UncheckedIOException {
        return incrementColumnValue(tableName, rowKey, toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), amount, durability);
    }

    /**
     * Increment column value.
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @param amount
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier, final long amount)
            throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.incrementColumnValue(toRowKeyBytes(rowKey), family, qualifier, amount);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Increment column value.
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @param amount
     * @param durability
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier, final long amount,
            final Durability durability) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.incrementColumnValue(toRowKeyBytes(rowKey), family, qualifier, amount, durability);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param tableName
     * @param rowKey
     * @return
     */
    public CoprocessorRpcChannel coprocessorService(final String tableName, final Object rowKey) {
        final Table table = getTable(tableName);

        try {
            return table.coprocessorService(toRowKeyBytes(rowKey));
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param <T>
     * @param <R>
     * @param tableName
     * @param service
     * @param startRowKey
     * @param endRowKey
     * @param callable
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     * @throws Exception the exception
     */
    public <T extends Service, R> Map<byte[], R> coprocessorService(final String tableName, final Class<T> service, final Object startRowKey,
            final Object endRowKey, final Batch.Call<T, R> callable) throws UncheckedIOException, Exception {
        final Table table = getTable(tableName);

        try {
            return table.coprocessorService(service, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), callable);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Throwable e) {
            throw new Exception(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     *
     * @param <T>
     * @param <R>
     * @param tableName
     * @param service
     * @param startRowKey
     * @param endRowKey
     * @param callable
     * @param callback
     * @throws UncheckedIOException the unchecked IO exception
     * @throws Exception the exception
     */
    public <T extends Service, R> void coprocessorService(final String tableName, final Class<T> service, final Object startRowKey, final Object endRowKey,
            final Batch.Call<T, R> callable, final Batch.Callback<R> callback) throws UncheckedIOException, Exception {
        final Table table = getTable(tableName);

        try {
            table.coprocessorService(service, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), callable, callback);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Throwable e) {
            throw new Exception(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Batch coprocessor service.
     *
     * @param <R>
     * @param tableName
     * @param methodDescriptor
     * @param request
     * @param startRowKey
     * @param endRowKey
     * @param responsePrototype
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     * @throws Exception the exception
     */
    public <R extends Message> Map<byte[], R> batchCoprocessorService(final String tableName, final Descriptors.MethodDescriptor methodDescriptor,
            final Message request, final Object startRowKey, final Object endRowKey, final R responsePrototype) throws UncheckedIOException, Exception {
        final Table table = getTable(tableName);

        try {
            return table.batchCoprocessorService(methodDescriptor, request, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), responsePrototype);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Throwable e) {
            throw new Exception(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Batch coprocessor service.
     *
     * @param <R>
     * @param tableName
     * @param methodDescriptor
     * @param request
     * @param startRowKey
     * @param endRowKey
     * @param responsePrototype
     * @param callback
     * @throws UncheckedIOException the unchecked IO exception
     * @throws Exception the exception
     */
    public <R extends Message> void batchCoprocessorService(final String tableName, final Descriptors.MethodDescriptor methodDescriptor, final Message request,
            final Object startRowKey, final Object endRowKey, final R responsePrototype, final Batch.Callback<R> callback)
            throws UncheckedIOException, Exception {
        final Table table = getTable(tableName);

        try {
            table.batchCoprocessorService(methodDescriptor, request, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), responsePrototype, callback);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Throwable e) {
            throw new Exception(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * To family qualifier bytes.
     *
     * @param str
     * @return
     */
    static byte[] toFamilyQualifierBytes(final String str) {
        if (str == null) {
            return null;
        }

        byte[] bytes = familyQualifierBytesPool.get(str);

        if (bytes == null) {
            bytes = Bytes.toBytes(str);

            familyQualifierBytesPool.put(str, bytes);
        }

        return bytes;
    }

    /**
     * To row key bytes.
     *
     * @param rowKey
     * @return
     */
    static byte[] toRowKeyBytes(final Object rowKey) {
        return toValueBytes(rowKey);
    }

    /**
     * To row bytes.
     *
     * @param row
     * @return
     */
    static byte[] toRowBytes(final Object row) {
        return toValueBytes(row);
    }

    /**
     * To value bytes.
     *
     * @param value
     * @return
     */
    static byte[] toValueBytes(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof byte[]) {
            return (byte[]) value;
        } else if (value instanceof ByteBuffer) {
            return ((ByteBuffer) value).array();
        } else if (value instanceof String) {
            return Bytes.toBytes((String) value);
        } else {
            return Bytes.toBytes(N.stringOf(value));
        }
    }

    //
    //    static byte[] toBytes(final String str) {
    //        return str == null ? null : Bytes.toBytes(str);
    //    }
    //
    //    static byte[] toBytes(final Object obj) {
    //        return obj == null ? null : (obj instanceof byte[] ? (byte[]) obj : toBytes(N.stringOf(obj)));
    //    }

    /**
     * To row key string.
     *
     * @param bytes
     * @param offset
     * @param len
     * @return
     */
    static String toRowKeyString(byte[] bytes, int offset, int len) {
        return Bytes.toString(bytes, offset, len);
    }

    /**
     * To family qualifier string.
     *
     * @param bytes
     * @param offset
     * @param len
     * @return
     */
    static String toFamilyQualifierString(byte[] bytes, int offset, int len) {
        return Bytes.toString(bytes, offset, len);
    }

    /**
     * To value string.
     *
     * @param bytes
     * @param offset
     * @param len
     * @return
     */
    static String toValueString(byte[] bytes, int offset, int len) {
        return Bytes.toString(bytes, offset, len);
    }

    /**
     * Gets the row key string.
     *
     * @param cell
     * @return
     */
    static String getRowKeyString(final Cell cell) {
        return toRowKeyString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    }

    /**
     * Gets the family string.
     *
     * @param cell
     * @return
     */
    static String getFamilyString(final Cell cell) {
        return toFamilyQualifierString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
    }

    /**
     * Gets the qualifier string.
     *
     * @param cell
     * @return
     */
    static String getQualifierString(final Cell cell) {
        return toFamilyQualifierString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    }

    /**
     * Gets the value string.
     *
     * @param cell
     * @return
     */
    static String getValueString(final Cell cell) {
        return toValueString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }

    /**
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public void close() throws IOException {
        if (conn.isClosed() == false) {
            conn.close();
        }
    }

    /**
     *
     * @param <T> target entity type.
     * @param <K> row key type
     */
    public static class HBaseMapper<T, K> {
        private final HBaseExecutor hbaseExecutor;
        private final String tableName;
        private final Class<T> targetEntityClass;
        private final String rowKeyPropName;
        private final NamingPolicy namingPolicy;

        HBaseMapper(final Class<T> targetEntityClass, final HBaseExecutor hbaseExecutor, final String tableName, final NamingPolicy namingPolicy) {
            N.checkArgNotNull(targetEntityClass, "targetEntityClass");
            N.checkArgNotNull(hbaseExecutor, "hbaseExecutor");
            N.checkArgNotNullOrEmpty(tableName, "tableName");

            N.checkArgument(ClassUtil.isEntity(targetEntityClass), "{} is not an entity class with getter/setter method", targetEntityClass);

            @SuppressWarnings("deprecation")
            final List<String> idPropNames = ClassUtil.getIdFieldNames(targetEntityClass);

            if (idPropNames.size() != 1) {
                throw new IllegalArgumentException(
                        "No or multiple ids: " + idPropNames + " defined/annotated in class: " + ClassUtil.getCanonicalClassName(targetEntityClass));
            }

            this.hbaseExecutor = hbaseExecutor;
            this.tableName = tableName;
            this.targetEntityClass = targetEntityClass;
            this.rowKeyPropName = idPropNames.get(0);

            this.namingPolicy = namingPolicy == null ? NamingPolicy.LOWER_CAMEL_CASE : namingPolicy;
        }

        /**
         * 
         * @param rowKey
         * @return
         * @throws UncheckedIOException
         */
        public boolean exists(final K rowKey) throws UncheckedIOException {
            return hbaseExecutor.exists(tableName, AnyGet.of(rowKey));
        }

        /**
         * 
         * @param rowKeys
         * @return
         * @throws UncheckedIOException
         */
        public List<Boolean> exists(final Collection<? extends K> rowKeys) throws UncheckedIOException {
            final List<AnyGet> anyGets = N.map(rowKeys, AnyGet::of);

            return hbaseExecutor.exists(tableName, anyGets);
        }

        /**
         * 
         * @param rowKey
         * @return
         * @throws UncheckedIOException
         */
        public T get(final K rowKey) throws UncheckedIOException {
            return hbaseExecutor.get(targetEntityClass, tableName, AnyGet.of(rowKey));
        }

        /**
         * 
         * @param rowKeys
         * @return
         * @throws UncheckedIOException
         */
        public List<T> get(final Collection<? extends K> rowKeys) throws UncheckedIOException {
            final List<AnyGet> anyGets = N.map(rowKeys, AnyGet::of);

            return hbaseExecutor.get(targetEntityClass, tableName, anyGets);
        }

        /**
         * 
         * @param entityToPut
         * @throws UncheckedIOException
         */
        public void put(final T entityToPut) throws UncheckedIOException {
            hbaseExecutor.put(tableName, AnyPut.from(entityToPut, namingPolicy));
        }

        /**
         * 
         * @param entitiesToPut
         * @throws UncheckedIOException
         */
        public void put(final Collection<? extends T> entitiesToPut) throws UncheckedIOException {
            hbaseExecutor.put(tableName, AnyPut.from(entitiesToPut, namingPolicy));
        }

        /**
         * 
         * @param entityToDelete
         * @throws UncheckedIOException
         */
        public void delete(T entityToDelete) throws UncheckedIOException {
            deleteByRowKey((K) ClassUtil.getPropValue(entityToDelete, rowKeyPropName));
        }

        /**
         * 
         * @param entitiesToDelete
         * @throws UncheckedIOException
         */
        public void delete(final Collection<? extends T> entitiesToDelete) throws UncheckedIOException {
            deleteByRowKey((List<K>) N.map(entitiesToDelete, entity -> ClassUtil.getPropValue(entity, rowKeyPropName)));
        }

        /**
         * 
         * @param rowKey
         * @throws UncheckedIOException
         */
        public void deleteByRowKey(K rowKey) throws UncheckedIOException {
            hbaseExecutor.delete(tableName, AnyDelete.of(rowKey));
        }

        /**
         * 
         * @param rowKeys
         * @throws UncheckedIOException
         */
        public void deleteByRowKey(final Collection<? extends K> rowKeys) throws UncheckedIOException {
            final List<AnyDelete> anyDeletes = N.map(rowKeys, AnyDelete::of);

            hbaseExecutor.delete(tableName, anyDeletes);
        }

        public boolean exists(final AnyGet anyGet) throws UncheckedIOException {
            return hbaseExecutor.exists(tableName, anyGet.val());
        }

        public List<Boolean> exists(final List<AnyGet> anyGets) throws UncheckedIOException {
            return hbaseExecutor.exists(tableName, toGet(anyGets));
        }

        public T get(final AnyGet anyGet) throws UncheckedIOException {
            return hbaseExecutor.get(targetEntityClass, tableName, anyGet);
        }

        public List<T> get(final List<AnyGet> anyGets) throws UncheckedIOException {
            return hbaseExecutor.get(targetEntityClass, tableName, anyGets);
        }

        public Stream<T> scan(final String family) {
            return hbaseExecutor.scan(targetEntityClass, tableName, family);
        }

        public Stream<T> scan(final String family, final String qualifier) {
            return hbaseExecutor.scan(targetEntityClass, tableName, family, qualifier);
        }

        public Stream<T> scan(final byte[] family) {
            return hbaseExecutor.scan(targetEntityClass, tableName, family);
        }

        public Stream<T> scan(final byte[] family, final byte[] qualifier) {
            return hbaseExecutor.scan(targetEntityClass, tableName, family, qualifier);
        }

        public Stream<T> scan(final AnyScan anyScan) {
            return hbaseExecutor.scan(targetEntityClass, tableName, anyScan);
        }

        public void put(final AnyPut anyPut) throws UncheckedIOException {
            hbaseExecutor.put(tableName, anyPut);
        }

        public void put(final List<AnyPut> anyPuts) throws UncheckedIOException {
            hbaseExecutor.put(tableName, anyPuts);
        }

        public void delete(final AnyDelete anyDelete) throws UncheckedIOException {
            hbaseExecutor.delete(tableName, anyDelete);
        }

        public void delete(final List<AnyDelete> anyDeletes) throws UncheckedIOException {
            hbaseExecutor.delete(tableName, anyDeletes);
        }

        public void mutateRow(final AnyRowMutations rm) throws UncheckedIOException {
            hbaseExecutor.mutateRow(tableName, rm);
        }

        public Result append(final AnyAppend append) throws UncheckedIOException {
            return hbaseExecutor.append(tableName, append);
        }

        public Result increment(final AnyIncrement increment) throws UncheckedIOException {
            return hbaseExecutor.increment(tableName, increment);
        }

        public long incrementColumnValue(final Object rowKey, final String family, final String qualifier, final long amount) throws UncheckedIOException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount);
        }

        public long incrementColumnValue(final Object rowKey, final String family, final String qualifier, final long amount, final Durability durability)
                throws UncheckedIOException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability);
        }

        public long incrementColumnValue(final Object rowKey, final byte[] family, final byte[] qualifier, final long amount) throws UncheckedIOException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount);
        }

        public long incrementColumnValue(final Object rowKey, final byte[] family, final byte[] qualifier, final long amount, final Durability durability)
                throws UncheckedIOException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability);
        }

        public CoprocessorRpcChannel coprocessorService(final Object rowKey) {
            return hbaseExecutor.coprocessorService(tableName, rowKey);
        }

        public <S extends Service, R> Map<byte[], R> coprocessorService(final Class<S> service, final Object startRowKey, final Object endRowKey,
                final Batch.Call<S, R> callable) throws UncheckedIOException, Exception {
            return hbaseExecutor.coprocessorService(tableName, service, startRowKey, endRowKey, callable);
        }

        public <S extends Service, R> void coprocessorService(final Class<S> service, final Object startRowKey, final Object endRowKey,
                final Batch.Call<S, R> callable, final Batch.Callback<R> callback) throws UncheckedIOException, Exception {
            hbaseExecutor.coprocessorService(tableName, service, startRowKey, endRowKey, callable, callback);
        }

        public <R extends Message> Map<byte[], R> batchCoprocessorService(final Descriptors.MethodDescriptor methodDescriptor, final Message request,
                final Object startRowKey, final Object endRowKey, final R responsePrototype) throws UncheckedIOException, Exception {
            return hbaseExecutor.batchCoprocessorService(tableName, methodDescriptor, request, startRowKey, endRowKey, responsePrototype);
        }

        public <R extends Message> void batchCoprocessorService(final Descriptors.MethodDescriptor methodDescriptor, final Message request,
                final Object startRowKey, final Object endRowKey, final R responsePrototype, final Batch.Callback<R> callback)
                throws UncheckedIOException, Exception {
            hbaseExecutor.batchCoprocessorService(tableName, methodDescriptor, request, startRowKey, endRowKey, responsePrototype, callback);
        }
    }
}
