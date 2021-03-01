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

package com.landawn.abacus.da.aws.dynamoDB;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.landawn.abacus.DataSet;
import com.landawn.abacus.DirtyMarker;
import com.landawn.abacus.core.DirtyMarkerUtil;
import com.landawn.abacus.core.RowDataSet;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.EntityInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.ObjIterator;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.stream.Stream;

/**
 * It's a simple wrapper of DynamoDB Java client.
 *
 * @author Haiyang Li
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/">com.amazonaws.services.dynamodbv2.AmazonDynamoDB</a>
 * @since 0.8
 */
public final class DynamoDBExecutor implements Closeable {

    static final AsyncExecutor DEFAULT_ASYNC_EXECUTOR = new AsyncExecutor(Math.min(Math.max(64, IOUtil.CPU_CORES * 8), (IOUtil.MAX_MEMORY_IN_MB / 1024) * 32),
            Math.max(256, (IOUtil.MAX_MEMORY_IN_MB / 1024) * 64), 180L, TimeUnit.SECONDS);

    private static final Type<AttributeValue> attrValueType = N.typeOf(AttributeValue.class);

    private final AmazonDynamoDBClient dynamoDB;

    private final DynamoDBMapper mapper;

    private final AsyncDynamoDBExecutor asyncDBExecutor;

    public DynamoDBExecutor(final AmazonDynamoDBClient dynamoDB) {
        this(dynamoDB, null);
    }

    public DynamoDBExecutor(final AmazonDynamoDBClient dynamoDB, final DynamoDBMapperConfig config) {
        this(dynamoDB, config, DEFAULT_ASYNC_EXECUTOR);
    }

    public DynamoDBExecutor(final AmazonDynamoDBClient dynamoDB, final DynamoDBMapperConfig config, final AsyncExecutor asyncExecutor) {
        this.dynamoDB = dynamoDB;
        this.asyncDBExecutor = new AsyncDynamoDBExecutor(this, asyncExecutor);
        this.mapper = config == null ? new DynamoDBMapper(dynamoDB) : new DynamoDBMapper(dynamoDB, config);
    }

    public AmazonDynamoDBClient dynamoDB() {
        return dynamoDB;
    }

    public DynamoDBMapper dynamoDBMapper() {
        return mapper;
    }

    /**
     *
     * @param config
     * @return
     */
    public DynamoDBMapper dynamoDBMapper(final DynamoDBMapperConfig config) {
        return new DynamoDBMapper(dynamoDB, config);
    }

    @SuppressWarnings("rawtypes")
    private final Map<Class<?>, Mapper> mapperPool = new ConcurrentHashMap<>();

    public <T> Mapper<T> mapper(final Class<T> targetEntityClass) {
        @SuppressWarnings("rawtypes")
        Mapper result = mapperPool.get(targetEntityClass);

        if (result == null) {
            final EntityInfo entityInfo = ParserUtil.getEntityInfo(targetEntityClass);

            if (entityInfo.tableName.isEmpty()) {
                throw new IllegalArgumentException("The target entity class: " + targetEntityClass
                        + " must be annotated with com.landawn.abacus.annotation.Table or javax.persistence.Table. Otherwise call  HBaseExecutor.mapper(final String tableName, final Class<T> targetEntityClass) instead");
            }

            result = mapper(targetEntityClass, entityInfo.tableName.get(), NamingPolicy.LOWER_CAMEL_CASE);

            mapperPool.put(targetEntityClass, result);
        }

        return result;
    }

    public <T> Mapper<T> mapper(final Class<T> targetEntityClass, final String tableName, final NamingPolicy namingPolicy) {
        return new Mapper<>(targetEntityClass, this, tableName, namingPolicy);
    }

    public AsyncDynamoDBExecutor async() {
        return asyncDBExecutor;
    }

    /**
     * Set value to <code>null</code> by <code>withNULL(Boolean.TRUE)</code> if the specified value is null,
     * or set it to <code>Boolean</code> by <code>setBOOL((Boolean) value)</code> if it's <code>Boolean</code>,
     * or set it to <code>ByteBuffer</code> by <code>setB((ByteBuffer) value)</code> if it's <code>ByteBuffer</code>,
     * otherwise, set it to String by <code>setS(N.stringOf(value))</code> for other types.
     * That's to say all the types except Number/Boolean/ByteBuffer are defined to String.
     *
     * @param value
     * @return
     */
    public static AttributeValue attrValueOf(Object value) {
        final AttributeValue attrVal = new AttributeValue();

        if (value == null) {
            attrVal.withNULL(Boolean.TRUE);
        } else {
            final Type<Object> type = N.typeOf(value.getClass());

            if (type.isNumber()) {
                attrVal.setN(type.stringOf(value));
            } else if (type.isBoolean()) {
                attrVal.setBOOL((Boolean) value);
            } else if (type.isByteBuffer()) {
                attrVal.setB((ByteBuffer) value);
            } else {
                attrVal.setS(type.stringOf(value));
            }
        }

        return attrVal;
    }

    /**
     * Returns the <code>AttributeValueUpdate</code> with default <code>AttributeAction.PUT</code>
     *
     * @param value
     * @return
     */
    public static AttributeValueUpdate attrValueUpdateOf(Object value) {
        return attrValueUpdateOf(value, AttributeAction.PUT);
    }

    /**
     * Attr value update of.
     *
     * @param value
     * @param action
     * @return
     */
    public static AttributeValueUpdate attrValueUpdateOf(Object value, AttributeAction action) {
        return new AttributeValueUpdate(attrValueOf(value), action);
    }

    /**
     *
     * @param keyName
     * @param value
     * @return
     */
    public static Map<String, AttributeValue> asKey(final String keyName, final Object value) {
        return asItem(keyName, value);
    }

    /**
     *
     * @param keyName
     * @param value
     * @param keyName2
     * @param value2
     * @return
     */
    public static Map<String, AttributeValue> asKey(final String keyName, final Object value, final String keyName2, final Object value2) {
        return asItem(keyName, value, keyName2, value2);
    }

    /**
     *
     * @param keyName
     * @param value
     * @param keyName2
     * @param value2
     * @param keyName3
     * @param value3
     * @return
     */
    public static Map<String, AttributeValue> asKey(final String keyName, final Object value, final String keyName2, final Object value2, final String keyName3,
            Object value3) {
        return asItem(keyName, value, keyName2, value2, keyName3, value3);
    }

    /**
     *
     * @param a
     * @return
     */
    // May misused with toItem
    @SafeVarargs
    public static Map<String, AttributeValue> asKey(Object... a) {
        return asItem(a);
    }

    /**
     *
     * @param attrName
     * @param value
     * @return
     */
    public static Map<String, AttributeValue> asItem(final String attrName, final Object value) {
        return N.asLinkedHashMap(attrName, attrValueOf(value));
    }

    /**
     *
     * @param attrName
     * @param value
     * @param attrName2
     * @param value2
     * @return
     */
    public static Map<String, AttributeValue> asItem(final String attrName, final Object value, final String attrName2, final Object value2) {
        return N.asLinkedHashMap(attrName, attrValueOf(value), attrName2, attrValueOf(value2));
    }

    /**
     *
     * @param attrName
     * @param value
     * @param attrName2
     * @param value2
     * @param attrName3
     * @param value3
     * @return
     */
    public static Map<String, AttributeValue> asItem(final String attrName, final Object value, final String attrName2, final Object value2,
            final String attrName3, Object value3) {
        return N.asLinkedHashMap(attrName, attrValueOf(value), attrName2, attrValueOf(value2), attrName3, attrValueOf(value3));
    }

    /**
     *
     * @param a
     * @return
     */
    // May misused with toItem
    @SafeVarargs
    public static Map<String, AttributeValue> asItem(Object... a) {
        if (0 != (a.length % 2)) {
            throw new IllegalArgumentException(
                    "The parameters must be the pairs of property name and value, or Map, or an entity class with getter/setter methods.");
        }

        final Map<String, AttributeValue> item = N.newLinkedHashMap(a.length / 2);

        for (int i = 0; i < a.length; i++) {
            item.put((String) a[i], attrValueOf(a[++i]));
        }

        return item;
    }

    /**
     * As update item.
     *
     * @param attrName
     * @param value
     * @return
     */
    public static Map<String, AttributeValueUpdate> asUpdateItem(final String attrName, final Object value) {
        return N.asLinkedHashMap(attrName, attrValueUpdateOf(value));
    }

    /**
     * As update item.
     *
     * @param attrName
     * @param value
     * @param attrName2
     * @param value2
     * @return
     */
    public static Map<String, AttributeValueUpdate> asUpdateItem(final String attrName, final Object value, final String attrName2, final Object value2) {
        return N.asLinkedHashMap(attrName, attrValueUpdateOf(value), attrName2, attrValueUpdateOf(value2));
    }

    /**
     * As update item.
     *
     * @param attrName
     * @param value
     * @param attrName2
     * @param value2
     * @param attrName3
     * @param value3
     * @return
     */
    public static Map<String, AttributeValueUpdate> asUpdateItem(final String attrName, final Object value, final String attrName2, final Object value2,
            final String attrName3, final Object value3) {
        return N.asLinkedHashMap(attrName, attrValueUpdateOf(value), attrName2, attrValueUpdateOf(value2), attrName3, attrValueUpdateOf(value3));
    }

    /**
     * As update item.
     *
     * @param a
     * @return
     */
    // May misused with toUpdateItem
    @SafeVarargs
    public static Map<String, AttributeValueUpdate> asUpdateItem(Object... a) {
        if (0 != (a.length % 2)) {
            throw new IllegalArgumentException(
                    "The parameters must be the pairs of property name and value, or Map, or an entity class with getter/setter methods.");
        }

        final Map<String, AttributeValueUpdate> item = N.newLinkedHashMap(a.length / 2);

        for (int i = 0; i < a.length; i++) {
            item.put((String) a[i], attrValueUpdateOf(a[++i]));
        }

        return item;
    }

    /**
     *
     * @param entity
     * @return
     */
    public static Map<String, AttributeValue> toItem(final Object entity) {
        return toItem(entity, NamingPolicy.LOWER_CAMEL_CASE);
    }

    /**
     *
     * @param entity
     * @param namingPolicy
     * @return
     */
    @SuppressWarnings("deprecation")
    public static Map<String, AttributeValue> toItem(final Object entity, NamingPolicy namingPolicy) {
        final boolean isLowerCamelCase = namingPolicy == NamingPolicy.LOWER_CAMEL_CASE;
        final Map<String, AttributeValue> attrs = new LinkedHashMap<>();
        final Class<?> cls = entity.getClass();

        if (ClassUtil.isEntity(cls)) {
            if (DirtyMarkerUtil.isDirtyMarker(cls)) {

                final Set<String> signedPropNames = ((DirtyMarker) entity).signedPropNames();
                final EntityInfo entityInfo = ParserUtil.getEntityInfo(cls);
                PropInfo propInfo = null;

                for (String propName : signedPropNames) {
                    propInfo = entityInfo.getPropInfo(propName);

                    attrs.put(getAttrName(propInfo, namingPolicy), attrValueOf(propInfo.getPropValue(entity)));
                }
            } else {
                final EntityInfo entityInfo = ParserUtil.getEntityInfo(cls);
                Object propValue = null;

                for (PropInfo propInfo : entityInfo.propInfoList) {
                    propValue = propInfo.getPropValue(entity);

                    if (propValue == null) {
                        continue;
                    }

                    attrs.put(getAttrName(propInfo, namingPolicy), attrValueOf(propValue));
                }
            }
        } else if (Map.class.isAssignableFrom(cls)) {
            final Map<String, Object> map = (Map<String, Object>) entity;

            if (isLowerCamelCase) {
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    attrs.put(entry.getKey(), attrValueOf(entry.getValue()));
                }
            } else {
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    attrs.put(namingPolicy.convert(entry.getKey()), attrValueOf(entry.getValue()));
                }
            }
        } else if (entity instanceof Object[]) {
            return toItem(N.asProps((Object[]) entity), namingPolicy);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(cls)
                    + ". Only Entity and Map<String, Object> class generated by CodeGenerator with getter/setter methods are supported");
        }

        return attrs;
    }

    /**
     * Only the dirty properties will be set to the result Map if the specified entity is a dirty marker entity.
     *
     * @param entity
     * @return
     */
    public static Map<String, AttributeValueUpdate> toUpdateItem(final Object entity) {
        return toUpdateItem(entity, NamingPolicy.LOWER_CAMEL_CASE);
    }

    /**
     * Only the dirty properties will be set to the result Map if the specified entity is a dirty marker entity.
     *
     * @param entity
     * @param namingPolicy
     * @return
     */
    @SuppressWarnings("deprecation")
    public static Map<String, AttributeValueUpdate> toUpdateItem(final Object entity, NamingPolicy namingPolicy) {
        final boolean isLowerCamelCase = namingPolicy == NamingPolicy.LOWER_CAMEL_CASE;
        final Map<String, AttributeValueUpdate> attrs = new LinkedHashMap<>();
        final Class<?> cls = entity.getClass();

        if (ClassUtil.isEntity(cls)) {
            if (DirtyMarkerUtil.isDirtyMarker(cls)) {

                final Set<String> dirtyPropNames = ((DirtyMarker) entity).dirtyPropNames();
                final EntityInfo entityInfo = ParserUtil.getEntityInfo(cls);
                PropInfo propInfo = null;

                for (String propName : dirtyPropNames) {
                    propInfo = entityInfo.getPropInfo(propName);

                    attrs.put(getAttrName(propInfo, namingPolicy), attrValueUpdateOf(propInfo.getPropValue(entity)));
                }

            } else {
                final EntityInfo entityInfo = ParserUtil.getEntityInfo(cls);
                Object propValue = null;

                for (PropInfo propInfo : entityInfo.propInfoList) {
                    propValue = propInfo.getPropValue(entity);

                    if (propValue == null) {
                        continue;
                    }

                    attrs.put(getAttrName(propInfo, namingPolicy), attrValueUpdateOf(propValue));
                }
            }
        } else if (Map.class.isAssignableFrom(cls)) {
            final Map<String, Object> map = (Map<String, Object>) entity;

            if (isLowerCamelCase) {
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    attrs.put(entry.getKey(), attrValueUpdateOf(entry.getValue()));
                }
            } else {
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    attrs.put(namingPolicy.convert(entry.getKey()), attrValueUpdateOf(entry.getValue()));
                }
            }
        } else if (entity instanceof Object[]) {
            return toUpdateItem(N.asProps((Object[]) entity), namingPolicy);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(cls)
                    + ". Only Entity and Map<String, Object> class generated by CodeGenerator with getter/setter methods are supported");
        }

        return attrs;
    }

    /**
     *
     * @param entities
     * @return
     */
    static List<Map<String, AttributeValue>> toItem(final Collection<?> entities) {
        return toItem(entities, NamingPolicy.LOWER_CAMEL_CASE);
    }

    /**
     *
     * @param entities
     * @param namingPolicy
     * @return
     */
    static List<Map<String, AttributeValue>> toItem(final Collection<?> entities, NamingPolicy namingPolicy) {
        final List<Map<String, AttributeValue>> attrsList = new ArrayList<>(entities.size());

        for (Object entity : entities) {
            attrsList.add(toItem(entity, namingPolicy));
        }

        return attrsList;
    }

    /**
     * To update item.
     *
     * @param entities
     * @return
     */
    static List<Map<String, AttributeValueUpdate>> toUpdateItem(final Collection<?> entities) {
        return toUpdateItem(entities, NamingPolicy.LOWER_CAMEL_CASE);
    }

    /**
     * To update item.
     *
     * @param entities
     * @param namingPolicy
     * @return
     */
    static List<Map<String, AttributeValueUpdate>> toUpdateItem(final Collection<?> entities, NamingPolicy namingPolicy) {
        final List<Map<String, AttributeValueUpdate>> attrsList = new ArrayList<>(entities.size());

        for (Object entity : entities) {
            attrsList.add(toUpdateItem(entity, namingPolicy));
        }

        return attrsList;
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param item
     * @return
     */
    public static <T> T toEntity(final Class<T> targetClass, final Map<String, AttributeValue> item) {
        final Type<T> targetType = N.typeOf(targetClass);

        return toValue(targetType, targetClass, item);
    }

    /**
     *
     * @param <T>
     * @param type
     * @param targetClass
     * @param item
     * @return
     */
    private static <T> T toValue(final Type<T> type, final Class<T> targetClass, final Map<String, AttributeValue> item) {
        if (item == null) {
            return null;
        }

        if (type.isMap()) {
            final Map<String, Object> map = (Map<String, Object>) N.newInstance(targetClass);

            for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
                map.put(entry.getKey(), toValue(entry.getValue()));
            }

            return (T) map;
        } else if (type.isEntity()) {
            if (N.isNullOrEmpty(item)) {
                return null;
            }

            final Map<String, String> column2FieldNameMap = ClassUtil.getColumn2PropNameMap(targetClass);
            final EntityInfo entityInfo = ParserUtil.getEntityInfo(targetClass);
            final T entity = N.newInstance(targetClass);
            String fieldName = null;

            PropInfo propInfo = null;

            for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
                propInfo = entityInfo.getPropInfo(entry.getKey());

                if (propInfo == null && (fieldName = column2FieldNameMap.get(entry.getKey())) != null) {
                    propInfo = entityInfo.getPropInfo(fieldName);
                }

                if (propInfo == null) {
                    continue;
                }

                propInfo.setPropValue(entity, propInfo.jsonXmlType.valueOf(attrValueType.stringOf(entry.getValue())));
            }

            if (DirtyMarkerUtil.isDirtyMarker(entity.getClass())) {
                DirtyMarkerUtil.markDirty((DirtyMarker) entity, false);
            }

            return entity;
        } else {
            if (N.isNullOrEmpty(item)) {
                return type.defaultValue();
            }

            return type.valueOf(attrValueType.stringOf(item.values().iterator().next()));
        }
    }

    /**
     *
     * @param <T>
     * @param x
     * @return
     */
    static <T> T toValue(AttributeValue x) {
        if (x == null || (x.getNULL() != null && x.isNULL())) {
            return null;
        }

        if (N.notNullOrEmpty(x.getS())) {
            return (T) x.getS();
        } else if (N.notNullOrEmpty(x.getN())) {
            return (T) x.getN();
        } else if (x.getBOOL() != null) {
            return (T) x.getBOOL();
        } else if (x.getB() != null) {
            return (T) x.getB();
        } else if (N.notNullOrEmpty(x.getSS())) {
            return (T) x.getSS();
        } else if (N.notNullOrEmpty(x.getNS())) {
            return (T) x.getNS();
        } else if (N.notNullOrEmpty(x.getBS())) {
            return (T) x.getBS();
        } else if (N.notNullOrEmpty(x.getL())) {
            final List<AttributeValue> attrVals = x.getL();
            final List<Object> tmp = new ArrayList<>(attrVals.size());

            for (AttributeValue attrVal : attrVals) {
                tmp.add(toValue(attrVal));
            }

            return (T) tmp;
        } else if (N.notNullOrEmpty(x.getM())) {
            final Map<String, AttributeValue> attrMap = x.getM();
            final Map<String, Object> tmp = attrMap instanceof HashMap ? N.newHashMap(attrMap.size()) : N.newLinkedHashMap(attrMap.size());

            for (Map.Entry<String, AttributeValue> entry : attrMap.entrySet()) {
                tmp.put(entry.getKey(), toValue(entry.getValue()));
            }

            return (T) tmp;
        } else if (x.getS() != null) {
            return (T) x.getS();
        } else if (x.getN() != null) {
            return (T) x.getN();
        } else if (x.getSS() != null) {
            return (T) x.getSS();
        } else if (x.getNS() != null) {
            return (T) x.getNS();
        } else if (x.getBS() != null) {
            return (T) x.getBS();
        } else if (x.getL() != null) {
            return (T) x.getL();
        } else if (x.getM() != null) {
            return (T) x.getM();
        } else if (x.getNULL() != null) {
            return (T) x.getNULL();
        } else {
            throw new IllegalArgumentException("Unsupported Attribute type: " + x.toString());
        }
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param tableItems
     * @return
     */
    static <T> Map<String, List<T>> toEntities(final Class<T> targetClass, final Map<String, List<Map<String, AttributeValue>>> tableItems) {
        final Map<String, List<T>> tableEntities = new LinkedHashMap<>();

        if (N.notNullOrEmpty(tableItems)) {
            for (Map.Entry<String, List<Map<String, AttributeValue>>> entry : tableItems.entrySet()) {
                tableEntities.put(entry.getKey(), toList(targetClass, entry.getValue()));
            }
        }

        return tableEntities;
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param queryResult
     * @return
     */
    public static <T> List<T> toList(final Class<T> targetClass, final QueryResult queryResult) {
        return toList(targetClass, queryResult, 0, Integer.MAX_VALUE);
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param queryResult
     * @param offset
     * @param count
     * @return
     */
    public static <T> List<T> toList(final Class<T> targetClass, final QueryResult queryResult, int offset, int count) {
        return toList(targetClass, queryResult.getItems(), offset, count);
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param scanResult
     * @return
     */
    public static <T> List<T> toList(final Class<T> targetClass, final ScanResult scanResult) {
        return toList(targetClass, scanResult, 0, Integer.MAX_VALUE);
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param scanResult
     * @param offset
     * @param count
     * @return
     */
    public static <T> List<T> toList(final Class<T> targetClass, final ScanResult scanResult, int offset, int count) {
        return toList(targetClass, scanResult.getItems(), offset, count);
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param items
     * @return
     */
    static <T> List<T> toList(final Class<T> targetClass, final List<Map<String, AttributeValue>> items) {
        return toList(targetClass, items, 0, Integer.MAX_VALUE);
    }

    /**
     *
     * @param <T>
     * @param targetClass entity classes with getter/setter methods or basic single value type(Primitive/String/Date...)
     * @param items
     * @param offset
     * @param count
     * @return
     */
    static <T> List<T> toList(final Class<T> targetClass, final List<Map<String, AttributeValue>> items, int offset, int count) {
        if (offset < 0 || count < 0) {
            throw new IllegalArgumentException("Offset and count can't be negative");
        }

        final Type<T> targetType = N.typeOf(targetClass);
        final List<T> resultList = new ArrayList<>();

        if (N.notNullOrEmpty(items)) {
            for (int i = offset, to = items.size(); i < to && count > 0; i++, count--) {
                resultList.add(toValue(targetType, targetClass, items.get(i)));
            }
        }

        return resultList;
    }

    /**
     *
     * @param queryResult
     * @return
     */
    public static DataSet extractData(final QueryResult queryResult) {
        return extractData(queryResult, 0, Integer.MAX_VALUE);
    }

    /**
     *
     * @param queryResult
     * @param offset
     * @param count
     * @return
     */
    public static DataSet extractData(final QueryResult queryResult, int offset, int count) {
        return extractData(queryResult.getItems(), offset, count);
    }

    /**
     *
     * @param scanResult
     * @return
     */
    public static DataSet extractData(final ScanResult scanResult) {
        return extractData(scanResult, 0, Integer.MAX_VALUE);
    }

    /**
     *
     * @param scanResult
     * @param offset
     * @param count
     * @return
     */
    public static DataSet extractData(final ScanResult scanResult, int offset, int count) {
        return extractData(scanResult.getItems(), offset, count);
    }

    /**
     *
     * @param items
     * @param offset
     * @param count
     * @return
     */
    static DataSet extractData(final List<Map<String, AttributeValue>> items, int offset, int count) {
        N.checkArgument(offset >= 0 && count >= 0, "'offset' and 'count' can't be negative: %s, %s", offset, count);

        if (N.isNullOrEmpty(items) || count == 0 || offset >= items.size()) {
            return N.newEmptyDataSet();
        }

        final int rowCount = N.min(count, items.size() - offset);
        final Set<String> columnNames = N.newLinkedHashSet();

        for (int i = offset, to = offset + rowCount; i < to; i++) {
            columnNames.addAll(items.get(i).keySet());
        }

        final int columnCount = columnNames.size();
        final List<String> columnNameList = new ArrayList<>(columnNames);
        final List<List<Object>> columnList = new ArrayList<>(columnCount);

        for (int i = 0; i < columnCount; i++) {
            columnList.add(new ArrayList<>(rowCount));
        }

        for (int i = offset, to = offset + rowCount; i < to; i++) {
            final Map<String, AttributeValue> item = items.get(i);

            for (int j = 0; j < columnCount; j++) {
                columnList.get(j).add(toValue(item.get(columnNameList.get(j))));
            }
        }

        return new RowDataSet(columnNameList, columnList);
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

    private static String getAttrName(final PropInfo propInfo, final NamingPolicy namingPolicy) {
        if (propInfo.columnName.isPresent()) {
            return propInfo.columnName.get();
        } else if (NamingPolicy.LOWER_CAMEL_CASE.equals(namingPolicy)) {
            return propInfo.name;
        } else {
            return namingPolicy.convert(propInfo.name);
        }
    }

    /**
     * Gets the item.
     *
     * @param tableName
     * @param key
     * @return
     */
    public Map<String, Object> getItem(final String tableName, final Map<String, AttributeValue> key) {
        return getItem(Map.class, tableName, key);
    }

    /**
     * Gets the item.
     *
     * @param tableName
     * @param key
     * @param consistentRead
     * @return
     */
    public Map<String, Object> getItem(final String tableName, final Map<String, AttributeValue> key, final Boolean consistentRead) {
        return getItem(Map.class, tableName, key, consistentRead);
    }

    /**
     * Gets the item.
     *
     * @param getItemRequest
     * @return
     */
    public Map<String, Object> getItem(final GetItemRequest getItemRequest) {
        return getItem(Map.class, getItemRequest);
    }

    /**
     * Gets the item.
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param key
     * @return
     */
    public <T> T getItem(final Class<T> targetClass, final String tableName, final Map<String, AttributeValue> key) {
        return toEntity(targetClass, dynamoDB.getItem(tableName, key).getItem());
    }

    /**
     * Gets the item.
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param key
     * @param consistentRead
     * @return
     */
    public <T> T getItem(final Class<T> targetClass, final String tableName, final Map<String, AttributeValue> key, final Boolean consistentRead) {
        return toEntity(targetClass, dynamoDB.getItem(tableName, key, consistentRead).getItem());
    }

    /**
     * Gets the item.
     *
     * @param <T>
     * @param targetClass
     * @param getItemRequest
     * @return
     */
    public <T> T getItem(final Class<T> targetClass, final GetItemRequest getItemRequest) {
        return toEntity(targetClass, dynamoDB.getItem(getItemRequest).getItem());
    }

    /**
     * Batch get item.
     *
     * @param requestItems
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Map<String, List<Map<String, Object>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems) {
        return (Map) batchGetItem(Map.class, requestItems);
    }

    /**
     * Batch get item.
     *
     * @param requestItems
     * @param returnConsumedCapacity
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Map<String, List<Map<String, Object>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems, final String returnConsumedCapacity) {
        return (Map) batchGetItem(Map.class, requestItems, returnConsumedCapacity);
    }

    /**
     * Batch get item.
     *
     * @param batchGetItemRequest
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Map<String, List<Map<String, Object>>> batchGetItem(final BatchGetItemRequest batchGetItemRequest) {
        return (Map) batchGetItem(Map.class, batchGetItemRequest);
    }

    /**
     * Batch get item.
     *
     * @param <T>
     * @param targetClass
     * @param requestItems
     * @return
     */
    public <T> Map<String, List<T>> batchGetItem(final Class<T> targetClass, final Map<String, KeysAndAttributes> requestItems) {
        return toEntities(targetClass, dynamoDB.batchGetItem(requestItems).getResponses());
    }

    /**
     * Batch get item.
     *
     * @param <T>
     * @param targetClass
     * @param requestItems
     * @param returnConsumedCapacity
     * @return
     */
    public <T> Map<String, List<T>> batchGetItem(final Class<T> targetClass, final Map<String, KeysAndAttributes> requestItems,
            final String returnConsumedCapacity) {
        return toEntities(targetClass, dynamoDB.batchGetItem(requestItems, returnConsumedCapacity).getResponses());
    }

    /**
     * Batch get item.
     *
     * @param <T>
     * @param targetClass
     * @param batchGetItemRequest
     * @return
     */
    public <T> Map<String, List<T>> batchGetItem(final Class<T> targetClass, final BatchGetItemRequest batchGetItemRequest) {
        return toEntities(targetClass, dynamoDB.batchGetItem(batchGetItemRequest).getResponses());
    }

    /**
     *
     * @param tableName
     * @param item
     * @return
     */
    public PutItemResult putItem(final String tableName, final Map<String, AttributeValue> item) {
        return dynamoDB.putItem(tableName, item);
    }

    /**
     *
     * @param tableName
     * @param item
     * @param returnValues
     * @return
     */
    public PutItemResult putItem(final String tableName, final Map<String, AttributeValue> item, final String returnValues) {
        return dynamoDB.putItem(tableName, item, returnValues);
    }

    /**
     *
     * @param putItemRequest
     * @return
     */
    public PutItemResult putItem(final PutItemRequest putItemRequest) {
        return dynamoDB.putItem(putItemRequest);
    }

    // There is no too much benefit to add method for "Object entity"
    /**
     *
     * @param tableName
     * @param entity
     * @return
     */
    // And it may cause error because the "Object" is ambiguous to any type.
    PutItemResult putItem(final String tableName, final Object entity) {
        return putItem(tableName, toItem(entity));
    }

    /**
     *
     * @param tableName
     * @param entity
     * @param returnValues
     * @return
     */
    PutItemResult putItem(final String tableName, final Object entity, final String returnValues) {
        return putItem(tableName, toItem(entity), returnValues);
    }

    /**
     * Batch write item.
     *
     * @param requestItems
     * @return
     */
    public BatchWriteItemResult batchWriteItem(final Map<String, List<WriteRequest>> requestItems) {
        return dynamoDB.batchWriteItem(requestItems);
    }

    /**
     * Batch write item.
     *
     * @param batchWriteItemRequest
     * @return
     */
    public BatchWriteItemResult batchWriteItem(final BatchWriteItemRequest batchWriteItemRequest) {
        return dynamoDB.batchWriteItem(batchWriteItemRequest);
    }

    /**
     *
     * @param tableName
     * @param key
     * @param attributeUpdates
     * @return
     */
    public UpdateItemResult updateItem(final String tableName, final Map<String, AttributeValue> key,
            final Map<String, AttributeValueUpdate> attributeUpdates) {
        return dynamoDB.updateItem(tableName, key, attributeUpdates);
    }

    /**
     *
     * @param tableName
     * @param key
     * @param attributeUpdates
     * @param returnValues
     * @return
     */
    public UpdateItemResult updateItem(final String tableName, final Map<String, AttributeValue> key, final Map<String, AttributeValueUpdate> attributeUpdates,
            final String returnValues) {
        return dynamoDB.updateItem(tableName, key, attributeUpdates, returnValues);
    }

    /**
     *
     * @param updateItemRequest
     * @return
     */
    public UpdateItemResult updateItem(final UpdateItemRequest updateItemRequest) {
        return dynamoDB.updateItem(updateItemRequest);
    }

    /**
     *
     * @param tableName
     * @param key
     * @return
     */
    public DeleteItemResult deleteItem(final String tableName, final Map<String, AttributeValue> key) {
        return dynamoDB.deleteItem(tableName, key);
    }

    /**
     *
     * @param tableName
     * @param key
     * @param returnValues
     * @return
     */
    public DeleteItemResult deleteItem(final String tableName, final Map<String, AttributeValue> key, final String returnValues) {
        return dynamoDB.deleteItem(tableName, key, returnValues);
    }

    /**
     *
     * @param deleteItemRequest
     * @return
     */
    public DeleteItemResult deleteItem(final DeleteItemRequest deleteItemRequest) {
        return dynamoDB.deleteItem(deleteItemRequest);
    }

    /**
     *
     * @param queryRequest
     * @return
     */
    @SuppressWarnings("rawtypes")
    public List<Map<String, Object>> list(final QueryRequest queryRequest) {
        return (List) list(Map.class, queryRequest);
    }

    /**
     *
     * @param <T>
     * @param targetClass <code>Map</code> or entity class with getter/setter method.
     * @param queryRequest
     * @return
     */
    public <T> List<T> list(final Class<T> targetClass, final QueryRequest queryRequest) {
        final QueryResult queryResult = dynamoDB.query(queryRequest);
        final List<T> res = toList(targetClass, queryResult);

        if (N.notNullOrEmpty(queryResult.getLastEvaluatedKey()) && N.isNullOrEmpty(queryRequest.getExclusiveStartKey())) {
            final QueryRequest newQueryRequest = queryRequest.clone();
            QueryResult newQueryResult = queryResult;

            while (N.notNullOrEmpty(newQueryResult.getLastEvaluatedKey())) {
                newQueryRequest.setExclusiveStartKey(newQueryResult.getLastEvaluatedKey());
                newQueryResult = dynamoDB.query(newQueryRequest);
                res.addAll(toList(targetClass, newQueryResult));
            }
        }

        return res;
    }

    //    /**
    //     *
    //     * @param targetClass <code>Map</code> or entity class with getter/setter method.
    //     * @param queryRequest
    //     * @param pageOffset
    //     * @param pageCount
    //     * @return
    //     * @see <a href="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Pagination">Query.Pagination</a>
    //     */
    //    public <T> List<T> list(final Class<T> targetClass, final QueryRequest queryRequest, int pageOffset, int pageCount) {
    //        N.checkArgument(pageOffset >= 0 && pageCount >= 0, "'pageOffset' and 'pageCount' can't be negative");
    //
    //        final List<T> res = new ArrayList<>();
    //        QueryRequest newQueryRequest = queryRequest;
    //        QueryResult queryResult = null;
    //
    //        do {
    //            if (queryResult != null && N.notNullOrEmpty(queryResult.getLastEvaluatedKey())) {
    //                if (newQueryRequest == queryRequest) {
    //                    newQueryRequest = queryRequest.clone();
    //                }
    //
    //                newQueryRequest.setExclusiveStartKey(queryResult.getLastEvaluatedKey());
    //            }
    //
    //            queryResult = dynamoDB.query(newQueryRequest);
    //        } while (pageOffset-- > 0 && N.notNullOrEmpty(queryResult.getItems()) && N.notNullOrEmpty(queryResult.getLastEvaluatedKey()));
    //
    //        if (pageOffset >= 0 || pageCount-- <= 0) {
    //            return res;
    //        } else {
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        while (pageCount-- > 0 && N.notNullOrEmpty(queryResult.getLastEvaluatedKey())) {
    //            if (newQueryRequest == queryRequest) {
    //                newQueryRequest = queryRequest.clone();
    //            }
    //
    //            newQueryRequest.setExclusiveStartKey(queryResult.getLastEvaluatedKey());
    //            queryResult = dynamoDB.query(newQueryRequest);
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        return res;
    //    }

    /**
     *
     * @param queryRequest
     * @return
     * @see #list(QueryRequest)
     */
    public DataSet query(final QueryRequest queryRequest) {
        return query(Map.class, queryRequest);
    }

    /**
     *
     * @param targetClass
     * @param queryRequest
     * @return
     * @see #list(Class, QueryRequest)
     */
    public DataSet query(final Class<?> targetClass, final QueryRequest queryRequest) {
        if (targetClass == null || Map.class.isAssignableFrom(targetClass)) {
            final QueryResult queryResult = dynamoDB.query(queryRequest);
            final List<Map<String, AttributeValue>> items = queryResult.getItems();

            if (N.notNullOrEmpty(queryResult.getLastEvaluatedKey()) && N.isNullOrEmpty(queryRequest.getExclusiveStartKey())) {
                final QueryRequest newQueryRequest = queryRequest.clone();
                QueryResult newQueryResult = queryResult;

                while (N.notNullOrEmpty(newQueryResult.getLastEvaluatedKey())) {
                    newQueryRequest.setExclusiveStartKey(newQueryResult.getLastEvaluatedKey());
                    newQueryResult = dynamoDB.query(newQueryRequest);
                    items.addAll(newQueryResult.getItems());
                }
            }

            return extractData(items, 0, items.size());
        } else {
            return N.newDataSet(list(targetClass, queryRequest));
        }
    }

    //    /**
    //     *
    //     * @param targetClass
    //     * @param queryRequest
    //     * @param pageOffset
    //     * @param pageCount
    //     * @return
    //     * @see #find(Class, QueryRequest, int, int)
    //     */
    //    public DataSet query(final Class<?> targetClass, final QueryRequest queryRequest, int pageOffset, int pageCount) {
    //        return N.newDataSet(find(targetClass, queryRequest, pageOffset, pageCount));
    //    }

    //    public <T> List<T> scan(final Class<T> targetClass, final ScanRequest scanRequest, int pageOffset, int pageCount) {
    //        N.checkArgument(pageOffset >= 0 && pageCount >= 0, "'pageOffset' and 'pageCount' can't be negative");
    //
    //        final List<T> res = new ArrayList<>();
    //        ScanRequest newQueryRequest = scanRequest;
    //        ScanResult queryResult = null;
    //
    //        do {
    //            if (queryResult != null && N.notNullOrEmpty(queryResult.getLastEvaluatedKey())) {
    //                if (newQueryRequest == scanRequest) {
    //                    newQueryRequest = scanRequest.clone();
    //                }
    //
    //                newQueryRequest.setExclusiveStartKey(queryResult.getLastEvaluatedKey());
    //            }
    //
    //            queryResult = dynamoDB.scan(newQueryRequest);
    //        } while (pageOffset-- > 0 && N.notNullOrEmpty(queryResult.getItems()) && N.notNullOrEmpty(queryResult.getLastEvaluatedKey()));
    //
    //        if (pageOffset >= 0 || pageCount-- <= 0) {
    //            return res;
    //        } else {
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        while (pageCount-- > 0 && N.notNullOrEmpty(queryResult.getLastEvaluatedKey())) {
    //            if (newQueryRequest == scanRequest) {
    //                newQueryRequest = scanRequest.clone();
    //            }
    //
    //            newQueryRequest.setExclusiveStartKey(queryResult.getLastEvaluatedKey());
    //            queryResult = dynamoDB.scan(newQueryRequest);
    //            res.addAll(toList(targetClass, queryResult));
    //        }
    //
    //        return res;
    //    }

    /**
     *
     * @param queryRequest
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Stream<Map<String, Object>> stream(final QueryRequest queryRequest) {
        return (Stream) stream(Map.class, queryRequest);
    }

    /**
     *
     * @param <T>
     * @param targetClass <code>Map</code> or entity class with getter/setter method.
     * @param queryRequest
     * @return
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final QueryRequest queryRequest) {
        final QueryResult queryResult = dynamoDB.query(queryRequest);

        final Iterator<Map<String, AttributeValue>> iterator = new ObjIterator<Map<String, AttributeValue>>() {
            private Iterator<Map<String, AttributeValue>> iter = iterate(queryResult.getItems());
            private Map<String, AttributeValue> lastEvaluatedKey = null;
            private QueryRequest newQueryRequest = null;

            {
                if (N.notNullOrEmpty(queryResult.getLastEvaluatedKey()) && N.isNullOrEmpty(queryRequest.getExclusiveStartKey())) {
                    lastEvaluatedKey = queryResult.getLastEvaluatedKey();
                    newQueryRequest = queryRequest.clone();
                }
            }

            @Override
            public boolean hasNext() {
                if (iter == null || iter.hasNext() == false) {
                    while ((iter == null || iter.hasNext() == false) && N.notNullOrEmpty(lastEvaluatedKey)) {
                        newQueryRequest.setExclusiveStartKey(lastEvaluatedKey);
                        QueryResult newQueryResult = dynamoDB.query(newQueryRequest);
                        lastEvaluatedKey = newQueryResult.getLastEvaluatedKey();
                        iter = iterate(newQueryResult.getItems());
                    }
                }

                return iter != null && iter.hasNext();
            }

            @Override
            public Map<String, AttributeValue> next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }

                return iter.next();
            }
        };

        final Type<T> targetType = N.typeOf(targetClass);

        return Stream.of(iterator).map(new Function<Map<String, AttributeValue>, T>() {
            @Override
            public T apply(Map<String, AttributeValue> t) {
                return toValue(targetType, targetClass, t);
            }
        });
    }

    /**
     *
     * @param tableName
     * @param attributesToGet
     * @return
     */
    public Stream<Map<String, Object>> scan(final String tableName, final List<String> attributesToGet) {
        return scan(new ScanRequest().withTableName(tableName).withAttributesToGet(attributesToGet));
    }

    /**
     *
     * @param tableName
     * @param scanFilter
     * @return
     */
    public Stream<Map<String, Object>> scan(final String tableName, final Map<String, Condition> scanFilter) {
        return scan(new ScanRequest().withTableName(tableName).withScanFilter(scanFilter));
    }

    /**
     *
     * @param tableName
     * @param attributesToGet
     * @param scanFilter
     * @return
     */
    public Stream<Map<String, Object>> scan(final String tableName, final List<String> attributesToGet, final Map<String, Condition> scanFilter) {
        return scan(new ScanRequest().withTableName(tableName).withAttributesToGet(attributesToGet).withScanFilter(scanFilter));
    }

    /**
     *
     * @param scanRequest
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Stream<Map<String, Object>> scan(final ScanRequest scanRequest) {
        return (Stream) scan(Map.class, scanRequest);
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param attributesToGet
     * @return
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final List<String> attributesToGet) {
        return scan(targetClass, new ScanRequest().withTableName(tableName).withAttributesToGet(attributesToGet));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param scanFilter
     * @return
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final Map<String, Condition> scanFilter) {
        return scan(targetClass, new ScanRequest().withTableName(tableName).withScanFilter(scanFilter));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param tableName
     * @param attributesToGet
     * @param scanFilter
     * @return
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final String tableName, final List<String> attributesToGet, final Map<String, Condition> scanFilter) {
        return scan(targetClass, new ScanRequest().withTableName(tableName).withAttributesToGet(attributesToGet).withScanFilter(scanFilter));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param scanRequest
     * @return
     */
    public <T> Stream<T> scan(final Class<T> targetClass, final ScanRequest scanRequest) {
        final ScanResult scanResult = dynamoDB.scan(scanRequest);

        final Iterator<Map<String, AttributeValue>> iterator = new ObjIterator<Map<String, AttributeValue>>() {
            private Iterator<Map<String, AttributeValue>> iter = iterate(scanResult.getItems());
            private Map<String, AttributeValue> lastEvaluatedKey = null;
            private ScanRequest newScanRequest = null;

            {
                if (N.notNullOrEmpty(scanResult.getLastEvaluatedKey()) && N.isNullOrEmpty(scanRequest.getExclusiveStartKey())) {
                    lastEvaluatedKey = scanResult.getLastEvaluatedKey();
                    newScanRequest = scanRequest.clone();
                }
            }

            @Override
            public boolean hasNext() {
                if (iter == null || iter.hasNext() == false) {
                    while ((iter == null || iter.hasNext() == false) && N.notNullOrEmpty(lastEvaluatedKey)) {
                        newScanRequest.setExclusiveStartKey(lastEvaluatedKey);
                        ScanResult newScanResult = dynamoDB.scan(newScanRequest);
                        lastEvaluatedKey = newScanResult.getLastEvaluatedKey();
                        iter = iterate(newScanResult.getItems());
                    }
                }

                return iter != null && iter.hasNext();
            }

            @Override
            public Map<String, AttributeValue> next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }

                return iter.next();
            }
        };

        final Type<T> targetType = N.typeOf(targetClass);

        return Stream.of(iterator).map(new Function<Map<String, AttributeValue>, T>() {
            @Override
            public T apply(Map<String, AttributeValue> t) {
                return toValue(targetType, targetClass, t);
            }
        });
    }

    /**
     *
     * @param items
     * @return
     */
    private static Iterator<Map<String, AttributeValue>> iterate(final List<Map<String, AttributeValue>> items) {
        return N.isNullOrEmpty(items) ? ObjIterator.<Map<String, AttributeValue>> empty() : items.iterator();
    }

    /**
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public void close() throws IOException {
        dynamoDB.shutdown();
    }

    /**
     *
     * @param <T> target entity type.
     */
    public static class Mapper<T> {
        private final DynamoDBExecutor dynamoDBExecutor;
        private final String tableName;
        private final Class<T> targetEntityClass;
        private final EntityInfo entityInfo;
        private final List<String> keyPropNames;
        private final List<PropInfo> keyPropInfos;
        private final NamingPolicy namingPolicy;

        Mapper(final Class<T> targetEntityClass, final DynamoDBExecutor dynamoDBExecutor, final String tableName, final NamingPolicy namingPolicy) {
            N.checkArgNotNull(targetEntityClass, "targetEntityClass");
            N.checkArgNotNull(dynamoDBExecutor, "dynamoDBExecutor");
            N.checkArgNotNullOrEmpty(tableName, "tableName");

            N.checkArgument(ClassUtil.isEntity(targetEntityClass), "{} is not an entity class with getter/setter method", targetEntityClass);

            @SuppressWarnings("deprecation")
            final List<String> idPropNames = ClassUtil.getIdFieldNames(targetEntityClass);

            if (idPropNames.size() != 1) {
                throw new IllegalArgumentException(
                        "No or multiple ids: " + idPropNames + " defined/annotated in class: " + ClassUtil.getCanonicalClassName(targetEntityClass));
            }

            this.dynamoDBExecutor = dynamoDBExecutor;
            this.targetEntityClass = targetEntityClass;
            this.tableName = tableName;
            this.entityInfo = ParserUtil.getEntityInfo(targetEntityClass);
            this.keyPropInfos = Stream.of(idPropNames).map(it -> entityInfo.getPropInfo(it)).toList();
            this.keyPropNames = Stream.of(keyPropInfos).map(it -> N.isNullOrEmpty(it.columnName.orNull()) ? it.name : it.columnName.orNull()).toList();

            this.namingPolicy = namingPolicy == null ? NamingPolicy.LOWER_CAMEL_CASE : namingPolicy;
        }

        public T getItem(final T entity) {
            return dynamoDBExecutor.getItem(targetEntityClass, tableName, createKey(entity));
        }

        public T getItem(final T entity, final Boolean consistentRead) {
            return dynamoDBExecutor.getItem(targetEntityClass, tableName, createKey(entity), consistentRead);
        }

        public T getItem(final Map<String, AttributeValue> key) {
            return dynamoDBExecutor.getItem(targetEntityClass, tableName, key);
        }

        public T getItem(final GetItemRequest getItemRequest) {
            return dynamoDBExecutor.getItem(targetEntityClass, checkItem(getItemRequest));
        }

        public List<T> batchGetItem(final Collection<? extends T> entities) {
            final Map<String, List<T>> map = dynamoDBExecutor.batchGetItem(targetEntityClass, createKeys(entities));

            if (N.isNullOrEmpty(map)) {
                return new ArrayList<>();
            } else {
                return map.values().iterator().next();
            }
        }

        public List<T> batchGetItem(final Collection<? extends T> entities, final String returnConsumedCapacity) {
            final Map<String, List<T>> map = dynamoDBExecutor.batchGetItem(targetEntityClass, createKeys(entities), returnConsumedCapacity);

            if (N.isNullOrEmpty(map)) {
                return new ArrayList<>();
            } else {
                return map.values().iterator().next();
            }
        }

        public List<T> batchGetItem(final BatchGetItemRequest batchGetItemRequest) {
            final Map<String, List<T>> map = dynamoDBExecutor.batchGetItem(targetEntityClass, checkItem(batchGetItemRequest));

            if (N.isNullOrEmpty(map)) {
                return new ArrayList<>();
            } else {
                return map.values().iterator().next();
            }
        }

        public PutItemResult putItem(final T entity) {
            return dynamoDBExecutor.putItem(tableName, DynamoDBExecutor.toItem(entity, namingPolicy));
        }

        public PutItemResult putItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.putItem(tableName, DynamoDBExecutor.toItem(entity, namingPolicy), returnValues);
        }

        public PutItemResult putItem(final PutItemRequest putItemRequest) {
            return dynamoDBExecutor.putItem(checkItem(putItemRequest));
        }

        public BatchWriteItemResult batchPutItem(final Collection<? extends T> entities) {
            return dynamoDBExecutor.batchWriteItem(createBatchPutRequest(entities));
        }

        public UpdateItemResult updateItem(final T entity) {
            return dynamoDBExecutor.updateItem(tableName, createKey(entity), DynamoDBExecutor.toUpdateItem(entity, namingPolicy));
        }

        public UpdateItemResult updateItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.updateItem(tableName, createKey(entity), DynamoDBExecutor.toUpdateItem(entity, namingPolicy), returnValues);
        }

        public UpdateItemResult updateItem(final UpdateItemRequest updateItemRequest) {
            return dynamoDBExecutor.updateItem(checkItem(updateItemRequest));
        }

        public DeleteItemResult deleteItem(final T entity) {
            return dynamoDBExecutor.deleteItem(tableName, createKey(entity));
        }

        public DeleteItemResult deleteItem(final T entity, final String returnValues) {
            return dynamoDBExecutor.deleteItem(tableName, createKey(entity), returnValues);
        }

        public DeleteItemResult deleteItem(final Map<String, AttributeValue> key) {
            return dynamoDBExecutor.deleteItem(tableName, key);
        }

        public DeleteItemResult deleteItem(final DeleteItemRequest deleteItemRequest) {
            return dynamoDBExecutor.deleteItem(checkItem(deleteItemRequest));
        }

        public BatchWriteItemResult batchDeleteItem(final Collection<? extends T> entities) {
            return dynamoDBExecutor.batchWriteItem(createBatchDeleteRequest(entities));
        }

        public BatchWriteItemResult batchWriteItem(final BatchWriteItemRequest batchWriteItemRequest) {
            return dynamoDBExecutor.batchWriteItem(checkItem(batchWriteItemRequest));
        }

        public List<T> list(final QueryRequest queryRequest) {
            return dynamoDBExecutor.list(targetEntityClass, checkQueryRequest(queryRequest));
        }

        public DataSet query(final QueryRequest queryRequest) {
            return dynamoDBExecutor.query(targetEntityClass, checkQueryRequest(queryRequest));
        }

        public Stream<T> stream(final QueryRequest queryRequest) {
            return dynamoDBExecutor.stream(targetEntityClass, checkQueryRequest(queryRequest));
        }

        public Stream<T> scan(final List<String> attributesToGet) {
            return dynamoDBExecutor.scan(targetEntityClass, tableName, attributesToGet);
        }

        public Stream<T> scan(final Map<String, Condition> scanFilter) {
            return dynamoDBExecutor.scan(targetEntityClass, tableName, scanFilter);
        }

        public Stream<T> scan(final List<String> attributesToGet, final Map<String, Condition> scanFilter) {
            return dynamoDBExecutor.scan(targetEntityClass, tableName, attributesToGet, scanFilter);
        }

        public Stream<T> scan(final ScanRequest scanRequest) {
            return dynamoDBExecutor.scan(targetEntityClass, checkScanRequest(scanRequest));
        }

        private Map<String, AttributeValue> createKey(final T entity) {
            final Map<String, AttributeValue> key = new HashMap<>(keyPropNames.size());

            for (int i = 0, len = keyPropNames.size(); i < len; i++) {
                key.put(keyPropNames.get(i), attrValueOf(keyPropInfos.get(i).getPropValue(entity)));
            }

            return key;
        }

        private Map<String, KeysAndAttributes> createKeys(final Collection<? extends T> entities) {
            final List<Map<String, AttributeValue>> keys = new ArrayList<>(entities.size());

            for (T entity : entities) {
                keys.add(asKey(entity));
            }

            return N.asMap(tableName, new KeysAndAttributes().withKeys(keys));
        }

        private Map<String, List<WriteRequest>> createBatchPutRequest(final Collection<? extends T> entities) {
            final List<WriteRequest> keys = new ArrayList<>(entities.size());

            for (T entity : entities) {
                keys.add(new WriteRequest().withPutRequest(new PutRequest().withItem(toItem(entity))));
            }

            return N.asMap(tableName, keys);
        }

        private Map<String, List<WriteRequest>> createBatchDeleteRequest(final Collection<? extends T> entities) {
            final List<WriteRequest> keys = new ArrayList<>(entities.size());

            for (T entity : entities) {
                keys.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(createKey(entity))));
            }

            return N.asMap(tableName, keys);
        }

        private GetItemRequest checkItem(GetItemRequest item) {
            if (N.isNullOrEmpty(item.getTableName())) {
                item.setTableName(tableName);
            } else {
                checkTableName(item.getTableName());
            }

            return item;
        }

        private BatchGetItemRequest checkItem(BatchGetItemRequest item) {
            for (String tableNameInRequest : item.getRequestItems().keySet()) {
                checkTableName(tableNameInRequest);
            }

            return item;

        }

        private BatchWriteItemRequest checkItem(BatchWriteItemRequest item) {
            for (String tableNameInRequest : item.getRequestItems().keySet()) {
                checkTableName(tableNameInRequest);
            }

            return item;
        }

        private PutItemRequest checkItem(PutItemRequest item) {
            if (N.isNullOrEmpty(item.getTableName())) {
                item.setTableName(tableName);
            } else {
                checkTableName(item.getTableName());
            }

            return item;
        }

        private UpdateItemRequest checkItem(UpdateItemRequest item) {
            if (N.isNullOrEmpty(item.getTableName())) {
                item.setTableName(tableName);
            } else {
                checkTableName(item.getTableName());
            }

            return item;
        }

        private DeleteItemRequest checkItem(DeleteItemRequest item) {
            if (N.isNullOrEmpty(item.getTableName())) {
                item.setTableName(tableName);
            } else {
                checkTableName(item.getTableName());
            }

            return item;
        }

        private QueryRequest checkQueryRequest(final QueryRequest queryRequest) {
            if (N.isNullOrEmpty(queryRequest.getTableName())) {
                queryRequest.setTableName(tableName);
            } else {
                checkTableName(queryRequest.getTableName());
            }

            return queryRequest;
        }

        private ScanRequest checkScanRequest(final ScanRequest scanRequest) {
            if (N.isNullOrEmpty(scanRequest.getTableName())) {
                scanRequest.setTableName(tableName);
            } else {
                checkTableName(scanRequest.getTableName());
            }

            return scanRequest;
        }

        private void checkTableName(final String tableNameInRequest) {
            if (!tableName.equals(tableNameInRequest)) {
                throw new IllegalArgumentException("The table name: " + tableNameInRequest
                        + " in the specfied request is not equal to the table name defined for the Mapper: " + tableName);
            }
        }
    }

    public static final class Filters {
        private Filters() {
            // singleton for Utility class
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public static Map<String, Condition> eq(String attrName, Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public static Map<String, Condition> ne(String attrName, Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.NE).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public static Map<String, Condition> gt(String attrName, Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.GT).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public static Map<String, Condition> ge(String attrName, Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.GE).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public static Map<String, Condition> lt(String attrName, Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.LT).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public static Map<String, Condition> le(String attrName, Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.LE).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         *
         * @param attrName
         * @param minAttrValue
         * @param maxAttrValue
         * @return
         */
        public static Map<String, Condition> bt(String attrName, Object minAttrValue, Object maxAttrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(attrValueOf(minAttrValue), attrValueOf(maxAttrValue)));
        }

        /**
         * Checks if is null.
         *
         * @param attrName
         * @return
         */
        public static Map<String, Condition> isNull(String attrName) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.NULL));
        }

        /**
         *
         * @param attrName
         * @return
         */
        public static Map<String, Condition> notNull(String attrName) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.NOT_NULL));
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public static Map<String, Condition> contains(String attrName, Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.CONTAINS).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public static Map<String, Condition> notContains(String attrName, Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.NOT_CONTAINS).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public static Map<String, Condition> beginsWith(String attrName, Object attrValue) {
            return N.asMap(attrName, new Condition().withComparisonOperator(ComparisonOperator.BEGINS_WITH).withAttributeValueList(attrValueOf(attrValue)));
        }

        /**
         *
         * @param attrName
         * @param attrValues
         * @return
         */
        @SafeVarargs
        public static Map<String, Condition> in(String attrName, Object... attrValues) {
            final Map<String, Condition> result = new HashMap<>(1);

            in(result, attrName, attrValues);

            return result;
        }

        /**
         *
         * @param attrName
         * @param attrValues
         * @return
         */
        public static Map<String, Condition> in(String attrName, Collection<?> attrValues) {
            final Map<String, Condition> result = new HashMap<>(1);

            in(result, attrName, attrValues);

            return result;
        }

        static void in(Map<String, Condition> output, String attrName, Object... attrValues) {
            final AttributeValue[] attributeValueList = new AttributeValue[attrValues.length];

            for (int i = 0, len = attrValues.length; i < len; i++) {
                attributeValueList[i] = attrValueOf(attrValues[i]);
            }

            final Condition cond = new Condition().withComparisonOperator(ComparisonOperator.IN).withAttributeValueList(attributeValueList);

            output.put(attrName, cond);
        }

        static void in(Map<String, Condition> output, String attrName, Collection<?> attrValues) {
            final AttributeValue[] attributeValueList = new AttributeValue[attrValues.size()];

            int i = 0;
            for (Object attrValue : attrValues) {
                attributeValueList[i++] = attrValueOf(attrValue);
            }

            final Condition cond = new Condition().withComparisonOperator(ComparisonOperator.IN).withAttributeValueList(attributeValueList);

            output.put(attrName, cond);
        }

        public static ConditionBuilder builder() {
            return new ConditionBuilder();
        }
    }

    public static final class ConditionBuilder {
        private Map<String, Condition> condMap;

        ConditionBuilder() {
            condMap = new HashMap<>();
        }

        /**
         * 
         * @return
         * @deprecated replaced with {@link Filters#builder()}
         */
        @Deprecated
        public static ConditionBuilder create() {
            return new ConditionBuilder();
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public ConditionBuilder eq(String attrName, Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public ConditionBuilder ne(String attrName, Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.NE).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public ConditionBuilder gt(String attrName, Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.GT).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public ConditionBuilder ge(String attrName, Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.GE).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public ConditionBuilder lt(String attrName, Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.LT).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public ConditionBuilder le(String attrName, Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.LE).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         *
         * @param attrName
         * @param minAttrValue
         * @param maxAttrValue
         * @return
         */
        public ConditionBuilder bt(String attrName, Object minAttrValue, Object maxAttrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(attrValueOf(minAttrValue), attrValueOf(maxAttrValue)));

            return this;
        }

        /**
         * Checks if is null.
         *
         * @param attrName
         * @return
         */
        public ConditionBuilder isNull(String attrName) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.NULL));

            return this;
        }

        /**
         *
         * @param attrName
         * @return
         */
        public ConditionBuilder notNull(String attrName) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.NOT_NULL));

            return this;
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public ConditionBuilder contains(String attrName, Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.CONTAINS).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public ConditionBuilder notContains(String attrName, Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.NOT_CONTAINS).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         *
         * @param attrName
         * @param attrValue
         * @return
         */
        public ConditionBuilder beginsWith(String attrName, Object attrValue) {
            condMap.put(attrName, new Condition().withComparisonOperator(ComparisonOperator.BEGINS_WITH).withAttributeValueList(attrValueOf(attrValue)));

            return this;
        }

        /**
         *
         * @param attrName
         * @param attrValues
         * @return
         */
        public ConditionBuilder in(String attrName, Object... attrValues) {
            Filters.in(condMap, attrName, attrValues);

            return this;
        }

        /**
         *
         * @param attrName
         * @param attrValues
         * @return
         */
        public ConditionBuilder in(String attrName, Collection<?> attrValues) {
            Filters.in(condMap, attrName, attrValues);

            return this;
        }

        public Map<String, Condition> build() {
            final Map<String, Condition> result = condMap;

            condMap = null;

            return result;
        }
    }
}
