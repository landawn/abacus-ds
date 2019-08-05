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

package com.landawn.abacus.da.couchbase;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.landawn.abacus.DataSet;
import com.landawn.abacus.DirtyMarker;
import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.core.DirtyMarkerUtil;
import com.landawn.abacus.core.RowDataSet;
import com.landawn.abacus.exception.AbacusException;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.PoolableWrapper;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.Iterables;
import com.landawn.abacus.util.Maps;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamedSQL;
import com.landawn.abacus.util.SQLMapper;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.u.OptionalBoolean;
import com.landawn.abacus.util.u.OptionalByte;
import com.landawn.abacus.util.u.OptionalChar;
import com.landawn.abacus.util.u.OptionalDouble;
import com.landawn.abacus.util.u.OptionalFloat;
import com.landawn.abacus.util.u.OptionalInt;
import com.landawn.abacus.util.u.OptionalLong;
import com.landawn.abacus.util.u.OptionalShort;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.function.ToBooleanFunction;
import com.landawn.abacus.util.function.ToByteFunction;
import com.landawn.abacus.util.function.ToCharFunction;
import com.landawn.abacus.util.function.ToDoubleFunction;
import com.landawn.abacus.util.function.ToFloatFunction;
import com.landawn.abacus.util.function.ToIntFunction;
import com.landawn.abacus.util.function.ToLongFunction;
import com.landawn.abacus.util.function.ToShortFunction;
import com.landawn.abacus.util.stream.Stream;

// TODO: Auto-generated Javadoc
/**
 * It's a simple wrapper of Couchbase Java client.
 * Raw/ibatis(myBatis)/Couchbase style parameterized sql are supported. The parameters can be array/list/map/entity/JsonArray/JsonObject:
 * <li> row parameterized sql with question mark: <code>SELECT * FROM account WHERE accountId = ?</li>.
 * <li> ibatis/myBatis parameterized sql with named parameter: <code>SELECT * FROM account WHERE accountId = #{accountId}</li>.
 * <li> Couchbase parameterized sql with named parameter: <code>SELECT * FROM account WHERE accountId = $accountId</code> or <code>SELECT * FROM account WHERE accountId = $1</li>.
 * 
 * <br>
 * <br>We recommend to define "id" property in java entity/bean as the object id in Couchbase to keep things as simple as possible.</br>
 * <br>
 *
 * @author Haiyang Li
 * @since 0.8
 */
public final class CouchbaseExecutor implements Closeable {
    /**
     * It's name of object id set in Map/Object array.
     */
    public static final String _ID = "_id";

    /**
     * Property name of id.
     */
    public static final String ID = "id";

    /** The Constant POOLABLE_LENGTH. */
    static final int POOLABLE_LENGTH = 1024;

    /** The Constant supportedTypes. */
    static final Set<Class<?>> supportedTypes = new HashSet<>();

    static {
        supportedTypes.add(Boolean.class);
        supportedTypes.add(Integer.class);
        supportedTypes.add(Long.class);
        supportedTypes.add(Double.class);
        supportedTypes.add(String.class);
        supportedTypes.add(JsonObject.class);
        supportedTypes.add(JsonArray.class);
    }

    /** The Constant classIdSetMethodPool. */
    private static final Map<Class<?>, Method> classIdSetMethodPool = new ConcurrentHashMap<>();

    /** The Constant bucketIdNamePool. */
    private static final Map<String, String> bucketIdNamePool = new ConcurrentHashMap<>();

    /** The stmt pool. */
    private final KeyedObjectPool<String, PoolableWrapper<N1qlQuery>> stmtPool = PoolFactory.createKeyedObjectPool(1024, 3000);
    // private final KeyedObjectPool<String, Wrapper<N1qlQueryPlan>> preStmtPool = PoolFactory.createKeyedObjectPool(1024, 3000);

    /** The cluster. */
    private final Cluster cluster;

    /** The bucket. */
    private final Bucket bucket;

    /** The sql mapper. */
    private final SQLMapper sqlMapper;

    /** The async executor. */
    private final AsyncExecutor asyncExecutor;

    /**
     * Instantiates a new couchbase executor.
     *
     * @param cluster the cluster
     */
    public CouchbaseExecutor(Cluster cluster) {
        this(cluster, cluster.openBucket());
    }

    /**
     * Instantiates a new couchbase executor.
     *
     * @param cluster the cluster
     * @param bucket the bucket
     */
    public CouchbaseExecutor(Cluster cluster, Bucket bucket) {
        this(cluster, bucket, null);
    }

    /**
     * Instantiates a new couchbase executor.
     *
     * @param cluster the cluster
     * @param bucket the bucket
     * @param sqlMapper the sql mapper
     */
    public CouchbaseExecutor(Cluster cluster, Bucket bucket, final SQLMapper sqlMapper) {
        this(cluster, bucket, sqlMapper, new AsyncExecutor(8, 64, 180L, TimeUnit.SECONDS));
    }

    /**
     * Instantiates a new couchbase executor.
     *
     * @param cluster the cluster
     * @param bucket the bucket
     * @param sqlMapper the sql mapper
     * @param asyncExecutor the async executor
     */
    public CouchbaseExecutor(Cluster cluster, Bucket bucket, final SQLMapper sqlMapper, final AsyncExecutor asyncExecutor) {
        this.cluster = cluster;
        this.bucket = bucket;
        this.sqlMapper = sqlMapper;
        this.asyncExecutor = asyncExecutor;
    }

    /**
     * Cluster.
     *
     * @return the cluster
     */
    public Cluster cluster() {
        return cluster;
    }

    /**
     * Bucket.
     *
     * @return the bucket
     */
    public Bucket bucket() {
        return bucket;
    }

    /**
     * The object id ("_id") property will be read from/write to the specified property .
     *
     * @param cls the cls
     * @param idPropertyName the id property name
     */
    public static void registerIdProperty(Class<?> cls, String idPropertyName) {
        if (ClassUtil.getPropGetMethod(cls, idPropertyName) == null || ClassUtil.getPropSetMethod(cls, idPropertyName) == null) {
            throw new IllegalArgumentException("The specified class: " + ClassUtil.getCanonicalClassName(cls)
                    + " doesn't have getter or setter method for the specified id propery: " + idPropertyName);
        }

        final Method setMethod = ClassUtil.getPropSetMethod(cls, idPropertyName);
        final Class<?> parameterType = setMethod.getParameterTypes()[0];

        if (!(String.class.isAssignableFrom(parameterType) || long.class.isAssignableFrom(parameterType) || Long.class.isAssignableFrom(parameterType))) {
            throw new IllegalArgumentException(
                    "The parameter type of the specified id setter method must be 'String' or long/Long: " + setMethod.toGenericString());
        }

        classIdSetMethodPool.put(cls, setMethod);
    }

    /**
     * Extract data.
     *
     * @param resultSet the result set
     * @return the data set
     */
    public static DataSet extractData(final N1qlQueryResult resultSet) {
        return extractData(Map.class, resultSet);
    }

    /**
     * Extract data.
     *
     * @param targetClass an entity class with getter/setter method or <code>Map.class</code>
     * @param resultSet the result set
     * @return the data set
     */
    public static DataSet extractData(final Class<?> targetClass, final N1qlQueryResult resultSet) {
        checkResultError(resultSet);
        checkTargetClass(targetClass);

        final List<N1qlQueryRow> allRows = resultSet.allRows();

        if (N.isNullOrEmpty(allRows)) {
            return N.newEmptyDataSet();
        }

        final Set<String> columnNames = new LinkedHashSet<>();

        for (N1qlQueryRow row : allRows) {
            columnNames.addAll(row.value().getNames());
        }

        final int rowCount = allRows.size();
        final int columnCount = columnNames.size();
        final List<String> columnNameList = new ArrayList<>(columnNames);

        if (Map.class.isAssignableFrom(targetClass)) {
            final List<List<Object>> columnList = new ArrayList<>(columnCount);

            for (int i = 0; i < columnCount; i++) {
                columnList.add(new ArrayList<>(rowCount));
            }

            JsonObject value = null;
            Object propValue = null;

            for (N1qlQueryRow row : allRows) {
                value = row.value();

                for (int i = 0; i < columnCount; i++) {
                    propValue = value.get(columnNameList.get(i));

                    if (propValue instanceof JsonObject) {
                        columnList.get(i).add(((JsonObject) propValue).toMap());
                    } else if (propValue instanceof JsonArray) {
                        columnList.get(i).add(((JsonArray) propValue).toList());
                    } else {
                        columnList.get(i).add(propValue);
                    }
                }
            }

            return new RowDataSet(columnNameList, columnList);
        } else {
            final List<Object> rowList = new ArrayList<>(rowCount);

            for (N1qlQueryRow row : allRows) {
                rowList.add(toEntity(targetClass, row.value()));
            }

            return N.newDataSet(columnNameList, rowList);
        }
    }

    /**
     * To list.
     *
     * @param <T> the generic type
     * @param targetClass an entity class with getter/setter method, <code>Map.class</code> or basic single value type(Primitive/String/Date...)
     * @param resultSet the result set
     * @return the list
     */
    public static <T> List<T> toList(Class<T> targetClass, N1qlQueryResult resultSet) {
        checkResultError(resultSet);

        final Type<T> type = N.typeOf(targetClass);
        final List<N1qlQueryRow> rowList = resultSet.allRows();

        if (N.isNullOrEmpty(rowList)) {
            return new ArrayList<>();
        }

        final List<Object> resultList = new ArrayList<>(rowList.size());

        if (targetClass.isAssignableFrom(JsonObject.class)) {
            for (N1qlQueryRow row : rowList) {
                resultList.add(row.value());
            }
        } else if (type.isEntity() || type.isMap()) {
            for (N1qlQueryRow row : rowList) {
                resultList.add(toEntity(targetClass, row));
            }
        } else {
            final JsonObject first = rowList.get(0).value();

            if (first.getNames().size() <= 2) {
                final String propName = Iterables.findFirst(first.getNames(), Fn.notEqual(_ID)).orElse(_ID);

                if (first.get(propName) != null && targetClass.isAssignableFrom(first.get(propName).getClass())) {
                    for (N1qlQueryRow row : rowList) {
                        resultList.add(row.value().get(propName));
                    }
                } else {
                    for (N1qlQueryRow row : rowList) {
                        resultList.add(N.convert(row.value().get(propName), targetClass));
                    }
                }
            } else {
                throw new IllegalArgumentException(
                        "Can't covert result with columns: " + first.getNames().toString() + " to class: " + ClassUtil.getCanonicalClassName(targetClass));
            }
        }

        return (List<T>) resultList;
    }

    /**
     * To entity.
     *
     * @param <T> the generic type
     * @param targetClass an entity class with getter/setter method or <code>Map.class</code>
     * @param row the row
     * @return the t
     */
    public static <T> T toEntity(final Class<T> targetClass, final N1qlQueryRow row) {
        return toEntity(targetClass, row.value());
    }

    /**
     * The id in the specified <code>jsonDocument</code> will be set to the returned object if and only if the id is not null or empty and the content in <code>jsonDocument</code> doesn't contain any "id" property, and it's acceptable to the <code>targetClass</code>.
     *
     * @param <T> the generic type
     * @param targetClass an entity class with getter/setter method or <code>Map.class</code>
     * @param jsonDocument the json document
     * @return the t
     */
    public static <T> T toEntity(final Class<T> targetClass, final JsonDocument jsonDocument) {
        final T result = toEntity(targetClass, jsonDocument.content());
        final String id = jsonDocument.id();

        if (N.notNullOrEmpty(id) && result != null) {
            if (Map.class.isAssignableFrom(targetClass)) {
                ((Map<String, Object>) result).put(_ID, id);
            } else {
                final Method idSetMethod = getObjectIdSetMethod(targetClass);

                if (idSetMethod != null) {
                    ClassUtil.setPropValue(result, idSetMethod, id);
                }
            }
        }

        return result;
    }

    /**
     * Gets the object id set method.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @return the object id set method
     */
    @SuppressWarnings("deprecation")
    private static <T> Method getObjectIdSetMethod(Class<T> targetClass) {
        Method idSetMethod = classIdSetMethodPool.get(targetClass);

        if (idSetMethod == null) {
            final List<String> idFieldNames = ClassUtil.getIdFieldNames(targetClass);
            Method idPropSetMethod = null;
            Class<?> parameterType = null;

            for (String fieldName : idFieldNames) {
                idPropSetMethod = ClassUtil.getPropSetMethod(targetClass, fieldName);
                parameterType = idPropSetMethod == null ? null : idPropSetMethod.getParameterTypes()[0];

                if (parameterType != null && (String.class.isAssignableFrom(parameterType) || long.class.isAssignableFrom(parameterType)
                        || Long.class.isAssignableFrom(parameterType))) {
                    idSetMethod = idPropSetMethod;

                    break;
                }
            }

            //            Method idPropSetMethod = N.getPropSetMethod(targetClass, ID);
            //            Class<?> parameterType = idPropSetMethod == null ? null : idPropSetMethod.getParameterTypes()[0];
            //
            //            if (parameterType != null && String.class.isAssignableFrom(parameterType)) {
            //                idSetMethod = idPropSetMethod;
            //            }
            //

            if (idSetMethod == null) {
                idSetMethod = ClassUtil.METHOD_MASK;
            }

            classIdSetMethodPool.put(targetClass, idSetMethod);
        }

        return idSetMethod == ClassUtil.METHOD_MASK ? null : idSetMethod;
    }

    /**
     * To entity.
     *
     * @param <T> the generic type
     * @param targetClass an entity class with getter/setter method or <code>Map.class</code>
     * @param jsonObject the json object
     * @return the t
     */
    public static <T> T toEntity(final Class<T> targetClass, final JsonObject jsonObject) {
        checkTargetClass(targetClass);

        if (jsonObject == null) {
            return null;
        }

        if (Map.class.isAssignableFrom(targetClass)) {
            final Map<String, Object> m = jsonObject.toMap();

            if (targetClass.isAssignableFrom(m.getClass())) {
                return (T) m;
            } else {
                final Map<String, Object> result = (Map<String, Object>) N.newInstance(targetClass);
                result.putAll(m);
                return (T) result;
            }
        } else if (ClassUtil.isEntity(targetClass)) {
            final T entity = N.newInstance(targetClass);
            final List<String> columnNameList = new ArrayList<>(jsonObject.getNames());
            Method propSetMethod = null;
            Class<?> parameterType = null;
            Object propValue = null;

            for (String propName : columnNameList) {
                propSetMethod = ClassUtil.getPropSetMethod(targetClass, propName);

                if (propSetMethod == null) {
                    continue;
                }

                parameterType = propSetMethod.getParameterTypes()[0];
                propValue = jsonObject.get(propName);

                if (propValue != null && !parameterType.isAssignableFrom(propValue.getClass())) {
                    if (propValue instanceof JsonObject) {
                        if (Map.class.isAssignableFrom(parameterType) || ClassUtil.isEntity(parameterType)) {
                            ClassUtil.setPropValue(entity, propSetMethod, toEntity(parameterType, (JsonObject) propValue));
                        } else {
                            ClassUtil.setPropValue(entity, propSetMethod, N.valueOf(parameterType, N.stringOf(toEntity(Map.class, (JsonObject) propValue))));
                        }
                    } else if (propValue instanceof JsonArray) {
                        if (List.class.isAssignableFrom(parameterType)) {
                            ClassUtil.setPropValue(entity, propSetMethod, ((JsonArray) propValue).toList());
                        } else {
                            ClassUtil.setPropValue(entity, propSetMethod, N.valueOf(parameterType, N.stringOf(((JsonArray) propValue).toList())));
                        }
                    } else {
                        ClassUtil.setPropValue(entity, propSetMethod, propValue);
                    }
                } else {
                    ClassUtil.setPropValue(entity, propSetMethod, propValue);
                }
            }

            if (DirtyMarkerUtil.isDirtyMarker(entity.getClass())) {
                DirtyMarkerUtil.markDirty((DirtyMarker) entity, false);
            }

            return entity;
        } else if (jsonObject.size() <= 2) {
            final String propName = Iterables.findFirst(jsonObject.getNames(), Fn.notEqual(_ID)).orElse(_ID);

            return N.convert(jsonObject.getObject(propName), targetClass);
        } else {
            throw new IllegalArgumentException("Unsupported target type: " + targetClass);
        }

    }

    /**
     * To JSON.
     *
     * @param jsonArray the json array
     * @return the string
     */
    public static String toJSON(JsonArray jsonArray) {
        return N.toJSON(jsonArray.toList());
    }

    /**
     * To JSON.
     *
     * @param jsonObject the json object
     * @return the string
     */
    public static String toJSON(JsonObject jsonObject) {
        return N.toJSON(jsonObject.toMap());
    }

    /**
     * To JSON.
     *
     * @param jsonDocument the json document
     * @return the string
     */
    public static String toJSON(JsonDocument jsonDocument) {
        final Map<String, Object> m = jsonDocument.content().toMap();

        if (N.notNullOrEmpty(jsonDocument.id())) {
            m.put(_ID, jsonDocument.id());
        }

        return N.toJSON(m);
    }

    /**
     * Returns an instance of the specified target class with the property values from the specified JSON String.
     *
     * @param <T> the generic type
     * @param targetClass <code>JsonArray.class</code>, <code>JsonObject.class</code> or <code>JsonDocument.class</code>
     * @param json the json
     * @return the t
     */
    public static <T> T fromJSON(final Class<T> targetClass, final String json) {
        if (targetClass.equals(JsonObject.class)) {
            return (T) JsonObject.from(N.fromJSON(Map.class, json));
        } else if (targetClass.equals(JsonArray.class)) {
            return (T) JsonArray.from(N.fromJSON(List.class, json));
        } else if (targetClass.equals(JsonDocument.class)) {
            final JsonObject jsonObject = JsonObject.from(N.fromJSON(Map.class, json));
            final String id = N.stringOf(jsonObject.get(_ID));

            jsonObject.removeKey(_ID);

            return (T) JsonDocument.create(id, jsonObject);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(targetClass));
        }
    }

    /**
     * To json object.
     *
     * @param obj an array of pairs of property name and value, or Map<String, Object>, or an entity with getter/setter methods.
     * @return the json object
     */
    public static JsonObject toJsonObject(final Object obj) {
        Map<String, Object> m = null;

        if (obj instanceof Map) {
            m = (Map<String, Object>) obj;
        } else if (ClassUtil.isEntity(obj.getClass())) {
            m = Maps.entity2Map(obj);
        } else if (obj instanceof Object[]) {
            m = N.asProps(obj);
        } else {
            throw new IllegalArgumentException("The parameters must be a Map, or an entity class with getter/setter methods");
        }

        final JsonObject result = JsonObject.create();

        for (Map.Entry<String, Object> entry : m.entrySet()) {
            if (entry.getValue() == null || supportedTypes.contains(entry.getValue().getClass())) {
                result.put(entry.getKey(), entry.getValue());
            } else {
                Type<Object> valueType = N.typeOf(entry.getValue().getClass());

                if (valueType.isMap() || valueType.isEntity()) {
                    result.put(entry.getKey(), toJsonObject(entry.getValue()));
                } else if (valueType.isObjectArray() || valueType.isCollection()) {
                    result.put(entry.getKey(), toJsonArray(entry.getValue()));
                } else {
                    result.put(entry.getKey(), N.stringOf(entry.getValue()));
                }
            }
        }

        return result;
    }

    /**
     * To json object.
     *
     * @param a the a
     * @return the json object
     */
    @SafeVarargs
    public static JsonObject toJsonObject(final Object... a) {
        if (N.isNullOrEmpty(a)) {
            return JsonObject.empty();
        }

        return a.length == 1 ? toJsonObject(a[0]) : toJsonObject((Object) a);
    }

    /**
     * To json array.
     *
     * @param obj the obj
     * @return the json array
     */
    public static JsonArray toJsonArray(final Object obj) {
        final Type<Object> type = N.typeOf(obj.getClass());
        final JsonArray jsonArray = JsonArray.create();

        if (type.isObjectArray()) {
            for (Object e : (Object[]) obj) {
                if (e == null || supportedTypes.contains(e.getClass())) {
                    jsonArray.add(e);
                } else {
                    Type<Object> eType = N.typeOf(e.getClass());

                    if (eType.isMap() || eType.isEntity()) {
                        jsonArray.add(toJsonObject(e));
                    } else if (eType.isObjectArray() || eType.isCollection()) {
                        jsonArray.add(toJsonArray(e));
                    } else {
                        jsonArray.add(N.stringOf(e));
                    }
                }
            }
        } else if (type.isCollection()) {
            for (Object e : (Collection<Object>) obj) {
                if (e == null || supportedTypes.contains(e.getClass())) {
                    jsonArray.add(e);
                } else {
                    Type<Object> eType = N.typeOf(e.getClass());

                    if (eType.isMap() || eType.isEntity()) {
                        jsonArray.add(toJsonObject(e));
                    } else if (eType.isObjectArray() || eType.isCollection()) {
                        jsonArray.add(toJsonArray(e));
                    } else {
                        jsonArray.add(N.stringOf(e));
                    }
                }
            }
        } else {
            jsonArray.add(N.stringOf(obj));
        }

        return jsonArray;
    }

    /**
     * To json array.
     *
     * @param a the a
     * @return the json array
     */
    @SafeVarargs
    public static JsonArray toJsonArray(final Object... a) {
        return N.isNullOrEmpty(a) ? JsonArray.empty() : toJsonArray((Object) a);
    }

    /**
     * The id for the target document is got from the "id" property in the specified <code>obj</code>.
     *
     * @param obj an array of pairs of property name and value, or Map<String, Object>, or an entity with getter/setter methods.
     * @return the json document
     * @throws IllegalArgumentException if the specified <code>obj</code> doesn't have any "id" property.
     */
    public static JsonDocument toJsonDocument(final Object obj) {
        return toJsonDocument(obj, toJsonObject(obj));
    }

    /**
     * The id for the target document is got from the "id" property in the specified <code>a</code>.
     *
     * @param a pairs of property name and value.
     * @return the json document
     * @throws IllegalArgumentException if the specified <code>a</code> doesn't have any "id" property.
     */
    @SafeVarargs
    public static JsonDocument toJsonDocument(final Object... a) {
        return a.length == 1 ? toJsonDocument(a[0], toJsonObject(a[0])) : toJsonDocument(a, toJsonObject(a));
    }

    /**
     * To json document.
     *
     * @param obj the obj
     * @param jsonObject the json object
     * @return the json document
     */
    static JsonDocument toJsonDocument(final Object obj, final JsonObject jsonObject) {
        final Class<?> cls = obj.getClass();
        final Method idSetMethod = getObjectIdSetMethod(obj.getClass());
        final String idPropertyName = ClassUtil.isEntity(cls) ? (idSetMethod == null ? null : ClassUtil.getPropNameByMethod(idSetMethod)) : _ID;

        String id = null;

        if (idPropertyName != null && jsonObject.containsKey(idPropertyName)) {
            id = N.stringOf(jsonObject.get(idPropertyName));

            jsonObject.removeKey(idPropertyName);
        }

        if (N.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("No id property included the specified object: " + N.toString(jsonObject));
        }

        return JsonDocument.create(id, jsonObject);
    }

    /**
     * Id name of.
     *
     * @param bucketName the bucket name
     * @return the string
     */
    public static String idNameOf(final String bucketName) {
        String idName = bucketIdNamePool.get(bucketName);

        if (idName == null) {
            idName = "meta(" + bucketName + ").id".intern();
            bucketIdNamePool.put(bucketName, idName);
        }

        return idName;
    }

    /**
     * Gets the.
     *
     * @param id the id
     * @return the optional
     * @see com.couchbase.client.java.Bucket#get(String)
     */
    public Optional<JsonDocument> get(final String id) {
        return Optional.ofNullable(gett(id));
    }

    /**
     * Gets the.
     *
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the optional
     * @see com.couchbase.client.java.Bucket#get(String, long, TimeUnit)
     */
    public Optional<JsonDocument> get(final String id, final long timeout, final TimeUnit timeUnit) {
        return Optional.ofNullable(gett(id, timeout, timeUnit));
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @return the optional
     * @see com.couchbase.client.java.Bucket#get(String, Class)
     */
    public <T> Optional<T> get(final Class<T> targetClass, final String id) {
        return Optional.ofNullable(gett(targetClass, id));
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the optional
     * @see com.couchbase.client.java.Bucket#get(String, Class, long, TimeUnit)
     */
    public <T> Optional<T> get(final Class<T> targetClass, final String id, final long timeout, final TimeUnit timeUnit) {
        return Optional.ofNullable(gett(targetClass, id, timeout, timeUnit));
    }

    /**
     * Gets the t.
     *
     * @param id the id
     * @return the t
     * @see com.couchbase.client.java.Bucket#get(String)
     */
    public JsonDocument gett(final String id) {
        return bucket.get(id);
    }

    /**
     * Gets the t.
     *
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the t
     * @see com.couchbase.client.java.Bucket#get(String, long, TimeUnit)
     */
    public JsonDocument gett(final String id, final long timeout, final TimeUnit timeUnit) {
        return bucket.get(id, timeout, timeUnit);
    }

    /**
     * Gets the t.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @return the t
     * @see com.couchbase.client.java.Bucket#get(String, Class)
     */
    public <T> T gett(final Class<T> targetClass, final String id) {
        return toEntityForGet(targetClass, gett(id));
    }

    /**
     * Gets the t.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the t
     * @see com.couchbase.client.java.Bucket#get(String, Class, long, TimeUnit)
     */
    public <T> T gett(final Class<T> targetClass, final String id, final long timeout, final TimeUnit timeUnit) {
        return toEntityForGet(targetClass, gett(id, timeout, timeUnit));
    }

    /**
     * To entity for get.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param doc the doc
     * @return the t
     */
    private <T> T toEntityForGet(final Class<T> targetClass, final JsonDocument doc) {
        if ((doc == null || doc.content() == null || doc.content().size() == 0)) {
            return null;
        }

        if (targetClass.isAssignableFrom(doc.getClass())) {
            return (T) doc;
        }

        return toEntity(targetClass, doc);
    }

    /**
     * Find first.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param query the query
     * @param parameters the parameters
     * @return the optional
     */
    @SafeVarargs
    public final <T> Optional<T> findFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        final N1qlQueryResult resultSet = execute(query, parameters);
        final Iterator<N1qlQueryRow> it = resultSet.rows();
        final JsonObject jsonObject = it.hasNext() ? it.next().value() : null;

        if (jsonObject == null || jsonObject.size() == 0) {
            return Optional.empty();
        } else {
            return Optional.of(toEntity(targetClass, jsonObject));
        }
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass an entity class with getter/setter method, <code>Map.class</code> or basic single value type(Primitive/String/Date...)
     * @param query the query
     * @param parameters the parameters
     * @return the list
     */
    @SafeVarargs
    public final <T> List<T> list(final Class<T> targetClass, final String query, final Object... parameters) {
        final N1qlQueryResult resultSet = execute(query, parameters);

        return toList(targetClass, resultSet);
    }

    /**
     * Always remember to set "<code>LIMIT 1</code>" in the sql statement for better performance.
     *
     * @param query the query
     * @param parameters the parameters
     * @return true, if successful
     */
    @SafeVarargs
    public final boolean exists(final String query, final Object... parameters) {
        final N1qlQueryResult resultSet = execute(query, parameters);

        return resultSet.iterator().hasNext();
    }

    /**
     * Count.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the long
     * @deprecated may be misused and it's inefficient.
     */
    @Deprecated
    @SafeVarargs
    public final long count(final String query, final Object... parameters) {
        return queryForSingleResult(long.class, query, parameters).orElse(0L);
    }

    /**
     * Query for boolean.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the optional boolean
     */
    @Beta
    @SafeVarargs
    public final OptionalBoolean queryForBoolean(final String query, final Object... parameters) {
        return queryForSingleResult(Boolean.class, query, parameters).mapToBoolean(ToBooleanFunction.UNBOX);
    }

    /**
     * Query for char.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the optional char
     */
    @Beta
    @SafeVarargs
    public final OptionalChar queryForChar(final String query, final Object... parameters) {
        return queryForSingleResult(Character.class, query, parameters).mapToChar(ToCharFunction.UNBOX);
    }

    /**
     * Query for byte.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the optional byte
     */
    @Beta
    @SafeVarargs
    public final OptionalByte queryForByte(final String query, final Object... parameters) {
        return queryForSingleResult(Byte.class, query, parameters).mapToByte(ToByteFunction.UNBOX);
    }

    /**
     * Query for short.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the optional short
     */
    @Beta
    @SafeVarargs
    public final OptionalShort queryForShort(final String query, final Object... parameters) {
        return queryForSingleResult(Short.class, query, parameters).mapToShort(ToShortFunction.UNBOX);
    }

    /**
     * Query for int.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the optional int
     */
    @Beta
    @SafeVarargs
    public final OptionalInt queryForInt(final String query, final Object... parameters) {
        return queryForSingleResult(Integer.class, query, parameters).mapToInt(ToIntFunction.UNBOX);
    }

    /**
     * Query for long.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the optional long
     */
    @Beta
    @SafeVarargs
    public final OptionalLong queryForLong(final String query, final Object... parameters) {
        return queryForSingleResult(Long.class, query, parameters).mapToLong(ToLongFunction.UNBOX);
    }

    /**
     * Query for float.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the optional float
     */
    @Beta
    @SafeVarargs
    public final OptionalFloat queryForFloat(final String query, final Object... parameters) {
        return queryForSingleResult(Float.class, query, parameters).mapToFloat(ToFloatFunction.UNBOX);
    }

    /**
     * Query for double.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the optional double
     */
    @Beta
    @SafeVarargs
    public final OptionalDouble queryForDouble(final String query, final Object... parameters) {
        return queryForSingleResult(Double.class, query, parameters).mapToDouble(ToDoubleFunction.UNBOX);
    }

    /**
     * Query for string.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the nullable
     */
    @Beta
    @SafeVarargs
    public final Nullable<String> queryForString(final String query, final Object... parameters) {
        return this.queryForSingleResult(String.class, query, parameters);
    }

    /**
     * Query for date.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the nullable
     */
    @Beta
    @SafeVarargs
    public final Nullable<Date> queryForDate(final String query, final Object... parameters) {
        return this.queryForSingleResult(Date.class, query, parameters);
    }

    /**
     * Query for date.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param query the query
     * @param parameters the parameters
     * @return the nullable
     */
    @Beta
    @SafeVarargs
    public final <T extends Date> Nullable<T> queryForDate(final Class<T> targetClass, final String query, final Object... parameters) {
        return this.queryForSingleResult(targetClass, query, parameters);
    }

    /**
     * Query for single result.
     *
     * @param <V> the value type
     * @param targetClass the target class
     * @param query the query
     * @param parameters the parameters
     * @return the nullable
     */
    @SafeVarargs
    public final <V> Nullable<V> queryForSingleResult(final Class<V> targetClass, final String query, final Object... parameters) {
        final N1qlQueryResult resultSet = execute(query, parameters);
        final Iterator<N1qlQueryRow> it = resultSet.rows();
        final JsonObject jsonObject = it.hasNext() ? it.next().value() : null;

        if (jsonObject == null || jsonObject.size() == 0) {
            return Nullable.empty();
        } else {
            return Nullable.of(N.convert(jsonObject.get(jsonObject.getNames().iterator().next()), targetClass));
        }
    }

    /**
     * Query.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the data set
     */
    @SafeVarargs
    public final DataSet query(final String query, final Object... parameters) {
        return extractData(execute(query, parameters));
    }

    /**
     * Query.
     *
     * @param targetClass the target class
     * @param query the query
     * @param parameters the parameters
     * @return the data set
     */
    @SafeVarargs
    public final DataSet query(final Class<?> targetClass, final String query, final Object... parameters) {
        return extractData(targetClass, execute(query, parameters));
    }

    /**
     * Query.
     *
     * @param query the query
     * @return the data set
     */
    public DataSet query(final N1qlQuery query) {
        return extractData(execute(query));
    }

    /**
     * Query.
     *
     * @param targetClass the target class
     * @param query the query
     * @return the data set
     */
    public DataSet query(final Class<?> targetClass, final N1qlQuery query) {
        return extractData(targetClass, execute(query));
    }

    /**
     * Query.
     *
     * @param query the query
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the data set
     */
    public DataSet query(final N1qlQuery query, final long timeout, final TimeUnit timeUnit) {
        return extractData(execute(query, timeout, timeUnit));
    }

    /**
     * Query.
     *
     * @param targetClass the target class
     * @param query the query
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the data set
     */
    public DataSet query(final Class<?> targetClass, final N1qlQuery query, final long timeout, final TimeUnit timeUnit) {
        return extractData(targetClass, execute(query, timeout, timeUnit));
    }

    /**
     * Stream.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the stream
     */
    @SafeVarargs
    public final Stream<JsonObject> stream(final String query, final Object... parameters) {
        return Stream.of(execute(query, parameters).rows()).map(new Function<N1qlQueryRow, JsonObject>() {
            @Override
            public JsonObject apply(N1qlQueryRow t) {
                return t.value();
            }
        });
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param query the query
     * @param parameters the parameters
     * @return the stream
     */
    @SafeVarargs
    public final <T> Stream<T> stream(final Class<T> targetClass, final String query, final Object... parameters) {
        return Stream.of(execute(query, parameters).rows()).map(new Function<N1qlQueryRow, T>() {
            @Override
            public T apply(N1qlQueryRow t) {
                return toEntity(targetClass, t.value());
            }
        });
    }

    /**
     * Stream.
     *
     * @param query the query
     * @return the stream
     */
    public Stream<JsonObject> stream(final N1qlQuery query) {
        return Stream.of(execute(query).rows()).map(new Function<N1qlQueryRow, JsonObject>() {
            @Override
            public JsonObject apply(N1qlQueryRow t) {
                return t.value();
            }
        });
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param query the query
     * @return the stream
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final N1qlQuery query) {
        return Stream.of(execute(query).rows()).map(new Function<N1qlQueryRow, T>() {
            @Override
            public T apply(N1qlQueryRow t) {
                return toEntity(targetClass, t.value());
            }
        });
    }

    /**
     * Stream.
     *
     * @param query the query
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the stream
     */
    public Stream<JsonObject> stream(final N1qlQuery query, final long timeout, final TimeUnit timeUnit) {
        return Stream.of(execute(query, timeout, timeUnit).rows()).map(new Function<N1qlQueryRow, JsonObject>() {
            @Override
            public JsonObject apply(N1qlQueryRow t) {
                return t.value();
            }
        });
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param query the query
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the stream
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final N1qlQuery query, final long timeout, final TimeUnit timeUnit) {
        return Stream.of(execute(query, timeout, timeUnit).rows()).map(new Function<N1qlQueryRow, T>() {
            @Override
            public T apply(N1qlQueryRow t) {
                return toEntity(targetClass, t.value());
            }
        });
    }

    /**
     * Insert.
     *
     * @param <T> the generic type
     * @param document the document
     * @return the t
     * @see com.couchbase.client.java.Bucket#insert(Document)
     */
    public <T> T insert(final T document) {
        return (T) toEntityForUpdate(document.getClass(), bucket.insert(toDocument(document)));
    }

    /**
     * Insert.
     *
     * @param <T> the generic type
     * @param document the document
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the t
     * @see com.couchbase.client.java.Bucket#insert(Document, long, TimeUnit)
     */
    public <T> T insert(final T document, final long timeout, final TimeUnit timeUnit) {
        return (T) toEntityForUpdate(document.getClass(), bucket.insert(toDocument(document), timeout, timeUnit));
    }

    /**
     * All the signed properties will be updated/inserted into data store.
     *
     * @param <T> the generic type
     * @param document the document
     * @return the t
     * @see com.couchbase.client.java.Bucket#upsert(Document)
     */
    public <T> T upsert(final T document) {
        return (T) toEntityForUpdate(document.getClass(), bucket.upsert(toDocument(document)));
    }

    /**
     * All the signed properties will be updated/inserted into data store.
     *
     * @param <T> the generic type
     * @param document the document
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the t
     * @see com.couchbase.client.java.Bucket#upsert(Document, long, TimeUnit)
     */
    public <T> T upsert(final T document, final long timeout, final TimeUnit timeUnit) {
        return (T) toEntityForUpdate(document.getClass(), bucket.upsert(toDocument(document), timeout, timeUnit));
    }

    /**
     * Replace.
     *
     * @param <T> the generic type
     * @param document the document
     * @return the t
     * @see com.couchbase.client.java.Bucket#replace(Document)
     */
    public <T> T replace(final T document) {
        return (T) toEntityForUpdate(document.getClass(), bucket.replace(toDocument(document)));
    }

    /**
     * Replace.
     *
     * @param <T> the generic type
     * @param document the document
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the t
     * @see com.couchbase.client.java.Bucket#replace(Document, long, TimeUnit)
     */
    public <T> T replace(final T document, final long timeout, final TimeUnit timeUnit) {
        return (T) toEntityForUpdate(document.getClass(), bucket.replace(toDocument(document), timeout, timeUnit));
    }

    /**
     * To document.
     *
     * @param <T> the generic type
     * @param obj the obj
     * @return the document
     */
    private <T> Document<T> toDocument(final Object obj) {
        final Class<?> cls = obj.getClass();

        if (Document.class.isAssignableFrom(cls)) {
            return (Document<T>) obj;
        } else {
            return (Document<T>) toJsonDocument(obj);
        }
    }

    /**
     * To entity for update.
     *
     * @param <T> the generic type
     * @param cls the cls
     * @param document the document
     * @return the t
     */
    private <T> T toEntityForUpdate(Class<T> cls, final Document<?> document) {
        if (cls.isAssignableFrom(document.getClass())) {
            return (T) document;
        } else {
            return toEntity(cls, (JsonDocument) document);
        }
    }

    /**
     * Removes the.
     *
     * @param id the id
     * @return the json document
     * @see com.couchbase.client.java.Bucket#remove(String)
     */
    public JsonDocument remove(String id) {
        return bucket.remove(id);
    }

    /**
     * Removes the.
     *
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the json document
     * @see com.couchbase.client.java.Bucket#remove(String, long, TimeUnit)
     */
    public JsonDocument remove(final String id, final long timeout, final TimeUnit timeUnit) {
        return bucket.remove(id, timeout, timeUnit);
    }

    /**
     * Removes the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @return the t
     * @see com.couchbase.client.java.Bucket#remove(String, Class)
     */
    public <T> T remove(final Class<T> targetClass, final String id) {
        return toEntityForUpdate(targetClass, bucket.remove(id, JsonDocument.class));
    }

    /**
     * Removes the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the t
     * @see com.couchbase.client.java.Bucket#remove(String, Class, long, TimeUnit)
     */
    public <T> T remove(final Class<T> targetClass, final String id, final long timeout, final TimeUnit timeUnit) {
        return toEntityForUpdate(targetClass, bucket.remove(id, JsonDocument.class, timeout, timeUnit));
    }

    /**
     * Removes the.
     *
     * @param <T> the generic type
     * @param document the document
     * @return the t
     * @see com.couchbase.client.java.Bucket#remove(Document)
     */
    public <T> T remove(final T document) {
        return (T) toEntityForUpdate(document.getClass(), bucket.remove(toDocument(document)));
    }

    /**
     * Removes the.
     *
     * @param <T> the generic type
     * @param document the document
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the t
     * @see com.couchbase.client.java.Bucket#remove(Document, long, TimeUnit)
     */
    public <T> T remove(final T document, final long timeout, final TimeUnit timeUnit) {
        return (T) toEntityForUpdate(document.getClass(), bucket.remove(toDocument(document), timeout, timeUnit));
    }

    /**
     * Execute.
     *
     * @param query the query
     * @return the n 1 ql query result
     */
    public N1qlQueryResult execute(final String query) {
        return execute(prepareN1qlQuery(query));
    }

    /**
     * Execute.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the n 1 ql query result
     */
    @SafeVarargs
    public final N1qlQueryResult execute(final String query, final Object... parameters) {
        return execute(prepareN1qlQuery(query, parameters));
    }

    /**
     * Execute.
     *
     * @param query the query
     * @return the n 1 ql query result
     */
    public N1qlQueryResult execute(final N1qlQuery query) {
        final N1qlQueryResult resultSet = bucket.query(query);

        checkResultError(resultSet);

        return resultSet;
    }

    /**
     * Execute.
     *
     * @param query the query
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the n 1 ql query result
     */
    public N1qlQueryResult execute(final N1qlQuery query, final long timeout, final TimeUnit timeUnit) {
        final N1qlQueryResult resultSet = bucket.query(query, timeout, timeUnit);

        checkResultError(resultSet);

        return resultSet;
    }

    /**
     * Async get.
     *
     * @param id the id
     * @return the continuable future
     */
    public ContinuableFuture<Optional<JsonDocument>> asyncGet(final String id) {
        return asyncExecutor.execute(new Callable<Optional<JsonDocument>>() {
            @Override
            public Optional<JsonDocument> call() throws Exception {
                return get(id);
            }
        });
    }

    /**
     * Async get.
     *
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public ContinuableFuture<Optional<JsonDocument>> asyncGet(final String id, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<Optional<JsonDocument>>() {
            @Override
            public Optional<JsonDocument> call() throws Exception {
                return get(id, timeout, timeUnit);
            }
        });
    }

    /**
     * Async get.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @return the continuable future
     */
    public <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final String id) {
        return asyncExecutor.execute(new Callable<Optional<T>>() {
            @Override
            public Optional<T> call() throws Exception {
                return get(targetClass, id);
            }
        });
    }

    /**
     * Async get.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final String id, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<Optional<T>>() {
            @Override
            public Optional<T> call() throws Exception {
                return get(targetClass, id, timeout, timeUnit);
            }
        });
    }

    /**
     * Async gett.
     *
     * @param id the id
     * @return the continuable future
     */
    public ContinuableFuture<JsonDocument> asyncGett(final String id) {
        return asyncExecutor.execute(new Callable<JsonDocument>() {
            @Override
            public JsonDocument call() throws Exception {
                return gett(id);
            }
        });
    }

    /**
     * Async gett.
     *
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public ContinuableFuture<JsonDocument> asyncGett(final String id, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<JsonDocument>() {
            @Override
            public JsonDocument call() throws Exception {
                return gett(id, timeout, timeUnit);
            }
        });
    }

    /**
     * Async gett.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final String id) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return gett(targetClass, id);
            }
        });
    }

    /**
     * Async gett.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final String id, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return gett(targetClass, id, timeout, timeUnit);
            }
        });
    }

    /**
     * Always remember to set "<code>LIMIT 1</code>" in the sql statement for better performance.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<Boolean> asyncExists(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return exists(query, parameters);
            }
        });
    }

    /**
     * Async count.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     * @deprecated may be misused and it's inefficient.
     */
    @Deprecated
    @SafeVarargs
    public final ContinuableFuture<Long> asyncCount(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return count(query, parameters);
            }
        });
    }

    /**
     * Async query for boolean.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<OptionalBoolean> asyncQueryForBoolean(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<OptionalBoolean>() {
            @Override
            public OptionalBoolean call() throws Exception {
                return queryForBoolean(query, parameters);
            }
        });
    }

    /**
     * Async query for char.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<OptionalChar> asyncQueryForChar(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<OptionalChar>() {
            @Override
            public OptionalChar call() throws Exception {
                return queryForChar(query, parameters);
            }
        });
    }

    /**
     * Async query for byte.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<OptionalByte> asyncQueryForByte(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<OptionalByte>() {
            @Override
            public OptionalByte call() throws Exception {
                return queryForByte(query, parameters);
            }
        });
    }

    /**
     * Async query for short.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<OptionalShort> asyncQueryForShort(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<OptionalShort>() {
            @Override
            public OptionalShort call() throws Exception {
                return queryForShort(query, parameters);
            }
        });
    }

    /**
     * Async query for int.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<OptionalInt> asyncQueryForInt(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<OptionalInt>() {
            @Override
            public OptionalInt call() throws Exception {
                return queryForInt(query, parameters);
            }
        });
    }

    /**
     * Async query for long.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<OptionalLong> asyncQueryForLong(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<OptionalLong>() {
            @Override
            public OptionalLong call() throws Exception {
                return queryForLong(query, parameters);
            }
        });
    }

    /**
     * Async query for float.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<OptionalFloat> asyncQueryForFloat(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<OptionalFloat>() {
            @Override
            public OptionalFloat call() throws Exception {
                return queryForFloat(query, parameters);
            }
        });
    }

    /**
     * Async query for double.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<OptionalDouble> asyncQueryForDouble(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<OptionalDouble>() {
            @Override
            public OptionalDouble call() throws Exception {
                return queryForDouble(query, parameters);
            }
        });
    }

    /**
     * Async query for string.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<Nullable<String>> asyncQueryForString(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<Nullable<String>>() {
            @Override
            public Nullable<String> call() throws Exception {
                return queryForString(query, parameters);
            }
        });
    }

    /**
     * Async query for date.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<Nullable<Date>> asyncQueryForDate(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<Nullable<Date>>() {
            @Override
            public Nullable<Date> call() throws Exception {
                return queryForDate(query, parameters);
            }
        });
    }

    /**
     * Async query for date.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final <T extends Date> ContinuableFuture<Nullable<T>> asyncQueryForDate(final Class<T> targetClass, final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<Nullable<T>>() {
            @Override
            public Nullable<T> call() throws Exception {
                return queryForDate(targetClass, query, parameters);
            }
        });
    }

    /**
     * Async query for single result.
     *
     * @param <V> the value type
     * @param targetClass the target class
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final <V> ContinuableFuture<Nullable<V>> asyncQueryForSingleResult(final Class<V> targetClass, final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<Nullable<V>>() {
            @Override
            public Nullable<V> call() throws Exception {
                return queryForSingleResult(targetClass, query, parameters);
            }
        });
    }

    /**
     * Async find first.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final <T> ContinuableFuture<Optional<T>> asyncFindFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<Optional<T>>() {
            @Override
            public Optional<T> call() throws Exception {
                return findFirst(targetClass, query, parameters);
            }
        });
    }

    /**
     * Async list.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final <T> ContinuableFuture<List<T>> asyncList(final Class<T> targetClass, final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                return list(targetClass, query, parameters);
            }
        });
    }

    /**
     * Async query.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<DataSet> asyncQuery(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return query(query, parameters);
            }
        });
    }

    /**
     * Async query.
     *
     * @param targetClass the target class
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<DataSet> asyncQuery(final Class<?> targetClass, final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return query(targetClass, query, parameters);
            }
        });
    }

    /**
     * Async query.
     *
     * @param query the query
     * @return the continuable future
     */
    public ContinuableFuture<DataSet> asyncQuery(final N1qlQuery query) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return query(query);
            }
        });
    }

    /**
     * Async query.
     *
     * @param targetClass the target class
     * @param query the query
     * @return the continuable future
     */
    public ContinuableFuture<DataSet> asyncQuery(final Class<?> targetClass, final N1qlQuery query) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return query(targetClass, query);
            }
        });
    }

    /**
     * Async query.
     *
     * @param query the query
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public ContinuableFuture<DataSet> asyncQuery(final N1qlQuery query, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return query(query, timeout, timeUnit);
            }
        });

    }

    /**
     * Async query.
     *
     * @param targetClass the target class
     * @param query the query
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public ContinuableFuture<DataSet> asyncQuery(final Class<?> targetClass, final N1qlQuery query, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return query(targetClass, query, timeout, timeUnit);
            }
        });
    }

    /**
     * Async stream.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<Stream<JsonObject>> asyncStream(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<Stream<JsonObject>>() {
            @Override
            public Stream<JsonObject> call() throws Exception {
                return stream(query, parameters);
            }
        });
    }

    /**
     * Async stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final <T> ContinuableFuture<Stream<T>> asyncStream(final Class<T> targetClass, final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return stream(targetClass, query, parameters);
            }
        });
    }

    /**
     * Async stream.
     *
     * @param query the query
     * @return the continuable future
     */
    public ContinuableFuture<Stream<JsonObject>> asyncStream(final N1qlQuery query) {
        return asyncExecutor.execute(new Callable<Stream<JsonObject>>() {
            @Override
            public Stream<JsonObject> call() throws Exception {
                return stream(query);
            }
        });
    }

    /**
     * Async stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param query the query
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> asyncStream(final Class<T> targetClass, final N1qlQuery query) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return stream(targetClass, query);
            }
        });
    }

    /**
     * Async stream.
     *
     * @param query the query
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public ContinuableFuture<Stream<JsonObject>> asyncStream(final N1qlQuery query, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<Stream<JsonObject>>() {
            @Override
            public Stream<JsonObject> call() throws Exception {
                return stream(query, timeout, timeUnit);
            }
        });

    }

    /**
     * Async stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param query the query
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> asyncStream(final Class<T> targetClass, final N1qlQuery query, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return stream(targetClass, query, timeout, timeUnit);
            }
        });
    }

    /**
     * Async insert.
     *
     * @param <T> the generic type
     * @param document the document
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncInsert(final T document) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return insert(document);
            }
        });
    }

    /**
     * Async insert.
     *
     * @param <T> the generic type
     * @param document the document
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncInsert(final T document, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return insert(document, timeout, timeUnit);
            }
        });
    }

    /**
     * Async upsert.
     *
     * @param <T> the generic type
     * @param document the document
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncUpsert(final T document) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return upsert(document);
            }
        });
    }

    /**
     * Async upsert.
     *
     * @param <T> the generic type
     * @param document the document
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncUpsert(final T document, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return upsert(document, timeout, timeUnit);
            }
        });
    }

    /**
     * Async replace.
     *
     * @param <T> the generic type
     * @param document the document
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncReplace(final T document) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return replace(document);
            }
        });
    }

    /**
     * Async replace.
     *
     * @param <T> the generic type
     * @param document the document
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncReplace(final T document, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return replace(document, timeout, timeUnit);
            }
        });
    }

    /**
     * Async remove.
     *
     * @param id the id
     * @return the continuable future
     */
    public ContinuableFuture<JsonDocument> asyncRemove(final String id) {
        return asyncExecutor.execute(new Callable<JsonDocument>() {
            @Override
            public JsonDocument call() throws Exception {
                return remove(id);
            }
        });
    }

    /**
     * Async remove.
     *
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public ContinuableFuture<JsonDocument> asyncRemove(final String id, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<JsonDocument>() {
            @Override
            public JsonDocument call() throws Exception {
                return remove(id, timeout, timeUnit);
            }
        });
    }

    /**
     * Async remove.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncRemove(final Class<T> targetClass, final String id) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return remove(targetClass, id);
            }
        });
    }

    /**
     * Async remove.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncRemove(final Class<T> targetClass, final String id, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return remove(targetClass, id, timeout, timeUnit);
            }
        });
    }

    /**
     * Async remove.
     *
     * @param <T> the generic type
     * @param document the document
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncRemove(final T document) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return remove(document);
            }
        });
    }

    /**
     * Async remove.
     *
     * @param <T> the generic type
     * @param document the document
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public <T> ContinuableFuture<T> asyncRemove(final T document, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return remove(document, timeout, timeUnit);
            }
        });
    }

    /**
     * Async execute.
     *
     * @param query the query
     * @return the continuable future
     */
    public ContinuableFuture<N1qlQueryResult> asyncExecute(final String query) {
        return asyncExecutor.execute(new Callable<N1qlQueryResult>() {
            @Override
            public N1qlQueryResult call() throws Exception {
                return execute(query);
            }
        });
    }

    /**
     * Async execute.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the continuable future
     */
    @SafeVarargs
    public final ContinuableFuture<N1qlQueryResult> asyncExecute(final String query, final Object... parameters) {
        return asyncExecutor.execute(new Callable<N1qlQueryResult>() {
            @Override
            public N1qlQueryResult call() throws Exception {
                return execute(query, parameters);
            }
        });
    }

    /**
     * Async execute.
     *
     * @param query the query
     * @return the continuable future
     */
    public ContinuableFuture<N1qlQueryResult> asyncExecute(final N1qlQuery query) {
        return asyncExecutor.execute(new Callable<N1qlQueryResult>() {
            @Override
            public N1qlQueryResult call() throws Exception {
                return execute(query);
            }
        });
    }

    /**
     * Async execute.
     *
     * @param query the query
     * @param timeout the timeout
     * @param timeUnit the time unit
     * @return the continuable future
     */
    public ContinuableFuture<N1qlQueryResult> asyncExecute(final N1qlQuery query, final long timeout, final TimeUnit timeUnit) {
        return asyncExecutor.execute(new Callable<N1qlQueryResult>() {
            @Override
            public N1qlQueryResult call() throws Exception {
                return execute(query, timeout, timeUnit);
            }
        });
    }

    /**
     * Check target class.
     *
     * @param targetClass the target class
     */
    private static void checkTargetClass(final Class<?> targetClass) {
        if (!(ClassUtil.isEntity(targetClass) || Map.class.isAssignableFrom(targetClass))) {
            throw new IllegalArgumentException("The target class must be an entity class with getter/setter methods or Map.class. But it is: "
                    + ClassUtil.getCanonicalClassName(targetClass));
        }
    }

    /**
     * Check result error.
     *
     * @param resultSet the result set
     */
    private static void checkResultError(N1qlQueryResult resultSet) {
        if (N.notNullOrEmpty(resultSet.errors())) {
            throw new AbacusException("Errors in query result: " + resultSet.errors());
        }
    }

    /**
     * Prepare N 1 ql query.
     *
     * @param query the query
     * @return the n 1 ql query
     */
    private N1qlQuery prepareN1qlQuery(final String query) {
        N1qlQuery result = null;

        if (query.length() <= POOLABLE_LENGTH) {
            PoolableWrapper<N1qlQuery> wrapper = stmtPool.get(query);

            if (wrapper != null) {
                result = wrapper.value();
            }
        }

        if (result == null) {
            result = N1qlQuery.simple(query);

            if (query.length() <= POOLABLE_LENGTH) {
                stmtPool.put(query, PoolableWrapper.of(result));
            }
        }

        return result;
    }

    /**
     * Prepare N 1 ql query.
     *
     * @param query the query
     * @param parameters the parameters
     * @return the n 1 ql query
     */
    private N1qlQuery prepareN1qlQuery(String query, Object... parameters) {
        if (N.isNullOrEmpty(parameters)) {
            return prepareN1qlQuery(query);
        }

        final NamedSQL namedSQL = getNamedSQL(query);
        final String sql = namedSQL.getParameterizedSQL(true);
        final int parameterCount = namedSQL.getParameterCount(true);
        final List<String> namedParameters = namedSQL.getNamedParameters(true);

        // Prepared query plan doens't work in Couchbase 4.0 Beta version?

        //        N1qlQueryPlan queryPlan = null;
        //
        //        if (query.length() <= POOLABLE_LENGTH) {
        //            Wrapper<N1qlQueryPlan> wrapper = preStmtPool.get(query);
        //            if (wrapper != null && wrapper.get() != null) {
        //                queryPlan = wrapper.get();
        //            }
        //        }
        //
        //        if (queryPlan == null) {
        //            queryPlan = bucket.prepare(sql);
        //
        //            if (query.length() <= POOLABLE_LENGTH) {
        //                preStmtPool.put(query, Wrapper.valueOf(queryPlan));
        //            }
        //        }
        //

        if (parameterCount == 0) {
            return N1qlQuery.simple(query);
        } else if (N.isNullOrEmpty(parameters)) {
            throw new IllegalArgumentException("Null or empty parameters for parameterized query: " + query);
        }

        //        if (parameters.length == 1) {
        //            if (parameters[0] instanceof JsonArray) {
        //                return N1qlQuery.parametrized(sql, (JsonArray) parameters[0]);
        //            } else if (parameters[0] instanceof JsonObject) {
        //                return N1qlQuery.parametrized(sql, (JsonObject) parameters[0]);
        //            }
        //        }

        Object[] values = parameters;

        if (N.notNullOrEmpty(namedParameters) && parameters.length == 1
                && (parameters[0] instanceof Map || parameters[0] instanceof JsonObject || ClassUtil.isEntity(parameters[0].getClass()))) {
            values = new Object[parameterCount];

            final Object parameter_0 = parameters[0];
            String parameterName = null;

            if (parameter_0 instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) parameter_0;

                for (int i = 0; i < parameterCount; i++) {
                    parameterName = namedParameters.get(i);
                    values[i] = m.get(parameterName);

                    if ((values[i] == null) && !m.containsKey(parameterName)) {
                        throw new IllegalArgumentException("Parameter for property '" + parameterName + "' is missed");
                    }
                }
            } else if (parameter_0 instanceof JsonObject) {
                @SuppressWarnings("unchecked")
                JsonObject jsonObject = (JsonObject) parameter_0;

                for (int i = 0; i < parameterCount; i++) {
                    parameterName = namedParameters.get(i);
                    values[i] = jsonObject.get(parameterName);

                    if ((values[i] == null) && !jsonObject.containsKey(parameterName)) {
                        throw new IllegalArgumentException("Parameter for property '" + parameterName + "' is missed");
                    }
                }
            } else {
                Object entity = parameter_0;
                Class<?> clazz = entity.getClass();
                Method propGetMethod = null;

                for (int i = 0; i < parameterCount; i++) {
                    parameterName = namedParameters.get(i);
                    propGetMethod = ClassUtil.getPropGetMethod(clazz, parameterName);

                    if (propGetMethod == null) {
                        throw new IllegalArgumentException("Parameter for property '" + parameterName + "' is missed");
                    }

                    values[i] = ClassUtil.invokeMethod(entity, propGetMethod);
                }
            }
        } else if ((parameters.length == 1) && (parameters[0] != null)) {
            if (parameters[0] instanceof Object[] && ((((Object[]) parameters[0]).length) >= parameterCount)) {
                values = (Object[]) parameters[0];
            } else if (parameters[0] instanceof List && (((List<?>) parameters[0]).size() >= parameterCount)) {
                final Collection<?> c = (Collection<?>) parameters[0];
                values = c.toArray(new Object[c.size()]);
            }
        }

        if (values.length == 1 && values[0] instanceof JsonArray) {
            return N1qlQuery.parameterized(sql, (JsonArray) values[0]);
        } else if (values.length > parameterCount) {
            return N1qlQuery.parameterized(sql, JsonArray.from(N.copyOfRange(values, 0, parameterCount)));
        } else {
            return N1qlQuery.parameterized(sql, JsonArray.from(values));
        }
    }

    /**
     * Gets the named SQL.
     *
     * @param sql the sql
     * @return the named SQL
     */
    private NamedSQL getNamedSQL(String sql) {
        NamedSQL namedSQL = null;

        if (sqlMapper != null) {
            namedSQL = sqlMapper.get(sql);
        }

        if (namedSQL == null) {
            namedSQL = NamedSQL.parse(sql);
        }

        return namedSQL;
    }

    /**
     * Close.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public void close() throws IOException {
        try {
            bucket.close();
        } finally {
            cluster.disconnect();
        }
    }

}
