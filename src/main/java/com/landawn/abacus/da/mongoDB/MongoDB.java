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

package com.landawn.abacus.da.mongoDB;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.DataSet;
import com.landawn.abacus.DirtyMarker;
import com.landawn.abacus.core.DirtyMarkerUtil;
import com.landawn.abacus.parser.JSONParser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.EntityInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.Maps;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.ObjectPool;
import com.landawn.abacus.util.QueryUtil;
import com.landawn.abacus.util.u.Optional;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

/**
 * It's a simple wrapper of MongoDB Java client.
 *
 * <br>We recommend to define "id" property in java entity/bean as the object "_id" in MongoDB to keep things as simple as possible.</br>
 *
 *
 * @author Haiyang Li
 * @see <a href="http://api.mongodb.org/java/3.1/?com/mongodb/client/model/Filters.html">com.mongodb.client.model.Filters</a>
 * @see <a href="http://api.mongodb.org/java/3.1/?com/mongodb/client/model/Aggregates.html">com.mongodb.client.model.Aggregates</a>
 * @see <a href="http://api.mongodb.org/java/3.1/?com/mongodb/client/model/Accumulators.html">com.mongodb.client.model.Accumulators</a>
 * @see <a href="http://api.mongodb.org/java/3.1/?com/mongodb/client/model/Projections.html">com.mongodb.client.model.Projections</a>
 * @see <a href="http://api.mongodb.org/java/3.1/?com/mongodb/client/model/Sorts.html">com.mongodb.client.model.Sorts</a>
 * @since 0.8
 */
public final class MongoDB {
    /**
     * It's name of object id set in Map/Object array.
     */
    public static final String _ID = "_id";

    /**
     * Property name of id.
     */
    public static final String ID = "id";

    static final AsyncExecutor DEFAULT_ASYNC_EXECUTOR = new AsyncExecutor(Math.max(64, Math.min(IOUtil.CPU_CORES * 8, IOUtil.MAX_MEMORY_IN_MB / 1024) * 32),
            Math.max(256, (IOUtil.MAX_MEMORY_IN_MB / 1024) * 64), 180L, TimeUnit.SECONDS);

    private static final JSONParser jsonParser = ParserFactory.createJSONParser();

    // private static CodecRegistry codecRegistry = CodecRegistries.fromCodecs(new CalendarCodec(), new TimeCodec(), new TimestampCodec());
    private static final CodecRegistry codecRegistry = CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry(), new GeneralCodecRegistry());

    private static final Map<Class<?>, Method> classIdSetMethodPool = new ConcurrentHashMap<>();

    private final Map<String, MongoCollectionExecutor> collExecutorPool = new ConcurrentHashMap<>();

    private final Map<Class<?>, MongoCollectionMapper<?>> collMapperPool = new ConcurrentHashMap<>();

    private final MongoDatabase mongoDB;

    private final AsyncExecutor asyncExecutor;

    public MongoDB(final MongoDatabase mongoDB) {
        this(mongoDB, DEFAULT_ASYNC_EXECUTOR);
    }

    public MongoDB(final MongoDatabase mongoDB, final AsyncExecutor asyncExecutor) {
        this.mongoDB = mongoDB.withCodecRegistry(codecRegistry);
        this.asyncExecutor = asyncExecutor;
    }

    public MongoDatabase db() {
        return mongoDB;
    }

    /**
     *
     * @param collectionName
     * @return
     */
    public MongoCollection<Document> collection(final String collectionName) {
        return mongoDB.getCollection(collectionName);
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param collectionName
     * @return
     */
    public <T> MongoCollection<T> collection(final Class<T> targetClass, final String collectionName) {
        return mongoDB.getCollection(collectionName, targetClass);
    }

    /**
     *
     * @param collectionName
     * @return
     */
    public MongoCollectionExecutor collExecutor(final String collectionName) {
        N.checkArgNotNull(collectionName, "collectionName");

        MongoCollectionExecutor collExecutor = collExecutorPool.get(collectionName);

        if (collExecutor == null) {
            collExecutor = new MongoCollectionExecutor(mongoDB.getCollection(collectionName), asyncExecutor);
            collExecutorPool.put(collectionName, collExecutor);
        }

        return collExecutor;
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @return
     */
    public <T> MongoCollectionMapper<T> collMapper(final Class<T> targetClass) {
        return collMapper(targetClass, ClassUtil.getSimpleClassName(targetClass));
    }

    /**
     *
     * @param <T>
     * @param targetClass
     * @param collectionName
     * @return
     */
    @SuppressWarnings("rawtypes")
    public <T> MongoCollectionMapper<T> collMapper(final Class<T> targetClass, String collectionName) {
        N.checkArgNotNull(targetClass, "targetClass");
        N.checkArgNotNull(collectionName, "collectionName");

        MongoCollectionMapper collMapper = collMapperPool.get(targetClass);

        if (collMapper == null) {
            collMapper = new MongoCollectionMapper(collExecutor(collectionName), targetClass);

            collMapperPool.put(targetClass, collMapper);
        }

        return collMapper;
    }

    /**
     * The object id ("_id") property will be read from/write to the specified property .
     *
     * @param cls
     * @param idPropertyName
     * @see com.landawn.abacus.annotation.Id
     * @see javax.persistence.Id
     *
     * @deprecated please defined or annotated the key/id field by {@code @Id}
     */
    @Deprecated
    public static void registerIdProeprty(final Class<?> cls, final String idPropertyName) {
        if (ClassUtil.getPropGetMethod(cls, idPropertyName) == null || ClassUtil.getPropSetMethod(cls, idPropertyName) == null) {
            throw new IllegalArgumentException("The specified class: " + ClassUtil.getCanonicalClassName(cls)
                    + " doesn't have getter or setter method for the specified id propery: " + idPropertyName);
        }

        final Method setMethod = ClassUtil.getPropSetMethod(cls, idPropertyName);
        final Class<?> parameterType = setMethod.getParameterTypes()[0];

        if (!(String.class.isAssignableFrom(parameterType) || ObjectId.class.isAssignableFrom(parameterType))) {
            throw new IllegalArgumentException(
                    "The parameter type of the specified id setter method must be 'String' or 'ObjectId': " + setMethod.toGenericString());
        }

        classIdSetMethodPool.put(cls, setMethod);
    }

    /**
     *
     * @param findIterable
     * @return
     */
    public static DataSet extractData(final MongoIterable<?> findIterable) {
        return extractData(Map.class, findIterable);
    }

    /**
     *
     * @param targetClass an entity class with getter/setter method or <code>Map.class/Document.class</code>
     * @param findIterable
     * @return
     */
    public static DataSet extractData(final Class<?> targetClass, final MongoIterable<?> findIterable) {
        return extractData(targetClass, null, findIterable);
    }

    /**
     *
     * @param targetClass an entity class with getter/setter method or <code>Map.class/Document.class</code>
     * @param selectPropNames
     * @param findIterable
     * @return
     */
    static DataSet extractData(final Class<?> targetClass, final Collection<String> selectPropNames, final MongoIterable<?> findIterable) {
        checkTargetClass(targetClass);

        final List<Object> rowList = findIterable.into(new ArrayList<>());
        final Optional<Object> first = N.firstNonNull(rowList);

        if (first.isPresent()) {
            /*
            if (Map.class.isAssignableFrom(first.get().getClass())) {
                if (N.isNullOrEmpty(selectPropNames)) {
                    final Set<String> columnNames = N.newLinkedHashSet();
                    @SuppressWarnings("rawtypes")
                    final List<Map<String, Object>> tmp = (List) rowList;

                    for (Map<String, Object> row : tmp) {
                        columnNames.addAll(row.keySet());
                    }

                    return N.newDataSet(columnNames, rowList);
                } else {
                    return N.newDataSet(selectPropNames, rowList);
                }
            } else {
                return N.newDataSet(rowList);
            }
            */

            if (Map.class.isAssignableFrom(targetClass) && Map.class.isAssignableFrom(first.get().getClass())) {
                if (N.isNullOrEmpty(selectPropNames)) {
                    final Set<String> columnNames = N.newLinkedHashSet();
                    @SuppressWarnings("rawtypes")
                    final List<Map<String, Object>> tmp = (List) rowList;

                    for (Map<String, Object> row : tmp) {
                        columnNames.addAll(row.keySet());
                    }

                    return N.newDataSet(columnNames, rowList);
                } else {
                    return N.newDataSet(selectPropNames, rowList);
                }
            } else if (Document.class.isAssignableFrom(first.get().getClass())) {
                final List<Object> newRowList = new ArrayList<>(rowList.size());

                for (Object row : rowList) {
                    newRowList.add(toEntity(targetClass, (Document) row));
                }

                return N.newDataSet(selectPropNames, newRowList);
            } else {
                return N.newDataSet(selectPropNames, rowList);
            }
        } else {
            return N.newEmptyDataSet();
        }
    }

    /**
     *
     * @param <T>
     * @param targetClass an entity class with getter/setter method, <code>Map.class</code> or basic single value type(Primitive/String/Date...)
     * @param findIterable
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static <T> List<T> toList(final Class<T> targetClass, final MongoIterable<?> findIterable) {
        final Type<T> targetType = N.typeOf(targetClass);
        final List<Object> rowList = findIterable.into(new ArrayList<>());
        final Optional<Object> firstNonNull = N.firstNonNull(rowList);

        if (firstNonNull.isPresent()) {
            if (targetClass.isAssignableFrom(firstNonNull.get().getClass())) {
                return (List<T>) rowList;
            } else {
                final List<Object> resultList = new ArrayList<>(rowList.size());

                if (targetType.isEntity() || targetType.isMap()) {
                    if (firstNonNull.get() instanceof Document) {
                        for (Object row : rowList) {
                            resultList.add(toEntity(targetClass, (Document) row));
                        }
                    } else if (targetType.isMap()) {
                        for (Object row : rowList) {
                            resultList.add(Maps.entity2Map((Map) N.newInstance(targetClass), row));
                        }
                    } else {
                        for (Object row : rowList) {
                            resultList.add(N.copy(targetClass, row));
                        }
                    }
                } else if (firstNonNull.get() instanceof Map && ((Map<String, Object>) firstNonNull.get()).size() <= 2) {
                    final Map<String, Object> m = (Map<String, Object>) firstNonNull.get();
                    final String propName = N.findFirst(m.keySet(), Fn.notEqual(_ID)).orElse(_ID);

                    if (m.get(propName) != null && targetClass.isAssignableFrom(m.get(propName).getClass())) {
                        for (Object row : rowList) {
                            resultList.add(((Map<String, Object>) row).get(propName));
                        }
                    } else {
                        for (Object row : rowList) {
                            resultList.add(N.convert(((Map<String, Object>) row).get(propName), targetClass));
                        }
                    }
                } else {
                    throw new IllegalArgumentException(
                            "Can't covert document: " + firstNonNull.toString() + " to class: " + ClassUtil.getCanonicalClassName(targetClass));
                }

                return (List<T>) resultList;
            }
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * The id in the specified <code>doc</code> will be set to the returned object if and only if the id is not null or empty and it's acceptable to the <code>targetClass</code>.
     *
     * @param <T>
     * @param targetClass an entity class with getter/setter method, or <code>Map.class</code>.
     * @param doc
     * @return
     */
    public static <T> T toEntity(final Class<T> targetClass, final Document doc) {
        checkTargetClass(targetClass);

        if (Map.class.isAssignableFrom(targetClass)) {
            // return (T) new LinkedHashMap<>(doc);
            if (targetClass.isAssignableFrom(doc.getClass())) {
                return (T) doc;
            } else {
                final Map<String, Object> map = (Map<String, Object>) N.newInstance(targetClass);
                map.putAll(doc);
                return (T) map;
            }
        } else if (ClassUtil.isEntity(targetClass)) {
            final Method idSetMethod = getObjectIdSetMethod(targetClass);
            final Class<?> parameterType = idSetMethod == null ? null : idSetMethod.getParameterTypes()[0];
            final Object objectId = doc.get(_ID);
            T entity = null;

            doc.remove(_ID);

            try {
                entity = Maps.map2Entity(targetClass, doc);

                if (objectId != null && parameterType != null) {
                    if (parameterType.isAssignableFrom(objectId.getClass())) {
                        ClassUtil.setPropValue(entity, idSetMethod, objectId);
                    } else if (parameterType.isAssignableFrom(String.class)) {
                        ClassUtil.setPropValue(entity, idSetMethod, objectId.toString());
                    } else {
                        ClassUtil.setPropValue(entity, idSetMethod, objectId);
                    }
                }
            } finally {
                doc.put(_ID, objectId);
            }

            if (DirtyMarkerUtil.isDirtyMarker(entity.getClass())) {
                DirtyMarkerUtil.markDirty((DirtyMarker) entity, false);
            }

            return entity;
        } else if (doc.size() <= 2) {
            final String propName = N.findFirst(doc.keySet(), Fn.notEqual(_ID)).orElse(_ID);

            return N.convert(doc.get(propName), targetClass);
        } else {
            throw new IllegalArgumentException("Unsupported target type: " + targetClass);
        }
    }

    /**
     * Gets the object id set method.
     *
     * @param <T>
     * @param targetClass
     * @return
     */
    @SuppressWarnings("deprecation")
    private static <T> Method getObjectIdSetMethod(final Class<T> targetClass) {
        Method idSetMethod = classIdSetMethodPool.get(targetClass);

        if (idSetMethod == null) {
            final List<String> idFieldNames = QueryUtil.getIdFieldNames(targetClass);
            Method idPropSetMethod = null;
            Class<?> parameterType = null;

            for (String fieldName : idFieldNames) {
                idPropSetMethod = ClassUtil.getPropSetMethod(targetClass, fieldName);
                parameterType = idPropSetMethod == null ? null : idPropSetMethod.getParameterTypes()[0];

                if (parameterType != null && (String.class.isAssignableFrom(parameterType) || ObjectId.class.isAssignableFrom(parameterType))) {
                    idSetMethod = idPropSetMethod;

                    break;
                }
            }

            if (idSetMethod == null) {
                idPropSetMethod = ClassUtil.getPropSetMethod(targetClass, ID);
                parameterType = idPropSetMethod == null ? null : idPropSetMethod.getParameterTypes()[0];

                //            if (parameterType != null && (ObjectId.class.isAssignableFrom(parameterType) || String.class.isAssignableFrom(parameterType))) {
                //                idSetMethod = idPropSetMethod;
                //            }

                if (parameterType != null && (String.class.isAssignableFrom(parameterType) || ObjectId.class.isAssignableFrom(parameterType))) {
                    idSetMethod = idPropSetMethod;
                }
            }

            if (idSetMethod == null) {
                idSetMethod = ClassUtil.METHOD_MASK;
            }

            classIdSetMethodPool.put(targetClass, idSetMethod);
        }

        return idSetMethod == ClassUtil.METHOD_MASK ? null : idSetMethod;
    }

    /**
     *
     * @param bson
     * @return
     */
    public static String toJSON(final Bson bson) {
        return bson instanceof Map ? N.toJSON(bson) : N.toJSON(bson.toBsonDocument(Document.class, codecRegistry));
    }

    /**
     *
     * @param bsonObject
     * @return
     */
    public static String toJSON(final BSONObject bsonObject) {
        return bsonObject instanceof Map ? N.toJSON(bsonObject) : N.toJSON(bsonObject.toMap());
    }

    /**
     *
     * @param bsonObject
     * @return
     */
    public static String toJSON(final BasicDBObject bsonObject) {
        return N.toJSON(bsonObject);
    }

    /**
     * Returns an instance of the specified target class with the property values from the specified JSON String.
     *
     * @param <T>
     * @param targetClass <code>Bson.class</code>, <code>Document.class</code>, <code>BasicBSONObject.class</code>, <code>BasicDBObject.class</code>
     * @param json
     * @return
     */
    public static <T> T fromJSON(final Class<T> targetClass, final String json) {
        if (targetClass.equals(Bson.class) || targetClass.equals(Document.class)) {
            final Document doc = new Document();
            jsonParser.readString(doc, json);
            return (T) doc;
        } else if (targetClass.equals(BasicBSONObject.class)) {
            final BasicBSONObject result = new BasicBSONObject();
            jsonParser.readString(result, json);
            return (T) result;
        } else if (targetClass.equals(BasicDBObject.class)) {
            final BasicDBObject result = new BasicDBObject();
            jsonParser.readString(result, json);
            return (T) result;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(targetClass));
        }
    }

    /**
     *
     * @param obj an array of pairs of property name and value/Map<String, Object> or an entity with getter/setter methods.
     * @return
     */
    public static Bson toBson(final Object obj) {
        return toDocument(obj);
    }

    /**
     * Create a new document with specified parameter(s). It could an array of property name and value, or a map, or an entity.
     *
     * @param a
     * @return
     */
    @SafeVarargs
    public static Bson toBson(final Object... a) {
        return toDocument(a);
    }

    /**
     *
     * @param obj an array of pairs of property name and value/Map<String, Object> or an entity with getter/setter methods.
     * @return
     */
    public static Document toDocument(final Object obj) {
        return toDocument(obj, false);
    }

    /**
     * Create a new document with specified parameter(s). It could an array of property name and value, or a map, or an entity.
     *
     * @param a
     * @return
     */
    @SafeVarargs
    public static Document toDocument(final Object... a) {
        if (N.isNullOrEmpty(a)) {
            return new Document();
        }

        return a.length == 1 ? toDocument(a[0]) : toDocument((Object) a);
    }

    /**
     *
     * @param obj
     * @param isForUpdate
     * @return
     */
    @SuppressWarnings("deprecation")
    static Document toDocument(final Object obj, final boolean isForUpdate) {
        final Document result = new Document();

        if (obj instanceof Map) {
            result.putAll((Map<String, Object>) obj);
        } else if (ClassUtil.isEntity(obj.getClass())) {
            if (obj instanceof DirtyMarker) {
                final Class<?> srCls = obj.getClass();
                final Set<String> propNamesToUpdate = isForUpdate ? ((DirtyMarker) obj).dirtyPropNames() : ((DirtyMarker) obj).signedPropNames();

                if (propNamesToUpdate.size() == 0) {
                    // logger.warn("No property is signed/updated in the specified source entity: " + N.toString(obj));
                } else {
                    final EntityInfo entityInfo = ParserUtil.getEntityInfo(srCls);
                    PropInfo propInfo = null;

                    for (String propName : propNamesToUpdate) {
                        propInfo = entityInfo.getPropInfo(propName);

                        result.put(propInfo.name, propInfo.getPropValue(obj));
                    }
                }
            } else {
                final Map<String, Method> getterMethodList = ClassUtil.getPropGetMethods(obj.getClass());

                if (getterMethodList.size() == 0) {
                    throw new IllegalArgumentException("No property getter/setter method found in the specified entity: " + obj.getClass().getCanonicalName());
                }

                String propName = null;
                Object propValue = null;

                for (Map.Entry<String, Method> entry : getterMethodList.entrySet()) {
                    propName = entry.getKey();
                    propValue = ClassUtil.getPropValue(obj, entry.getValue());

                    if (propValue == null) {
                        continue;
                    }

                    result.put(propName, propValue);
                }
            }
        } else if (obj instanceof Object[]) {
            final Object[] a = (Object[]) obj;

            if (0 != (a.length % 2)) {
                throw new IllegalArgumentException(
                        "The parameters must be the pairs of property name and value, or Map, or an entity class with getter/setter methods.");
            }

            for (int i = 0; i < a.length; i++) {
                result.put((String) a[i], a[++i]);
            }
        } else {
            throw new IllegalArgumentException("The parameters must be a Map, or an entity class with getter/setter methods");
        }

        resetObjectId(obj, result);

        return result;
    }

    /**
     * To BSON object.
     *
     * @param obj an array of pairs of property name and value/Map<String, Object> or an entity with getter/setter methods.
     * @return
     */
    public static BasicBSONObject toBSONObject(final Object obj) {
        final BasicBSONObject result = new BasicBSONObject();

        if (obj instanceof Map) {
            result.putAll((Map<String, Object>) obj);
        } else if (ClassUtil.isEntity(obj.getClass())) {
            Maps.deepEntity2Map(result, obj);
        } else if (obj instanceof Object[]) {
            final Object[] a = (Object[]) obj;

            if (0 != (a.length % 2)) {
                throw new IllegalArgumentException(
                        "The parameters must be the pairs of property name and value, or Map, or an entity class with getter/setter methods.");
            }

            for (int i = 0; i < a.length; i++) {
                result.put((String) a[i], a[++i]);
            }
        } else {
            throw new IllegalArgumentException("The parameters must be a Map, or an entity class with getter/setter methods");
        }

        resetObjectId(obj, result);

        return result;
    }

    /**
     * To BSON object.
     *
     * @param a
     * @return
     */
    @SafeVarargs
    public static BasicBSONObject toBSONObject(final Object... a) {
        if (N.isNullOrEmpty(a)) {
            return new BasicBSONObject();
        }

        return a.length == 1 ? toBSONObject(a[0]) : toBSONObject((Object) a);
    }

    /**
     * To DB object.
     *
     * @param obj an array of pairs of property name and value/Map<String, Object> or an entity with getter/setter methods.
     * @return
     */
    public static BasicDBObject toDBObject(final Object obj) {
        final BasicDBObject result = new BasicDBObject();

        if (obj instanceof Map) {
            result.putAll((Map<String, Object>) obj);
        } else if (ClassUtil.isEntity(obj.getClass())) {
            Maps.deepEntity2Map(result, obj);
        } else if (obj instanceof Object[]) {
            final Object[] a = (Object[]) obj;

            if (0 != (a.length % 2)) {
                throw new IllegalArgumentException(
                        "The parameters must be the pairs of property name and value, or Map, or an entity class with getter/setter methods.");
            }

            for (int i = 0; i < a.length; i++) {
                result.put((String) a[i], a[++i]);
            }
        } else {
            throw new IllegalArgumentException("The parameters must be a Map, or an entity class with getter/setter methods");
        }

        resetObjectId(obj, result);

        return result;
    }

    /**
     * To DB object.
     *
     * @param a
     * @return
     */
    @SafeVarargs
    public static BasicDBObject toDBObject(final Object... a) {
        if (N.isNullOrEmpty(a)) {
            return new BasicDBObject();
        }

        return a.length == 1 ? toDBObject(a[0]) : toDBObject((Object) a);
    }

    /**
     * Reset object id.
     *
     * @param obj
     * @param doc
     */
    private static void resetObjectId(final Object obj, final Map<String, Object> doc) {
        final Class<?> cls = obj.getClass();
        final Method idSetMethod = getObjectIdSetMethod(obj.getClass());
        final String idPropertyName = ClassUtil.isEntity(cls) ? (idSetMethod == null ? null : ClassUtil.getPropNameByMethod(idSetMethod)) : _ID;

        if (idPropertyName != null && doc.containsKey(idPropertyName)) {
            Object id = doc.remove(idPropertyName);

            try {
                if (id instanceof String) {
                    id = new ObjectId((String) id);
                } else if (id instanceof Date) {
                    id = new ObjectId((Date) id);
                } else if (id instanceof byte[]) {
                    id = new ObjectId((byte[]) id);
                }
            } finally {
                doc.put(_ID, id);
            }
        }
    }

    /**
     * Check target class.
     *
     * @param <T>
     * @param targetClass
     */
    private static <T> void checkTargetClass(final Class<T> targetClass) {
        if (!(ClassUtil.isEntity(targetClass) || Map.class.isAssignableFrom(targetClass))) {
            throw new IllegalArgumentException("The target class must be an entity class with getter/setter methods or Map.class/Document.class. But it is: "
                    + ClassUtil.getCanonicalClassName(targetClass));
        }
    }

    /**
     * The Class GeneralCodecRegistry.
     */
    private static class GeneralCodecRegistry implements CodecRegistry {

        /** The Constant pool. */
        private static final Map<Class<?>, Codec<?>> pool = new ObjectPool<>(128);

        /**
         *
         * @param <T>
         * @param clazz
         * @return
         */
        @Override
        public <T> Codec<T> get(final Class<T> clazz) {
            Codec<?> codec = pool.get(clazz);

            if (codec == null) {
                codec = new GeneralCodec<>(clazz);

                pool.put(clazz, codec);
            }

            return (Codec<T>) codec;
        }
    }

    /**
     * The Class GeneralCodec.
     *
     * @param <T>
     */
    private static class GeneralCodec<T> implements Codec<T> {

        /** The Constant documentCodec. */
        private static final DocumentCodec documentCodec = new DocumentCodec(codecRegistry, new BsonTypeClassMap());

        /** The cls. */
        private final Class<T> cls;

        /** The is entity class. */
        private final boolean isEntityClass;

        /**
         * Instantiates a new general codec.
         *
         * @param cls
         */
        public GeneralCodec(final Class<T> cls) {
            this.cls = cls;
            isEntityClass = ClassUtil.isEntity(cls);
        }

        /**
         *
         * @param writer
         * @param value
         * @param encoderContext
         */
        @Override
        public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
            if (isEntityClass) {
                documentCodec.encode(writer, toDocument(value), encoderContext);
            } else {
                writer.writeString(N.stringOf(value));
            }
        }

        /**
         *
         * @param reader
         * @param decoderContext
         * @return
         */
        @Override
        public T decode(final BsonReader reader, final DecoderContext decoderContext) {
            if (isEntityClass) {
                return toEntity(cls, documentCodec.decode(reader, decoderContext));
            } else {
                return N.valueOf(cls, reader.readString());
            }
        }

        /**
         * Gets the encoder class.
         *
         * @return
         */
        @Override
        public Class<T> getEncoderClass() {
            return cls;
        }
    }
}
