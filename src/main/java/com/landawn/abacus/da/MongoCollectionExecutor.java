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

package com.landawn.abacus.da;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.DataSet;
import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.da.AsyncMongoCollectionExecutor;
import com.landawn.abacus.da.MongoDB;
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
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.N;
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
import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

// TODO: Auto-generated Javadoc
/**
 * The Class MongoCollectionExecutor.
 *
 * @author Haiyang Li
 * @since 0.8
 */
public final class MongoCollectionExecutor {

    /** The Constant _$. */
    private static final String _$ = "$";

    /** The Constant _$SET. */
    private static final String _$SET = "$set";

    /** The Constant _$GROUP. */
    private static final String _$GROUP = "$group";

    /** The Constant _$SUM. */
    private static final String _$SUM = "$sum";

    /** The Constant _COUNT. */
    private static final String _COUNT = "count";

    /** The coll. */
    private final MongoCollection<Document> coll;

    /** The async coll executor. */
    private final AsyncMongoCollectionExecutor asyncCollExecutor;

    /**
     * Call <code>mongoDB.withCodecRegistry(CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry(), new GeneralCodecRegistry()));</code> to support the encode/decode for general type
     *
     * @param dbExecutor the db executor
     * @param coll the coll
     */
    MongoCollectionExecutor(final MongoDB dbExecutor, final MongoCollection<Document> coll) {
        this(dbExecutor, coll, new AsyncExecutor(64, 300, TimeUnit.SECONDS));
    }

    /**
     * Instantiates a new mongo collection executor.
     *
     * @param dbExecutor the db executor
     * @param coll the coll
     * @param asyncExecutor the async executor
     */
    MongoCollectionExecutor(final MongoDB dbExecutor, final MongoCollection<Document> coll, final AsyncExecutor asyncExecutor) {
        this.coll = coll;
        this.asyncCollExecutor = new AsyncMongoCollectionExecutor(this, asyncExecutor);
    }

    /**
     * Coll.
     *
     * @return the mongo collection
     */
    public MongoCollection<Document> coll() {
        return coll;
    }

    /**
     * Async.
     *
     * @return the async mongo collection executor
     */
    public AsyncMongoCollectionExecutor async() {
        return asyncCollExecutor;
    }

    /**
     * Exists.
     *
     * @param objectId the object id
     * @return true, if successful
     */
    public boolean exists(final String objectId) {
        return exists(createObjectId(objectId));
    }

    /**
     * Exists.
     *
     * @param objectId the object id
     * @return true, if successful
     */
    public boolean exists(final ObjectId objectId) {
        return exists(createFilter(objectId));
    }

    /**
     * Exists.
     *
     * @param filter the filter
     * @return true, if successful
     */
    public boolean exists(final Bson filter) {
        return count(filter, new CountOptions().limit(1)) > 0;
    }

    /**
     * Count.
     *
     * @return the long
     */
    public long count() {
        return coll.countDocuments();
    }

    /**
     * Count.
     *
     * @param filter the filter
     * @return the long
     */
    public long count(final Bson filter) {
        return coll.countDocuments(filter);
    }

    /**
     * Count.
     *
     * @param filter the filter
     * @param options the options
     * @return the long
     */
    public long count(final Bson filter, final CountOptions options) {
        if (options == null) {
            return coll.countDocuments(filter);
        } else {
            return coll.countDocuments(filter, options);
        }
    }

    /**
     * Gets the.
     *
     * @param objectId the object id
     * @return the optional
     */
    public Optional<Document> get(final String objectId) {
        return get(createObjectId(objectId));
    }

    /**
     * Gets the.
     *
     * @param objectId the object id
     * @return the optional
     */
    public Optional<Document> get(final ObjectId objectId) {
        return get(Document.class, objectId);
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @return the optional
     */
    public <T> Optional<T> get(final Class<T> targetClass, final String objectId) {
        return get(targetClass, createObjectId(objectId));
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @return the optional
     */
    public <T> Optional<T> get(final Class<T> targetClass, final ObjectId objectId) {
        return get(targetClass, objectId, null);
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @param selectPropNames the select prop names
     * @return the optional
     */
    public <T> Optional<T> get(final Class<T> targetClass, final String objectId, final Collection<String> selectPropNames) {
        return get(targetClass, createObjectId(objectId), selectPropNames);
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @param selectPropNames the select prop names
     * @return the optional
     */
    public <T> Optional<T> get(final Class<T> targetClass, final ObjectId objectId, final Collection<String> selectPropNames) {
        return findFirst(targetClass, selectPropNames, createFilter(objectId), null);
    }

    /**
     * Gets the t.
     *
     * @param objectId the object id
     * @return the t
     */
    public Document gett(final String objectId) {
        return gett(createObjectId(objectId));
    }

    /**
     * Gets the t.
     *
     * @param objectId the object id
     * @return the t
     */
    public Document gett(final ObjectId objectId) {
        return gett(Document.class, objectId);
    }

    /**
     * Gets the t.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @return the t
     */
    public <T> T gett(final Class<T> targetClass, final String objectId) {
        return gett(targetClass, createObjectId(objectId));
    }

    /**
     * Gets the t.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @return the t
     */
    public <T> T gett(final Class<T> targetClass, final ObjectId objectId) {
        return gett(targetClass, objectId, null);
    }

    /**
     * Gets the t.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @param selectPropNames the select prop names
     * @return the t
     */
    public <T> T gett(final Class<T> targetClass, final String objectId, final Collection<String> selectPropNames) {
        return gett(targetClass, createObjectId(objectId), selectPropNames);
    }

    /**
     * Gets the t.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @param selectPropNames the select prop names
     * @return the t
     */
    public <T> T gett(final Class<T> targetClass, final ObjectId objectId, final Collection<String> selectPropNames) {
        return findFirst(targetClass, selectPropNames, createFilter(objectId), null).orElse(null);
    }

    /**
     * Find first.
     *
     * @param filter the filter
     * @return the optional
     */
    public Optional<Document> findFirst(final Bson filter) {
        return findFirst(Document.class, filter);
    }

    /**
     * Find first.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @return the optional
     */
    public <T> Optional<T> findFirst(final Class<T> targetClass, final Bson filter) {
        return findFirst(targetClass, null, filter);
    }

    /**
     * Find first.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the optional
     */
    public <T> Optional<T> findFirst(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter) {
        return findFirst(targetClass, selectPropNames, filter, null);
    }

    /**
     * Find first.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the optional
     */
    public <T> Optional<T> findFirst(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        final FindIterable<Document> findIterable = query(selectPropNames, filter, sort, 0, 1);

        final T result = toEntity(targetClass, findIterable);

        return result == null ? (Optional<T>) Optional.empty() : Optional.of(result);
    }

    /**
     * Find first.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the optional
     */
    public <T> Optional<T> findFirst(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection) {
        final FindIterable<Document> findIterable = query(filter, sort, projection, 0, 1);

        final T result = toEntity(targetClass, findIterable);

        return result == null ? (Optional<T>) Optional.empty() : Optional.of(result);
    }

    /**
     * List.
     *
     * @param filter the filter
     * @return the list
     */
    public List<Document> list(final Bson filter) {
        return list(Document.class, filter);
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @return the list
     */
    public <T> List<T> list(final Class<T> targetClass, final Bson filter) {
        return list(targetClass, null, filter);
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the list
     */
    public <T> List<T> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter) {
        return list(targetClass, selectPropNames, filter, 0, Integer.MAX_VALUE);
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param offset the offset
     * @param count the count
     * @return the list
     */
    public <T> List<T> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return list(targetClass, selectPropNames, filter, null, offset, count);
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the list
     */
    public <T> List<T> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return list(targetClass, selectPropNames, filter, sort, 0, Integer.MAX_VALUE);
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass an entity class with getter/setter method, <code>Map.class</code> or basic single value type(Primitive/String/Date...)
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @param offset the offset
     * @param count the count
     * @return the list
     */
    public <T> List<T> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset,
            final int count) {
        final FindIterable<Document> findIterable = query(selectPropNames, filter, sort, offset, count);

        return MongoDB.toList(targetClass, findIterable);
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the list
     */
    public <T> List<T> list(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection) {
        return list(targetClass, filter, sort, projection, 0, Integer.MAX_VALUE);
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass an entity class with getter/setter method, <code>Map.class</code> or basic single value type(Primitive/String/Date...)
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @param offset the offset
     * @param count the count
     * @return the list
     */
    public <T> List<T> list(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection, final int offset, final int count) {
        final FindIterable<Document> findIterable = query(filter, sort, projection, offset, count);

        return MongoDB.toList(targetClass, findIterable);
    }

    /**
     * Query for boolean.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the optional boolean
     */
    @Beta
    public OptionalBoolean queryForBoolean(final String propName, final Bson filter) {
        return queryForSingleResult(Boolean.class, propName, filter).mapToBoolean(ToBooleanFunction.UNBOX);
    }

    /**
     * Query for char.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the optional char
     */
    @Beta
    public OptionalChar queryForChar(final String propName, final Bson filter) {
        return queryForSingleResult(Character.class, propName, filter).mapToChar(ToCharFunction.UNBOX);
    }

    /**
     * Query for byte.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the optional byte
     */
    @Beta
    public OptionalByte queryForByte(final String propName, final Bson filter) {
        return queryForSingleResult(Byte.class, propName, filter).mapToByte(ToByteFunction.UNBOX);
    }

    /**
     * Query for short.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the optional short
     */
    @Beta
    public OptionalShort queryForShort(final String propName, final Bson filter) {
        return queryForSingleResult(Short.class, propName, filter).mapToShort(ToShortFunction.UNBOX);
    }

    /**
     * Query for int.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the optional int
     */
    @Beta
    public OptionalInt queryForInt(final String propName, final Bson filter) {
        return queryForSingleResult(Integer.class, propName, filter).mapToInt(ToIntFunction.UNBOX);
    }

    /**
     * Query for long.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the optional long
     */
    @Beta
    public OptionalLong queryForLong(final String propName, final Bson filter) {
        return queryForSingleResult(Long.class, propName, filter).mapToLong(ToLongFunction.UNBOX);
    }

    /**
     * Query for float.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the optional float
     */
    @Beta
    public OptionalFloat queryForFloat(final String propName, final Bson filter) {
        return queryForSingleResult(Float.class, propName, filter).mapToFloat(ToFloatFunction.UNBOX);
    }

    /**
     * Query for double.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the optional double
     */
    @Beta
    public OptionalDouble queryForDouble(final String propName, final Bson filter) {
        return queryForSingleResult(Double.class, propName, filter).mapToDouble(ToDoubleFunction.UNBOX);
    }

    /**
     * Query for string.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the nullable
     */
    @Beta
    public Nullable<String> queryForString(final String propName, final Bson filter) {
        return queryForSingleResult(String.class, propName, filter);
    }

    /**
     * Query for date.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the nullable
     */
    @Beta
    public Nullable<Date> queryForDate(final String propName, final Bson filter) {
        return queryForSingleResult(Date.class, propName, filter);
    }

    /**
     * Query for date.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param propName the prop name
     * @param filter the filter
     * @return the nullable
     */
    public <T extends Date> Nullable<T> queryForDate(final Class<T> targetClass, final String propName, final Bson filter) {
        return queryForSingleResult(targetClass, propName, filter);
    }

    /**
     * Query for single result.
     *
     * @param <V> the value type
     * @param targetClass the target class
     * @param propName the prop name
     * @param filter the filter
     * @return the nullable
     */
    public <V> Nullable<V> queryForSingleResult(final Class<V> targetClass, final String propName, final Bson filter) {
        final FindIterable<Document> findIterable = query(N.asList(propName), filter, null, 0, 1);

        Document doc = findIterable.first();

        return N.isNullOrEmpty(doc) ? (Nullable<V>) Nullable.empty() : Nullable.of(N.convert(doc.get(propName), targetClass));
    }

    /**
     * To entity.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param findIterable the find iterable
     * @return the t
     */
    private <T> T toEntity(final Class<T> targetClass, final FindIterable<Document> findIterable) {
        final Document doc = findIterable.first();

        return N.isNullOrEmpty(doc) ? null : MongoDB.toEntity(targetClass, doc);
    }

    /**
     * Query.
     *
     * @param filter the filter
     * @return the data set
     */
    public DataSet query(final Bson filter) {
        return query(Document.class, filter);
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @return the data set
     */
    public <T> DataSet query(final Class<T> targetClass, final Bson filter) {
        return query(targetClass, null, filter);
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the data set
     */
    public <T> DataSet query(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter) {
        return query(targetClass, selectPropNames, filter, 0, Integer.MAX_VALUE);
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param offset the offset
     * @param count the count
     * @return the data set
     */
    public <T> DataSet query(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return query(targetClass, selectPropNames, filter, null, offset, count);
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the data set
     */
    public <T> DataSet query(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return query(targetClass, selectPropNames, filter, sort, 0, Integer.MAX_VALUE);
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @param offset the offset
     * @param count the count
     * @return the data set
     */
    public <T> DataSet query(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset,
            final int count) {
        final FindIterable<Document> findIterable = query(selectPropNames, filter, sort, offset, count);

        if (N.isNullOrEmpty(selectPropNames)) {
            return MongoDB.extractData(targetClass, findIterable);
        } else {
            return MongoDB.extractData(targetClass, selectPropNames, findIterable);
        }
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the data set
     */
    public <T> DataSet query(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection) {
        return query(targetClass, filter, sort, projection, 0, Integer.MAX_VALUE);
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @param offset the offset
     * @param count the count
     * @return the data set
     */
    public <T> DataSet query(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection, final int offset, final int count) {
        final FindIterable<Document> findIterable = query(filter, sort, projection, offset, count);

        return MongoDB.extractData(targetClass, findIterable);
    }

    /**
     * Stream.
     *
     * @param filter the filter
     * @return the stream
     */
    public Stream<Document> stream(final Bson filter) {
        return stream(Document.class, filter);
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @return the stream
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Bson filter) {
        return stream(targetClass, null, filter);
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the stream
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter) {
        return stream(targetClass, selectPropNames, filter, 0, Integer.MAX_VALUE);
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param offset the offset
     * @param count the count
     * @return the stream
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return stream(targetClass, selectPropNames, filter, null, offset, count);
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the stream
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return stream(targetClass, selectPropNames, filter, sort, 0, Integer.MAX_VALUE);
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @param offset the offset
     * @param count the count
     * @return the stream
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset,
            final int count) {
        final FindIterable<Document> findIterable = query(selectPropNames, filter, sort, offset, count);

        if (targetClass.isAssignableFrom(Document.class)) {
            return (Stream<T>) Stream.of(findIterable.iterator());
        } else {
            return Stream.of(findIterable.iterator()).map(toEntity(targetClass));
        }
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the stream
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection) {
        return stream(targetClass, filter, sort, projection, 0, Integer.MAX_VALUE);
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @param offset the offset
     * @param count the count
     * @return the stream
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection, final int offset, final int count) {
        final FindIterable<Document> findIterable = query(filter, sort, projection, offset, count);

        if (targetClass.isAssignableFrom(Document.class)) {
            return (Stream<T>) Stream.of(findIterable.iterator());
        } else {
            return Stream.of(findIterable.iterator()).map(toEntity(targetClass));
        }
    }

    /**
     * To entity.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @return the function
     */
    private <T> Function<Document, T> toEntity(final Class<T> targetClass) {
        return new Function<Document, T>() {
            @Override
            public T apply(Document t) {
                return MongoDB.toEntity(targetClass, t);
            }
        };
    }

    /**
     * Query.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @param offset the offset
     * @param count the count
     * @return the find iterable
     */
    private FindIterable<Document> query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        if (N.isNullOrEmpty(selectPropNames)) {
            return this.query(filter, sort, null, offset, count);
        } else if (selectPropNames instanceof List) {
            return this.query(filter, sort, Projections.include((List<String>) selectPropNames), offset, count);
        } else {
            return this.query(filter, sort, Projections.include(selectPropNames.toArray(new String[selectPropNames.size()])), offset, count);
        }
    }

    /**
     * Query.
     *
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @param offset the offset
     * @param count the count
     * @return the find iterable
     */
    private FindIterable<Document> query(final Bson filter, final Bson sort, final Bson projection, final int offset, final int count) {
        if (offset < 0 || count < 0) {
            throw new IllegalArgumentException("offset (" + offset + ") and count(" + count + ") can't be negative");
        }

        FindIterable<Document> findIterable = coll.find(filter);

        if (projection != null) {
            findIterable = findIterable.projection(projection);
        }

        if (sort != null) {
            findIterable = findIterable.sort(sort);
        }

        if (offset > 0) {
            findIterable = findIterable.skip(offset);
        }

        if (count < Integer.MAX_VALUE) {
            findIterable = findIterable.limit(count);
        }

        return findIterable;
    }

    /**
     * Insert.
     *
     * @param obj can be <code>Document/Map<String, Object>/entity</code> class with getter/setter method.
     */
    public void insert(final Object obj) {
        coll.insertOne(createDocument(obj));
    }

    /**
     * Insert.
     *
     * @param obj the obj
     * @param options the options
     */
    public void insert(final Object obj, final InsertOneOptions options) {
        coll.insertOne(createDocument(obj), options);
    }

    /**
     * Insert all.
     *
     * @param objList list of <code>Document/Map<String, Object>/entity</code> class with getter/setter method.
     */
    public void insertAll(final Collection<?> objList) {
        insertAll(objList, null);
    }

    /**
     * Insert all.
     *
     * @param objList list of <code>Document/Map<String, Object>/entity</code> class with getter/setter method.
     * @param options the options
     */
    public void insertAll(final Collection<?> objList, final InsertManyOptions options) {
        List<Document> docs = null;

        if (objList.iterator().next() instanceof Document) {
            if (objList instanceof List) {
                docs = (List<Document>) objList;
            } else {
                docs = new ArrayList<>((Collection<Document>) objList);
            }
        } else {
            docs = new ArrayList<>(objList.size());

            for (Object entity : objList) {
                docs.add(createDocument(entity));
            }
        }

        if (options == null) {
            coll.insertMany(docs);
        } else {
            coll.insertMany(docs, options);
        }
    }

    /**
     * Update the record in data store with the properties which have been updated/set in the specified <code>update</code> by the specified <code>objectId</code>.
     * if the <code>update</code> implements <code>DirtyMarker</code> interface, just update the dirty properties.
     *
     * @param objectId the object id
     * @param update can be <code>Bson/Document/Map<String, Object>/entity class with getter/setter method.
     * @return the update result
     */
    public UpdateResult update(final String objectId, final Object update) {
        return update(createObjectId(objectId), update);
    }

    /**
     * Update the record in data store with the properties which have been updated/set in the specified <code>update</code> by the specified <code>objectId</code>.
     * if the <code>update</code> implements <code>DirtyMarker</code> interface, just update the dirty properties.
     *
     * @param objectId the object id
     * @param update can be <code>Bson/Document/Map<String, Object>/entity class with getter/setter method.
     * @return the update result
     */
    public UpdateResult update(final ObjectId objectId, final Object update) {
        return updateOne(createFilter(objectId), update);
    }

    /**
     * Just update one record in data store with the properties which have been updated/set in the specified <code>update</code> by the specified <code>filter</code>.
     * if the <code>update</code> implements <code>DirtyMarker</code> interface, just update the dirty properties.
     *
     * @param filter the filter
     * @param update can be <code>Bson/Document/Map<String, Object>/entity class with getter/setter method.
     * @return the update result
     */
    public UpdateResult updateOne(final Bson filter, final Object update) {
        return updateOne(filter, update, null);
    }

    /**
     * Just update one record in data store with the properties which have been updated/set in the specified <code>update</code> by the specified <code>filter</code>.
     * if the <code>update</code> implements <code>DirtyMarker</code> interface, just update the dirty properties.
     *
     * @param filter the filter
     * @param update can be <code>Bson/Document/Map<String, Object>/entity class with getter/setter method.
     * @param options the options
     * @return the update result
     */
    public UpdateResult updateOne(final Bson filter, final Object update, final UpdateOptions options) {
        if (options == null) {
            return coll.updateOne(filter, checkUpdate(update));
        } else {
            return coll.updateOne(filter, checkUpdate(update), options);
        }
    }

    /**
     * Update the records in data store with the properties which have been updated/set in the specified <code>update</code> by the specified <code>filter</code>.
     * if the <code>update</code> implements <code>DirtyMarker</code> interface, just update the dirty properties.
     *
     * @param filter the filter
     * @param update can be <code>Bson/Document/Map<String, Object>/entity class with getter/setter method.
     * @return the update result
     */
    public UpdateResult updateAll(final Bson filter, final Object update) {
        return updateAll(filter, update, null);
    }

    /**
     * Update the records in data store with the properties which have been updated/set in the specified <code>update</code> by the specified <code>filter</code>.
     * if the <code>update</code> implements <code>DirtyMarker</code> interface, just update the dirty properties.
     *
     * @param filter the filter
     * @param update can be <code>Bson/Document/Map<String, Object>/entity class with getter/setter method.
     * @param options the options
     * @return the update result
     */
    public UpdateResult updateAll(final Bson filter, final Object update, final UpdateOptions options) {
        if (options == null) {
            return coll.updateMany(filter, checkUpdate(update));
        } else {
            return coll.updateMany(filter, checkUpdate(update), options);
        }
    }

    /**
     * Replace.
     *
     * @param objectId the object id
     * @param replacement can be <code>Document/Map<String, Object>/entity</code> class with getter/setter method.
     * @return the update result
     */
    public UpdateResult replace(final String objectId, final Object replacement) {
        return replace(createObjectId(objectId), replacement);
    }

    /**
     * Replace.
     *
     * @param objectId the object id
     * @param replacement can be <code>Document/Map<String, Object>/entity</code> class with getter/setter method.
     * @return the update result
     */
    public UpdateResult replace(final ObjectId objectId, final Object replacement) {
        return replaceOne(createFilter(objectId), replacement);
    }

    /**
     * Replace one.
     *
     * @param filter the filter
     * @param replacement can be <code>Document/Map<String, Object>/entity</code> class with getter/setter method.
     * @return the update result
     */
    public UpdateResult replaceOne(final Bson filter, final Object replacement) {
        return replaceOne(filter, replacement, null);
    }

    /**
     * Replace one.
     *
     * @param filter the filter
     * @param replacement can be <code>Document/Map<String, Object>/entity</code> class with getter/setter method.
     * @param options the options
     * @return the update result
     */
    public UpdateResult replaceOne(final Bson filter, final Object replacement, final ReplaceOptions options) {
        if (options == null) {
            return coll.replaceOne(filter, createDocument(replacement));
        } else {
            return coll.replaceOne(filter, createDocument(replacement), options);
        }
    }

    /**
     * Delete.
     *
     * @param objectId the object id
     * @return the delete result
     */
    public DeleteResult delete(final String objectId) {
        return delete(createObjectId(objectId));
    }

    /**
     * Delete.
     *
     * @param objectId the object id
     * @return the delete result
     */
    public DeleteResult delete(final ObjectId objectId) {
        return deleteOne(createFilter(objectId));
    }

    /**
     * Delete one.
     *
     * @param filter the filter
     * @return the delete result
     */
    public DeleteResult deleteOne(final Bson filter) {
        return coll.deleteOne(filter);
    }

    /**
     * Delete one.
     *
     * @param filter the filter
     * @param options the options
     * @return the delete result
     */
    public DeleteResult deleteOne(final Bson filter, final DeleteOptions options) {
        return coll.deleteOne(filter, options);
    }

    /**
     * Delete all.
     *
     * @param filter the filter
     * @return the delete result
     */
    public DeleteResult deleteAll(final Bson filter) {
        return coll.deleteMany(filter);
    }

    /**
     * Delete all.
     *
     * @param filter the filter
     * @param options the options
     * @return the delete result
     */
    public DeleteResult deleteAll(final Bson filter, final DeleteOptions options) {
        return coll.deleteMany(filter, options);
    }

    /**
     * Bulk insert.
     *
     * @param entities the entities
     * @return the int
     */
    public int bulkInsert(final Collection<?> entities) {
        return bulkInsert(entities, null);
    }

    /**
     * Bulk insert.
     *
     * @param entities the entities
     * @param options the options
     * @return the int
     */
    public int bulkInsert(final Collection<?> entities, final BulkWriteOptions options) {
        final List<InsertOneModel<Document>> list = new ArrayList<>(entities.size());

        for (Object entity : entities) {
            if (entity instanceof Document) {
                list.add(new InsertOneModel<Document>((Document) entity));
            } else {
                list.add(new InsertOneModel<Document>(MongoDB.toDocument(entity)));
            }
        }

        return bulkWrite(list, options).getInsertedCount();
    }

    /**
     * Bulk write.
     *
     * @param requests the requests
     * @return the bulk write result
     */
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends Document>> requests) {
        return bulkWrite(requests, null);
    }

    /**
     * Bulk write.
     *
     * @param requests the requests
     * @param options the options
     * @return the bulk write result
     */
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends Document>> requests, final BulkWriteOptions options) {
        if (options == null) {
            return coll.bulkWrite(requests);
        } else {
            return coll.bulkWrite(requests, options);
        }
    }

    /**
     * Distinct.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param fieldName the field name
     * @return the stream
     */
    public <T> Stream<T> distinct(final Class<T> targetClass, final String fieldName) {
        return Stream.of(coll.distinct(fieldName, targetClass).iterator());
    }

    /**
     * Distinct.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param fieldName the field name
     * @param filter the filter
     * @return the stream
     */
    public <T> Stream<T> distinct(final Class<T> targetClass, final String fieldName, final Bson filter) {
        return Stream.of(coll.distinct(fieldName, filter, targetClass).iterator());
    }

    /**
     * Aggregate.
     *
     * @param pipeline the pipeline
     * @return the stream
     */
    public Stream<Document> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(Document.class, pipeline);
    }

    /**
     * Aggregate.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param pipeline the pipeline
     * @return the stream
     */
    public <T> Stream<T> aggregate(final Class<T> targetClass, final List<? extends Bson> pipeline) {
        return Stream.of(coll.aggregate(pipeline, Document.class).iterator()).map(toEntity(targetClass));
    }

    /**
     * Map reduce.
     *
     * @param mapFunction the map function
     * @param reduceFunction the reduce function
     * @return the stream
     */
    public Stream<Document> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(Document.class, mapFunction, reduceFunction);
    }

    /**
     * Map reduce.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param mapFunction the map function
     * @param reduceFunction the reduce function
     * @return the stream
     */
    public <T> Stream<T> mapReduce(final Class<T> targetClass, final String mapFunction, final String reduceFunction) {
        return Stream.of(coll.mapReduce(mapFunction, reduceFunction, Document.class).iterator()).map(toEntity(targetClass));
    }

    /**
     * Group by.
     *
     * @param fieldName the field name
     * @return the stream
     */
    @Beta
    public Stream<Document> groupBy(final String fieldName) {
        return aggregate(N.asList(new Document(_$GROUP, new Document(MongoDB._ID, _$ + fieldName))));
    }

    /**
     * Group by.
     *
     * @param fieldNames the field names
     * @return the stream
     */
    @Beta
    public Stream<Document> groupBy(final Collection<String> fieldNames) {
        final Document groupFields = new Document();

        for (String fieldName : fieldNames) {
            groupFields.put(fieldName, _$ + fieldName);
        }

        return aggregate(N.asList(new Document(_$GROUP, new Document(MongoDB._ID, groupFields))));
    }

    /**
     * Group by and count.
     *
     * @param fieldName the field name
     * @return the stream
     */
    @Beta
    public Stream<Document> groupByAndCount(final String fieldName) {
        return aggregate(N.asList(new Document(_$GROUP, new Document(MongoDB._ID, _$ + fieldName).append(_COUNT, new Document(_$SUM, 1)))));
    }

    /**
     * Group by and count.
     *
     * @param fieldNames the field names
     * @return the stream
     */
    @Beta
    public Stream<Document> groupByAndCount(final Collection<String> fieldNames) {
        final Document groupFields = new Document();

        for (String fieldName : fieldNames) {
            groupFields.put(fieldName, _$ + fieldName);
        }

        return aggregate(N.asList(new Document(_$GROUP, new Document(MongoDB._ID, groupFields).append(_COUNT, new Document(_$SUM, 1)))));
    }

    //
    //    private String getCollectionName(final Class<?> cls) {
    //        final String collectionName = classCollectionMapper.get(cls);
    //
    //        if (N.isNullOrEmpty(collectionName)) {
    //            throw new IllegalArgumentException("No collection is mapped to class: " + cls);
    //        }
    //        return collectionName;
    //    }

    //
    //    private String getObjectId(Object obj) {
    //        String objectId = null;
    //
    //        try {
    //            objectId = N.convert(String.class, N.getPropValue(obj, "id"));
    //        } catch (Exception e) {
    //            // ignore
    //
    //            try {
    //                objectId = N.convert(String.class, N.getPropValue(obj, "objectId"));
    //            } catch (Exception e2) {
    //                // ignore
    //            }
    //        }
    //
    //        if (N.isNullOrEmpty(objectId)) {
    //            throw new IllegalArgumentException("Property value of 'id' or 'objectId' can't be null or empty for update or delete");
    //        }
    //
    //        return objectId;
    //    }
    //

    /**
     * Check update.
     *
     * @param update the update
     * @return the bson
     */
    private Bson checkUpdate(final Object update) {
        Bson bson = update instanceof Bson ? (Bson) update : MongoDB.toDocument(update, true);

        if (bson instanceof Document) {
            Document doc = (Document) bson;

            if (doc.size() > 0 && doc.keySet().iterator().next().startsWith(_$)) {
                return doc;
            }
        } else if (bson instanceof BasicDBObject) {
            BasicDBObject dbObject = (BasicDBObject) bson;

            if (dbObject.size() > 0 && dbObject.keySet().iterator().next().startsWith(_$)) {
                return dbObject;
            }
        }

        return new Document(_$SET, bson);
    }

    /**
     * Creates the object id.
     *
     * @param objectId the object id
     * @return the object id
     */
    private ObjectId createObjectId(final String objectId) {
        if (N.isNullOrEmpty(objectId)) {
            throw new IllegalArgumentException("Object id cant' be null or empty");
        }

        return new ObjectId(objectId);
    }

    /**
     * Creates the filter.
     *
     * @param objectId the object id
     * @return the bson
     */
    private Bson createFilter(final ObjectId objectId) {
        return new Document(MongoDB._ID, objectId);
    }

    /**
     * Creates the document.
     *
     * @param obj the obj
     * @return the document
     */
    private Document createDocument(final Object obj) {
        return obj instanceof Document ? (Document) obj : MongoDB.toDocument(obj);
    }
}
