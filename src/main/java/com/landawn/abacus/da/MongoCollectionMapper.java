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

import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.DataSet;
import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.da.MongoCollectionExecutor;
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
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

// TODO: Auto-generated Javadoc
/**
 * The Class MongoCollectionMapper.
 *
 * @author Haiyang Li
 * @param <T> the generic type
 * @since 0.8
 */
public final class MongoCollectionMapper<T> {

    /** The coll executor. */
    private final MongoCollectionExecutor collExecutor;

    /** The target class. */
    private final Class<T> targetClass;

    /**
     * Instantiates a new mongo collection mapper.
     *
     * @param collExecutor the coll executor
     * @param targetClass the target class
     */
    MongoCollectionMapper(final MongoCollectionExecutor collExecutor, final Class<T> targetClass) {
        this.collExecutor = collExecutor;
        this.targetClass = targetClass;
    }

    /**
     * Coll executor.
     *
     * @return the mongo collection executor
     */
    public MongoCollectionExecutor collExecutor() {
        return collExecutor;
    }

    /**
     * Exists.
     *
     * @param objectId the object id
     * @return true, if successful
     */
    public boolean exists(final String objectId) {
        return collExecutor.exists(objectId);
    }

    /**
     * Exists.
     *
     * @param objectId the object id
     * @return true, if successful
     */
    public boolean exists(final ObjectId objectId) {
        return collExecutor.exists(objectId);
    }

    /**
     * Exists.
     *
     * @param filter the filter
     * @return true, if successful
     */
    public boolean exists(final Bson filter) {
        return collExecutor.exists(filter);
    }

    /**
     * Count.
     *
     * @return the long
     */
    public long count() {
        return collExecutor.count();
    }

    /**
     * Count.
     *
     * @param filter the filter
     * @return the long
     */
    public long count(final Bson filter) {
        return collExecutor.count(filter);
    }

    /**
     * Count.
     *
     * @param filter the filter
     * @param options the options
     * @return the long
     */
    public long count(final Bson filter, final CountOptions options) {
        return collExecutor.count(filter, options);
    }

    /**
     * Gets the.
     *
     * @param objectId the object id
     * @return the optional
     */
    public Optional<T> get(final String objectId) {
        return collExecutor.get(targetClass, objectId);
    }

    /**
     * Gets the.
     *
     * @param objectId the object id
     * @return the optional
     */
    public Optional<T> get(final ObjectId objectId) {
        return collExecutor.get(targetClass, objectId);
    }

    /**
     * Gets the.
     *
     * @param objectId the object id
     * @param selectPropNames the select prop names
     * @return the optional
     */
    public Optional<T> get(final String objectId, final Collection<String> selectPropNames) {
        return collExecutor.get(targetClass, objectId, selectPropNames);
    }

    /**
     * Gets the.
     *
     * @param objectId the object id
     * @param selectPropNames the select prop names
     * @return the optional
     */
    public Optional<T> get(final ObjectId objectId, final Collection<String> selectPropNames) {
        return collExecutor.get(targetClass, objectId, selectPropNames);
    }

    /**
     * Gets the t.
     *
     * @param objectId the object id
     * @return the t
     */
    public T gett(final String objectId) {
        return collExecutor.gett(targetClass, objectId);
    }

    /**
     * Gets the t.
     *
     * @param objectId the object id
     * @return the t
     */
    public T gett(final ObjectId objectId) {
        return collExecutor.gett(targetClass, objectId);
    }

    /**
     * Gets the t.
     *
     * @param objectId the object id
     * @param selectPropNames the select prop names
     * @return the t
     */
    public T gett(final String objectId, final Collection<String> selectPropNames) {
        return collExecutor.gett(targetClass, objectId, selectPropNames);
    }

    /**
     * Gets the t.
     *
     * @param objectId the object id
     * @param selectPropNames the select prop names
     * @return the t
     */
    public T gett(final ObjectId objectId, final Collection<String> selectPropNames) {
        return collExecutor.gett(targetClass, objectId, selectPropNames);
    }

    /**
     * Find first.
     *
     * @param filter the filter
     * @return the optional
     */
    public Optional<T> findFirst(final Bson filter) {
        return collExecutor.findFirst(targetClass, filter);
    }

    /**
     * Find first.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the optional
     */
    public Optional<T> findFirst(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.findFirst(targetClass, selectPropNames, filter);
    }

    /**
     * Find first.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the optional
     */
    public Optional<T> findFirst(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.findFirst(targetClass, selectPropNames, filter, sort);
    }

    /**
     * Find first.
     *
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the optional
     */
    public Optional<T> findFirst(final Bson filter, final Bson sort, final Bson projection) {
        return collExecutor.findFirst(targetClass, filter, sort, projection);
    }

    /**
     * List.
     *
     * @param filter the filter
     * @return the list
     */
    public List<T> list(final Bson filter) {
        return collExecutor.list(targetClass, filter);
    }

    /**
     * List.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the list
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.list(targetClass, selectPropNames, filter);
    }

    /**
     * List.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param offset the offset
     * @param count the count
     * @return the list
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collExecutor.list(targetClass, selectPropNames, filter, offset, count);
    }

    /**
     * List.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the list
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.list(targetClass, selectPropNames, filter, sort);
    }

    /**
     * List.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @param offset the offset
     * @param count the count
     * @return the list
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.list(targetClass, selectPropNames, filter, sort, offset, count);
    }

    /**
     * List.
     *
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the list
     */
    public List<T> list(final Bson filter, final Bson sort, final Bson projection) {
        return collExecutor.list(targetClass, filter, sort, projection);
    }

    /**
     * List.
     *
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @param offset the offset
     * @param count the count
     * @return the list
     */
    public List<T> list(final Bson filter, final Bson sort, final Bson projection, final int offset, final int count) {
        return collExecutor.list(targetClass, filter, sort, projection, offset, count);
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
        return collExecutor.queryForBoolean(propName, filter);
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
        return collExecutor.queryForChar(propName, filter);
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
        return collExecutor.queryForByte(propName, filter);
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
        return collExecutor.queryForShort(propName, filter);
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
        return collExecutor.queryForInt(propName, filter);
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
        return collExecutor.queryForLong(propName, filter);
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
        return collExecutor.queryForFloat(propName, filter);
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
        return collExecutor.queryForDouble(propName, filter);
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
        return collExecutor.queryForString(propName, filter);
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
        return collExecutor.queryForDate(propName, filter);
    }

    /**
     * Query for date.
     *
     * @param <P> the generic type
     * @param targetPropClass the target prop class
     * @param propName the prop name
     * @param filter the filter
     * @return the nullable
     */
    public <P extends Date> Nullable<P> queryForDate(final Class<P> targetPropClass, final String propName, final Bson filter) {
        return collExecutor.queryForDate(targetPropClass, propName, filter);
    }

    /**
     * Query for single result.
     *
     * @param <V> the value type
     * @param targetPropClass the target prop class
     * @param propName the prop name
     * @param filter the filter
     * @return the nullable
     */
    public <V> Nullable<V> queryForSingleResult(final Class<V> targetPropClass, final String propName, final Bson filter) {
        return collExecutor.queryForSingleResult(targetPropClass, propName, filter);
    }

    /**
     * Query.
     *
     * @param filter the filter
     * @return the data set
     */
    public DataSet query(final Bson filter) {
        return collExecutor.query(targetClass, filter);
    }

    /**
     * Query.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the data set
     */
    public DataSet query(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.query(targetClass, selectPropNames, filter);
    }

    /**
     * Query.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param offset the offset
     * @param count the count
     * @return the data set
     */
    public DataSet query(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collExecutor.query(targetClass, selectPropNames, filter, offset, count);
    }

    /**
     * Query.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the data set
     */
    public DataSet query(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.query(targetClass, selectPropNames, filter, sort);
    }

    /**
     * Query.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @param offset the offset
     * @param count the count
     * @return the data set
     */
    public DataSet query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.query(targetClass, selectPropNames, filter, sort, offset, count);
    }

    /**
     * Query.
     *
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the data set
     */
    public DataSet query(final Bson filter, final Bson sort, final Bson projection) {
        return collExecutor.query(targetClass, filter, sort, projection);
    }

    /**
     * Query.
     *
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @param offset the offset
     * @param count the count
     * @return the data set
     */
    public DataSet query(final Bson filter, final Bson sort, final Bson projection, final int offset, final int count) {
        return collExecutor.query(targetClass, filter, sort, projection, offset, count);
    }

    /**
     * Stream.
     *
     * @param filter the filter
     * @return the stream
     */
    public Stream<T> stream(final Bson filter) {
        return collExecutor.stream(targetClass, filter);
    }

    /**
     * Stream.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the stream
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.stream(targetClass, selectPropNames, filter);
    }

    /**
     * Stream.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param offset the offset
     * @param count the count
     * @return the stream
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collExecutor.stream(targetClass, selectPropNames, filter, offset, count);
    }

    /**
     * Stream.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the stream
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.stream(targetClass, selectPropNames, filter, sort);
    }

    /**
     * Stream.
     *
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @param offset the offset
     * @param count the count
     * @return the stream
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.stream(targetClass, selectPropNames, filter, sort, offset, count);
    }

    /**
     * Stream.
     *
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the stream
     */
    public Stream<T> stream(final Bson filter, final Bson sort, final Bson projection) {
        return collExecutor.stream(targetClass, filter, sort, projection);
    }

    /**
     * Stream.
     *
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @param offset the offset
     * @param count the count
     * @return the stream
     */
    public Stream<T> stream(final Bson filter, final Bson sort, final Bson projection, final int offset, final int count) {
        return collExecutor.stream(targetClass, filter, sort, projection, offset, count);
    }

    /**
     * Insert.
     *
     * @param obj the obj
     */
    public void insert(final T obj) {
        collExecutor.insert(obj);
    }

    /**
     * Insert.
     *
     * @param obj the obj
     * @param options the options
     */
    public void insert(final T obj, final InsertOneOptions options) {
        collExecutor.insert(obj, options);
    }

    /**
     * Insert all.
     *
     * @param objList the obj list
     */
    public void insertAll(final Collection<? extends T> objList) {
        collExecutor.insertAll(objList);
    }

    /**
     * Insert all.
     *
     * @param objList the obj list
     * @param options the options
     */
    public void insertAll(final Collection<? extends T> objList, final InsertManyOptions options) {
        collExecutor.insertAll(objList, options);
    }

    /**
     * Update.
     *
     * @param objectId the object id
     * @param update the update
     * @return the update result
     */
    public UpdateResult update(final String objectId, final T update) {
        return collExecutor.update(objectId, update);
    }

    /**
     * Update.
     *
     * @param objectId the object id
     * @param update the update
     * @return the update result
     */
    public UpdateResult update(final ObjectId objectId, final T update) {
        return collExecutor.update(objectId, update);
    }

    /**
     * Update one.
     *
     * @param filter the filter
     * @param update the update
     * @return the update result
     */
    public UpdateResult updateOne(final Bson filter, final T update) {
        return collExecutor.updateOne(filter, update);
    }

    /**
     * Update one.
     *
     * @param filter the filter
     * @param update the update
     * @param options the options
     * @return the update result
     */
    public UpdateResult updateOne(final Bson filter, final T update, final UpdateOptions options) {
        return collExecutor.updateOne(filter, update, options);
    }

    /**
     * Update all.
     *
     * @param filter the filter
     * @param update the update
     * @return the update result
     */
    public UpdateResult updateAll(final Bson filter, final T update) {
        return collExecutor.updateAll(filter, update);
    }

    /**
     * Update all.
     *
     * @param filter the filter
     * @param update the update
     * @param options the options
     * @return the update result
     */
    public UpdateResult updateAll(final Bson filter, final T update, final UpdateOptions options) {
        return collExecutor.updateAll(filter, update, options);
    }

    /**
     * Replace.
     *
     * @param objectId the object id
     * @param replacement the replacement
     * @return the update result
     */
    public UpdateResult replace(final String objectId, final T replacement) {
        return collExecutor.replace(objectId, replacement);
    }

    /**
     * Replace.
     *
     * @param objectId the object id
     * @param replacement the replacement
     * @return the update result
     */
    public UpdateResult replace(final ObjectId objectId, final T replacement) {
        return collExecutor.replace(objectId, replacement);
    }

    /**
     * Replace one.
     *
     * @param filter the filter
     * @param replacement the replacement
     * @return the update result
     */
    public UpdateResult replaceOne(final Bson filter, final T replacement) {
        return collExecutor.replaceOne(filter, replacement);
    }

    /**
     * Replace one.
     *
     * @param filter the filter
     * @param replacement the replacement
     * @param options the options
     * @return the update result
     */
    public UpdateResult replaceOne(final Bson filter, final T replacement, final ReplaceOptions options) {
        return collExecutor.replaceOne(filter, replacement, options);
    }

    /**
     * Delete.
     *
     * @param objectId the object id
     * @return the delete result
     */
    public DeleteResult delete(final String objectId) {
        return collExecutor.delete(objectId);
    }

    /**
     * Delete.
     *
     * @param objectId the object id
     * @return the delete result
     */
    public DeleteResult delete(final ObjectId objectId) {
        return collExecutor.delete(objectId);
    }

    /**
     * Delete one.
     *
     * @param filter the filter
     * @return the delete result
     */
    public DeleteResult deleteOne(final Bson filter) {
        return collExecutor.deleteOne(filter);
    }

    /**
     * Delete one.
     *
     * @param filter the filter
     * @param options the options
     * @return the delete result
     */
    public DeleteResult deleteOne(final Bson filter, final DeleteOptions options) {
        return collExecutor.deleteOne(filter, options);
    }

    /**
     * Delete all.
     *
     * @param filter the filter
     * @return the delete result
     */
    public DeleteResult deleteAll(final Bson filter) {
        return collExecutor.deleteAll(filter);
    }

    /**
     * Delete all.
     *
     * @param filter the filter
     * @param options the options
     * @return the delete result
     */
    public DeleteResult deleteAll(Bson filter, DeleteOptions options) {
        return collExecutor.deleteAll(filter, options);
    }

    /**
     * Bulk insert.
     *
     * @param entities the entities
     * @return the int
     */
    public int bulkInsert(final Collection<? extends T> entities) {
        return collExecutor.bulkInsert(entities);
    }

    /**
     * Bulk insert.
     *
     * @param entities the entities
     * @param options the options
     * @return the int
     */
    public int bulkInsert(final Collection<? extends T> entities, final BulkWriteOptions options) {
        return collExecutor.bulkInsert(entities, options);
    }

    /**
     * Bulk write.
     *
     * @param requests the requests
     * @return the bulk write result
     */
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends Document>> requests) {
        return collExecutor.bulkWrite(requests);
    }

    /**
     * Bulk write.
     *
     * @param requests the requests
     * @param options the options
     * @return the bulk write result
     */
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends Document>> requests, final BulkWriteOptions options) {
        return collExecutor.bulkWrite(requests, options);
    }

    /**
     * Distinct.
     *
     * @param fieldName the field name
     * @return the stream
     */
    public Stream<T> distinct(final String fieldName) {
        return collExecutor.distinct(targetClass, fieldName);
    }

    /**
     * Distinct.
     *
     * @param fieldName the field name
     * @param filter the filter
     * @return the stream
     */
    public Stream<T> distinct(final String fieldName, final Bson filter) {
        return collExecutor.distinct(targetClass, fieldName, filter);
    }

    /**
     * Aggregate.
     *
     * @param pipeline the pipeline
     * @return the stream
     */
    public Stream<T> aggregate(final List<? extends Bson> pipeline) {
        return collExecutor.aggregate(targetClass, pipeline);
    }

    /**
     * Map reduce.
     *
     * @param mapFunction the map function
     * @param reduceFunction the reduce function
     * @return the stream
     */
    public Stream<T> mapReduce(final String mapFunction, final String reduceFunction) {
        return collExecutor.mapReduce(targetClass, mapFunction, reduceFunction);
    }

    // TODO
    //    @Beta
    //    public Stream<Document> groupBy(final String fieldName) {
    //        return collExecutor.groupBy(fieldName);
    //    }
    //
    //    @Beta
    //    public Stream<Document> groupBy(final Collection<String> fieldNames) {
    //        return collExecutor.groupBy(fieldNames);
    //    }
    //
    //    @Beta
    //    public Stream<Document> groupByAndCount(final String fieldName) {
    //        return collExecutor.groupByAndCount(fieldName);
    //    }
    //
    //    @Beta
    //    public Stream<Document> groupByAndCount(final Collection<String> fieldNames) {
    //        return collExecutor.groupByAndCount(fieldNames);
    //    }
}
