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

import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.DataSet;
import com.landawn.abacus.annotation.Beta;
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

/**
 *
 * @author Haiyang Li
 * @param <T>
 * @since 0.8
 */
public final class MongoCollectionMapper<T> {

    private final MongoCollectionExecutor collExecutor;

    private final Class<T> targetClass;

    MongoCollectionMapper(final MongoCollectionExecutor collExecutor, final Class<T> targetClass) {
        this.collExecutor = collExecutor;
        this.targetClass = targetClass;
    }

    public MongoCollectionExecutor collExecutor() {
        return collExecutor;
    }

    /**
     *
     * @param objectId
     * @return true, if successful
     */
    public boolean exists(final String objectId) {
        return collExecutor.exists(objectId);
    }

    /**
     *
     * @param objectId
     * @return true, if successful
     */
    public boolean exists(final ObjectId objectId) {
        return collExecutor.exists(objectId);
    }

    /**
     *
     * @param filter
     * @return true, if successful
     */
    public boolean exists(final Bson filter) {
        return collExecutor.exists(filter);
    }

    public long count() {
        return collExecutor.count();
    }

    /**
     *
     * @param filter
     * @return
     */
    public long count(final Bson filter) {
        return collExecutor.count(filter);
    }

    /**
     *
     * @param filter
     * @param options
     * @return
     */
    public long count(final Bson filter, final CountOptions options) {
        return collExecutor.count(filter, options);
    }

    /**
     *
     * @param objectId
     * @return
     */
    public Optional<T> get(final String objectId) {
        return collExecutor.get(targetClass, objectId);
    }

    /**
     *
     * @param objectId
     * @return
     */
    public Optional<T> get(final ObjectId objectId) {
        return collExecutor.get(targetClass, objectId);
    }

    /**
     *
     * @param objectId
     * @param selectPropNames
     * @return
     */
    public Optional<T> get(final String objectId, final Collection<String> selectPropNames) {
        return collExecutor.get(targetClass, objectId, selectPropNames);
    }

    /**
     *
     * @param objectId
     * @param selectPropNames
     * @return
     */
    public Optional<T> get(final ObjectId objectId, final Collection<String> selectPropNames) {
        return collExecutor.get(targetClass, objectId, selectPropNames);
    }

    /**
     * Gets the t.
     *
     * @param objectId
     * @return
     */
    public T gett(final String objectId) {
        return collExecutor.gett(targetClass, objectId);
    }

    /**
     * Gets the t.
     *
     * @param objectId
     * @return
     */
    public T gett(final ObjectId objectId) {
        return collExecutor.gett(targetClass, objectId);
    }

    /**
     * Gets the t.
     *
     * @param objectId
     * @param selectPropNames
     * @return
     */
    public T gett(final String objectId, final Collection<String> selectPropNames) {
        return collExecutor.gett(targetClass, objectId, selectPropNames);
    }

    /**
     * Gets the t.
     *
     * @param objectId
     * @param selectPropNames
     * @return
     */
    public T gett(final ObjectId objectId, final Collection<String> selectPropNames) {
        return collExecutor.gett(targetClass, objectId, selectPropNames);
    }

    /**
     *
     * @param filter
     * @return
     */
    public Optional<T> findFirst(final Bson filter) {
        return collExecutor.findFirst(targetClass, filter);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @return
     */
    public Optional<T> findFirst(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.findFirst(targetClass, selectPropNames, filter);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @param sort
     * @return
     */
    public Optional<T> findFirst(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.findFirst(targetClass, selectPropNames, filter, sort);
    }

    /**
     *
     * @param filter
     * @param sort
     * @param projection
     * @return
     */
    public Optional<T> findFirst(final Bson filter, final Bson sort, final Bson projection) {
        return collExecutor.findFirst(targetClass, filter, sort, projection);
    }

    /**
     *
     * @param filter
     * @return
     */
    public List<T> list(final Bson filter) {
        return collExecutor.list(targetClass, filter);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @return
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.list(targetClass, selectPropNames, filter);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @param offset
     * @param count
     * @return
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collExecutor.list(targetClass, selectPropNames, filter, offset, count);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @param sort
     * @return
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.list(targetClass, selectPropNames, filter, sort);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @param sort
     * @param offset
     * @param count
     * @return
     */
    public List<T> list(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.list(targetClass, selectPropNames, filter, sort, offset, count);
    }

    /**
     *
     * @param filter
     * @param sort
     * @param projection
     * @return
     */
    public List<T> list(final Bson filter, final Bson sort, final Bson projection) {
        return collExecutor.list(targetClass, filter, sort, projection);
    }

    /**
     *
     * @param filter
     * @param sort
     * @param projection
     * @param offset
     * @param count
     * @return
     */
    public List<T> list(final Bson filter, final Bson sort, final Bson projection, final int offset, final int count) {
        return collExecutor.list(targetClass, filter, sort, projection, offset, count);
    }

    /**
     * Query for boolean.
     *
     * @param propName
     * @param filter
     * @return
     */
    @Beta
    public OptionalBoolean queryForBoolean(final String propName, final Bson filter) {
        return collExecutor.queryForBoolean(propName, filter);
    }

    /**
     * Query for char.
     *
     * @param propName
     * @param filter
     * @return
     */
    @Beta
    public OptionalChar queryForChar(final String propName, final Bson filter) {
        return collExecutor.queryForChar(propName, filter);
    }

    /**
     * Query for byte.
     *
     * @param propName
     * @param filter
     * @return
     */
    @Beta
    public OptionalByte queryForByte(final String propName, final Bson filter) {
        return collExecutor.queryForByte(propName, filter);
    }

    /**
     * Query for short.
     *
     * @param propName
     * @param filter
     * @return
     */
    @Beta
    public OptionalShort queryForShort(final String propName, final Bson filter) {
        return collExecutor.queryForShort(propName, filter);
    }

    /**
     * Query for int.
     *
     * @param propName
     * @param filter
     * @return
     */
    @Beta
    public OptionalInt queryForInt(final String propName, final Bson filter) {
        return collExecutor.queryForInt(propName, filter);
    }

    /**
     * Query for long.
     *
     * @param propName
     * @param filter
     * @return
     */
    @Beta
    public OptionalLong queryForLong(final String propName, final Bson filter) {
        return collExecutor.queryForLong(propName, filter);
    }

    /**
     * Query for float.
     *
     * @param propName
     * @param filter
     * @return
     */
    @Beta
    public OptionalFloat queryForFloat(final String propName, final Bson filter) {
        return collExecutor.queryForFloat(propName, filter);
    }

    /**
     * Query for double.
     *
     * @param propName
     * @param filter
     * @return
     */
    @Beta
    public OptionalDouble queryForDouble(final String propName, final Bson filter) {
        return collExecutor.queryForDouble(propName, filter);
    }

    /**
     * Query for string.
     *
     * @param propName
     * @param filter
     * @return
     */
    @Beta
    public Nullable<String> queryForString(final String propName, final Bson filter) {
        return collExecutor.queryForString(propName, filter);
    }

    /**
     * Query for date.
     *
     * @param propName
     * @param filter
     * @return
     */
    @Beta
    public Nullable<Date> queryForDate(final String propName, final Bson filter) {
        return collExecutor.queryForDate(propName, filter);
    }

    /**
     * Query for date.
     *
     * @param <P>
     * @param targetPropClass
     * @param propName
     * @param filter
     * @return
     */
    public <P extends Date> Nullable<P> queryForDate(final Class<P> targetPropClass, final String propName, final Bson filter) {
        return collExecutor.queryForDate(targetPropClass, propName, filter);
    }

    /**
     * Query for single result.
     *
     * @param <V> the value type
     * @param targetPropClass
     * @param propName
     * @param filter
     * @return
     */
    public <V> Nullable<V> queryForSingleResult(final Class<V> targetPropClass, final String propName, final Bson filter) {
        return collExecutor.queryForSingleResult(targetPropClass, propName, filter);
    }

    /**
     *
     * @param filter
     * @return
     */
    public DataSet query(final Bson filter) {
        return collExecutor.query(targetClass, filter);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @return
     */
    public DataSet query(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.query(targetClass, selectPropNames, filter);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @param offset
     * @param count
     * @return
     */
    public DataSet query(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collExecutor.query(targetClass, selectPropNames, filter, offset, count);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @param sort
     * @return
     */
    public DataSet query(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.query(targetClass, selectPropNames, filter, sort);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @param sort
     * @param offset
     * @param count
     * @return
     */
    public DataSet query(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.query(targetClass, selectPropNames, filter, sort, offset, count);
    }

    /**
     *
     * @param filter
     * @param sort
     * @param projection
     * @return
     */
    public DataSet query(final Bson filter, final Bson sort, final Bson projection) {
        return collExecutor.query(targetClass, filter, sort, projection);
    }

    /**
     *
     * @param filter
     * @param sort
     * @param projection
     * @param offset
     * @param count
     * @return
     */
    public DataSet query(final Bson filter, final Bson sort, final Bson projection, final int offset, final int count) {
        return collExecutor.query(targetClass, filter, sort, projection, offset, count);
    }

    /**
     *
     * @param filter
     * @return
     */
    public Stream<T> stream(final Bson filter) {
        return collExecutor.stream(targetClass, filter);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @return
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter) {
        return collExecutor.stream(targetClass, selectPropNames, filter);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @param offset
     * @param count
     * @return
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final int offset, final int count) {
        return collExecutor.stream(targetClass, selectPropNames, filter, offset, count);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @param sort
     * @return
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return collExecutor.stream(targetClass, selectPropNames, filter, sort);
    }

    /**
     *
     * @param selectPropNames
     * @param filter
     * @param sort
     * @param offset
     * @param count
     * @return
     */
    public Stream<T> stream(final Collection<String> selectPropNames, final Bson filter, final Bson sort, final int offset, final int count) {
        return collExecutor.stream(targetClass, selectPropNames, filter, sort, offset, count);
    }

    /**
     *
     * @param filter
     * @param sort
     * @param projection
     * @return
     */
    public Stream<T> stream(final Bson filter, final Bson sort, final Bson projection) {
        return collExecutor.stream(targetClass, filter, sort, projection);
    }

    /**
     *
     * @param filter
     * @param sort
     * @param projection
     * @param offset
     * @param count
     * @return
     */
    public Stream<T> stream(final Bson filter, final Bson sort, final Bson projection, final int offset, final int count) {
        return collExecutor.stream(targetClass, filter, sort, projection, offset, count);
    }

    /**
     *
     * @param obj
     */
    public void insert(final T obj) {
        collExecutor.insert(obj);
    }

    /**
     *
     * @param obj
     * @param options
     */
    public void insert(final T obj, final InsertOneOptions options) {
        collExecutor.insert(obj, options);
    }

    /**
     *
     * @param objList
     */
    public void insertAll(final Collection<? extends T> objList) {
        collExecutor.insertAll(objList);
    }

    /**
     *
     * @param objList
     * @param options
     */
    public void insertAll(final Collection<? extends T> objList, final InsertManyOptions options) {
        collExecutor.insertAll(objList, options);
    }

    /**
     *
     * @param objectId
     * @param update
     * @return
     */
    public UpdateResult update(final String objectId, final T update) {
        return collExecutor.update(objectId, update);
    }

    /**
     *
     * @param objectId
     * @param update
     * @return
     */
    public UpdateResult update(final ObjectId objectId, final T update) {
        return collExecutor.update(objectId, update);
    }

    /**
     *
     * @param filter
     * @param update
     * @return
     */
    public UpdateResult updateOne(final Bson filter, final T update) {
        return collExecutor.updateOne(filter, update);
    }

    /**
     *
     * @param filter
     * @param update
     * @param options
     * @return
     */
    public UpdateResult updateOne(final Bson filter, final T update, final UpdateOptions options) {
        return collExecutor.updateOne(filter, update, options);
    }

    /**
     *
     * @param filter
     * @param update
     * @return
     */
    public UpdateResult updateAll(final Bson filter, final T update) {
        return collExecutor.updateAll(filter, update);
    }

    /**
     *
     * @param filter
     * @param update
     * @param options
     * @return
     */
    public UpdateResult updateAll(final Bson filter, final T update, final UpdateOptions options) {
        return collExecutor.updateAll(filter, update, options);
    }

    /**
     *
     * @param objectId
     * @param replacement
     * @return
     */
    public UpdateResult replace(final String objectId, final T replacement) {
        return collExecutor.replace(objectId, replacement);
    }

    /**
     *
     * @param objectId
     * @param replacement
     * @return
     */
    public UpdateResult replace(final ObjectId objectId, final T replacement) {
        return collExecutor.replace(objectId, replacement);
    }

    /**
     *
     * @param filter
     * @param replacement
     * @return
     */
    public UpdateResult replaceOne(final Bson filter, final T replacement) {
        return collExecutor.replaceOne(filter, replacement);
    }

    /**
     *
     * @param filter
     * @param replacement
     * @param options
     * @return
     */
    public UpdateResult replaceOne(final Bson filter, final T replacement, final ReplaceOptions options) {
        return collExecutor.replaceOne(filter, replacement, options);
    }

    /**
     *
     * @param objectId
     * @return
     */
    public DeleteResult delete(final String objectId) {
        return collExecutor.delete(objectId);
    }

    /**
     *
     * @param objectId
     * @return
     */
    public DeleteResult delete(final ObjectId objectId) {
        return collExecutor.delete(objectId);
    }

    /**
     *
     * @param filter
     * @return
     */
    public DeleteResult deleteOne(final Bson filter) {
        return collExecutor.deleteOne(filter);
    }

    /**
     *
     * @param filter
     * @param options
     * @return
     */
    public DeleteResult deleteOne(final Bson filter, final DeleteOptions options) {
        return collExecutor.deleteOne(filter, options);
    }

    /**
     *
     * @param filter
     * @return
     */
    public DeleteResult deleteAll(final Bson filter) {
        return collExecutor.deleteAll(filter);
    }

    /**
     *
     * @param filter
     * @param options
     * @return
     */
    public DeleteResult deleteAll(Bson filter, DeleteOptions options) {
        return collExecutor.deleteAll(filter, options);
    }

    /**
     *
     * @param entities
     * @return
     */
    public int bulkInsert(final Collection<? extends T> entities) {
        return collExecutor.bulkInsert(entities);
    }

    /**
     *
     * @param entities
     * @param options
     * @return
     */
    public int bulkInsert(final Collection<? extends T> entities, final BulkWriteOptions options) {
        return collExecutor.bulkInsert(entities, options);
    }

    /**
     *
     * @param requests
     * @return
     */
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends Document>> requests) {
        return collExecutor.bulkWrite(requests);
    }

    /**
     *
     * @param requests
     * @param options
     * @return
     */
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends Document>> requests, final BulkWriteOptions options) {
        return collExecutor.bulkWrite(requests, options);
    }

    /**
     *
     * @param fieldName
     * @return
     */
    public Stream<T> distinct(final String fieldName) {
        return collExecutor.distinct(targetClass, fieldName);
    }

    /**
     *
     * @param fieldName
     * @param filter
     * @return
     */
    public Stream<T> distinct(final String fieldName, final Bson filter) {
        return collExecutor.distinct(targetClass, fieldName, filter);
    }

    /**
     *
     * @param pipeline
     * @return
     */
    public Stream<T> aggregate(final List<? extends Bson> pipeline) {
        return collExecutor.aggregate(targetClass, pipeline);
    }

    /**
     *
     * @param mapFunction
     * @param reduceFunction
     * @return
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
