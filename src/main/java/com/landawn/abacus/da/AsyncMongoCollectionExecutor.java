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
import java.util.concurrent.Callable;

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
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
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
 * Asynchronous <code>MongoCollectionExecutor</code>.
 *
 * @author Haiyang Li
 * @since 0.8
 */
public final class AsyncMongoCollectionExecutor {

    /** The coll executor. */
    private final MongoCollectionExecutor collExecutor;

    /** The async executor. */
    private final AsyncExecutor asyncExecutor;

    /**
     * Instantiates a new async mongo collection executor.
     *
     * @param collExecutor the coll executor
     * @param asyncExecutor the async executor
     */
    AsyncMongoCollectionExecutor(final MongoCollectionExecutor collExecutor, final AsyncExecutor asyncExecutor) {
        this.collExecutor = collExecutor;
        this.asyncExecutor = asyncExecutor;
    }

    /**
     * Sync.
     *
     * @return the mongo collection executor
     */
    public MongoCollectionExecutor sync() {
        return collExecutor;
    }

    /**
     * Exists.
     *
     * @param objectId the object id
     * @return the continuable future
     */
    public ContinuableFuture<Boolean> exists(final String objectId) {
        return asyncExecutor.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return collExecutor.exists(objectId);
            }
        });
    }

    /**
     * Exists.
     *
     * @param objectId the object id
     * @return the continuable future
     */
    public ContinuableFuture<Boolean> exists(final ObjectId objectId) {
        return asyncExecutor.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return collExecutor.exists(objectId);
            }
        });
    }

    /**
     * Exists.
     *
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<Boolean> exists(final Bson filter) {
        return asyncExecutor.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return collExecutor.exists(filter);
            }
        });
    }

    /**
     * Count.
     *
     * @return the continuable future
     */
    public ContinuableFuture<Long> count() {
        return asyncExecutor.execute(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return collExecutor.count();
            }
        });
    }

    /**
     * Count.
     *
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<Long> count(final Bson filter) {
        return asyncExecutor.execute(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return collExecutor.count(filter);
            }
        });
    }

    /**
     * Count.
     *
     * @param filter the filter
     * @param options the options
     * @return the continuable future
     */
    public ContinuableFuture<Long> count(final Bson filter, final CountOptions options) {
        return asyncExecutor.execute(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return collExecutor.count(filter, options);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param objectId the object id
     * @return the continuable future
     */
    public ContinuableFuture<Optional<Document>> get(final String objectId) {
        return asyncExecutor.execute(new Callable<Optional<Document>>() {
            @Override
            public Optional<Document> call() throws Exception {
                return collExecutor.get(objectId);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param objectId the object id
     * @return the continuable future
     */
    public ContinuableFuture<Optional<Document>> get(final ObjectId objectId) {
        return asyncExecutor.execute(new Callable<Optional<Document>>() {
            @Override
            public Optional<Document> call() throws Exception {
                return collExecutor.get(objectId);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @return the continuable future
     */
    public <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final String objectId) {
        return asyncExecutor.execute(new Callable<Optional<T>>() {
            @Override
            public Optional<T> call() throws Exception {
                return collExecutor.get(targetClass, objectId);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @return the continuable future
     */
    public <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final ObjectId objectId) {
        return asyncExecutor.execute(new Callable<Optional<T>>() {
            @Override
            public Optional<T> call() throws Exception {
                return collExecutor.get(targetClass, objectId);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @param selectPropNames the select prop names
     * @return the continuable future
     */
    public <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final String objectId, final Collection<String> selectPropNames) {
        return asyncExecutor.execute(new Callable<Optional<T>>() {
            @Override
            public Optional<T> call() throws Exception {
                return collExecutor.get(targetClass, objectId, selectPropNames);
            }
        });
    }

    /**
     * Gets the.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @param selectPropNames the select prop names
     * @return the continuable future
     */
    public <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final ObjectId objectId, final Collection<String> selectPropNames) {
        return asyncExecutor.execute(new Callable<Optional<T>>() {
            @Override
            public Optional<T> call() throws Exception {
                return collExecutor.get(targetClass, objectId, selectPropNames);
            }
        });
    }

    /**
     * Gets the t.
     *
     * @param objectId the object id
     * @return the t
     */
    public ContinuableFuture<Document> gett(final String objectId) {
        return asyncExecutor.execute(new Callable<Document>() {
            @Override
            public Document call() throws Exception {
                return collExecutor.gett(objectId);
            }
        });
    }

    /**
     * Gets the t.
     *
     * @param objectId the object id
     * @return the t
     */
    public ContinuableFuture<Document> gett(final ObjectId objectId) {
        return asyncExecutor.execute(new Callable<Document>() {
            @Override
            public Document call() throws Exception {
                return collExecutor.gett(objectId);
            }
        });
    }

    /**
     * Gets the t.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @return the t
     */
    public <T> ContinuableFuture<T> gett(final Class<T> targetClass, final String objectId) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return collExecutor.gett(targetClass, objectId);
            }
        });
    }

    /**
     * Gets the t.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param objectId the object id
     * @return the t
     */
    public <T> ContinuableFuture<T> gett(final Class<T> targetClass, final ObjectId objectId) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return collExecutor.gett(targetClass, objectId);
            }
        });
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
    public <T> ContinuableFuture<T> gett(final Class<T> targetClass, final String objectId, final Collection<String> selectPropNames) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return collExecutor.gett(targetClass, objectId, selectPropNames);
            }
        });
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
    public <T> ContinuableFuture<T> gett(final Class<T> targetClass, final ObjectId objectId, final Collection<String> selectPropNames) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return collExecutor.gett(targetClass, objectId, selectPropNames);
            }
        });
    }

    /**
     * Find first.
     *
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<Optional<Document>> findFirst(final Bson filter) {
        return asyncExecutor.execute(new Callable<Optional<Document>>() {
            @Override
            public Optional<Document> call() throws Exception {
                return collExecutor.findFirst(filter);
            }
        });
    }

    /**
     * Find first.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @return the continuable future
     */
    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final Bson filter) {
        return asyncExecutor.execute(new Callable<Optional<T>>() {
            @Override
            public Optional<T> call() throws Exception {
                return collExecutor.findFirst(targetClass, filter);
            }
        });
    }

    /**
     * Find first.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the continuable future
     */
    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter) {
        return asyncExecutor.execute(new Callable<Optional<T>>() {
            @Override
            public Optional<T> call() throws Exception {
                return collExecutor.findFirst(targetClass, selectPropNames, filter);
            }
        });
    }

    /**
     * Find first.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the continuable future
     */
    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter,
            final Bson sort) {
        return asyncExecutor.execute(new Callable<Optional<T>>() {
            @Override
            public Optional<T> call() throws Exception {
                return collExecutor.findFirst(targetClass, selectPropNames, filter, sort);
            }
        });
    }

    /**
     * Find first.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the continuable future
     */
    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection) {
        return asyncExecutor.execute(new Callable<Optional<T>>() {
            @Override
            public Optional<T> call() throws Exception {
                return collExecutor.findFirst(targetClass, filter, sort, projection);
            }
        });
    }

    /**
     * List.
     *
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<List<Document>> list(final Bson filter) {
        return asyncExecutor.execute(new Callable<List<Document>>() {
            @Override
            public List<Document> call() throws Exception {
                return collExecutor.list(filter);
            }
        });
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @return the continuable future
     */
    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final Bson filter) {
        return asyncExecutor.execute(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                return collExecutor.list(targetClass, filter);
            }
        });
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the continuable future
     */
    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter) {
        return asyncExecutor.execute(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                return collExecutor.list(targetClass, selectPropNames, filter);
            }
        });
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
     * @return the continuable future
     */
    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final int offset,
            final int count) {
        return asyncExecutor.execute(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                return collExecutor.list(targetClass, selectPropNames, filter, offset, count);
            }
        });
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the continuable future
     */
    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return asyncExecutor.execute(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                return collExecutor.list(targetClass, selectPropNames, filter, sort);
            }
        });
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @param offset the offset
     * @param count the count
     * @return the continuable future
     */
    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort,
            final int offset, final int count) {
        return asyncExecutor.execute(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                return collExecutor.list(targetClass, selectPropNames, filter, sort, offset, count);
            }
        });
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the continuable future
     */
    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection) {
        return asyncExecutor.execute(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                return collExecutor.list(targetClass, filter, sort, projection);
            }
        });
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @param offset the offset
     * @param count the count
     * @return the continuable future
     */
    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection, final int offset,
            final int count) {
        return asyncExecutor.execute(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                return collExecutor.list(targetClass, filter, sort, projection, offset, count);
            }
        });
    }

    /**
     * Query for boolean.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<OptionalBoolean> queryForBoolean(final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<OptionalBoolean>() {
            @Override
            public OptionalBoolean call() throws Exception {
                return collExecutor.queryForBoolean(propName, filter);
            }
        });
    }

    /**
     * Query for char.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<OptionalChar> queryForChar(final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<OptionalChar>() {
            @Override
            public OptionalChar call() throws Exception {
                return collExecutor.queryForChar(propName, filter);
            }
        });
    }

    /**
     * Query for byte.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<OptionalByte> queryForByte(final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<OptionalByte>() {
            @Override
            public OptionalByte call() throws Exception {
                return collExecutor.queryForByte(propName, filter);
            }
        });
    }

    /**
     * Query for short.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<OptionalShort> queryForShort(final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<OptionalShort>() {
            @Override
            public OptionalShort call() throws Exception {
                return collExecutor.queryForShort(propName, filter);
            }
        });
    }

    /**
     * Query for int.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<OptionalInt> queryForInt(final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<OptionalInt>() {
            @Override
            public OptionalInt call() throws Exception {
                return collExecutor.queryForInt(propName, filter);
            }
        });
    }

    /**
     * Query for long.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<OptionalLong> queryForLong(final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<OptionalLong>() {
            @Override
            public OptionalLong call() throws Exception {
                return collExecutor.queryForLong(propName, filter);
            }
        });
    }

    /**
     * Query for float.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<OptionalFloat> queryForFloat(final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<OptionalFloat>() {
            @Override
            public OptionalFloat call() throws Exception {
                return collExecutor.queryForFloat(propName, filter);
            }
        });
    }

    /**
     * Query for double.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<OptionalDouble> queryForDouble(final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<OptionalDouble>() {
            @Override
            public OptionalDouble call() throws Exception {
                return collExecutor.queryForDouble(propName, filter);
            }
        });
    }

    /**
     * Query for string.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<Nullable<String>> queryForString(final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<Nullable<String>>() {
            @Override
            public Nullable<String> call() throws Exception {
                return collExecutor.queryForString(propName, filter);
            }
        });
    }

    /**
     * Query for date.
     *
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<Nullable<Date>> queryForDate(final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<Nullable<Date>>() {
            @Override
            public Nullable<Date> call() throws Exception {
                return collExecutor.queryForDate(propName, filter);
            }
        });
    }

    /**
     * Query for date.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public <T extends Date> ContinuableFuture<Nullable<T>> queryForDate(final Class<T> targetClass, final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<Nullable<T>>() {
            @Override
            public Nullable<T> call() throws Exception {
                return collExecutor.queryForDate(targetClass, propName, filter);
            }
        });
    }

    /**
     * Query for single result.
     *
     * @param <V> the value type
     * @param targetClass the target class
     * @param propName the prop name
     * @param filter the filter
     * @return the continuable future
     */
    public <V> ContinuableFuture<Nullable<V>> queryForSingleResult(final Class<V> targetClass, final String propName, final Bson filter) {
        return asyncExecutor.execute(new Callable<Nullable<V>>() {
            @Override
            public Nullable<V> call() throws Exception {
                return collExecutor.queryForSingleResult(targetClass, propName, filter);
            }
        });
    }

    /**
     * Query.
     *
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<DataSet> query(final Bson filter) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return collExecutor.query(filter);
            }
        });
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @return the continuable future
     */
    public <T> ContinuableFuture<DataSet> query(final Class<T> targetClass, final Bson filter) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return collExecutor.query(targetClass, filter);
            }
        });
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the continuable future
     */
    public <T> ContinuableFuture<DataSet> query(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return collExecutor.query(targetClass, selectPropNames, filter);
            }
        });
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
     * @return the continuable future
     */
    public <T> ContinuableFuture<DataSet> query(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final int offset,
            final int count) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return collExecutor.query(targetClass, selectPropNames, filter, offset, count);
            }
        });
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the continuable future
     */
    public <T> ContinuableFuture<DataSet> query(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return collExecutor.query(targetClass, selectPropNames, filter, sort);
            }
        });
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
     * @return the continuable future
     */
    public <T> ContinuableFuture<DataSet> query(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort,
            final int offset, final int count) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return collExecutor.query(targetClass, selectPropNames, filter, sort, offset, count);
            }
        });
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the continuable future
     */
    public <T> ContinuableFuture<DataSet> query(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return collExecutor.query(targetClass, filter, sort, projection);
            }
        });
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
     * @return the continuable future
     */
    public <T> ContinuableFuture<DataSet> query(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection, final int offset,
            final int count) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return collExecutor.query(targetClass, filter, sort, projection, offset, count);
            }
        });
    }

    /**
     * Stream.
     *
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Document>> stream(final Bson filter) {
        return asyncExecutor.execute(new Callable<Stream<Document>>() {
            @Override
            public Stream<Document> call() throws Exception {
                return collExecutor.stream(filter);
            }
        });
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final Bson filter) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return collExecutor.stream(targetClass, filter);
            }
        });
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return collExecutor.stream(targetClass, selectPropNames, filter);
            }
        });
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
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final int offset,
            final int count) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return collExecutor.stream(targetClass, selectPropNames, filter, offset, count);
            }
        });
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param selectPropNames the select prop names
     * @param filter the filter
     * @param sort the sort
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return collExecutor.stream(targetClass, selectPropNames, filter, sort);
            }
        });
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
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Bson filter, final Bson sort,
            final int offset, final int count) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return collExecutor.stream(targetClass, selectPropNames, filter, sort, offset, count);
            }
        });
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sort the sort
     * @param projection the projection
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return collExecutor.stream(targetClass, filter, sort, projection);
            }
        });
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
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final Bson filter, final Bson sort, final Bson projection, final int offset,
            final int count) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return collExecutor.stream(targetClass, filter, sort, projection, offset, count);
            }
        });
    }

    /**
     * Insert.
     *
     * @param obj the obj
     * @return the continuable future
     */
    public ContinuableFuture<Void> insert(final Object obj) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                collExecutor.insert(obj);
                return null;
            }
        });
    }

    /**
     * Insert.
     *
     * @param obj the obj
     * @param options the options
     * @return the continuable future
     */
    public ContinuableFuture<Void> insert(final Object obj, final InsertOneOptions options) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                collExecutor.insert(obj, options);
                return null;
            }
        });
    }

    /**
     * Insert all.
     *
     * @param objList the obj list
     * @return the continuable future
     */
    public ContinuableFuture<Void> insertAll(final Collection<?> objList) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                collExecutor.insertAll(objList);
                return null;
            }
        });
    }

    /**
     * Insert all.
     *
     * @param objList the obj list
     * @param options the options
     * @return the continuable future
     */
    public ContinuableFuture<Void> insertAll(final Collection<?> objList, final InsertManyOptions options) {
        return asyncExecutor.execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                collExecutor.insertAll(objList, options);
                return null;
            }
        });
    }

    /**
     * Update.
     *
     * @param objectId the object id
     * @param update the update
     * @return the continuable future
     */
    public ContinuableFuture<UpdateResult> update(final String objectId, final Object update) {
        return asyncExecutor.execute(new Callable<UpdateResult>() {
            @Override
            public UpdateResult call() throws Exception {
                return collExecutor.update(objectId, update);
            }
        });
    }

    /**
     * Update.
     *
     * @param objectId the object id
     * @param update the update
     * @return the continuable future
     */
    public ContinuableFuture<UpdateResult> update(final ObjectId objectId, final Object update) {
        return asyncExecutor.execute(new Callable<UpdateResult>() {
            @Override
            public UpdateResult call() throws Exception {
                return collExecutor.update(objectId, update);
            }
        });
    }

    /**
     * Update one.
     *
     * @param filter the filter
     * @param update the update
     * @return the continuable future
     */
    public ContinuableFuture<UpdateResult> updateOne(final Bson filter, final Object update) {
        return asyncExecutor.execute(new Callable<UpdateResult>() {
            @Override
            public UpdateResult call() throws Exception {
                return collExecutor.updateOne(filter, update);
            }
        });
    }

    /**
     * Update one.
     *
     * @param filter the filter
     * @param update the update
     * @param options the options
     * @return the continuable future
     */
    public ContinuableFuture<UpdateResult> updateOne(final Bson filter, final Object update, final UpdateOptions options) {
        return asyncExecutor.execute(new Callable<UpdateResult>() {
            @Override
            public UpdateResult call() throws Exception {
                return collExecutor.updateOne(filter, update, options);
            }
        });
    }

    /**
     * Update all.
     *
     * @param filter the filter
     * @param update the update
     * @return the continuable future
     */
    public ContinuableFuture<UpdateResult> updateAll(final Bson filter, final Object update) {
        return asyncExecutor.execute(new Callable<UpdateResult>() {
            @Override
            public UpdateResult call() throws Exception {
                return collExecutor.updateAll(filter, update);
            }
        });
    }

    /**
     * Update all.
     *
     * @param filter the filter
     * @param update the update
     * @param options the options
     * @return the continuable future
     */
    public ContinuableFuture<UpdateResult> updateAll(final Bson filter, final Object update, final UpdateOptions options) {
        return asyncExecutor.execute(new Callable<UpdateResult>() {
            @Override
            public UpdateResult call() throws Exception {
                return collExecutor.updateAll(filter, update, options);
            }
        });
    }

    /**
     * Replace.
     *
     * @param objectId the object id
     * @param replacement the replacement
     * @return the continuable future
     */
    public ContinuableFuture<UpdateResult> replace(final String objectId, final Object replacement) {
        return asyncExecutor.execute(new Callable<UpdateResult>() {
            @Override
            public UpdateResult call() throws Exception {
                return collExecutor.replace(objectId, replacement);
            }
        });
    }

    /**
     * Replace.
     *
     * @param objectId the object id
     * @param replacement the replacement
     * @return the continuable future
     */
    public ContinuableFuture<UpdateResult> replace(final ObjectId objectId, final Object replacement) {
        return asyncExecutor.execute(new Callable<UpdateResult>() {
            @Override
            public UpdateResult call() throws Exception {
                return collExecutor.replace(objectId, replacement);
            }
        });
    }

    /**
     * Replace one.
     *
     * @param filter the filter
     * @param replacement the replacement
     * @return the continuable future
     */
    public ContinuableFuture<UpdateResult> replaceOne(final Bson filter, final Object replacement) {
        return asyncExecutor.execute(new Callable<UpdateResult>() {
            @Override
            public UpdateResult call() throws Exception {
                return collExecutor.replaceOne(filter, replacement);
            }
        });
    }

    /**
     * Replace one.
     *
     * @param filter the filter
     * @param replacement the replacement
     * @param options the options
     * @return the continuable future
     */
    public ContinuableFuture<UpdateResult> replaceOne(final Bson filter, final Object replacement, final ReplaceOptions options) {
        return asyncExecutor.execute(new Callable<UpdateResult>() {
            @Override
            public UpdateResult call() throws Exception {
                return collExecutor.replaceOne(filter, replacement, options);
            }
        });
    }

    /**
     * Delete.
     *
     * @param objectId the object id
     * @return the continuable future
     */
    public ContinuableFuture<DeleteResult> delete(final String objectId) {
        return asyncExecutor.execute(new Callable<DeleteResult>() {
            @Override
            public DeleteResult call() throws Exception {
                return collExecutor.delete(objectId);
            }
        });
    }

    /**
     * Delete.
     *
     * @param objectId the object id
     * @return the continuable future
     */
    public ContinuableFuture<DeleteResult> delete(final ObjectId objectId) {
        return asyncExecutor.execute(new Callable<DeleteResult>() {
            @Override
            public DeleteResult call() throws Exception {
                return collExecutor.delete(objectId);
            }
        });
    }

    /**
     * Delete one.
     *
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<DeleteResult> deleteOne(final Bson filter) {
        return asyncExecutor.execute(new Callable<DeleteResult>() {
            @Override
            public DeleteResult call() throws Exception {
                return collExecutor.deleteOne(filter);
            }
        });
    }

    /**
     * Delete one.
     *
     * @param filter the filter
     * @param options the options
     * @return the continuable future
     */
    public ContinuableFuture<DeleteResult> deleteOne(final Bson filter, final DeleteOptions options) {
        return asyncExecutor.execute(new Callable<DeleteResult>() {
            @Override
            public DeleteResult call() throws Exception {
                return collExecutor.deleteOne(filter, options);
            }
        });
    }

    /**
     * Delete all.
     *
     * @param filter the filter
     * @return the continuable future
     */
    public ContinuableFuture<DeleteResult> deleteAll(final Bson filter) {
        return asyncExecutor.execute(new Callable<DeleteResult>() {
            @Override
            public DeleteResult call() throws Exception {
                return collExecutor.deleteAll(filter);
            }
        });
    }

    /**
     * Delete all.
     *
     * @param filter the filter
     * @param options the options
     * @return the continuable future
     */
    public ContinuableFuture<DeleteResult> deleteAll(final Bson filter, final DeleteOptions options) {
        return asyncExecutor.execute(new Callable<DeleteResult>() {
            @Override
            public DeleteResult call() throws Exception {
                return collExecutor.deleteAll(filter, options);
            }
        });
    }

    /**
     * Bulk insert.
     *
     * @param entities the entities
     * @return the continuable future
     */
    public ContinuableFuture<Integer> bulkInsert(final Collection<?> entities) {
        return asyncExecutor.execute(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return collExecutor.bulkInsert(entities);
            }
        });
    }

    /**
     * Bulk insert.
     *
     * @param entities the entities
     * @param options the options
     * @return the continuable future
     */
    public ContinuableFuture<Integer> bulkInsert(final Collection<?> entities, final BulkWriteOptions options) {
        return asyncExecutor.execute(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return collExecutor.bulkInsert(entities, options);
            }
        });
    }

    /**
     * Bulk write.
     *
     * @param requests the requests
     * @return the continuable future
     */
    public ContinuableFuture<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests) {
        return asyncExecutor.execute(new Callable<BulkWriteResult>() {
            @Override
            public BulkWriteResult call() throws Exception {
                return collExecutor.bulkWrite(requests);
            }
        });
    }

    /**
     * Bulk write.
     *
     * @param requests the requests
     * @param options the options
     * @return the continuable future
     */
    public ContinuableFuture<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends Document>> requests, final BulkWriteOptions options) {
        return asyncExecutor.execute(new Callable<BulkWriteResult>() {
            @Override
            public BulkWriteResult call() throws Exception {
                return collExecutor.bulkWrite(requests, options);
            }
        });
    }

    /**
     * Distinct.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param fieldName the field name
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> distinct(final Class<T> targetClass, final String fieldName) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return collExecutor.distinct(targetClass, fieldName);
            }
        });
    }

    /**
     * Distinct.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param fieldName the field name
     * @param filter the filter
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> distinct(final Class<T> targetClass, final String fieldName, final Bson filter) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return collExecutor.distinct(targetClass, fieldName, filter);
            }
        });
    }

    /**
     * Aggregate.
     *
     * @param pipeline the pipeline
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Document>> aggregate(final List<? extends Bson> pipeline) {
        return asyncExecutor.execute(new Callable<Stream<Document>>() {
            @Override
            public Stream<Document> call() throws Exception {
                return collExecutor.aggregate(pipeline);
            }
        });
    }

    /**
     * Aggregate.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param pipeline the pipeline
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> aggregate(final Class<T> targetClass, final List<? extends Bson> pipeline) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return collExecutor.aggregate(targetClass, pipeline);
            }
        });
    }

    /**
     * Map reduce.
     *
     * @param mapFunction the map function
     * @param reduceFunction the reduce function
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Document>> mapReduce(final String mapFunction, final String reduceFunction) {
        return asyncExecutor.execute(new Callable<Stream<Document>>() {
            @Override
            public Stream<Document> call() throws Exception {
                return collExecutor.mapReduce(mapFunction, reduceFunction);
            }
        });
    }

    /**
     * Map reduce.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param mapFunction the map function
     * @param reduceFunction the reduce function
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> mapReduce(final Class<T> targetClass, final String mapFunction, final String reduceFunction) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return collExecutor.mapReduce(targetClass, mapFunction, reduceFunction);
            }
        });
    }

    /**
     * Group by.
     *
     * @param fieldName the field name
     * @return the continuable future
     */
    @Beta
    public ContinuableFuture<Stream<Document>> groupBy(final String fieldName) {
        return asyncExecutor.execute(new Callable<Stream<Document>>() {
            @Override
            public Stream<Document> call() throws Exception {
                return collExecutor.groupBy(fieldName);
            }
        });
    }

    /**
     * Group by.
     *
     * @param fieldNames the field names
     * @return the continuable future
     */
    @Beta
    public ContinuableFuture<Stream<Document>> groupBy(final Collection<String> fieldNames) {
        return asyncExecutor.execute(new Callable<Stream<Document>>() {
            @Override
            public Stream<Document> call() throws Exception {
                return collExecutor.groupBy(fieldNames);
            }
        });
    }

    /**
     * Group by and count.
     *
     * @param fieldName the field name
     * @return the continuable future
     */
    @Beta
    public ContinuableFuture<Stream<Document>> groupByAndCount(final String fieldName) {
        return asyncExecutor.execute(new Callable<Stream<Document>>() {
            @Override
            public Stream<Document> call() throws Exception {
                return collExecutor.groupByAndCount(fieldName);
            }
        });
    }

    /**
     * Group by and count.
     *
     * @param fieldNames the field names
     * @return the continuable future
     */
    @Beta
    public ContinuableFuture<Stream<Document>> groupByAndCount(final Collection<String> fieldNames) {
        return asyncExecutor.execute(new Callable<Stream<Document>>() {
            @Override
            public Stream<Document> call() throws Exception {
                return collExecutor.groupByAndCount(fieldNames);
            }
        });
    }
}
