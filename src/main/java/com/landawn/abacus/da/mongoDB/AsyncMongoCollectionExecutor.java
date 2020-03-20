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
import java.util.concurrent.Callable;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.landawn.abacus.DataSet;
import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
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
     * @param collExecutor
     * @param asyncExecutor
     */
    AsyncMongoCollectionExecutor(final MongoCollectionExecutor collExecutor, final AsyncExecutor asyncExecutor) {
        this.collExecutor = collExecutor;
        this.asyncExecutor = asyncExecutor;
    }

    /**
     *
     * @return
     */
    public MongoCollectionExecutor sync() {
        return collExecutor;
    }

    /**
     *
     * @param objectId
     * @return
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
     *
     * @param objectId
     * @return
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
     *
     * @param filter
     * @return
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
     *
     * @return
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
     *
     * @param filter
     * @return
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
     *
     * @param filter
     * @param options
     * @return
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
     *
     * @param objectId
     * @return
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
     *
     * @param objectId
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param objectId
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param objectId
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param objectId
     * @param selectPropNames
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param objectId
     * @param selectPropNames
     * @return
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
     * @param objectId
     * @return
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
     * @param objectId
     * @return
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
     * @param <T>
     * @param targetClass
     * @param objectId
     * @return
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
     * @param <T>
     * @param targetClass
     * @param objectId
     * @return
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
     * @param <T>
     * @param targetClass
     * @param objectId
     * @param selectPropNames
     * @return
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
     * @param <T>
     * @param targetClass
     * @param objectId
     * @param selectPropNames
     * @return
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
     *
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @param sort
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param filter
     * @param sort
     * @param projection
     * @return
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
     *
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @param offset
     * @param count
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @param sort
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @param sort
     * @param offset
     * @param count
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param filter
     * @param sort
     * @param projection
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param filter
     * @param sort
     * @param projection
     * @param offset
     * @param count
     * @return
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
     * @param propName
     * @param filter
     * @return
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
     * @param propName
     * @param filter
     * @return
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
     * @param propName
     * @param filter
     * @return
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
     * @param propName
     * @param filter
     * @return
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
     * @param propName
     * @param filter
     * @return
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
     * @param propName
     * @param filter
     * @return
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
     * @param propName
     * @param filter
     * @return
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
     * @param propName
     * @param filter
     * @return
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
     * @param propName
     * @param filter
     * @return
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
     * @param propName
     * @param filter
     * @return
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
     * @param <T>
     * @param targetClass
     * @param propName
     * @param filter
     * @return
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
     * @param targetClass
     * @param propName
     * @param filter
     * @return
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
     *
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @param offset
     * @param count
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @param sort
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @param sort
     * @param offset
     * @param count
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param filter
     * @param sort
     * @param projection
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param filter
     * @param sort
     * @param projection
     * @param offset
     * @param count
     * @return
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
     *
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @param offset
     * @param count
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @param sort
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param selectPropNames
     * @param filter
     * @param sort
     * @param offset
     * @param count
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param filter
     * @param sort
     * @param projection
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param filter
     * @param sort
     * @param projection
     * @param offset
     * @param count
     * @return
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
     *
     * @param obj
     * @return
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
     *
     * @param obj
     * @param options
     * @return
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
     *
     * @param objList
     * @return
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
     *
     * @param objList
     * @param options
     * @return
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
     *
     * @param objectId
     * @param update
     * @return
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
     *
     * @param objectId
     * @param update
     * @return
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
     *
     * @param filter
     * @param update
     * @return
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
     *
     * @param filter
     * @param update
     * @param options
     * @return
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
     *
     * @param filter
     * @param update
     * @return
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
     *
     * @param filter
     * @param update
     * @param options
     * @return
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
     *
     * @param objectId
     * @param replacement
     * @return
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
     *
     * @param objectId
     * @param replacement
     * @return
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
     *
     * @param filter
     * @param replacement
     * @return
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
     *
     * @param filter
     * @param replacement
     * @param options
     * @return
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
     *
     * @param objectId
     * @return
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
     *
     * @param objectId
     * @return
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
     *
     * @param filter
     * @return
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
     *
     * @param filter
     * @param options
     * @return
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
     *
     * @param filter
     * @return
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
     *
     * @param filter
     * @param options
     * @return
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
     *
     * @param entities
     * @return
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
     *
     * @param entities
     * @param options
     * @return
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
     *
     * @param requests
     * @return
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
     *
     * @param requests
     * @param options
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param fieldName
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param fieldName
     * @param filter
     * @return
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
     *
     * @param pipeline
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param pipeline
     * @return
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
     *
     * @param mapFunction
     * @param reduceFunction
     * @return
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
     *
     * @param <T>
     * @param targetClass
     * @param mapFunction
     * @param reduceFunction
     * @return
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
     *
     * @param fieldName
     * @return
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
     *
     * @param fieldNames
     * @return
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
     * @param fieldName
     * @return
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
     * @param fieldNames
     * @return
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
