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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.landawn.abacus.DataSet;
import com.landawn.abacus.da.aws.dynamoDB.DynamoDBExecutor;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.stream.Stream;

// TODO: Auto-generated Javadoc
/**
 * Asynchronous <code>DynamoDBExecutor</code>.
 *
 * @author Haiyang Li
 * @since 0.8
 */
public final class AsyncDynamoDBExecutor {

    /** The db executor. */
    private final DynamoDBExecutor dbExecutor;

    /** The async executor. */
    private final AsyncExecutor asyncExecutor;

    /**
     * Instantiates a new async dynamo DB executor.
     *
     * @param dbExecutor the db executor
     * @param asyncExecutor the async executor
     */
    AsyncDynamoDBExecutor(final DynamoDBExecutor dbExecutor, final AsyncExecutor asyncExecutor) {
        this.dbExecutor = dbExecutor;
        this.asyncExecutor = asyncExecutor;
    }

    /**
     * Sync.
     *
     * @return the dynamo DB executor
     */
    public DynamoDBExecutor sync() {
        return dbExecutor;
    }

    /**
     * Gets the item.
     *
     * @param tableName the table name
     * @param key the key
     * @return the item
     */
    public ContinuableFuture<Map<String, Object>> getItem(final String tableName, final Map<String, AttributeValue> key) {
        return asyncExecutor.execute(new Callable<Map<String, Object>>() {
            @Override
            public Map<String, Object> call() throws Exception {
                return dbExecutor.getItem(tableName, key);
            }
        });
    }

    /**
     * Gets the item.
     *
     * @param tableName the table name
     * @param key the key
     * @param consistentRead the consistent read
     * @return the item
     */
    public ContinuableFuture<Map<String, Object>> getItem(final String tableName, final Map<String, AttributeValue> key, final Boolean consistentRead) {
        return asyncExecutor.execute(new Callable<Map<String, Object>>() {
            @Override
            public Map<String, Object> call() throws Exception {
                return dbExecutor.getItem(tableName, key, consistentRead);
            }
        });
    }

    /**
     * Gets the item.
     *
     * @param getItemRequest the get item request
     * @return the item
     */
    public ContinuableFuture<Map<String, Object>> getItem(final GetItemRequest getItemRequest) {
        return asyncExecutor.execute(new Callable<Map<String, Object>>() {
            @Override
            public Map<String, Object> call() throws Exception {
                return dbExecutor.getItem(getItemRequest);
            }
        });
    }

    /**
     * Gets the item.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param key the key
     * @return the item
     */
    public <T> ContinuableFuture<T> getItem(final Class<T> targetClass, final String tableName, final Map<String, AttributeValue> key) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return dbExecutor.getItem(targetClass, tableName, key);
            }
        });
    }

    /**
     * Gets the item.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param key the key
     * @param consistentRead the consistent read
     * @return the item
     */
    public <T> ContinuableFuture<T> getItem(final Class<T> targetClass, final String tableName, final Map<String, AttributeValue> key,
            final Boolean consistentRead) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return dbExecutor.getItem(targetClass, tableName, key, consistentRead);
            }
        });
    }

    /**
     * Gets the item.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param getItemRequest the get item request
     * @return the item
     */
    public <T> ContinuableFuture<T> getItem(final Class<T> targetClass, final GetItemRequest getItemRequest) {
        return asyncExecutor.execute(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return dbExecutor.getItem(targetClass, getItemRequest);
            }
        });
    }

    /**
     * Batch get item.
     *
     * @param requestItems the request items
     * @return the continuable future
     */
    public ContinuableFuture<Map<String, List<Map<String, Object>>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems) {
        return asyncExecutor.execute(new Callable<Map<String, List<Map<String, Object>>>>() {
            @Override
            public Map<String, List<Map<String, Object>>> call() throws Exception {
                return dbExecutor.batchGetItem(requestItems);
            }
        });
    }

    /**
     * Batch get item.
     *
     * @param requestItems the request items
     * @param returnConsumedCapacity the return consumed capacity
     * @return the continuable future
     */
    public ContinuableFuture<Map<String, List<Map<String, Object>>>> batchGetItem(final Map<String, KeysAndAttributes> requestItems,
            final String returnConsumedCapacity) {
        return asyncExecutor.execute(new Callable<Map<String, List<Map<String, Object>>>>() {
            @Override
            public Map<String, List<Map<String, Object>>> call() throws Exception {
                return dbExecutor.batchGetItem(requestItems, returnConsumedCapacity);
            }
        });
    }

    /**
     * Batch get item.
     *
     * @param batchGetItemRequest the batch get item request
     * @return the continuable future
     */
    public ContinuableFuture<Map<String, List<Map<String, Object>>>> batchGetItem(final BatchGetItemRequest batchGetItemRequest) {
        return asyncExecutor.execute(new Callable<Map<String, List<Map<String, Object>>>>() {
            @Override
            public Map<String, List<Map<String, Object>>> call() throws Exception {
                return dbExecutor.batchGetItem(batchGetItemRequest);
            }
        });
    }

    /**
     * Batch get item.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param requestItems the request items
     * @return the continuable future
     */
    public <T> ContinuableFuture<Map<String, List<T>>> batchGetItem(final Class<T> targetClass, final Map<String, KeysAndAttributes> requestItems) {
        return asyncExecutor.execute(new Callable<Map<String, List<T>>>() {
            @Override
            public Map<String, List<T>> call() throws Exception {
                return dbExecutor.batchGetItem(targetClass, requestItems);
            }
        });
    }

    /**
     * Batch get item.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param requestItems the request items
     * @param returnConsumedCapacity the return consumed capacity
     * @return the continuable future
     */
    public <T> ContinuableFuture<Map<String, List<T>>> batchGetItem(final Class<T> targetClass, final Map<String, KeysAndAttributes> requestItems,
            final String returnConsumedCapacity) {
        return asyncExecutor.execute(new Callable<Map<String, List<T>>>() {
            @Override
            public Map<String, List<T>> call() throws Exception {
                return dbExecutor.batchGetItem(targetClass, requestItems, returnConsumedCapacity);
            }
        });
    }

    /**
     * Batch get item.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param batchGetItemRequest the batch get item request
     * @return the continuable future
     */
    public <T> ContinuableFuture<Map<String, List<T>>> batchGetItem(final Class<T> targetClass, final BatchGetItemRequest batchGetItemRequest) {
        return asyncExecutor.execute(new Callable<Map<String, List<T>>>() {
            @Override
            public Map<String, List<T>> call() throws Exception {
                return dbExecutor.batchGetItem(targetClass, batchGetItemRequest);
            }
        });
    }

    /**
     * Put item.
     *
     * @param tableName the table name
     * @param item the item
     * @return the continuable future
     */
    public ContinuableFuture<PutItemResult> putItem(final String tableName, final Map<String, AttributeValue> item) {
        return asyncExecutor.execute(new Callable<PutItemResult>() {
            @Override
            public PutItemResult call() throws Exception {
                return dbExecutor.putItem(tableName, item);
            }
        });
    }

    /**
     * Put item.
     *
     * @param tableName the table name
     * @param item the item
     * @param returnValues the return values
     * @return the continuable future
     */
    public ContinuableFuture<PutItemResult> putItem(final String tableName, final Map<String, AttributeValue> item, final String returnValues) {
        return asyncExecutor.execute(new Callable<PutItemResult>() {
            @Override
            public PutItemResult call() throws Exception {
                return dbExecutor.putItem(tableName, item, returnValues);
            }
        });
    }

    /**
     * Put item.
     *
     * @param putItemRequest the put item request
     * @return the continuable future
     */
    public ContinuableFuture<PutItemResult> putItem(final PutItemRequest putItemRequest) {
        return asyncExecutor.execute(new Callable<PutItemResult>() {
            @Override
            public PutItemResult call() throws Exception {
                return dbExecutor.putItem(putItemRequest);
            }
        });
    }

    // There is no too much benefit to add method for "Object entity"
    /**
     * Put item.
     *
     * @param tableName the table name
     * @param entity the entity
     * @return the continuable future
     */
    // And it may cause error because the "Object" is ambiguous to any type. 
    ContinuableFuture<PutItemResult> putItem(final String tableName, final Object entity) {
        return asyncExecutor.execute(new Callable<PutItemResult>() {
            @Override
            public PutItemResult call() throws Exception {
                return dbExecutor.putItem(tableName, entity);
            }
        });
    }

    // There is no too much benefit to add method for "Object entity"
    /**
     * Put item.
     *
     * @param tableName the table name
     * @param entity the entity
     * @param returnValues the return values
     * @return the continuable future
     */
    // And it may cause error because the "Object" is ambiguous to any type. 
    ContinuableFuture<PutItemResult> putItem(final String tableName, final Object entity, final String returnValues) {
        return asyncExecutor.execute(new Callable<PutItemResult>() {
            @Override
            public PutItemResult call() throws Exception {
                return dbExecutor.putItem(tableName, entity, returnValues);
            }
        });
    }

    /**
     * Batch write item.
     *
     * @param requestItems the request items
     * @return the continuable future
     */
    public ContinuableFuture<BatchWriteItemResult> batchWriteItem(final Map<String, List<WriteRequest>> requestItems) {
        return asyncExecutor.execute(new Callable<BatchWriteItemResult>() {
            @Override
            public BatchWriteItemResult call() throws Exception {
                return dbExecutor.batchWriteItem(requestItems);
            }
        });
    }

    /**
     * Batch write item.
     *
     * @param batchWriteItemRequest the batch write item request
     * @return the continuable future
     */
    public ContinuableFuture<BatchWriteItemResult> batchWriteItem(final BatchWriteItemRequest batchWriteItemRequest) {
        return asyncExecutor.execute(new Callable<BatchWriteItemResult>() {
            @Override
            public BatchWriteItemResult call() throws Exception {
                return dbExecutor.batchWriteItem(batchWriteItemRequest);
            }
        });
    }

    /**
     * Update item.
     *
     * @param tableName the table name
     * @param key the key
     * @param attributeUpdates the attribute updates
     * @return the continuable future
     */
    public ContinuableFuture<UpdateItemResult> updateItem(final String tableName, final Map<String, AttributeValue> key,
            final Map<String, AttributeValueUpdate> attributeUpdates) {
        return asyncExecutor.execute(new Callable<UpdateItemResult>() {
            @Override
            public UpdateItemResult call() throws Exception {
                return dbExecutor.updateItem(tableName, key, attributeUpdates);
            }
        });
    }

    /**
     * Update item.
     *
     * @param tableName the table name
     * @param key the key
     * @param attributeUpdates the attribute updates
     * @param returnValues the return values
     * @return the continuable future
     */
    public ContinuableFuture<UpdateItemResult> updateItem(final String tableName, final Map<String, AttributeValue> key,
            final Map<String, AttributeValueUpdate> attributeUpdates, final String returnValues) {
        return asyncExecutor.execute(new Callable<UpdateItemResult>() {
            @Override
            public UpdateItemResult call() throws Exception {
                return dbExecutor.updateItem(tableName, key, attributeUpdates, returnValues);
            }
        });
    }

    /**
     * Update item.
     *
     * @param updateItemRequest the update item request
     * @return the continuable future
     */
    public ContinuableFuture<UpdateItemResult> updateItem(final UpdateItemRequest updateItemRequest) {
        return asyncExecutor.execute(new Callable<UpdateItemResult>() {
            @Override
            public UpdateItemResult call() throws Exception {
                return dbExecutor.updateItem(updateItemRequest);
            }
        });
    }

    /**
     * Delete item.
     *
     * @param tableName the table name
     * @param key the key
     * @return the continuable future
     */
    public ContinuableFuture<DeleteItemResult> deleteItem(final String tableName, final Map<String, AttributeValue> key) {
        return asyncExecutor.execute(new Callable<DeleteItemResult>() {
            @Override
            public DeleteItemResult call() throws Exception {
                return dbExecutor.deleteItem(tableName, key);
            }
        });
    }

    /**
     * Delete item.
     *
     * @param tableName the table name
     * @param key the key
     * @param returnValues the return values
     * @return the continuable future
     */
    public ContinuableFuture<DeleteItemResult> deleteItem(final String tableName, final Map<String, AttributeValue> key, final String returnValues) {
        return asyncExecutor.execute(new Callable<DeleteItemResult>() {
            @Override
            public DeleteItemResult call() throws Exception {
                return dbExecutor.deleteItem(tableName, key, returnValues);
            }
        });
    }

    /**
     * Delete item.
     *
     * @param deleteItemRequest the delete item request
     * @return the continuable future
     */
    public ContinuableFuture<DeleteItemResult> deleteItem(final DeleteItemRequest deleteItemRequest) {
        return asyncExecutor.execute(new Callable<DeleteItemResult>() {
            @Override
            public DeleteItemResult call() throws Exception {
                return dbExecutor.deleteItem(deleteItemRequest);
            }
        });
    }

    /**
     * List.
     *
     * @param queryRequest the query request
     * @return the continuable future
     */
    public ContinuableFuture<List<Map<String, Object>>> list(final QueryRequest queryRequest) {
        return asyncExecutor.execute(new Callable<List<Map<String, Object>>>() {
            @Override
            public List<Map<String, Object>> call() throws Exception {
                return dbExecutor.list(queryRequest);
            }
        });
    }

    /**
     * List.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param queryRequest the query request
     * @return the continuable future
     */
    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final QueryRequest queryRequest) {
        return asyncExecutor.execute(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                return dbExecutor.list(targetClass, queryRequest);
            }
        });
    }

    /**
     * Query.
     *
     * @param queryRequest the query request
     * @return the continuable future
     */
    public ContinuableFuture<DataSet> query(final QueryRequest queryRequest) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return dbExecutor.query(queryRequest);
            }
        });
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param queryRequest the query request
     * @return the continuable future
     */
    public <T> ContinuableFuture<DataSet> query(final Class<T> targetClass, final QueryRequest queryRequest) {
        return asyncExecutor.execute(new Callable<DataSet>() {
            @Override
            public DataSet call() throws Exception {
                return dbExecutor.query(targetClass, queryRequest);
            }
        });
    }

    /**
     * Stream.
     *
     * @param queryRequest the query request
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Map<String, Object>>> stream(final QueryRequest queryRequest) {
        return asyncExecutor.execute(new Callable<Stream<Map<String, Object>>>() {
            @Override
            public Stream<Map<String, Object>> call() throws Exception {
                return dbExecutor.stream(queryRequest);
            }
        });
    }

    /**
     * Stream.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param queryRequest the query request
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final QueryRequest queryRequest) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return dbExecutor.stream(targetClass, queryRequest);
            }
        });
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param attributesToGet the attributes to get
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Map<String, Object>>> scan(final String tableName, final List<String> attributesToGet) {
        return asyncExecutor.execute(new Callable<Stream<Map<String, Object>>>() {
            @Override
            public Stream<Map<String, Object>> call() throws Exception {
                return dbExecutor.scan(tableName, attributesToGet);
            }
        });
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param scanFilter the scan filter
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Map<String, Object>>> scan(final String tableName, final Map<String, Condition> scanFilter) {
        return asyncExecutor.execute(new Callable<Stream<Map<String, Object>>>() {
            @Override
            public Stream<Map<String, Object>> call() throws Exception {
                return dbExecutor.scan(tableName, scanFilter);
            }
        });
    }

    /**
     * Scan.
     *
     * @param tableName the table name
     * @param attributesToGet the attributes to get
     * @param scanFilter the scan filter
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Map<String, Object>>> scan(final String tableName, final List<String> attributesToGet,
            final Map<String, Condition> scanFilter) {
        return asyncExecutor.execute(new Callable<Stream<Map<String, Object>>>() {
            @Override
            public Stream<Map<String, Object>> call() throws Exception {
                return dbExecutor.scan(tableName, attributesToGet, scanFilter);
            }
        });
    }

    /**
     * Scan.
     *
     * @param scanRequest the scan request
     * @return the continuable future
     */
    public ContinuableFuture<Stream<Map<String, Object>>> scan(final ScanRequest scanRequest) {
        return asyncExecutor.execute(new Callable<Stream<Map<String, Object>>>() {
            @Override
            public Stream<Map<String, Object>> call() throws Exception {
                return dbExecutor.scan(scanRequest);
            }
        });
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param attributesToGet the attributes to get
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> scan(final Class<T> targetClass, final String tableName, final List<String> attributesToGet) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return dbExecutor.scan(targetClass, tableName, attributesToGet);
            }
        });
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param scanFilter the scan filter
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> scan(final Class<T> targetClass, final String tableName, final Map<String, Condition> scanFilter) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return dbExecutor.scan(targetClass, tableName, scanFilter);
            }
        });
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param tableName the table name
     * @param attributesToGet the attributes to get
     * @param scanFilter the scan filter
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> scan(final Class<T> targetClass, final String tableName, final List<String> attributesToGet,
            final Map<String, Condition> scanFilter) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return dbExecutor.scan(targetClass, tableName, attributesToGet, scanFilter);
            }
        });
    }

    /**
     * Scan.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param scanRequest the scan request
     * @return the continuable future
     */
    public <T> ContinuableFuture<Stream<T>> scan(final Class<T> targetClass, final ScanRequest scanRequest) {
        return asyncExecutor.execute(new Callable<Stream<T>>() {
            @Override
            public Stream<T> call() throws Exception {
                return dbExecutor.scan(targetClass, scanRequest);
            }
        });
    }
}
