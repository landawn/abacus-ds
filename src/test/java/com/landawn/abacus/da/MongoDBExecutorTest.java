/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da;

import static com.landawn.abacus.da.MongoDB._ID;
import static com.landawn.abacus.da.MongoDB.fromJSON;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Test;

import com.landawn.abacus.DataSet;
import com.landawn.abacus.da.AsyncMongoCollectionExecutor;
import com.landawn.abacus.da.MongoCollectionExecutor;
import com.landawn.abacus.da.MongoDB;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.Maps;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Seq;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

/**
 *
 * @since 0.8
 * 
 * @author Haiyang Li
 */
public class MongoDBExecutorTest extends AbstractNoSQLTest {
    static final MongoClient mongoClient = new MongoClient("localhost", 27017);
    static final MongoDatabase mongoDB = mongoClient.getDatabase("test");
    static final String collectionName = "account";
    static final MongoDB dbExecutor = new MongoDB(mongoDB);
    static final MongoCollectionExecutor collExecutor = dbExecutor.collExecutor(collectionName);
    static final AsyncMongoCollectionExecutor asyncCollExecutor = collExecutor.async();

    @Test
    public void test_collection() {
        Account account = createAccount();
        collExecutor.insert(account);

        MongoCollection<Account> collection = dbExecutor.collection(Account.class, collectionName);

        FindIterable<Account> it = collection.find();
        Block<? super Account> block = new Block<Account>() {
            @Override
            public void apply(Account t) {
                N.println(t);
            }
        };

        it.forEach(block);
    }

    @Test
    public void test_util() {
        Account account = createAccount();
        collExecutor.insert(account);

        MongoCollection<Document> collection = collExecutor.coll();

        Bson filter = new Document("lastName", account.getLastName());
        FindIterable<Document> findIterable = collection.find(filter);

        DataSet dataSet = MongoDB.extractData(findIterable);
        dataSet.println();

        findIterable = collection.find(filter).projection(MongoDB.toBson(_ID, 0));
        dataSet = MongoDB.extractData(Account.class, findIterable);
        dataSet.println();

        findIterable = collection.find(filter).projection(MongoDB.toBson(_ID, 0));
        Account dbAccount = MongoDB.toEntity(Account.class, findIterable.first());
        N.println(dbAccount);

        Document doc = MongoDB.toDocument(dbAccount);
        N.println(doc);

        BSONObject bsonObject = MongoDB.toBSONObject(account);
        N.println(bsonObject);

        bsonObject = MongoDB.toBSONObject(Maps.deepEntity2Map(account));
        N.println(bsonObject);
    }

    @Test
    public void test_distinct() {
        collExecutor.coll().drop();

        Account account = createAccount();
        collExecutor.insert(account);
        collExecutor.insert(createAccount());
        account.setId(generateId());
        collExecutor.insert(account);

        List<String> firstNameList = collExecutor.distinct(String.class, "firstName").toList();
        N.println(firstNameList);

        collExecutor.deleteAll(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_groupBy() {
        collExecutor.coll().drop();

        Account account = createAccount();
        collExecutor.insert(account);
        collExecutor.insert(createAccount());
        account.setId(generateId());
        collExecutor.insert(account);
        account.setId(generateId());
        account.setFirstName("firstName123");
        collExecutor.insert(account);

        collExecutor.groupBy("firstName").println();

        collExecutor.groupByAndCount("firstName").println();

        collExecutor.groupBy(N.asList("firstName")).println();

        collExecutor.groupByAndCount(N.asList("firstName")).println();

        collExecutor.groupBy(N.asList("firstName", "lastName")).println();

        collExecutor.groupByAndCount(N.asList("firstName", "lastName")).println();

        collExecutor.deleteAll(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_aggregate() {
        collExecutor.coll().drop();

        Account account = createAccount();
        collExecutor.insert(account);
        collExecutor.insert(createAccount());
        account.setId(generateId());
        collExecutor.insert(account);

        List<Bson> pipeline = N.asList();

        pipeline.add(fromJSON(Bson.class, "{$match : {firstName : '" + account.getFirstName() + "'}}"));
        pipeline.add(fromJSON(Bson.class, "{$group : {_id : $firstName, total : {$sum : $status}}}"));

        List<Document> resultList = collExecutor.aggregate(pipeline).toList();
        N.println(resultList);

        collExecutor.deleteAll(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_mapReduce() {
        collExecutor.coll().drop();

        Account account = createAccount();
        collExecutor.insert(account);
        collExecutor.insert(createAccount());
        account.setId(generateId());
        collExecutor.insert(account);

        List<Bson> pipeline = N.asList();
        pipeline.add(fromJSON(Bson.class, "{$match : {firstName : '" + account.getFirstName() + "'}}"));
        pipeline.add(fromJSON(Bson.class, "{$group : {_id : $firstName, total : {$sum : $status}}}"));

        String mapFunction = "function() {emit(this.firstName, this.status)}";
        String reduceFunction = "function(key, values) { return Array.sum(values)}";

        List<Document> resultList = collExecutor.mapReduce(mapFunction, reduceFunction).toList();
        N.println(resultList);

        List<Map<String, Object>> mapList = collExecutor.mapReduce(Clazz.PROPS_MAP, mapFunction, reduceFunction).toList();
        N.println(mapList);

        collExecutor.deleteAll(Filters.eq("firstName", account.getFirstName()));
    }

    @Test
    public void test_exist_count_get() {
        collExecutor.deleteAll(Filters.ne("lastName", N.uuid()));

        Account account = createAccount();
        collExecutor.insert(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        ObjectId objectId = doc.getObjectId(_ID);

        assertTrue(collExecutor.exists(objectId.toString()));
        assertTrue(collExecutor.exists(objectId));

        assertTrue(collExecutor.exists(Filters.eq(_ID, objectId)));
        assertFalse(collExecutor.exists(Filters.ne(_ID, objectId)));
        assertTrue(collExecutor.exists(Filters.eq("lastName", account.getLastName())));

        assertEquals(1, collExecutor.count(Filters.eq(_ID, objectId)));
        assertEquals(0, collExecutor.count(Filters.ne(_ID, objectId)));
        assertEquals(1, collExecutor.count(Filters.eq("lastName", account.getLastName())));

        assertEquals(objectId, collExecutor.gett(objectId.toString()).getObjectId(_ID));
        assertEquals(objectId, collExecutor.gett(objectId).getObjectId(_ID));

        String firstName = account.getFirstName();
        assertEquals(firstName, collExecutor.gett(Account.class, objectId.toString()).getFirstName());
        assertEquals(firstName, collExecutor.gett(Account.class, objectId).getFirstName());

        List<Document> result = collExecutor.list(Document.class, Filters.eq("lastName", account.getLastName()));

        N.println(result);

        collExecutor.delete(objectId);
    }

    @Test
    public void test_exist_count_get_2() {
        collExecutor.deleteAll(Filters.ne("lastName", N.uuid()));

        Account account = createAccount();
        collExecutor.insert(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        ObjectId objectId = doc.getObjectId(_ID);

        assertTrue(collExecutor.exists(objectId.toString()));
        assertTrue(collExecutor.exists(objectId));

        assertTrue(collExecutor.exists(Filters.eq(_ID, objectId)));
        assertFalse(collExecutor.exists(Filters.ne(_ID, objectId)));
        assertTrue(collExecutor.exists(Filters.eq("lastName", account.getLastName())));

        assertEquals(1, collExecutor.count(Filters.eq(_ID, objectId)));
        assertEquals(0, collExecutor.count(Filters.ne(_ID, objectId)));
        assertEquals(1, collExecutor.count(Filters.eq("lastName", account.getLastName())));

        assertEquals(objectId, collExecutor.gett(objectId.toString()).getObjectId(_ID));
        assertEquals(objectId, collExecutor.gett(objectId).getObjectId(_ID));

        String firstName = account.getFirstName();
        assertEquals(firstName, collExecutor.gett(Account.class, objectId.toString()).getFirstName());
        assertEquals(firstName, collExecutor.gett(Account.class, objectId).getFirstName());

        List<Document> result = collExecutor.list(Document.class, Filters.eq("lastName", account.getLastName()));

        N.println(result);

        collExecutor.delete(objectId);
    }

    @Test
    public void test_insert() {
        collExecutor.deleteAll(Filters.ne("lastName", N.uuid()));

        Map<String, Object> m = N.asProps("lastName", N.uuid(), "firstName", N.uuid());
        m.put("props", N.asProps("prop1", 1, "prop2", 2));

        collExecutor.insert(m);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", m.get("lastName"))).orElse(null);
        N.println(doc);

        collExecutor.deleteAll(Filters.eq("lastName", m.get("lastName")));
    }

    @Test
    public void test_query() {
        collExecutor.deleteAll(Filters.ne("lastName", N.uuid()));

        Account account = createAccount();
        collExecutor.insert(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);
        Bson filter = Filters.eq("firstName", account.getFirstName());

        assertEquals(objectId, collExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collExecutor.findFirst(Account.class, filter).orElse(null).getFirstName());

        List<Document> docList = collExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        docList = collExecutor.list(Document.class, N.asList("lastName"), filter);

        Seq.of(collExecutor.list(String.class, N.asList("lastName"), filter)).foreach(Fn.println());

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        List<Account> accountList = collExecutor.list(Account.class, filter);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collExecutor.list(Account.class, N.asList("lastName"), filter);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        DataSet dataSet = collExecutor.query(filter);
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = collExecutor.query(Document.class, N.asList("lastName", "birthDate"), filter);

        assertFalse(dataSet.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataSet.get("lastName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = collExecutor.query(Account.class, filter);
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = collExecutor.query(Account.class, N.asList("lastName", "birthDate"), filter);

        assertFalse(dataSet.containsColumn("firstName"));
        N.println(dataSet);
        assertEquals(account.getLastName(), dataSet.get("lastName"));

        // ########################################################################
        Bson projection = Projections.include("id", "firstName", "lastName");

        assertEquals(objectId, collExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collExecutor.findFirst(Account.class, filter, null, projection).orElse(null).getFirstName());

        docList = collExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        projection = Projections.include("id", "lastName");
        docList = collExecutor.list(Document.class, filter, null, projection);

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        accountList = collExecutor.list(Account.class, filter);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collExecutor.list(Account.class, filter, null, projection);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        dataSet = collExecutor.query(filter);
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        projection = Projections.include("id", "lastName", "birthDate");
        dataSet = collExecutor.query(Document.class, filter, null, projection);

        assertFalse(dataSet.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataSet.get("lastName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = collExecutor.query(Account.class, filter);
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = collExecutor.query(Account.class, filter, null, projection);

        assertFalse(dataSet.containsColumn("firstName"));
        N.println(dataSet);
        assertEquals(account.getLastName(), dataSet.get("lastName"));

        // ===================
        assertEquals(objectId, collExecutor.queryForSingleResult(ObjectId.class, _ID, filter).get());
    }

    @Test
    public void test_query_asyn() throws InterruptedException, ExecutionException {
        asyncCollExecutor.deleteAll(Filters.ne("lastName", N.uuid()));

        Account account = createAccount();
        asyncCollExecutor.insert(account).get();

        Document doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);
        Bson filter = Filters.eq("firstName", account.getFirstName());

        assertEquals(objectId, asyncCollExecutor.findFirst(filter).get().orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(filter).get().orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(Account.class, filter).get().orElse(null).getFirstName());

        List<Document> docList = asyncCollExecutor.list(filter).get();
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        docList = asyncCollExecutor.list(Document.class, N.asList("lastName"), filter).get();

        Seq.of(asyncCollExecutor.list(String.class, N.asList("lastName"), filter).get()).foreach(Fn.println());

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        List<Account> accountList = asyncCollExecutor.list(Account.class, filter).get();
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = asyncCollExecutor.list(Account.class, N.asList("lastName"), filter).get();

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        DataSet dataSet = asyncCollExecutor.query(filter).get();
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = asyncCollExecutor.query(Document.class, N.asList("lastName", "birthDate"), filter).get();

        assertFalse(dataSet.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataSet.get("lastName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = asyncCollExecutor.query(Account.class, filter).get();
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = asyncCollExecutor.query(Account.class, N.asList("lastName", "birthDate"), filter).get();

        assertFalse(dataSet.containsColumn("firstName"));
        N.println(dataSet);
        assertEquals(account.getLastName(), dataSet.get("lastName"));

        // ########################################################################
        Bson projection = Projections.include("id", "firstName", "lastName");

        assertEquals(objectId, asyncCollExecutor.findFirst(filter).get().orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(filter).get().orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(Account.class, filter, null, projection).get().orElse(null).getFirstName());

        docList = asyncCollExecutor.list(filter).get();
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        projection = Projections.include("id", "lastName");
        docList = asyncCollExecutor.list(Document.class, filter, null, projection).get();

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        accountList = asyncCollExecutor.list(Account.class, filter).get();
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = asyncCollExecutor.list(Account.class, filter, null, projection).get();

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        dataSet = asyncCollExecutor.query(filter).get();
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        projection = Projections.include("id", "lastName", "birthDate");
        dataSet = asyncCollExecutor.query(Document.class, filter, null, projection).get();

        assertFalse(dataSet.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataSet.get("lastName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = asyncCollExecutor.query(Account.class, filter).get();
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = asyncCollExecutor.query(Account.class, filter, null, projection).get();

        assertFalse(dataSet.containsColumn("firstName"));
        N.println(dataSet);
        assertEquals(account.getLastName(), dataSet.get("lastName"));

        // ===================
        assertEquals(objectId, asyncCollExecutor.queryForSingleResult(ObjectId.class, _ID, filter).get().get());
    }

    @Test
    public void test_query_2() {
        collExecutor.deleteAll(Filters.ne("lastName", N.uuid()));

        Account account = createAccount();
        collExecutor.insert(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);
        Bson filter = Filters.eq("firstName", account.getFirstName());

        assertEquals(objectId, collExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collExecutor.findFirst(Account.class, filter).orElse(null).getFirstName());

        List<Document> docList = collExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        docList = collExecutor.list(Document.class, N.asList("lastName"), filter);

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        List<Account> accountList = collExecutor.list(Account.class, filter);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collExecutor.list(Account.class, N.asList("lastName"), filter);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        DataSet dataSet = collExecutor.query(filter);
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = collExecutor.query(Document.class, N.asList("lastName", "birthDate"), filter);

        assertFalse(dataSet.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataSet.get("lastName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = collExecutor.query(Account.class, filter);
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = collExecutor.query(Account.class, N.asList("lastName", "birthDate"), filter);

        assertFalse(dataSet.containsColumn("firstName"));
        N.println(dataSet);
        assertEquals(account.getLastName(), dataSet.get("lastName"));

        // ########################################################################
        Bson projection = Projections.include("id", "firstName", "lastName");

        assertEquals(objectId, collExecutor.findFirst(filter).orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), collExecutor.findFirst(filter).orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), collExecutor.findFirst(Account.class, filter, null, projection).orElse(null).getFirstName());

        docList = collExecutor.list(filter);
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        projection = Projections.include("id", "lastName");
        docList = collExecutor.list(Document.class, filter, null, projection);

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        accountList = collExecutor.list(Account.class, filter);
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = collExecutor.list(Account.class, filter, null, projection);

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        dataSet = collExecutor.query(filter);
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        projection = Projections.include("id", "lastName", "birthDate");
        dataSet = collExecutor.query(Document.class, filter, null, projection);

        assertFalse(dataSet.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataSet.get("lastName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = collExecutor.query(Account.class, filter);
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = collExecutor.query(Account.class, filter, null, projection);

        assertFalse(dataSet.containsColumn("firstName"));
        N.println(dataSet);
        assertEquals(account.getLastName(), dataSet.get("lastName"));

        // ===================
        assertEquals(objectId, collExecutor.queryForSingleResult(ObjectId.class, _ID, filter).get());
    }

    @Test
    public void test_query_async_2() throws InterruptedException, ExecutionException {
        asyncCollExecutor.deleteAll(Filters.ne("lastName", N.uuid()));

        Account account = createAccount();
        asyncCollExecutor.insert(account).get();

        Document doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);
        Bson filter = Filters.eq("firstName", account.getFirstName());

        assertEquals(objectId, asyncCollExecutor.findFirst(filter).get().orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(filter).get().orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(Account.class, filter).get().orElse(null).getFirstName());

        List<Document> docList = asyncCollExecutor.list(filter).get();
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        docList = asyncCollExecutor.list(Document.class, N.asList("lastName"), filter).get();

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        List<Account> accountList = asyncCollExecutor.list(Account.class, filter).get();
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = asyncCollExecutor.list(Account.class, N.asList("lastName"), filter).get();

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        DataSet dataSet = asyncCollExecutor.query(filter).get();
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = asyncCollExecutor.query(Document.class, N.asList("lastName", "birthDate"), filter).get();

        assertFalse(dataSet.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataSet.get("lastName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = asyncCollExecutor.query(Account.class, filter).get();
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = asyncCollExecutor.query(Account.class, N.asList("lastName", "birthDate"), filter).get();

        assertFalse(dataSet.containsColumn("firstName"));
        N.println(dataSet);
        assertEquals(account.getLastName(), dataSet.get("lastName"));

        // ########################################################################
        Bson projection = Projections.include("id", "firstName", "lastName");

        assertEquals(objectId, asyncCollExecutor.findFirst(filter).get().orElse(null).getObjectId(_ID));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(filter).get().orElse(null).get("firstName"));
        assertEquals(account.getFirstName(), asyncCollExecutor.findFirst(Account.class, filter, null, projection).get().orElse(null).getFirstName());

        docList = asyncCollExecutor.list(filter).get();
        assertEquals(account.getFirstName(), docList.get(0).get("firstName"));

        projection = Projections.include("id", "lastName");
        docList = asyncCollExecutor.list(Document.class, filter, null, projection).get();

        assertNull(docList.get(0).get("firstName"));
        assertEquals(account.getLastName(), docList.get(0).get("lastName"));

        accountList = asyncCollExecutor.list(Account.class, filter).get();
        assertEquals(account.getFirstName(), accountList.get(0).getFirstName());

        accountList = asyncCollExecutor.list(Account.class, filter, null, projection).get();

        assertNull(accountList.get(0).getFirstName());
        assertEquals(account.getLastName(), accountList.get(0).getLastName());

        dataSet = asyncCollExecutor.query(filter).get();
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        projection = Projections.include("id", "lastName", "birthDate");
        dataSet = asyncCollExecutor.query(Document.class, filter, null, projection).get();

        assertFalse(dataSet.containsColumn("firstName"));
        assertEquals(account.getLastName(), dataSet.get("lastName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = asyncCollExecutor.query(Account.class, filter).get();
        assertEquals(account.getFirstName(), dataSet.get("firstName"));
        assertTrue(dataSet.get("birthDate") instanceof Date);

        dataSet = asyncCollExecutor.query(Account.class, filter, null, projection).get();

        assertFalse(dataSet.containsColumn("firstName"));
        N.println(dataSet);
        assertEquals(account.getLastName(), dataSet.get("lastName"));

        // ===================
        assertEquals(objectId, asyncCollExecutor.queryForSingleResult(ObjectId.class, _ID, filter).get().get());
    }

    @Test
    public void test_update() {
        collExecutor.deleteAll(Filters.ne("lastName", N.uuid()));

        Account account = createAccount();
        collExecutor.insert(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);

        // =======================================================================================
        String newFirstName = N.uuid();
        collExecutor.update(objectId, N.asProps("firstName", newFirstName));
        Account dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        Account tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.update(objectId, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = N.uuid();
        collExecutor.update(objectId.toString(), N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.update(objectId.toString(), tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        Bson filter = Filters.eq(_ID, objectId);
        newFirstName = N.uuid();
        collExecutor.updateAll(filter, N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateAll(filter, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = N.uuid();
        collExecutor.updateAll(filter, N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateAll(filter, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = N.uuid();
        collExecutor.updateOne(filter, N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateOne(filter, MongoDB.toDBObject("$set", MongoDB.toDocument("firstName", newFirstName)));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateOne(filter, MongoDB.toDocument("$set", MongoDB.toDBObject("firstName", newFirstName)));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateOne(filter, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        //++++++++++++++++++++++++++++++++++++++++++++++ replace.

        // =======================================================================================
        newFirstName = N.uuid();
        collExecutor.replace(objectId, N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replace(objectId, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = N.uuid();
        collExecutor.replace(objectId.toString(), N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replace(objectId.toString(), tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq(_ID, objectId);
        newFirstName = N.uuid();
        collExecutor.replaceOne(filter, N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replaceOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replaceOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replaceOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replaceOne(filter, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());
        //++++++++++++++++++++++++++++++++++++++++++++++ delete.

        // =======================================================================================
        account.setId(generateId());
        collExecutor.insert(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collExecutor.delete(objectId);
        assertNull(collExecutor.gett(objectId));

        collExecutor.insert(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collExecutor.delete(objectId.toHexString());
        assertNull(collExecutor.gett(objectId));

        collExecutor.insert(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq(_ID, objectId);
        N.println(collExecutor.list(filter));
        collExecutor.deleteAll(filter);
        assertEquals(0, collExecutor.list(filter).size());

        collExecutor.insert(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collExecutor.list(filter));
        collExecutor.deleteAll(filter);
        assertEquals(0, collExecutor.list(filter).size());

        collExecutor.insert(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collExecutor.list(filter));
        collExecutor.deleteOne(filter);
        assertEquals(0, collExecutor.list(filter).size());
    }

    @Test
    public void test_update_async() throws InterruptedException, ExecutionException {
        asyncCollExecutor.deleteAll(Filters.ne("lastName", N.uuid()));

        Account account = createAccount();
        asyncCollExecutor.insert(account).get();

        Document doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);

        // =======================================================================================
        String newFirstName = N.uuid();
        asyncCollExecutor.update(objectId, N.asProps("firstName", newFirstName)).get();
        Account dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        Account tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.update(objectId, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId.toString(), N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId.toString(), MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.update(objectId.toString(), tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        Bson filter = Filters.eq(_ID, objectId);
        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateAll(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateAll(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDBObject("$set", MongoDB.toDocument("firstName", newFirstName))).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDocument("$set", MongoDB.toDBObject("firstName", newFirstName))).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateOne(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        //++++++++++++++++++++++++++++++++++++++++++++++ replace.

        // =======================================================================================
        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId, N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replace(objectId, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId.toString(), N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId.toString(), MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replace(objectId.toString(), tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq(_ID, objectId);
        newFirstName = N.uuid();
        asyncCollExecutor.replaceOne(filter, N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replaceOne(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());
        //++++++++++++++++++++++++++++++++++++++++++++++ delete.

        // =======================================================================================
        account.setId(generateId());
        asyncCollExecutor.insert(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        asyncCollExecutor.delete(objectId).get();
        assertNull(asyncCollExecutor.gett(objectId).get());

        asyncCollExecutor.insert(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        asyncCollExecutor.delete(objectId.toHexString()).get();
        assertNull(asyncCollExecutor.gett(objectId).get());

        asyncCollExecutor.insert(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq(_ID, objectId);
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteAll(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());

        asyncCollExecutor.insert(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteAll(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());

        asyncCollExecutor.insert(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteOne(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());
    }

    @Test
    public void test_update_2() {
        collExecutor.deleteAll(Filters.ne("lastName", N.uuid()));

        Account account = createAccount();
        collExecutor.insert(account);

        Document doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);

        // =======================================================================================
        String newFirstName = N.uuid();
        collExecutor.update(objectId, N.asProps("firstName", newFirstName));
        Account dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        Account tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.update(objectId, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = N.uuid();
        collExecutor.update(objectId.toString(), N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.update(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.update(objectId.toString(), tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        Bson filter = Filters.eq(_ID, objectId);
        newFirstName = N.uuid();
        collExecutor.updateAll(filter, N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateAll(filter, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = N.uuid();
        collExecutor.updateAll(filter, N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateAll(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateAll(filter, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = N.uuid();
        collExecutor.updateOne(filter, N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.updateOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.updateOne(filter, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        //++++++++++++++++++++++++++++++++++++++++++++++ replace.

        // =======================================================================================
        newFirstName = N.uuid();
        collExecutor.replace(objectId, N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replace(objectId, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = N.uuid();
        collExecutor.replace(objectId.toString(), N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId.toString(), MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replace(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replace(objectId.toString(), tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq(_ID, objectId);
        newFirstName = N.uuid();
        collExecutor.replaceOne(filter, N.asProps("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replaceOne(filter, MongoDB.toDocument("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replaceOne(filter, MongoDB.toBSONObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        collExecutor.replaceOne(filter, MongoDB.toDBObject("firstName", newFirstName));
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        collExecutor.replaceOne(filter, tmp);
        dbAccount = collExecutor.gett(Account.class, objectId);
        assertEquals(newFirstName, dbAccount.getFirstName());
        //++++++++++++++++++++++++++++++++++++++++++++++ delete.

        // =======================================================================================
        account.setId(generateId());
        collExecutor.insert(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collExecutor.delete(objectId);
        assertNull(collExecutor.gett(objectId));

        collExecutor.insert(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        collExecutor.delete(objectId.toHexString());
        assertNull(collExecutor.gett(objectId));

        collExecutor.insert(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq(_ID, objectId);
        N.println(collExecutor.list(filter));
        collExecutor.deleteAll(filter);
        assertEquals(0, collExecutor.list(filter).size());

        collExecutor.insert(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collExecutor.list(filter));
        collExecutor.deleteAll(filter);
        assertEquals(0, collExecutor.list(filter).size());

        collExecutor.insert(account);
        doc = collExecutor.findFirst(Filters.eq("lastName", account.getLastName())).orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(collExecutor.list(filter));
        collExecutor.deleteOne(filter);
        assertEquals(0, collExecutor.list(filter).size());
    }

    @Test
    public void test_update_async_2() throws InterruptedException, ExecutionException {
        asyncCollExecutor.deleteAll(Filters.ne("lastName", N.uuid()));

        Account account = createAccount();
        asyncCollExecutor.insert(account).get();

        Document doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        N.println(doc);

        ObjectId objectId = doc.getObjectId(MongoDB._ID);

        // =======================================================================================
        String newFirstName = N.uuid();
        asyncCollExecutor.update(objectId, N.asProps("firstName", newFirstName)).get();
        Account dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        Account tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.update(objectId, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId.toString(), N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId.toString(), MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.update(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.update(objectId.toString(), tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        Bson filter = Filters.eq(_ID, objectId);
        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateAll(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateAll(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateAll(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq("lastName", account.getLastName());
        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDBObject("$set", MongoDB.toDocument("firstName", newFirstName))).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.updateOne(filter, MongoDB.toDocument("$set", MongoDB.toDBObject("firstName", newFirstName))).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.updateOne(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        //++++++++++++++++++++++++++++++++++++++++++++++ replace.

        // =======================================================================================
        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId, N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replace(objectId, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId.toString(), N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId.toString(), MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId.toString(), MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replace(objectId.toString(), MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replace(objectId.toString(), tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        // =======================================================================================
        filter = Filters.eq(_ID, objectId);
        newFirstName = N.uuid();
        asyncCollExecutor.replaceOne(filter, N.asProps("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toDocument("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toBSONObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        asyncCollExecutor.replaceOne(filter, MongoDB.toDBObject("firstName", newFirstName)).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());

        newFirstName = N.uuid();
        tmp = new Account();
        tmp.setFirstName(newFirstName);
        asyncCollExecutor.replaceOne(filter, tmp).get();
        dbAccount = asyncCollExecutor.gett(Account.class, objectId).get();
        assertEquals(newFirstName, dbAccount.getFirstName());
        //++++++++++++++++++++++++++++++++++++++++++++++ delete.

        // =======================================================================================
        account.setId(generateId());
        asyncCollExecutor.insert(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        asyncCollExecutor.delete(objectId).get();
        assertNull(asyncCollExecutor.gett(objectId).get());

        asyncCollExecutor.insert(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        asyncCollExecutor.delete(objectId.toHexString()).get();
        assertNull(asyncCollExecutor.gett(objectId).get());

        asyncCollExecutor.insert(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq(_ID, objectId);
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteAll(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());

        asyncCollExecutor.insert(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteAll(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());

        asyncCollExecutor.insert(account).get();
        doc = asyncCollExecutor.findFirst(Filters.eq("lastName", account.getLastName())).get().orElse(null);
        objectId = doc.getObjectId(MongoDB._ID);
        filter = Filters.eq("lastName", account.getLastName());
        N.println(asyncCollExecutor.list(filter).get());
        asyncCollExecutor.deleteOne(filter).get();
        assertEquals(0, asyncCollExecutor.list(filter).get().size());
    }

    public void test_toDocument() {
        // MongoDBExecutor.registerIdProeprty(Account.class, MongoDBExecutor.ID);
        Account account = createAccount();
        // account.setId(ObjectId.get().toString());
        Document doc = MongoDB.toDocument(account);
        String json = MongoDB.toJSON(doc);
        N.println(json);

        Document doc2 = MongoDB.fromJSON(Document.class, json);

        Account account2 = MongoDB.toEntity(Account.class, doc2);
        assertEquals(account, account2);
    }

    public void test_toBasicBSONObject() {
        // MongoDBExecutor.registerIdProeprty(Account.class, MongoDBExecutor.ID);
        Account account = createAccount();
        // account.setId(ObjectId.get().toString());
        BasicBSONObject bsonObject = MongoDB.toBSONObject(account);
        String json = MongoDB.toJSON(bsonObject);
        N.println(json);

        BasicBSONObject bsonObject2 = MongoDB.fromJSON(BasicBSONObject.class, json);

        String json2 = MongoDB.toJSON(bsonObject2);
        Account account2 = N.fromJSON(Account.class, json2);

        assertEquals(account, account2);
    }

    public void test_toDBObject() {
        // MongoDBExecutor.registerIdProeprty(Account.class, MongoDBExecutor.ID);
        Account account = createAccount();
        // account.setId(ObjectId.get().toString());
        BasicDBObject bsonObject = MongoDB.toDBObject(account);
        String json = MongoDB.toJSON(bsonObject);
        N.println(json);

        BasicDBObject bsonObject2 = MongoDB.fromJSON(BasicDBObject.class, json);

        String json2 = MongoDB.toJSON(bsonObject2);
        Account account2 = N.fromJSON(Account.class, json2);

        assertEquals(account, account2);
    }

    public void test_bulkInsert() {
        Account account = createAccount();
        Account account2 = createAccount();
        Account account3 = createAccount();
        Account account4 = createAccount();
        Account account5 = createAccount();

        assertEquals(5, collExecutor.bulkInsert(N.asList(account, account2, account3, MongoDB.toDocument(account4), MongoDB.toDocument(account5))));
    }

    public void test_01() {
        collExecutor.deleteAll(Filters.eq("title", "A blog post"));

        Map<String, Object> m = N.fromJSON(Map.class, "{\"title\" : \"A blog post\",\r\n" + "\"content\" : \"...\",\r\n" + "\"comments\" : [\r\n" + "{\r\n"
                + "\"name\" : \"joe\",\r\n" + "\"email\" : \"joe@example.com\",\r\n" + "\"content\" : \"nice post.\"\r\n" + "}\r\n" + "]}");
        collExecutor.insert(m);

        N.println(collExecutor.list(Map.class, Filters.eq("title", "A blog post")));
    }

}
