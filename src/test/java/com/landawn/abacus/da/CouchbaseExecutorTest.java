/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da;

/**
 *
 * @since 0.8
 * 
 * @author Haiyang Li
 */
public class CouchbaseExecutorTest extends AbstractNoSQLTest {
    //    //
    //    static final JSONSerializationConfig jsc = JSC.of(true, true);
    //    static final Cluster cluster = CouchbaseCluster.create();
    //    // static final Bucket bucket = cluster.openBucket("Administrator", "admin123");
    //    static final Bucket bucket = cluster.openBucket();
    //    static final CouchbaseExecutor couchbaseExecutor = new CouchbaseExecutor(cluster, bucket);
    //    static final String collectionName = "testData";
    //
    //    static {
    //        CouchbaseExecutor.registerIdProeprty(Account.class, "id");
    //        System.setProperty("com.couchbase.queryEnabled", "true");
    //
    //        //        bucket.query(Query.simple("CREATE PRIMARY INDEX `default-primary-index` ON `default' USING GSI;"));
    //        //        bucket.query(Query.simple("CREATE INDEX `default-firstName-index` ON `default`(firstName) USING GSI;"));
    //        //        bucket.query(Query.simple("CREATE INDEX `default-lastName-index` ON `default`(lastName) USING GSI;"));
    //    }
    //
    //    @Test
    //    public void test_hello() {
    //        Account account = createAccount();
    //
    //        bucket.insert(JsonDocument.create(N.uuid(), JsonObject.fromJson(jsonParser.serialize(account, jsc))));
    //
    //        N.sleep(1000);
    //
    //        String sql = "SELECT firstName, lastName FROM default where lastName = '" + account.getLastName() + "'";
    //        couchbaseExecutor.query(sql).println();
    //
    //        couchbaseExecutor.query(Query.simple(sql)).println();
    //
    //        couchbaseExecutor.query(Query.simple("DELETE FROM default where lastName = '" + account.getLastName() + "'"));
    //    }
    //
    //    @Test
    //    public void test_query() {
    //        Account account = createAccount();
    //        account.setId(null);
    //
    //        couchbaseExecutor.insert(JsonDocument.create(N.uuid(), JsonObject.fromJson(jsonParser.serialize(account, jsc))));
    //
    //        N.sleep(1000);
    //
    //        Object parameters = JsonArray.from(account.getLastName());
    //
    //        String sql = E3.select("firstName", "lastName").from("default").where("lastName = $1").sql();
    //        N.println(sql);
    //
    //        assertTrue(couchbaseExecutor.exists(sql, parameters));
    //
    //        Map<String, Object> map = couchbaseExecutor.findFirst(Map.class, sql, parameters).orElse(null);
    //        N.println(map);
    //        assertEquals(account.getLastName(), map.get("lastName"));
    //
    //        Account dbAccount = couchbaseExecutor.findFirst(Account.class, sql, parameters).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        DataSet dataSet = couchbaseExecutor.query(sql, parameters);
    //        dataSet.println();
    //        assertEquals(account.getLastName(), dataSet.get("lastName"));
    //
    //        long uuid = couchbaseExecutor.queryForSingleResult(long.class, "SELECT id FROM default WHERE lastName = $1", parameters).orElse(null);
    //        N.println(uuid);
    //        assertEquals(0, uuid);
    //
    //        String strLastName = couchbaseExecutor.queryForSingleResult(String.class, "SELECT lastName FROM default WHERE lastName = $1", parameters).orElse(null);
    //        N.println(strLastName);
    //        assertEquals(account.getLastName(), strLastName);
    //
    //        assertTrue(couchbaseExecutor.exists(sql, parameters));
    //        couchbaseExecutor.execute("DELETE FROM default WHERE lastName = ?", account.getLastName());
    //        assertFalse(couchbaseExecutor.exists(sql, parameters));
    //    }
    //
    //    @Test
    //    public void test_update() {
    //        Account account = createAccount();
    //        account.setId(null);
    //
    //        bucket.insert(JsonDocument.create(N.uuid(), JsonObject.fromJson(jsonParser.serialize(account, jsc))));
    //
    //        N.sleep(1000);
    //
    //        Object parameters = JsonArray.from(account.getLastName());
    //
    //        String sql = E3.select(idNameOf("default"), "firstName", "lastName").from("default").where("lastName = $1").sql();
    //        N.println(sql);
    //        Map<String, Object> map = couchbaseExecutor.findFirst(Map.class, sql, account.getLastName()).orElse(null);
    //        N.println(map);
    //        String id = map.get("id").toString();
    //
    //        JsonDocument updateResult = couchbaseExecutor
    //                .upsert(toJsonDocument(N.asProps("_id", id, "firstName", "newFirstName", "lastName", account.getLastName())));
    //        N.println(updateResult);
    //
    //        map = couchbaseExecutor.findFirst(Map.class, sql, account.getLastName()).orElse(null);
    //        assertEquals("newFirstName", map.get("firstName"));
    //
    //        assertTrue(couchbaseExecutor.exists(sql, parameters));
    //        couchbaseExecutor.execute("DELETE FROM default WHERE lastName = ?", account.getLastName());
    //        assertFalse(couchbaseExecutor.exists(sql, parameters));
    //    }
    //
    //    @Test
    //    public void test_parameterized() {
    //        Account account = createAccount();
    //
    //        account.setId(null);
    //        couchbaseExecutor.insert(JsonDocument.create(N.uuid(), JsonObject.fromJson(jsonParser.serialize(account, jsc))));
    //
    //        N.sleep(1000);
    //
    //        Account dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = ?", account.getLastName()).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = ?", N.asList(account.getLastName())).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = #{lastName}", account.getLastName()).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = #{lastName}", N.asList(account.getLastName()))
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = #{lastName}",
    //                N.asProps("lastName", account.getLastName())).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = #{lastName}", account)
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $lastName", account.getLastName()).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $lastName", N.asList(account.getLastName()))
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $lastName",
    //                N.asProps("lastName", account.getLastName())).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $lastName", account)
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $a", account.getLastName())
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $a", N.asList(account.getLastName())).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        try {
    //            couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $a",
    //                    N.asProps("lastName", account.getLastName()));
    //            fail("Should throw IllegalArgumentException");
    //        } catch (IllegalArgumentException e) {
    //
    //        }
    //
    //        try {
    //            couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $a", account);
    //            fail("Should throw IllegalArgumentException");
    //        } catch (IllegalArgumentException e) {
    //
    //        }
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = ?", account.getLastName().toString()).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = ?", N.asList(account.getLastName().toString()))
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        try {
    //            dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = ?",
    //                    N.asProps("lastName", account.getLastName().toString())).orElse(null);
    //            fail("Should throw IllegalArgumentException");
    //        } catch (IllegalArgumentException e) {
    //
    //        }
    //        try {
    //            dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = ?", account).orElse(null);
    //            fail("Should throw IllegalArgumentException");
    //        } catch (IllegalArgumentException e) {
    //
    //        }
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = #{lastName}", account.getLastName().toString())
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = #{lastName}",
    //                N.asList(account.getLastName().toString())).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = #{lastName}",
    //                N.asProps("lastName", account.getLastName().toString())).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = #{lastName}", account)
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $lastName", account.getLastName().toString())
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $lastName",
    //                N.asList(account.getLastName().toString())).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $lastName",
    //                N.asProps("lastName", account.getLastName().toString())).orElse(null);
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $lastName", account)
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $a", account.getLastName().toString())
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        dbAccount = couchbaseExecutor
    //                .findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $a", N.asList(account.getLastName().toString()))
    //                .orNull();
    //        N.println(dbAccount);
    //        assertEquals(account.getLastName(), dbAccount.getLastName());
    //
    //        try {
    //            couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $a",
    //                    N.asProps("lastName", account.getLastName().toString()));
    //            fail("Should throw IllegalArgumentException");
    //        } catch (IllegalArgumentException e) {
    //
    //        }
    //
    //        try {
    //            couchbaseExecutor.findFirst(Account.class, "SELECT id, firstName, lastName FROM default WHERE lastName = $a", account);
    //            fail("Should throw IllegalArgumentException");
    //        } catch (IllegalArgumentException e) {
    //
    //        }
    //
    //        couchbaseExecutor.execute("DELETE FROM default WHERE lastName = #{lastName}", account.getLastName());
    //        assertFalse(couchbaseExecutor.exists("SELECT id, firstName, lastName FROM default WHERE lastName = #{lastName}", account.getLastName()));
    //    }
    //
    //    @Test
    //    public void test_crud() {
    //        Account account = createAccount();
    //
    //        Account dbAccount = couchbaseExecutor.insert(account);
    //        N.println(dbAccount);
    //
    //        dbAccount = couchbaseExecutor.get(Account.class, dbAccount.getId());
    //
    //        String newFirstName = "newFirstName";
    //        dbAccount.setFirstName(newFirstName);
    //        couchbaseExecutor.upsert(dbAccount);
    //
    //        dbAccount = couchbaseExecutor.get(Account.class, dbAccount.getId());
    //
    //        assertEquals(newFirstName, dbAccount.getFirstName());
    //
    //        couchbaseExecutor.remove(dbAccount.getId());
    //
    //        assertNull(couchbaseExecutor.get(Account.class, dbAccount.getId()));
    //    }
    //
    //    @Test
    //    public void test_crud_2() {
    //        Account account = createAccount();
    //
    //        Account dbAccount = couchbaseExecutor.insert(account);
    //        N.println(dbAccount);
    //
    //        dbAccount = couchbaseExecutor.get(Account.class, dbAccount.getId());
    //
    //        String newFirstName = "newFirstName";
    //        dbAccount.setFirstName(newFirstName);
    //        couchbaseExecutor.upsert(dbAccount);
    //
    //        dbAccount = couchbaseExecutor.get(Account.class, dbAccount.getId());
    //
    //        assertEquals(newFirstName, dbAccount.getFirstName());
    //
    //        Account removedAccount = couchbaseExecutor.remove(Account.class, dbAccount.getId());
    //        N.println(removedAccount);
    //
    //        assertNull(couchbaseExecutor.get(Account.class, dbAccount.getId()));
    //    }
    //
    //    @Test
    //    public void test_crud_3() {
    //        Account account = createAccount();
    //
    //        Account dbAccount = couchbaseExecutor.insert(account);
    //        N.println(dbAccount);
    //
    //        dbAccount = couchbaseExecutor.get(Account.class, dbAccount.getId());
    //
    //        String newFirstName = "newFirstName";
    //        dbAccount.setFirstName(newFirstName);
    //        couchbaseExecutor.upsert(dbAccount);
    //
    //        dbAccount = couchbaseExecutor.get(Account.class, dbAccount.getId());
    //
    //        assertEquals(newFirstName, dbAccount.getFirstName());
    //
    //        JsonDocument removedAccount = couchbaseExecutor.remove(JsonDocument.class, dbAccount.getId());
    //        N.println(removedAccount);
    //
    //        assertNull(couchbaseExecutor.get(Account.class, dbAccount.getId()));
    //    }
    //
    //    public void test_json() {
    //        Account account = createAccount();
    //
    //        JsonDocument doc = CouchbaseExecutor.toJsonDocument(account);
    //        Account account2 = CouchbaseExecutor.toEntity(Account.class, doc);
    //
    //        assertEquals(account, account2);
    //
    //        JsonObject jsonObject = CouchbaseExecutor.toJsonObject(account);
    //
    //        account2 = CouchbaseExecutor.toEntity(Account.class, jsonObject);
    //
    //        assertEquals(account, account2);
    //
    //        String json = CouchbaseExecutor.toJSON(jsonObject);
    //
    //        String json2 = CouchbaseExecutor.toJSON(CouchbaseExecutor.fromJSON(JsonObject.class, json));
    //
    //        assertEquals(account, N.fromJSON(Account.class, json2));
    //
    //        json = CouchbaseExecutor.toJSON(doc);
    //
    //        json2 = CouchbaseExecutor.toJSON(CouchbaseExecutor.fromJSON(JsonDocument.class, json));
    //
    //        assertEquals(account, N.fromJSON(Account.class, json2));
    //    }
}
