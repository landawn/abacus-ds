/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.util.N;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class HBaseExecutorTest {
    // Install HBase and create tables:
    // create 'account', 'id', 'gui', 'name', 'emailAddress', 'lastUpdateTime', 'createTime', 'contact'
    // create 'contact', 'id', 'accountId', 'telephone', 'city', 'state', 'zipCode', 'status', 'lastUpdateTime', 'createTime'

    static final HBaseExecutor hbaseExecutor;

    static {
        Configuration config = HBaseConfiguration.create();
        config.setInt("timeout", 120000);
        config.set("hbase.master", "localhost:9000");
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            hbaseExecutor = new HBaseExecutor(ConnectionFactory.createConnection(config));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        HBaseExecutor.registerRowKeyProperty(Account.class, "id");
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Account {
        private String id;
        private String gui;
        private Name name;
        private String emailAddress;
        private Timestamp lastUpdateTime;
        private Timestamp createTime;
        private Contact contact;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Name {
        private String firstName;
        private String middleName;
        private String lastName;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Contact {
        private long id;
        private long accountId;
        private String telephone;
        private String city;
        private String state;
        private String country;
        private String zipCode;
        private int status;
        private Timestamp lastUpdateTime;
        private Timestamp createTime;
    }

    @Test
    public void test_toAnyPut() {
        Account account = Account.builder()
                .id("1002")
                .gui(N.uuid())
                .name(Name.builder().firstName("fn").lastName("lm").build())
                .contact(Contact.builder().city("San Jose").state("CA").build())
                .build();
        N.println(account);

        hbaseExecutor.delete("account", AnyDelete.of(account.getId()));

        AnyPut put = HBaseExecutor.toAnyPut(account);
        N.println(put.toString(100));

        hbaseExecutor.put("account", put);

        Account dbAccount = hbaseExecutor.get(Account.class, "account", AnyGet.of(account.getId()));
        N.println(dbAccount);

        assertEquals(account, dbAccount);
    }

    //    @Test
    //    public void test_01() throws ZooKeeperConnectionException, ServiceException, IOException {
    //        Result result = hbaseExecutor.get("account", AnyGet.of("row1"));
    //
    //        String row = Bytes.toString(result.getRow());
    //
    //        N.println(row);
    //
    //        if (result.advance()) {
    //            Cell cell = result.current();
    //            N.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    //            N.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
    //
    //            N.println(cell.getTimestamp());
    //        }
    //    }
    //
    //    @Test
    //    public void test_crud() {
    //        Account account = createAccount2();
    //
    //        // Insert is supported by Model/entity
    //        hbaseExecutor.put("account", toAnyPut(account));
    //
    //        // Get is supported by Model/entity
    //        Account dbAccount = hbaseExecutor.get(Account.class, "account", AnyGet.of(account.getId()));
    //        N.println(dbAccount);
    //
    //        N.println(hbaseExecutor.get(Account.class, "account", N.asList(AnyGet.of(account.getId()))));
    //        N.println(hbaseExecutor.get(Account.class, "account", N.asList(AnyGet.of(account.getId()).value())));
    //
    //        assertEquals(hbaseExecutor.get(Account.class, "account", N.asList(AnyGet.of(account.getId()))),
    //                hbaseExecutor.get(Account.class, "account", N.asList(AnyGet.of(account.getId()).value())));
    //
    //        Timestamp ceateTime = hbaseExecutor.get(Timestamp.class, "account", AnyGet.of(account.getId()).addColumn("createTime", ""));
    //        N.println(ceateTime);
    //
    //        N.println(hbaseExecutor.get(Timestamp.class, "account", N.asList(AnyGet.of(account.getId()).addColumn("createTime", ""))));
    //        N.println(hbaseExecutor.get(Timestamp.class, "account", N.asList(AnyGet.of(account.getId()).addColumn("createTime", "").value())));
    //
    //        assertEquals(hbaseExecutor.get(Timestamp.class, "account", N.asList(AnyGet.of(account.getId()).addColumn("createTime", ""))),
    //                hbaseExecutor.get(Timestamp.class, "account", N.asList(AnyGet.of(account.getId()).addColumn("createTime", "").value())));
    //
    //        // Delete the inserted account
    //        hbaseExecutor.delete("account", AnyDelete.of(account.getId()));
    //        dbAccount = hbaseExecutor.get(Account.class, "account", AnyGet.of(account.getId()));
    //    }
    //
    //    @Test
    //    public void test_crud_2() throws IOException {
    //        Account account = createAccount2();
    //
    //        // Insert an account into HBase store
    //        Put put = new Put(Bytes.toBytes(account.getId()));
    //        put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("firstName"), Bytes.toBytes(account.getName().firstName().value()));
    //        put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("lastName"), Bytes.toBytes(account.getName().lastName().value()));
    //        put.addColumn(Bytes.toBytes("contact"), Bytes.toBytes("city"), Bytes.toBytes(account.getContact().city().value()));
    //        put.addColumn(Bytes.toBytes("contact"), Bytes.toBytes("state"), Bytes.toBytes(account.getContact().state().value()));
    //        put.addColumn(Bytes.toBytes("createTime"), Bytes.toBytes(""), Bytes.toBytes(N.stringOf(account.createTime().value())));
    //
    //        hbaseExecutor.put("account", put);
    //
    //        // Get the inserted account from HBase store
    //        Result result = hbaseExecutor.get("account", new Get(Bytes.toBytes(account.getId())));
    //        CellScanner cellScanner = result.cellScanner();
    //        while (cellScanner.advance()) {
    //            final Cell cell = cellScanner.current();
    //            N.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
    //            N.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
    //            N.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    //            // ... a lot of work to do
    //        }
    //
    //        // Delete the inserted account from HBase store.
    //        hbaseExecutor.delete("account", new Delete(Bytes.toBytes(account.getId())));
    //    }
    //
    //    private Account createAccount2() {
    //        Account account = new Account();
    //        account.setId(123);
    //        account.setName(new Name().setFirstName("firstName123").setLastName("lastName123"));
    //        account.addCreateTime(N.currentTimestamp());
    //        account.setStrSet(N.asSet("a", "b", "c"));
    //        Map<String, Long> strMap = N.asMap("b", 2l);
    //        account.setStrMap(strMap);
    //        account.setContact(new AccountContact().setCity("Sunnyvale").setState("CA"));
    //        return account;
    //    }
    //
    //    @Test
    //    public void test_scan() throws IOException {
    //        for (int i = 0; i < 100; i++) {
    //            Account account = new Account();
    //            account.setId(i + 10000);
    //            account.setName(new Name().setFirstName("firstName123").setLastName("lastName123"));
    //            account.addCreateTime(N.currentTimestamp());
    //            account.setContact(new AccountContact().setCity("Sunnyvale").setState("CA"));
    //
    //            hbaseExecutor.put("account", HBaseExecutor.toAnyPut(account));
    //        }
    //
    //        N.sleep(1000);
    //        List<Result> results = hbaseExecutor.scan("account", "name");
    //        N.println(HBaseExecutor.toList(Account.class, results));
    //
    //        List<Account> accounts = hbaseExecutor.scan(Account.class, "account", "name");
    //        N.println(accounts);
    //
    //        List<Timestamp> createTimes = hbaseExecutor.scan(Timestamp.class, "account", "createTime");
    //        N.println(createTimes);
    //    }
    //
    //    @Test
    //    public void test_scan_2() throws IOException {
    //        for (int i = 0; i < 100; i++) {
    //            Account account = new Account();
    //            account.setId(i + 10000);
    //            account.setName(new Name().setFirstName("firstName123").setLastName("lastName123"));
    //            account.addCreateTime(N.currentTimestamp());
    //            account.setContact(new AccountContact().setCity("Sunnyvale").setState("CA"));
    //
    //            hbaseExecutor.put("account", HBaseExecutor.toAnyPut(account));
    //        }
    //
    //        N.sleep(1000);
    //        ResultScanner resultScanner = hbaseExecutor.getScanner("account", "name");
    //        N.println(HBaseExecutor.toList(Account.class, resultScanner));
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(99, HBaseExecutor.toList(Account.class, resultScanner, 1, 99).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(99, HBaseExecutor.toList(Account.class, resultScanner, 1, 100).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(1, HBaseExecutor.toList(Account.class, resultScanner, 99, 9).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 100, 9).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 101, 9).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 0, 0).size());
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 99, 0).size());
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 100, 0).size());
    //
    //        resultScanner = hbaseExecutor.getScanner("account", "name");
    //        try {
    //            assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, -1, 0).size());
    //            fail("Should throw IllegalArgumentException");
    //        } catch (IllegalArgumentException e) {
    //
    //        }
    //
    //        try {
    //            assertEquals(0, HBaseExecutor.toList(Account.class, resultScanner, 0, -1).size());
    //            fail("Should throw IllegalArgumentException");
    //        } catch (IllegalArgumentException e) {
    //
    //        }
    //
    //        Result result = null;
    //        while ((result = resultScanner.next()) != null) {
    //            Account dbAccount = HBaseExecutor.toEntity(Account.class, result);
    //            N.println(dbAccount);
    //            N.println(hbaseExecutor.get(Account.class, "account", AnyGet.of(dbAccount.getId())));
    //            hbaseExecutor.delete("account", AnyDelete.of(dbAccount.getId()));
    //        }
    //    }
    //
    //    @Test
    //    public void test_serialize_json() {
    //        Account account = createAccount2();
    //
    //        String json = jsonParser.serialize(account, JSC.of(DateTimeFormat.LONG));
    //
    //        N.println(json);
    //
    //        Account account2 = jsonParser.deserialize(Account.class, json);
    //        N.println(account);
    //        N.println(account2);
    //
    //        Timestamp sysTime = N.currentTimestamp();
    //        N.println(sysTime.getTime());
    //
    //        Timestamp sysTime2 = N.asTimestamp(sysTime.getTime());
    //
    //        assertEquals(sysTime, sysTime2);
    //
    //        N.println(N.stringOf(sysTime));
    //
    //        sysTime2 = N.valueOf(Timestamp.class, N.stringOf(sysTime));
    //
    //        assertEquals(sysTime, sysTime2);
    //
    //        assertEquals(account, account2);
    //    }
    //
    //    @Test
    //    public void test_serialize_xml() {
    //        Account account = createAccount2();
    //        account.setCreateTime((Map) null);
    //
    //        String xml = xmlParser.serialize(account);
    //
    //        N.println(xml);
    //
    //        Account account2 = xmlParser.deserialize(Account.class, xml);
    //        N.println(account);
    //        N.println(account2);
    //
    //        assertEquals(account, account2);
    //    }
}
