/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.landawn.abacus.da.DynamoDBExecutor;
import com.landawn.abacus.da.DynamoDBExecutor.Filters;
import com.landawn.abacus.da.entity.AccountContact;
import com.landawn.abacus.da.entity.AccountDevice;
import com.landawn.abacus.util.N;

/**
 *
 * @since 0.8
 * 
 * @author Haiyang Li
 */
public class DynamoDBExecutorTest extends AbstractNoSQLTest {
    static final DynamoDBExecutor dbExecutor;
    static {
        AWSCredentials awsCredentials = new BasicAWSCredentials("AKIAJXDY6KEWJBMXBXZQ", "qezOsp4icconyR0boRPeNyIwldFkpOxYOETGPw9g");
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(awsCredentials);
        dynamoDBClient.setRegion(Region.getRegion(Regions.US_WEST_2));
        dbExecutor = new DynamoDBExecutor(dynamoDBClient);
    }

    public void test_crud() {
        Account account = createAccount2();
        N.println(dbExecutor.dynamoDB().listTables());

        dbExecutor.putItem("account", account);

        Account dbAccount = dbExecutor.getItem(Account.class, "account", DynamoDBExecutor.asKey("id", account.getId()));
        N.println(dbAccount);

        final QueryRequest queryRequest = new QueryRequest("account").withKeyConditions(Filters.eq("id", "abc123"));
        List<Account> accounts = dbExecutor.list(Account.class, queryRequest);
        N.println(accounts);
        assertTrue(accounts.size() == 0);

        ScanRequest scanRequest = new ScanRequest("account").withScanFilter(Filters.isNull("id"));
        accounts = dbExecutor.scan(Account.class, scanRequest).toList();
        N.println(accounts);
        assertTrue(accounts.size() == 0);

        scanRequest = new ScanRequest("account").withScanFilter(Filters.notNull("id"));
        accounts = dbExecutor.scan(Account.class, scanRequest).toList();
        N.println(accounts);
        assertTrue(accounts.size() > 0);

        scanRequest = new ScanRequest("account").withScanFilter(Filters.eq("id", "abc123"));
        accounts = dbExecutor.scan(Account.class, scanRequest).toList();
        N.println(accounts);
        assertTrue(accounts.size() == 0);

        scanRequest = new ScanRequest("account").withScanFilter(Filters.ne("id", "abc123"));
        accounts = dbExecutor.scan(Account.class, scanRequest).toList();
        N.println(accounts);
        assertTrue(accounts.size() > 0);

        scanRequest = new ScanRequest("account").withScanFilter(Filters.contains("id", "abc123"));
        accounts = dbExecutor.scan(Account.class, scanRequest).toList();
        N.println(accounts);
        assertTrue(accounts.size() == 0);

        scanRequest = new ScanRequest("account").withScanFilter(Filters.notContains("id", "abc123"));
        accounts = dbExecutor.scan(Account.class, scanRequest).toList();
        N.println(accounts);
        assertTrue(accounts.size() > 0);

        scanRequest = new ScanRequest("account").withScanFilter(Filters.notContains("id", "abc123")).withAttributesToGet(N.asList("firstName"));
        List<String> strs = dbExecutor.scan(String.class, scanRequest).toList();
        N.println(strs);
        assertTrue(strs.size() > 0);

        scanRequest = new ScanRequest("account").withScanFilter(Filters.bt("id", "aaa", "aab"));
        accounts = dbExecutor.scan(Account.class, scanRequest).toList();
        N.println(accounts);
        assertTrue(accounts.size() == 0);

        scanRequest = new ScanRequest("account").withScanFilter(Filters.bt("id", "000", "xxx"));
        accounts = dbExecutor.scan(Account.class, scanRequest).toList();
        N.println(accounts);
        assertTrue(accounts.size() > 0);

        scanRequest = new ScanRequest("account").withScanFilter(Filters.in("id", "aaa", "aab"));
        accounts = dbExecutor.scan(Account.class, scanRequest).toList();
        N.println(accounts);
        assertTrue(accounts.size() == 0);

        dbExecutor.deleteItem("account", DynamoDBExecutor.asKey("id", account.getId()));
    }

    public void test_00() {
        AttributeValue attrValue = DynamoDBExecutor.attrValueOf(ByteBuffer.wrap("abc".getBytes(), 3, 0));
        String str = N.stringOf(attrValue);
        N.println(str);
        assertEquals("abc", new String(N.base64Decode(str)));
    }

    public void test_01() {
        Account account = createAccount2();

        Map<String, AttributeValue> item = DynamoDBExecutor.toItem(account);
        N.println(item);

        Account account2 = DynamoDBExecutor.toEntity(Account.class, item);
        N.println(account);
        N.println(account2);

        assertEquals(account, account2);
    }

    private Account createAccount2() {
        Account account = createAccount();

        AccountContact contact = new AccountContact();
        contact.setCity("Sunnyvalue");
        contact.setState("CA");
        account.setContact(contact);

        AccountDevice device1 = new AccountDevice();
        device1.setManufacturer("Abacus");
        device1.setModel("X");

        AccountDevice device2 = new AccountDevice();
        device2.setManufacturer("Abacus");
        device2.setModel("X");

        account.setDevices(N.asList(device1, device2));

        return account;
    }

    public void test_02() {
        List<Account> accountList = N.asList(createAccount2());

        List<Map<String, AttributeValue>> itemList = DynamoDBExecutor.toItem(accountList);
        N.println(itemList);

        List<Account> accountList2 = DynamoDBExecutor.toList(Account.class, itemList);
        N.println(accountList2);

        assertEquals(accountList, accountList2);
    }

    public void test_03() {
        List<Account> accountList = N.asList(createAccount(), createAccount2(), createAccount2());

        List<Map<String, AttributeValue>> itemList = DynamoDBExecutor.toItem(accountList);
        N.println(itemList);

        List<Account> accountList2 = DynamoDBExecutor.toList(Account.class, itemList);
        N.println(accountList2);

        assertEquals(accountList, accountList2);
    }
}
