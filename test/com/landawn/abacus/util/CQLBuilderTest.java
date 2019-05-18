/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import java.util.Map;

import org.junit.Test;

import com.landawn.abacus.condition.ConditionFactory.L;
import com.landawn.abacus.entity.extendDirty.basic.Account;
import com.landawn.abacus.util.CQLBuilder.E;
import com.landawn.abacus.util.CQLBuilder.E2;
import com.landawn.abacus.util.CQLBuilder.E3;
import com.landawn.abacus.util.CQLBuilder.NE;
import com.landawn.abacus.util.CQLBuilder.RE;

/**
 * 
 * @since 0.8
 * 
 * @author Haiyang Li
 */
public class CQLBuilderTest extends AbstractNoSQLTest {

    @Test
    public void test_performance() {
        for (int i = 0; i < 10; i++) {
            String cql = E.insert("gui", "firstName", "lastName", "lastUpdateTime", "createTime").into("account").cql();
            assertEquals(102, cql.length());

            cql = NE.select("gui", "firstName", "lastName", "lastUpdateTime", "createTime").from("account").where(L.eq("id", 1)).cql();
            assertEquals(166, cql.length());
        }

        Profiler.run(16, 100000, 3, new Try.Runnable() {
            @Override
            public void run() {
                String cql = E.insert("gui", "firstName", "lastName", "lastUpdateTime", "createTime").into("account").cql();
                assertEquals(102, cql.length());

                cql = NE.select("gui", "firstName", "lastName", "lastUpdateTime", "createTime").from("account").where(L.eq("id", 1)).cql();
                assertEquals(166, cql.length());
            }
        }).writeHtmlResult(System.out);
    }

    public void test_11() {

        N.println(NE.update(Account.class).set("firstName", "lastName").iF(L.eq("firstName", "123")).cql());

        String cql = E.insert("gui", "firstName", "lastName").into("account").cql();
        N.println(cql);

        cql = RE.insert("gui", "firstName", "lastName").into("account").cql();
        N.println(cql);

        Map<String, Object> props = N.asProps("gui", N.uuid(), "firstName", "fn", "lastName", "ln");

        cql = E.insert(props).into("account").cql();
        N.println(cql);
        N.println(E.insert(props).into("account").parameters());

        cql = RE.insert(props).into("account").cql();
        N.println(cql);
        N.println(RE.insert(props).into("account").parameters());

        cql = E.select(E.DISTINCT, N.asList("firstName", "lastName")).from("account2", "account2").where("id > ?").cql();
        N.println(cql);

        Map<String, String> m = N.asMap("firstName", "lastName");
        cql = E.select(E.DISTINCT, m).from("account2", "account2").where("id > ?").cql();
        N.println(cql);
    }

    public void test_set() {
        N.println(E.update(Account.class, N.asSet(Account.ID)).using("a11", "b11").where(L.eq(Account.ID, L.QME)).cql());
        N.println(E.update(Account.class, N.asSet(Account.FIRST_NAME)).using("a11", "b11").where(L.eq(Account.ID, L.QME)).cql());
        N.println(E2.update(Account.class, N.asSet(Account.FIRST_NAME)).using("a11", "b11").where(L.eq(Account.ID, L.QME)).cql());
        N.println(E3.update(Account.class, N.asSet(Account.FIRST_NAME)).using("a11", "b11").where(L.eq(Account.ID, L.QME)).cql());

        String cql = "UPDATE account SET id = ?, first_name=? WHERE id > 0";
        assertEquals(cql, E.update("account").set("id = ?, first_name=?").where("id > 0").cql());

        cql = "UPDATE account SET first_name = ? WHERE id > 0";
        assertEquals(cql, E.update("account").set("first_name").where("id > 0").cql());

        cql = "UPDATE account SET id = ?, first_name = ? WHERE id > 0";
        assertEquals(cql, E.update("account").set("id", "first_name").where("id > 0").cql());

        cql = "UPDATE account SET id = ?, first_name = ? WHERE id > 0";
        assertEquals(cql, E.update("account").set(N.asList("id", "first_name")).where("id > 0").cql());

        cql = "UPDATE account SET id = 1, first_name = 'updatedFM' WHERE id > 0";
        assertEquals(cql, E.update("account").set(N.asMap("id", 1, "first_name", "updatedFM")).where("id > 0").cql());

        cql = "UPDATE account SET id = ?, first_name = ? WHERE id > 0";
        assertEquals(cql, E.update("account").set(N.asMap("id", L.QME, "first_name", L.QME)).where("id > 0").cql());
    }

    public void testCQLBuilder_2() {
        Account account = TestUtil.createEntity(Account.class);
        String cql = E.insert(account).into("account").cql();

        N.println(cql);

        N.println(E.update("account").set("first_name = ?").where("id = ?").cql());
    }

    public void testSQL_1() {
        String cql = E2.insert("id", "first_name", "last_name").into("account").cql();
        N.println(cql);

        cql = E2.insert(N.asList("id", "first_name", "last_name")).into("account").cql();
        N.println(cql);

        cql = E.insert(N.asProps("id", 1, "first_name", "firstNamae", "last_name", "last_name")).into("account").cql();
        N.println(cql);

        cql = E.select("id, first_name").from("account").where("id > 0").limit(10).cql();
        N.println(cql);

        cql = E.select("id", "first_name").from("account").where("id > 0").limit(10).cql();
        N.println(cql);

        cql = E.select(N.asList("id", "first_name")).from("account").where("id > 0").limit(10).cql();
        N.println(cql);

        cql = E.select("id, first_name").from("account").where("id > 0").limit(10).cql();
        N.println(cql);

        Account account = new Account();
        account.setId(123);
        account.setFirstName("first_name");

        cql = E.update("account").set("first_name=?").where("id = 1").cql();
        N.println(cql);
        assertEquals("UPDATE account SET first_name=? WHERE id = 1", cql);

        cql = E.update("account").set("first_name = ?").where("id = 1").cql();
        N.println(cql);
        assertEquals("UPDATE account SET first_name = ? WHERE id = 1", cql);

        cql = E.update("account").set("first_name= ?").where("id = 1").cql();
        N.println(cql);
        assertEquals("UPDATE account SET first_name= ? WHERE id = 1", cql);

        cql = E.update("account").set("first_name =?").where("id = 1").cql();
        N.println(cql);
        assertEquals("UPDATE account SET first_name =? WHERE id = 1", cql);

    }

    //
    //    public void test_perf() {
    //        Profiler.run(new Try.Runnable() {
    //            @Override
    //            public void run() {
    //                E.batchInsert(createAccountPropsList(99)).into("account").cql().length();
    //            }
    //        }, 32, 10000, 3).printResult();
    //
    //        Profiler.run(new Try.Runnable() {
    //            @Override
    //            public void run() {
    //                E.batchInsert(createAccountPropsList(99)).into("account").cql().length();
    //            }
    //        }, 32, 10000, 3).printResult();
    //    }
    //
    public void test_QME() {
        String cql = E.select("first_name", "last_name").from("account").where(L.eq("first_name", L.QME)).cql();
        N.println(cql);

        assertEquals("SELECT first_name AS 'first_name', last_name AS 'last_name' FROM account WHERE first_name = ?", cql);

        Map<String, Object> props = N.asProps("first_name", "?", "last_name", "?");
        cql = E.insert(props).into("account").cql();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES ('?', '?')", cql);

        props = N.asProps("first_name", L.QME, "last_name", L.QME);
        cql = E.insert(props).into("account").cql();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);
    }

    public void test_NPE() {
        String cql = E.select("firstName", "last_name").from("account").where(L.eq("firstName", L.QME)).cql();
        N.println(cql);

        // assertEquals("SELECT first_name, last_name FROM account WHERE first_name = #{firstName}", cql);

        cql = E.insert(N.asProps("firstName", L.QME, "lastName", L.QME)).into("account").cql();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);

        cql = E.insert(N.asProps("first_name", L.QME, "last_name", L.QME)).into("account").cql();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);

        cql = E.update("account").set(N.asProps("first_name", L.QME, "last_name", L.QME)).cql();
        N.println(cql);
        assertEquals("UPDATE account SET first_name = ?, last_name = ?", cql);

        cql = E.update("account").set(N.asProps("firstNmae", L.QME, "lastName", L.QME)).cql();
        N.println(cql);
        assertEquals("UPDATE account SET first_nmae = ?, last_name = ?", cql);
    }

    public void test_excludedPropNames() throws Exception {
        String cql = E.select(Account.class, N.asSet("firstName")).from("account").cql();
        N.println(cql);
        assertEquals(
                "SELECT id AS 'id', gui AS 'gui', email_address AS 'emailAddress', middle_name AS 'middleName', last_name AS 'lastName', birth_date AS 'birthDate', status AS 'status', last_update_time AS 'lastUpdateTime', create_time AS 'createTime', contact AS 'contact', devices AS 'devices' FROM account",
                cql);
    }

    public void test_expr_cond() throws Exception {
        String cql = NE.select("id", "firstName").from("account").where("firstName=?").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name=?", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName=:firstName").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name=:firstName", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName = :firstName").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name = :firstName", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName=$firstName").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name=$firstName", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName = $firstName").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name = $firstName", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName=#{firstName}").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name=#{firstName}", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName = #{firstName}").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name = #{firstName}", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName='firstName'").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name='firstName'", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName = 'firstName'").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name = 'firstName'", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName='firstName'").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name='firstName'", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName = 'firstName'").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name = 'firstName'", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName(abc, 123) = 'firstName'").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE firstName(abc, 123) = 'firstName'", cql);

        cql = NE.select("id", "firstName").from("account").where("firstName (abc, 123) = 'firstName'").cql();
        N.println(cql);
        assertEquals("SELECT id AS 'id', first_name AS 'firstName' FROM account WHERE first_name (abc, 123) = 'firstName'", cql);
    }

    @Test
    public void test_limit_offset() {
        String cql = NE.select("firstName", "lastName").from("account").where(L.eq("id", L.QME)).limit(9).cql();
        N.println(cql);
        assertEquals("SELECT first_name AS 'firstName', last_name AS 'lastName' FROM account WHERE id = :id LIMIT 9", cql);
    }
}
