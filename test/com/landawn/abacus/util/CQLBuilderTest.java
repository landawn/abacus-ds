/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import java.util.Map;

import org.junit.Test;

import com.landawn.abacus.condition.ConditionFactory.CF;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Profiler;
import com.landawn.abacus.util.Try;
import com.landawn.abacus.util.CQLBuilder.ACCB;
import com.landawn.abacus.util.CQLBuilder.LCCB;
import com.landawn.abacus.util.CQLBuilder.NAC;
import com.landawn.abacus.util.CQLBuilder.NLC;
import com.landawn.abacus.util.CQLBuilder.NSC;
import com.landawn.abacus.util.CQLBuilder.PAC;
import com.landawn.abacus.util.CQLBuilder.PLC;
import com.landawn.abacus.util.CQLBuilder.PSC;
import com.landawn.abacus.util.CQLBuilder.SCCB;
import com.landawn.abacus.util.entity.Account;

/**
 * 
 * @since 0.8
 * 
 * @author Haiyang Li
 */
public class CQLBuilderTest extends AbstractNoSQLTest {
    public void test_00() {
        N.println(SCCB.select("firstName", "lastName").from("account").where(CF.eq("id", 1)).cql());
        N.println(ACCB.select("firstName", "lastName").from("account").where(CF.eq("id", 1)).cql());
        N.println(LCCB.select("firstName", "lastName").from("account").where(CF.eq("id", 1)).cql());

        N.println(PSC.select("firstName", "lastName").from("account").where(CF.eq("id", 1)).cql());
        N.println(PAC.select("firstName", "lastName").from("account").where(CF.eq("id", 1)).cql());
        N.println(PLC.select("firstName", "lastName").from("account").where(CF.eq("id", 1)).cql());

        N.println(NSC.select("firstName", "lastName").from("account").where(CF.eq("id", 1)).cql());
        N.println(NAC.select("firstName", "lastName").from("account").where(CF.eq("id", 1)).cql());
        N.println(NLC.select("firstName", "lastName").from("account").where(CF.eq("id", 1)).cql());

        N.println(LCCB.select("firstName", "last_name").from("account").where(CF.eq("id", 1).and(CF.ne("first_name", "fn"))).cql());
        N.println(PLC.select("firstName", "last_name").from("account").where(CF.eq("id", 1).and(CF.ne("first_name", "fn"))).cql());
        N.println(NLC.select("firstName", "last_name").from("account").where(CF.eq("id", 1).and(CF.ne("first_name", "fn"))).cql());
    }

    @Test
    public void test_performance() {
        for (int i = 0; i < 10; i++) {
            String cql = SCCB.insert("gui", "firstName", "lastName", "lastUpdateTime", "createTime").into("account").cql();
            assertEquals(102, cql.length());

            cql = NSC.select("gui", "firstName", "lastName", "lastUpdateTime", "createTime").from("account").where(CF.eq("id", 1)).cql();
            assertEquals(166, cql.length());
        }

        Profiler.run(16, 100000, 3, new Try.Runnable() {
            @Override
            public void run() {
                String cql = SCCB.insert("gui", "firstName", "lastName", "lastUpdateTime", "createTime").into("account").cql();
                assertEquals(102, cql.length());

                cql = NSC.select("gui", "firstName", "lastName", "lastUpdateTime", "createTime").from("account").where(CF.eq("id", 1)).cql();
                assertEquals(166, cql.length());
            }
        }).writeHtmlResult(System.out);
    }

    public void test_11() {

        N.println(NSC.update(Account.class).set("firstName", "lastName").iF(CF.eq("firstName", "123")).cql());

        String cql = SCCB.insert("gui", "firstName", "lastName").into("account").cql();
        N.println(cql);

        cql = PSC.insert("gui", "firstName", "lastName").into("account").cql();
        N.println(cql);

        Map<String, Object> props = N.asProps("gui", N.uuid(), "firstName", "fn", "lastName", "ln");

        cql = SCCB.insert(props).into("account").cql();
        N.println(cql);
        N.println(SCCB.insert(props).into("account").parameters());

        cql = PSC.insert(props).into("account").cql();
        N.println(cql);
        N.println(PSC.insert(props).into("account").parameters());

        cql = SCCB.select(SCCB.DISTINCT, N.asList("firstName", "lastName")).from("account2", "account2").where("id > ?").cql();
        N.println(cql);

        Map<String, String> m = N.asMap("firstName", "lastName");
        cql = SCCB.select(SCCB.DISTINCT, m).from("account2", "account2").where("id > ?").cql();
        N.println(cql);
    }

    public void test_set() {
        N.println(SCCB.update(Account.class, N.asSet(Account.ID)).using("a11", "b11").where(CF.eq(Account.ID, CF.QME)).cql());
        N.println(SCCB.update(Account.class, N.asSet(Account.FIRST_NAME)).using("a11", "b11").where(CF.eq(Account.ID, CF.QME)).cql());
        N.println(ACCB.update(Account.class, N.asSet(Account.FIRST_NAME)).using("a11", "b11").where(CF.eq(Account.ID, CF.QME)).cql());
        N.println(LCCB.update(Account.class, N.asSet(Account.FIRST_NAME)).using("a11", "b11").where(CF.eq(Account.ID, CF.QME)).cql());

        String cql = "UPDATE account SET id = ?, first_name=? WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set("id = ?, first_name=?").where("id > 0").cql());

        cql = "UPDATE account SET first_name = ? WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set("first_name").where("id > 0").cql());

        cql = "UPDATE account SET id = ?, first_name = ? WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set("id", "first_name").where("id > 0").cql());

        cql = "UPDATE account SET id = ?, first_name = ? WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set(N.asList("id", "first_name")).where("id > 0").cql());

        cql = "UPDATE account SET id = 1, first_name = 'updatedFM' WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set(N.asMap("id", 1, "first_name", "updatedFM")).where("id > 0").cql());

        cql = "UPDATE account SET id = ?, first_name = ? WHERE id > 0";
        assertEquals(cql, SCCB.update("account").set(N.asMap("id", CF.QME, "first_name", CF.QME)).where("id > 0").cql());
    }

    public void testCQLBuilder_2() {
        Account account = UnitTestUtil.createEntity(Account.class);
        String cql = SCCB.insert(account).into("account").cql();

        N.println(cql);

        N.println(SCCB.update("account").set("first_name = ?").where("id = ?").cql());
    }

    public void testSQL_1() {
        String cql = ACCB.insert("id", "first_name", "last_name").into("account").cql();
        N.println(cql);

        cql = ACCB.insert(N.asList("id", "first_name", "last_name")).into("account").cql();
        N.println(cql);

        cql = SCCB.insert(N.asProps("id", 1, "first_name", "firstNamae", "last_name", "last_name")).into("account").cql();
        N.println(cql);

        cql = SCCB.select("id, first_name").from("account").where("id > 0").limit(10).cql();
        N.println(cql);

        cql = SCCB.select("id", "first_name").from("account").where("id > 0").limit(10).cql();
        N.println(cql);

        cql = SCCB.select(N.asList("id", "first_name")).from("account").where("id > 0").limit(10).cql();
        N.println(cql);

        cql = SCCB.select("id, first_name").from("account").where("id > 0").limit(10).cql();
        N.println(cql);

        Account account = new Account();
        account.setId(123);
        account.setFirstName("first_name");

        cql = SCCB.update("account").set("first_name=?").where("id = 1").cql();
        N.println(cql);
        assertEquals("UPDATE account SET first_name=? WHERE id = 1", cql);

        cql = SCCB.update("account").set("first_name = ?").where("id = 1").cql();
        N.println(cql);
        assertEquals("UPDATE account SET first_name = ? WHERE id = 1", cql);

        cql = SCCB.update("account").set("first_name= ?").where("id = 1").cql();
        N.println(cql);
        assertEquals("UPDATE account SET first_name= ? WHERE id = 1", cql);

        cql = SCCB.update("account").set("first_name =?").where("id = 1").cql();
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
        String cql = SCCB.select("first_name", "last_name").from("account").where(CF.eq("first_name", CF.QME)).cql();
        N.println(cql);

        assertEquals("SELECT first_name AS \"first_name\", last_name AS \"last_name\" FROM account WHERE first_name = ?", cql);

        Map<String, Object> props = N.asProps("first_name", "?", "last_name", "?");
        cql = SCCB.insert(props).into("account").cql();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES ('?', '?')", cql);

        props = N.asProps("first_name", CF.QME, "last_name", CF.QME);
        cql = SCCB.insert(props).into("account").cql();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);
    }

    public void test_NPE() {
        String cql = SCCB.select("firstName", "last_name").from("account").where(CF.eq("firstName", CF.QME)).cql();
        N.println(cql);

        // assertEquals("SELECT first_name, last_name FROM account WHERE first_name = #{firstName}", cql);

        cql = SCCB.insert(N.asProps("firstName", CF.QME, "lastName", CF.QME)).into("account").cql();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);

        cql = SCCB.insert(N.asProps("first_name", CF.QME, "last_name", CF.QME)).into("account").cql();
        N.println(cql);
        assertEquals("INSERT INTO account (first_name, last_name) VALUES (?, ?)", cql);

        cql = SCCB.update("account").set(N.asProps("first_name", CF.QME, "last_name", CF.QME)).cql();
        N.println(cql);
        assertEquals("UPDATE account SET first_name = ?, last_name = ?", cql);

        cql = SCCB.update("account").set(N.asProps("firstNmae", CF.QME, "lastName", CF.QME)).cql();
        N.println(cql);
        assertEquals("UPDATE account SET first_nmae = ?, last_name = ?", cql);
    }

    public void test_excludedPropNames() throws Exception {
        String cql = SCCB.select(Account.class, N.asSet("firstName")).from("account").cql();
        N.println(cql);
        assertEquals(
                "SELECT id AS \"id\", gui AS \"gui\", email_address AS \"emailAddress\", middle_name AS \"middleName\", last_name AS \"lastName\", birth_date AS \"birthDate\", status AS \"status\", last_update_time AS \"lastUpdateTime\", create_time AS \"createTime\", contact AS \"contact\", devices AS \"devices\" FROM account",
                cql);
    }

    public void test_expr_cond() throws Exception {
        String cql = NSC.select("id", "firstName").from("account").where("firstName=?").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=?", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName=:firstName").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=:firstName", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName = :firstName").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name = :firstName", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName=$firstName").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=$firstName", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName = $firstName").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name = $firstName", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName=#{firstName}").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=#{firstName}", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName = #{firstName}").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name = #{firstName}", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName=\"firstName\"").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=\"firstName\"", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName = \"firstName\"").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name = \"firstName\"", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName=\"firstName\"").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name=\"firstName\"", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName = \"firstName\"").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name = \"firstName\"", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName(abc, 123) = \"firstName\"").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE firstName(abc, 123) = \"firstName\"", cql);

        cql = NSC.select("id", "firstName").from("account").where("firstName (abc, 123) = \"firstName\"").cql();
        N.println(cql);
        assertEquals("SELECT id AS \"id\", first_name AS \"firstName\" FROM account WHERE first_name (abc, 123) = \"firstName\"", cql);
    }

    @Test
    public void test_limit_offset() {
        String cql = NSC.select("firstName", "lastName").from("account").where(CF.eq("id", CF.QME)).limit(9).cql();
        N.println(cql);
        assertEquals("SELECT first_name AS \"firstName\", last_name AS \"lastName\" FROM account WHERE id = :id LIMIT 9", cql);
    }
}
