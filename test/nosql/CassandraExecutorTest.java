/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package nosql;

import java.util.List;

import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.landawn.abacus.condition.ConditionFactory.CF;
import com.landawn.abacus.util.CassandraExecutor;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Seq;
import com.landawn.abacus.util.CQLBuilder.NSC;
import com.landawn.abacus.util.function.Function;

import junit.framework.TestCase;

/**
 *
 * @since 0.8
 * 
 * @author Haiyang Li
 */
public class CassandraExecutorTest extends TestCase {

    static {
        CassandraExecutor.registerKeys(Account.class, N.asList("id", "gui"));
    }

    static final String keySpace = "codes";

    static final String sql_createKeySpace = "CREATE KEYSPACE IF NOT EXISTS " + keySpace
            + " WITH replication={'class':'SimpleStrategy', 'replication_factor':3}";
    static final String sql_dropTable = "DROP TABLE IF EXISTS account";
    static final String sql_createTable = "CREATE TABLE IF NOT EXISTS account(id varchar, gui varchar, first_name varchar, last_name varchar, "
            + "status int, last_update_time timestamp, create_time timestamp, PRIMARY KEY (id, gui));";

    static final CassandraExecutor cassandraExecutor;

    static {
        try {
            cassandraExecutor = new CassandraExecutor(Cluster.builder().addContactPoint("127.0.0.1").build().connect(keySpace));
        } catch (Throwable e) {
            e.printStackTrace();
            throw N.toRuntimeException(e);
        }
    }

    static {
        cassandraExecutor.execute(sql_createKeySpace);
        cassandraExecutor.execute(sql_dropTable);
        cassandraExecutor.execute(sql_createTable);
    }

    @Test
    public void test_crud() {
        Account account = N.fill(Account.class);
        // create
        cassandraExecutor.insert(account);

        cassandraExecutor.query("select count(*) from account").println();

        assertTrue(cassandraExecutor.exists(Account.class, CF.eq("id", account.getId())));

        cassandraExecutor.query("select id as \"id\" from account where id = ?", account.getId()).println();

        // read
        Account dbAccount = cassandraExecutor.gett(Account.class, account.getId(), account.getGUI());
        assertEquals(account.getGUI(), dbAccount.getGUI());

        // update
        dbAccount.setFirstName("newFirstName");
        cassandraExecutor.update(dbAccount);

        dbAccount = cassandraExecutor.gett(Account.class, CF.eq("id", account.getId()).and(CF.eq("gui", account.getGUI())));
        assertEquals("newFirstName", dbAccount.getFirstName());

        // delete
        cassandraExecutor.delete(Account.class, N.asList("first_name"), CF.eq("id", account.getId()).and(CF.eq("gui", account.getGUI())));

        // delete
        cassandraExecutor.delete(Account.class, CF.eq("id", account.getId()).and(CF.eq("gui", account.getGUI())));

        // check
        assertNull(cassandraExecutor.gett(Account.class, CF.eq("id", account.getId()).and(CF.eq("gui", account.getGUI()))));
    }

    @Test
    public void test_crud_async() throws Exception {
        Account account = N.fill(Account.class);
        // create
        cassandraExecutor.asyncInsert(account).get();

        cassandraExecutor.asyncQuery("select count(*) from account").get().println();

        cassandraExecutor.asyncQuery("select id as \"id\" from account where id = ?", account.getId()).get().println();

        // read
        Account dbAccount = cassandraExecutor.asyncGett(Account.class, account.getId(), account.getGUI()).get();
        assertEquals(account.getGUI(), dbAccount.getGUI());

        // update
        dbAccount.setFirstName("newFirstName");
        cassandraExecutor.asyncUpdate(dbAccount).get();

        dbAccount = cassandraExecutor.asyncGett(Account.class, CF.eq("id", account.getId()).and(CF.eq("gui", account.getGUI()))).get();
        assertEquals("newFirstName", dbAccount.getFirstName());

        // delete
        cassandraExecutor.asyncDelete(Account.class, N.asList("first_name"), CF.eq("id", account.getId()).and(CF.eq("gui", account.getGUI()))).get();

        // delete
        cassandraExecutor.asyncDelete(Account.class, CF.eq("id", account.getId()).and(CF.eq("gui", account.getGUI()))).get();

        // check
        assertNull(cassandraExecutor.asyncGett(Account.class, CF.eq("id", account.getId()).and(CF.eq("gui", account.getGUI()))).get());
    }

    @Test
    public void test_crud_2() throws Exception {
        List<Account> accounts = N.fill(Account.class, 11);

        cassandraExecutor.batchInsert(accounts, BatchStatement.Type.UNLOGGED);

        assertFalse(cassandraExecutor.exists(Account.class, CF.eq("id", "111")));

        assertEquals(0, cassandraExecutor.count(Account.class, CF.eq("id", "111")));

        cassandraExecutor.query(Account.class, CF.eq("id", "111"));

        cassandraExecutor.batchUpdate(accounts, BatchStatement.Type.LOGGED);
        cassandraExecutor.query(NSC.selectFrom(Account.class).cql()).println();

        cassandraExecutor.stream(NSC.selectFrom(Account.class).cql()).forEach(Fn.println());

        cassandraExecutor.stream(Account.class, NSC.selectFrom(Account.class).cql()).forEach(Fn.println());

        Seq.of(cassandraExecutor.list(NSC.selectFrom(Account.class).cql())).foreach(Fn.println());

        Seq.of(cassandraExecutor.list(String.class, NSC.select("first_name").from(Account.class).cql())).foreach(Fn.println());

        Seq.of(cassandraExecutor.asyncList(String.class, NSC.select("first_name").from(Account.class).cql()).get()).foreach(Fn.println());

        Seq.of(cassandraExecutor.list(Account.class, NSC.selectFrom(Account.class).cql())).foreach(Fn.println());

        cassandraExecutor.delete(Account.class, CF.in("id", Seq.of(accounts).map(new Function<Account, String>() {
            @Override
            public String apply(Account t) {
                return t.getId();
            }
        })));

        assertEquals(0, cassandraExecutor.stream(NSC.selectFrom(Account.class).cql()).count());
    }

    @Test
    public void test_crud_2_async() throws Exception {
        List<Account> accounts = N.fill(Account.class, 11);

        cassandraExecutor.asyncBatchInsert(accounts, BatchStatement.Type.UNLOGGED).get();

        assertFalse(cassandraExecutor.asyncExists(Account.class, CF.eq("id", "111")).get());

        assertEquals(0, cassandraExecutor.asyncCount(Account.class, CF.eq("id", "111")).get().intValue());

        cassandraExecutor.asyncQuery(Account.class, CF.eq("id", "111")).get().println();

        cassandraExecutor.asyncBatchUpdate(accounts, BatchStatement.Type.LOGGED).get();
        cassandraExecutor.asyncQuery(NSC.selectFrom(Account.class).cql()).get().println();

        cassandraExecutor.asyncStream(NSC.selectFrom(Account.class).cql()).get().forEach(Fn.println());

        cassandraExecutor.asyncStream(Account.class, NSC.selectFrom(Account.class).cql()).get().forEach(Fn.println());

        Seq.of(cassandraExecutor.asyncList(NSC.selectFrom(Account.class).cql()).get()).foreach(Fn.println());

        Seq.of(cassandraExecutor.list(String.class, NSC.select("first_name").from(Account.class).cql())).foreach(Fn.println());

        Seq.of(cassandraExecutor.asyncList(String.class, NSC.select("first_name").from(Account.class).cql()).get()).foreach(Fn.println());

        Seq.of(cassandraExecutor.asyncList(Account.class, NSC.selectFrom(Account.class).cql()).get()).foreach(Fn.println());

        cassandraExecutor.asyncDelete(Account.class, CF.in("id", Seq.of(accounts).map(new Function<Account, String>() {
            @Override
            public String apply(Account t) {
                return t.getId();
            }
        })));

        assertEquals(0, cassandraExecutor.asyncStream(NSC.selectFrom(Account.class).cql()).get().count());
    }

    @Test
    public void test_query() {
        Account account = N.fill(Account.class);
        // create
        cassandraExecutor.insert(account);

        cassandraExecutor.query("select count(*) from account").println();

        // read
        Account dbAccount = cassandraExecutor.gett(Account.class, account.getId(), account.getGUI());
        assertEquals(account.getGUI(), dbAccount.getGUI());

        // update
        dbAccount.setFirstName("newFirstName");
        cassandraExecutor.update(dbAccount);

        dbAccount = cassandraExecutor.gett(Account.class, CF.eq("id", account.getId()).and(CF.eq("gui", account.getGUI())));
        assertEquals("newFirstName", dbAccount.getFirstName());

        // delete
        cassandraExecutor.delete(Account.class, CF.eq("id", account.getId()).and(CF.eq("gui", account.getGUI())));

        // check
        assertTrue(cassandraExecutor.get(Account.class, CF.eq("id", account.getId()).and(CF.eq("gui", account.getGUI()))).isEmpty());
    }

    @Test
    public void test_batch() {
        List<Account> accounts = N.fill(Account.class, 11);

        cassandraExecutor.batchInsert(accounts, BatchStatement.Type.UNLOGGED);

        cassandraExecutor.query(NSC.selectFrom(Account.class).cql()).println();

        cassandraExecutor.batchUpdate(accounts, BatchStatement.Type.LOGGED);
        cassandraExecutor.query(NSC.selectFrom(Account.class).cql()).println();

        cassandraExecutor.stream(NSC.selectFrom(Account.class).cql()).forEach(Fn.println());

        cassandraExecutor.stream(Account.class, NSC.selectFrom(Account.class).cql()).forEach(Fn.println());

        cassandraExecutor.delete(Account.class, CF.in("id", Seq.of(accounts).map(new Function<Account, String>() {
            @Override
            public String apply(Account t) {
                return t.getId();
            }
        })));

        assertEquals(0, cassandraExecutor.stream(NSC.selectFrom(Account.class).cql()).count());
    }

}
