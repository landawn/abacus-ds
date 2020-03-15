package com.landawn.abacus.da;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.condition.ConditionFactory.CF;
import com.landawn.abacus.da.canssandra.CQLBuilder.PSC;
import com.landawn.abacus.util.N;

class HelloTest {

    @Test
    public void test_expr() {

        String cql = PSC.update(Account.class)
                .set(N.asProps("firstName", CF.expr("first_name + 'abc'")))
                .where(CF.eq("firstName", CF.expr("first_name + 'abc'")))
                .cql();

        N.println(cql);

        assertEquals("UPDATE account SET first_name = first_name + 'abc' WHERE first_name = first_name + 'abc'", cql);
    }

}
