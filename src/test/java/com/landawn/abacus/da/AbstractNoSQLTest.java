/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da;

import com.landawn.abacus.parser.JSONParser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.parser.XMLParser;
import com.landawn.abacus.util.Hex;
import com.landawn.abacus.util.N;

import junit.framework.TestCase;

/**
 * 
 * @since 0.8
 * 
 * @author Haiyang Li
 */
public class AbstractNoSQLTest extends TestCase {
    static final JSONParser jsonParser = ParserFactory.createJSONParser();
    static final XMLParser xmlParser = ParserFactory.createXMLParser();

    protected Account createAccount() {
        Account account = N.fill(Account.class);
        account.setId(generateId());
        return account;
    }

    protected String generateId() {
        return Hex.encodeToString(N.uuid().getBytes()).substring(0, 24);
    }
}
