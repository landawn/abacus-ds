/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import com.landawn.abacus.parser.JSONParser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.parser.XMLParser;

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

    static Account createAccount() {
        return TestUtil.createEntity(Account.class);
    }
}
