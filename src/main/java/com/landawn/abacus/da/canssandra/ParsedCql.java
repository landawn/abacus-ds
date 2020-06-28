/*
 * Copyright (C) 2015 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.da.canssandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.PoolableWrapper;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Objectory;
import com.landawn.abacus.util.SQLParser;
import com.landawn.abacus.util.WD;

/**
 *
 * @author Haiyang Li
 * @since 0.8
 */
public final class ParsedCql {

    /** The Constant EVICT_TIME. */
    private static final int EVICT_TIME = 60 * 1000;

    /** The Constant LIVE_TIME. */
    private static final int LIVE_TIME = 24 * 60 * 60 * 1000;

    /** The Constant MAX_IDLE_TIME. */
    private static final int MAX_IDLE_TIME = 24 * 60 * 60 * 1000;

    /** The Constant namedCQLPrefixSet. */
    private static final Set<String> namedCQLPrefixSet = N.asSet(WD.INSERT, WD.SELECT, WD.UPDATE, WD.DELETE);

    /** The Constant pool. */
    private static final KeyedObjectPool<String, PoolableWrapper<ParsedCql>> pool = PoolFactory.createKeyedObjectPool(10000, EVICT_TIME);

    /** The Constant PREFIX_OF_NAMED_PARAMETER. */
    private static final String PREFIX_OF_NAMED_PARAMETER = ":";

    /** The Constant LEFT_OF_IBATIS_NAMED_PARAMETER. */
    private static final String LEFT_OF_IBATIS_NAMED_PARAMETER = "#{";

    /** The Constant RIGHT_OF_IBATIS_NAMED_PARAMETER. */
    private static final String RIGHT_OF_IBATIS_NAMED_PARAMETER = "}";

    private final String cql;

    /** The pure CQL. */
    private final String parameterizedCql;

    /** The named parameters. */
    private final Map<Integer, String> namedParameters;

    /** The parameter count. */
    private final int parameterCount;

    /** The attrs. */
    private final Map<String, String> attrs;

    /** 
     *
     * @param cql
     * @param attrs
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private ParsedCql(String cql, Map<String, String> attrs) {
        this.cql = cql.trim();
        this.namedParameters = new HashMap<>();
        this.attrs = N.isNullOrEmpty(attrs) ? (Map) new HashMap<>() : new HashMap<>(attrs);

        final List<String> words = SQLParser.parse(cql);

        boolean isNamedCQLPrefix = false;
        for (String word : words) {
            if (N.notNullOrEmpty(word)) {
                isNamedCQLPrefix = namedCQLPrefixSet.contains(word.toUpperCase());
                break;
            }
        }

        if (isNamedCQLPrefix) {
            final StringBuilder sb = Objectory.createStringBuilder();
            int countOfParameter = 0;

            for (String word : words) {
                if (word.equals(WD.QUESTION_MARK)) {
                    if (namedParameters.size() > 0) {
                        throw new RuntimeException("can't mix '?' and '#{propName}' in the same cql script");
                    }

                    countOfParameter++;
                } else if (word.startsWith(LEFT_OF_IBATIS_NAMED_PARAMETER) && word.endsWith(RIGHT_OF_IBATIS_NAMED_PARAMETER)) {
                    namedParameters.put(countOfParameter++, word.substring(2, word.length() - 1));

                    word = WD.QUESTION_MARK;
                } else if (word.startsWith(PREFIX_OF_NAMED_PARAMETER)) {
                    namedParameters.put(countOfParameter++, word.substring(1));

                    word = WD.QUESTION_MARK;
                }

                sb.append(word);
            }

            parameterCount = countOfParameter;
            parameterizedCql = sb.toString();

            Objectory.recycle(sb);
        } else {
            parameterizedCql = cql;
            parameterCount = 0;
        }
    }

    /**
     *
     * @param cql
     * @param attrs
     * @return
     */
    public static ParsedCql parse(String cql, Map<String, String> attrs) {
        ParsedCql result = null;
        PoolableWrapper<ParsedCql> w = pool.get(cql);

        if ((w == null) || (w.value() == null)) {
            synchronized (pool) {
                result = new ParsedCql(cql, attrs);
                pool.put(cql, PoolableWrapper.of(result, LIVE_TIME, MAX_IDLE_TIME));
            }
        } else {
            result = w.value();
        }

        return result;
    }

    public String cql() {
        return cql;
    }

    public String getParameterizedCql() {
        return parameterizedCql;
    }

    public Map<Integer, String> getNamedParameters() {
        return namedParameters;
    }

    public int getParameterCount() {
        return parameterCount;
    }

    public Map<String, String> getAttribes() {
        return attrs;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + ((cql == null) ? 0 : cql.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof ParsedCql) {
            ParsedCql other = (ParsedCql) obj;

            return N.equals(cql, other.cql);
        }

        return false;
    }

    @Override
    public String toString() {
        return "[cql] " + cql + " [parameterizedCql] " + parameterizedCql + " [Attribues] " + attrs;
    }
}
