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

import com.landawn.abacus.exception.AbacusException;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.PoolableWrapper;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Objectory;
import com.landawn.abacus.util.SQLParser;
import com.landawn.abacus.util.WD;

// TODO: Auto-generated Javadoc
/**
 * The Class NamedCQL.
 *
 * @author Haiyang Li
 * @since 0.8
 */
public final class NamedCQL {

    /** The Constant EVICT_TIME. */
    private static final int EVICT_TIME = 60 * 1000;

    /** The Constant LIVE_TIME. */
    private static final int LIVE_TIME = 24 * 60 * 60 * 1000;

    /** The Constant MAX_IDLE_TIME. */
    private static final int MAX_IDLE_TIME = 24 * 60 * 60 * 1000;

    /** The Constant namedCQLPrefixSet. */
    private static final Set<String> namedCQLPrefixSet = N.asSet(WD.INSERT, WD.SELECT, WD.UPDATE, WD.DELETE);

    /** The Constant pool. */
    private static final KeyedObjectPool<String, PoolableWrapper<NamedCQL>> pool = PoolFactory.createKeyedObjectPool(10000, EVICT_TIME);

    /** The Constant PREFIX_OF_NAMED_PARAMETER. */
    private static final String PREFIX_OF_NAMED_PARAMETER = ":";

    /** The Constant LEFT_OF_IBATIS_NAMED_PARAMETER. */
    private static final String LEFT_OF_IBATIS_NAMED_PARAMETER = "#{";

    /** The Constant RIGHT_OF_IBATIS_NAMED_PARAMETER. */
    private static final String RIGHT_OF_IBATIS_NAMED_PARAMETER = "}";

    /** The Constant PREFIX_OF_COUCHBASE_NAMED_PARAMETER. */
    private static final String PREFIX_OF_COUCHBASE_NAMED_PARAMETER = "$";

    /** The named CQL. */
    private final String namedCQL;

    /** The pure CQL. */
    private final String pureCQL;

    /** The couchbase pure CQL. */
    private String couchbasePureCQL;

    /** The named parameters. */
    private final Map<Integer, String> namedParameters;

    /** The couchbase named parameters. */
    private Map<Integer, String> couchbaseNamedParameters;

    /** The parameter count. */
    private final int parameterCount;

    /** The couchbase parameter count. */
    private int couchbaseParameterCount;

    /** The attrs. */
    private final Map<String, String> attrs;

    /**
     * Instantiates a new named CQL.
     *
     * @param cql
     * @param attrs
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private NamedCQL(String cql, Map<String, String> attrs) {
        this.namedCQL = cql.trim();
        this.namedParameters = new HashMap<>();
        this.attrs = N.isNullOrEmpty(attrs) ? (Map) new HashMap<>() : new HashMap<>(attrs);

        final List<String> words = SQLParser.parse(namedCQL);

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
                        throw new AbacusException("can't mix '?' and '#{propName}' in the same cql script");
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
            pureCQL = sb.toString();

            Objectory.recycle(sb);
        } else {
            pureCQL = cql;
            parameterCount = 0;
        }
    }

    /**
     *
     * @param cql
     * @param attrs
     * @return
     */
    public static NamedCQL parse(String cql, Map<String, String> attrs) {
        NamedCQL result = null;
        PoolableWrapper<NamedCQL> w = pool.get(cql);

        if ((w == null) || (w.value() == null)) {
            synchronized (pool) {
                result = new NamedCQL(cql, attrs);
                pool.put(cql, PoolableWrapper.of(result, LIVE_TIME, MAX_IDLE_TIME));
            }
        } else {
            result = w.value();
        }

        return result;
    }

    /**
     * Gets the named CQL.
     *
     * @return
     */
    public String getNamedCQL() {
        return namedCQL;
    }

    /**
     * Gets the pure CQL.
     *
     * @return
     */
    public String getPureCQL() {
        return pureCQL;
    }

    /**
     * Gets the pure CQL.
     *
     * @param isForCouchbase
     * @return
     */
    public String getPureCQL(boolean isForCouchbase) {
        if (isForCouchbase) {
            if (N.isNullOrEmpty(couchbasePureCQL)) {
                parseForCouchbase();
            }

            return couchbasePureCQL;
        } else {
            return pureCQL;
        }
    }

    /**
     * Gets the named parameters.
     *
     * @return
     */
    public Map<Integer, String> getNamedParameters() {
        return namedParameters;
    }

    /**
     * Gets the named parameters.
     *
     * @param isForCouchbase
     * @return
     */
    public Map<Integer, String> getNamedParameters(boolean isForCouchbase) {
        if (isForCouchbase) {
            if (N.isNullOrEmpty(couchbasePureCQL)) {
                parseForCouchbase();
            }

            return couchbaseNamedParameters;
        } else {
            return namedParameters;
        }
    }

    /**
     * Gets the attribes.
     *
     * @return
     */
    public Map<String, String> getAttribes() {
        return attrs;
    }

    /**
     * Gets the parameter count.
     *
     * @return
     */
    public int getParameterCount() {
        return parameterCount;
    }

    /**
     * Gets the parameter count.
     *
     * @param isForCouchbase
     * @return
     */
    public int getParameterCount(boolean isForCouchbase) {
        if (isForCouchbase) {
            if (N.isNullOrEmpty(couchbasePureCQL)) {
                parseForCouchbase();
            }

            return couchbaseParameterCount;
        } else {
            return parameterCount;
        }
    }

    /**
     * Parses the for couchbase.
     */
    private void parseForCouchbase() {
        this.couchbaseNamedParameters = new HashMap<>();

        final List<String> words = SQLParser.parse(namedCQL);

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
                    if (couchbaseNamedParameters.size() > 0) {
                        throw new AbacusException("can't mix '?' and '#{propName}' in the same cql script");
                    }

                    countOfParameter++;
                    word = PREFIX_OF_COUCHBASE_NAMED_PARAMETER + countOfParameter;
                } else if (word.startsWith(LEFT_OF_IBATIS_NAMED_PARAMETER) && word.endsWith(RIGHT_OF_IBATIS_NAMED_PARAMETER)) {
                    couchbaseNamedParameters.put(countOfParameter++, word.substring(2, word.length() - 1));

                    word = PREFIX_OF_COUCHBASE_NAMED_PARAMETER + countOfParameter;
                } else if (word.startsWith(PREFIX_OF_NAMED_PARAMETER) || word.startsWith(PREFIX_OF_COUCHBASE_NAMED_PARAMETER)) {
                    couchbaseNamedParameters.put(countOfParameter++, word.substring(1));

                    word = PREFIX_OF_COUCHBASE_NAMED_PARAMETER + countOfParameter;
                }

                sb.append(word);
            }

            boolean isNamedParametersByNum = true;

            for (int i = 0; i < countOfParameter; i++) {
                try {
                    if (N.parseInt(couchbaseNamedParameters.get(i)) != i + 1) {
                        isNamedParametersByNum = false;
                        break;
                    }
                } catch (Exception e) {
                    // ignore;
                    isNamedParametersByNum = false;
                    break;
                }
            }

            if (isNamedParametersByNum) {
                couchbaseNamedParameters.clear();
            }

            couchbaseParameterCount = countOfParameter;
            couchbasePureCQL = sb.toString();

            Objectory.recycle(sb);
        } else {
            couchbaseParameterCount = 0;
            couchbasePureCQL = namedCQL;
        }
    }

    /**
     *
     * @return
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + ((namedCQL == null) ? 0 : namedCQL.hashCode());

        return result;
    }

    /**
     *
     * @param obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof NamedCQL) {
            NamedCQL other = (NamedCQL) obj;

            return N.equals(namedCQL, other.namedCQL);
        }

        return false;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return "[NamedCQL] " + namedCQL + " [PureCQL] " + pureCQL + " [Attribues] " + attrs;
    }
}
