/*
 * Copyright (c) 2016, Haiyang Li.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.landawn.abacus.da.canssandra;

import static com.landawn.abacus.util.WD._PARENTHESES_L;
import static com.landawn.abacus.util.WD._PARENTHESES_R;
import static com.landawn.abacus.util.WD._SPACE;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.landawn.abacus.DirtyMarker;
import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.NonUpdatable;
import com.landawn.abacus.annotation.ReadOnly;
import com.landawn.abacus.annotation.ReadOnlyId;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.annotation.Transient;
import com.landawn.abacus.condition.Between;
import com.landawn.abacus.condition.Binary;
import com.landawn.abacus.condition.Cell;
import com.landawn.abacus.condition.Condition;
import com.landawn.abacus.condition.ConditionFactory.CF;
import com.landawn.abacus.condition.Expression;
import com.landawn.abacus.condition.In;
import com.landawn.abacus.condition.Junction;
import com.landawn.abacus.condition.SubQuery;
import com.landawn.abacus.core.DirtyMarkerUtil;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.EntityInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.Array;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.ImmutableSet;
import com.landawn.abacus.util.Maps;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.ObjectPool;
import com.landawn.abacus.util.Objectory;
import com.landawn.abacus.util.OperationType;
import com.landawn.abacus.util.Pair;
import com.landawn.abacus.util.SQLBuilder;
import com.landawn.abacus.util.SQLParser;
import com.landawn.abacus.util.SortDirection;
import com.landawn.abacus.util.Splitter;
import com.landawn.abacus.util.StringUtil;
import com.landawn.abacus.util.Throwables;
import com.landawn.abacus.util.WD;

// TODO: Auto-generated Javadoc
/**
 * It's easier to write/maintain the CQL by <code>CQLBuilder</code> and more efficient, comparing to write Cassandra CQL in plain text.
 * <br>The <code>cql()</code> or <code>pair()</code> method must be called to release resources.
 * <br />Here is a sample:
 * <p>
 * String cql = NE.insert("gui", "firstName", "lastName").into("account").cql();
 * <br />// CQL: INSERT INTO account (gui, first_name, last_name) VALUES (:gui, :firstName, :lastName)
 * </p>
 *
 * The {@code tableName} will NOT be formalized.
 * <li>{@code select(...).from(String tableName).where(...)}</li>
 * <li>{@code insert(...).into(String tableName).values(...)}</li>
 * <li>{@code update(String tableName).set(...).where(...)}</li>
 * <li>{@code deleteFrom(String tableName).where(...)}</li>
 *
 * <br />
 *
 * @author Haiyang Li
 * @since 0.8
 */
public abstract class CQLBuilder {

    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(CQLBuilder.class);

    /** The Constant commaSplitter. */
    private static final Splitter commaSplitter = Splitter.with(WD._COMMA).trim(true);

    /** The Constant spaceSplitter. */
    private static final Splitter spaceSplitter = Splitter.with(WD._COMMA).trim(true);

    /** The Constant DISTINCT. */
    public static final String DISTINCT = WD.DISTINCT;

    /** The Constant COUNT_ALL. */
    public static final String COUNT_ALL = "count(*)";

    /** The Constant POOL_SIZE. */
    static final int POOL_SIZE;

    static {
        int multi = (int) (Runtime.getRuntime().maxMemory() / ((1024 * 1024) * 256));

        POOL_SIZE = Math.max(1000, Math.min(1000 * multi, 8192));
    }

    /** The Constant entityTablePropColumnNameMap. */
    private static final Map<Class<?>, Map<NamingPolicy, Map<String, String>>> entityTablePropColumnNameMap = new ObjectPool<>(POOL_SIZE);

    /** The Constant defaultPropNamesPool. */
    private static final Map<Class<?>, Set<String>[]> defaultPropNamesPool = new ObjectPool<>(POOL_SIZE);

    /** The Constant classTableNameMap. */
    private static final Map<Class<?>, String[]> classTableNameMap = new ConcurrentHashMap<>();

    /** The Constant tableDeleteFrom. */
    private static final Map<String, char[]> tableDeleteFrom = new ConcurrentHashMap<>();

    /** The Constant activeStringBuilderCounter. */
    private static final AtomicInteger activeStringBuilderCounter = new AtomicInteger();

    /** The Constant _SPACE_USING_TIMESTAMP_SPACE. */
    static final char[] _SPACE_USING_TIMESTAMP_SPACE = " USING TIMESTAMP ".toCharArray();

    /** The Constant _SPACE_USING_TTL_SPACE. */
    static final char[] _SPACE_USING_TTL_SPACE = " USING TTL ".toCharArray();

    /** The Constant _SPACE_IF_SPACE. */
    static final char[] _SPACE_IF_SPACE = " IF ".toCharArray();

    /** The Constant _SPACE_IF_EXISTS. */
    static final char[] _SPACE_IF_EXISTS = " IF EXISTS".toCharArray();

    /** The Constant _SPACE_IF_NOT_EXISTS. */
    static final char[] _SPACE_IF_NOT_EXISTS = " IF NOT EXISTS".toCharArray();

    /** The Constant _SPACE_ALLOW_FILTERING. */
    static final char[] _SPACE_ALLOW_FILTERING = " ALLOW FILTERING".toCharArray();

    /** The Constant _INSERT. */
    static final char[] _INSERT = WD.INSERT.toCharArray();

    /** The Constant _SPACE_INSERT_SPACE. */
    static final char[] _SPACE_INSERT_SPACE = (WD.SPACE + WD.INSERT + WD.SPACE).toCharArray();

    /** The Constant _INTO. */
    static final char[] _INTO = WD.INTO.toCharArray();

    /** The Constant _SPACE_INTO_SPACE. */
    static final char[] _SPACE_INTO_SPACE = (WD.SPACE + WD.INTO + WD.SPACE).toCharArray();

    /** The Constant _VALUES. */
    static final char[] _VALUES = WD.VALUES.toCharArray();

    /** The Constant _SPACE_VALUES_SPACE. */
    static final char[] _SPACE_VALUES_SPACE = (WD.SPACE + WD.VALUES + WD.SPACE).toCharArray();

    /** The Constant _SELECT. */
    static final char[] _SELECT = WD.SELECT.toCharArray();

    /** The Constant _SPACE_SELECT_SPACE. */
    static final char[] _SPACE_SELECT_SPACE = (WD.SPACE + WD.SELECT + WD.SPACE).toCharArray();

    /** The Constant _FROM. */
    static final char[] _FROM = WD.FROM.toCharArray();

    /** The Constant _SPACE_FROM_SPACE. */
    static final char[] _SPACE_FROM_SPACE = (WD.SPACE + WD.FROM + WD.SPACE).toCharArray();

    /** The Constant _UPDATE. */
    static final char[] _UPDATE = WD.UPDATE.toCharArray();

    /** The Constant _SPACE_UPDATE_SPACE. */
    static final char[] _SPACE_UPDATE_SPACE = (WD.SPACE + WD.UPDATE + WD.SPACE).toCharArray();

    /** The Constant _SET. */
    static final char[] _SET = WD.SET.toCharArray();

    /** The Constant _SPACE_SET_SPACE. */
    static final char[] _SPACE_SET_SPACE = (WD.SPACE + WD.SET + WD.SPACE).toCharArray();

    /** The Constant _DELETE. */
    static final char[] _DELETE = WD.DELETE.toCharArray();

    /** The Constant _SPACE_DELETE_SPACE. */
    static final char[] _SPACE_DELETE_SPACE = (WD.SPACE + WD.DELETE + WD.SPACE).toCharArray();

    /** The Constant _USING. */
    static final char[] _USING = WD.USING.toCharArray();

    /** The Constant _SPACE_USING_SPACE. */
    static final char[] _SPACE_USING_SPACE = (WD.SPACE + WD.USING + WD.SPACE).toCharArray();

    /** The Constant _WHERE. */
    static final char[] _WHERE = WD.WHERE.toCharArray();

    /** The Constant _SPACE_WHERE_SPACE. */
    static final char[] _SPACE_WHERE_SPACE = (WD.SPACE + WD.WHERE + WD.SPACE).toCharArray();

    /** The Constant _GROUP_BY. */
    static final char[] _GROUP_BY = WD.GROUP_BY.toCharArray();

    /** The Constant _SPACE_GROUP_BY_SPACE. */
    static final char[] _SPACE_GROUP_BY_SPACE = (WD.SPACE + WD.GROUP_BY + WD.SPACE).toCharArray();

    /** The Constant _HAVING. */
    static final char[] _HAVING = WD.HAVING.toCharArray();

    /** The Constant _SPACE_HAVING_SPACE. */
    static final char[] _SPACE_HAVING_SPACE = (WD.SPACE + WD.HAVING + WD.SPACE).toCharArray();

    /** The Constant _ORDER_BY. */
    static final char[] _ORDER_BY = WD.ORDER_BY.toCharArray();

    /** The Constant _SPACE_ORDER_BY_SPACE. */
    static final char[] _SPACE_ORDER_BY_SPACE = (WD.SPACE + WD.ORDER_BY + WD.SPACE).toCharArray();

    /** The Constant _LIMIT. */
    static final char[] _LIMIT = (WD.SPACE + WD.LIMIT + WD.SPACE).toCharArray();

    /** The Constant _SPACE_LIMIT_SPACE. */
    static final char[] _SPACE_LIMIT_SPACE = (WD.SPACE + WD.LIMIT + WD.SPACE).toCharArray();

    /** The Constant _OFFSET. */
    static final char[] _OFFSET = WD.OFFSET.toCharArray();

    /** The Constant _SPACE_OFFSET_SPACE. */
    static final char[] _SPACE_OFFSET_SPACE = (WD.SPACE + WD.OFFSET + WD.SPACE).toCharArray();

    /** The Constant _AND. */
    static final char[] _AND = WD.AND.toCharArray();

    /** The Constant _SPACE_AND_SPACE. */
    static final char[] _SPACE_AND_SPACE = (WD.SPACE + WD.AND + WD.SPACE).toCharArray();

    /** The Constant _OR. */
    static final char[] _OR = WD.OR.toCharArray();

    /** The Constant _SPACE_OR_SPACE. */
    static final char[] _SPACE_OR_SPACE = (WD.SPACE + WD.OR + WD.SPACE).toCharArray();

    /** The Constant _AS. */
    static final char[] _AS = WD.AS.toCharArray();

    /** The Constant _SPACE_AS_SPACE. */
    static final char[] _SPACE_AS_SPACE = (WD.SPACE + WD.AS + WD.SPACE).toCharArray();

    /** The Constant _SPACE_EQUAL_SPACE. */
    static final char[] _SPACE_EQUAL_SPACE = (WD.SPACE + WD.EQUAL + WD.SPACE).toCharArray();

    /** The Constant _SPACE_FOR_UPDATE. */
    static final char[] _SPACE_FOR_UPDATE = (WD.SPACE + WD.FOR_UPDATE).toCharArray();

    /** The Constant _COMMA_SPACE. */
    static final char[] _COMMA_SPACE = WD.COMMA_SPACE.toCharArray();

    /** The Constant SPACE_AS_SPACE. */
    static final String SPACE_AS_SPACE = WD.SPACE + WD.AS + WD.SPACE;

    /** The naming policy. */
    private final NamingPolicy namingPolicy;

    /** The cql policy. */
    private final CQLPolicy cqlPolicy;

    /** The parameters. */
    private final List<Object> parameters = new ArrayList<>();

    /** The sb. */
    private StringBuilder sb;

    /** The op. */
    private OperationType op;

    /** The entity class. */
    private Class<?> entityClass;

    /** The table name. */
    private String tableName;

    /** The predicates. */
    private String predicates;

    /** The column names. */
    private String[] columnNames;

    /** The column name list. */
    private Collection<String> columnNameList;

    /** The column aliases. */
    private Map<String, String> columnAliases;

    /** The props. */
    private Map<String, Object> props;

    /** The props list. */
    private Collection<Map<String, Object>> propsList;

    /**
     * Instantiates a new CQL builder.
     *
     * @param namingPolicy
     * @param cqlPolicy
     */
    CQLBuilder(final NamingPolicy namingPolicy, final CQLPolicy cqlPolicy) {
        if (activeStringBuilderCounter.incrementAndGet() > 1024) {
            logger.error("Too many(" + activeStringBuilderCounter.get()
                    + ") StringBuilder instances are created in CQLBuilder. The method cql()/pair() must be called to release resources and close CQLBuilder");
        }

        this.sb = Objectory.createStringBuilder();

        this.namingPolicy = namingPolicy == null ? NamingPolicy.LOWER_CASE_WITH_UNDERSCORE : namingPolicy;
        this.cqlPolicy = cqlPolicy == null ? CQLPolicy.CQL : cqlPolicy;
    }

    /**
     * Gets the table name.
     *
     * @param entityClass
     * @param namingPolicy
     * @return
     */
    static String getTableName(final Class<?> entityClass, final NamingPolicy namingPolicy) {
        String[] entityTableNames = classTableNameMap.get(entityClass);

        if (entityTableNames == null) {
            if (entityClass.isAnnotationPresent(Table.class)) {
                entityTableNames = Array.repeat(entityClass.getAnnotation(Table.class).value(), 3);
            } else {
                try {
                    if (entityClass.isAnnotationPresent(javax.persistence.Table.class)) {
                        entityTableNames = Array.repeat(entityClass.getAnnotation(javax.persistence.Table.class).name(), 3);
                    }
                } catch (Throwable e) {
                    logger.warn("To support javax.persistence.Table/Column, please add dependence javax.persistence:persistence-api");
                }
            }

            if (entityTableNames == null) {
                final String simpleClassName = ClassUtil.getSimpleClassName(entityClass);
                entityTableNames = new String[] { ClassUtil.toLowerCaseWithUnderscore(simpleClassName), ClassUtil.toUpperCaseWithUnderscore(simpleClassName),
                        ClassUtil.toCamelCase(simpleClassName) };
            }

            classTableNameMap.put(entityClass, entityTableNames);
        }

        switch (namingPolicy) {
            case LOWER_CASE_WITH_UNDERSCORE:
                return entityTableNames[0];

            case UPPER_CASE_WITH_UNDERSCORE:
                return entityTableNames[1];

            default:
                return entityTableNames[2];
        }
    }

    /**
     * Gets the select prop names by class.
     *
     * @param entityClass
     * @param includeSubEntityProperties
     * @param excludedPropNames
     * @return
     */
    static Collection<String> getSelectPropNamesByClass(final Class<?> entityClass, final boolean includeSubEntityProperties,
            final Set<String> excludedPropNames) {
        final Collection<String>[] val = loadPropNamesByClass(entityClass);
        final Collection<String> propNames = includeSubEntityProperties ? val[0] : val[1];

        if (N.isNullOrEmpty(excludedPropNames)) {
            return propNames;
        } else {
            final List<String> tmp = new ArrayList<>(propNames);
            tmp.removeAll(excludedPropNames);
            return tmp;
        }
    }

    /**
     * Gets the insert prop names by class.
     *
     * @param entityClass
     * @param excludedPropNames
     * @return
     */
    static Collection<String> getInsertPropNamesByClass(final Class<?> entityClass, final Set<String> excludedPropNames) {
        final Collection<String>[] val = loadPropNamesByClass(entityClass);
        final Collection<String> propNames = val[2];

        if (N.isNullOrEmpty(excludedPropNames)) {
            return propNames;
        } else {
            final List<String> tmp = new ArrayList<>(propNames);
            tmp.removeAll(excludedPropNames);
            return tmp;
        }
    }

    /**
     * Gets the update prop names by class.
     *
     * @param entityClass
     * @param excludedPropNames
     * @return
     */
    static Collection<String> getUpdatePropNamesByClass(final Class<?> entityClass, final Set<String> excludedPropNames) {
        final Collection<String>[] val = loadPropNamesByClass(entityClass);
        final Collection<String> propNames = val[3];

        if (N.isNullOrEmpty(excludedPropNames)) {
            return propNames;
        } else {
            final List<String> tmp = new ArrayList<>(propNames);
            tmp.removeAll(excludedPropNames);
            return tmp;
        }
    }

    /**
     * Gets the delete prop names by class.
     *
     * @param entityClass
     * @param excludedPropNames
     * @return
     */
    private static Collection<String> getDeletePropNamesByClass(final Class<?> entityClass, final Set<String> excludedPropNames) {
        if (N.isNullOrEmpty(excludedPropNames)) {
            return N.emptyList();
        }

        final Collection<String>[] val = loadPropNamesByClass(entityClass);
        final Collection<String> propNames = val[0];

        if (N.isNullOrEmpty(excludedPropNames)) {
            return propNames;
        } else {
            final List<String> tmp = new ArrayList<>(propNames);
            tmp.removeAll(excludedPropNames);
            return tmp;
        }
    }

    /**
     * Load prop names by class.
     *
     * @param entityClass
     * @return
     */
    static Collection<String>[] loadPropNamesByClass(final Class<?> entityClass) {
        Set<String>[] val = defaultPropNamesPool.get(entityClass);

        if (val == null) {
            synchronized (entityClass) {
                final Set<String> entityPropNames = N.newLinkedHashSet(ClassUtil.getPropNameList(entityClass));
                final EntityInfo entityInfo = ParserUtil.getEntityInfo(entityClass);

                val = new Set[4];
                val[0] = N.newLinkedHashSet(entityPropNames);
                val[1] = N.newLinkedHashSet(entityPropNames);
                val[2] = N.newLinkedHashSet(entityPropNames);
                val[3] = N.newLinkedHashSet(entityPropNames);

                final Set<String> readOnlyPropNames = N.newHashSet();
                final Set<String> nonUpdatablePropNames = N.newHashSet();
                final Set<String> transientPropNames = N.newHashSet();

                for (PropInfo propInfo : entityInfo.propInfoList) {
                    if (propInfo.isAnnotationPresent(ReadOnly.class) || propInfo.isAnnotationPresent(ReadOnlyId.class)) {
                        readOnlyPropNames.add(propInfo.name);
                    }

                    if (propInfo.isAnnotationPresent(NonUpdatable.class)) {
                        nonUpdatablePropNames.add(propInfo.name);
                    }

                    if (propInfo.isAnnotationPresent(Transient.class) || (propInfo.field != null && Modifier.isTransient(propInfo.field.getModifiers()))) {
                        readOnlyPropNames.add(propInfo.name);
                        transientPropNames.add(propInfo.name);
                    }
                }

                nonUpdatablePropNames.addAll(readOnlyPropNames);

                val[0].removeAll(transientPropNames);
                val[1].removeAll(transientPropNames);
                val[2].removeAll(readOnlyPropNames);
                val[3].removeAll(nonUpdatablePropNames);

                val[0] = ImmutableSet.of(val[0]);
                val[1] = ImmutableSet.of(val[1]);
                val[2] = ImmutableSet.of(val[2]);
                val[3] = ImmutableSet.of(val[3]);

                defaultPropNamesPool.put(entityClass, val);
            }
        }

        return val;
    }

    /**
     *
     * @param propNames
     * @return
     */
    @Beta
    static Map<String, Expression> named(final String... propNames) {
        final Map<String, Expression> m = new LinkedHashMap<>(N.initHashCapacity(propNames.length));

        for (String propName : propNames) {
            m.put(propName, CF.QME);
        }

        return m;
    }

    /**
     *
     * @param propNames
     * @return
     */
    @Beta
    static Map<String, Expression> named(final Collection<String> propNames) {
        final Map<String, Expression> m = new LinkedHashMap<>(N.initHashCapacity(propNames.size()));

        for (String propName : propNames) {
            m.put(propName, CF.QME);
        }

        return m;
    }

    /**
     *
     * @param n
     * @return
     */
    public static String repeatQM(int n) {
        return SQLBuilder.repeatQM(n);
    }

    /**
     *
     * @param tableName
     * @return
     */
    public CQLBuilder into(final String tableName) {
        if (op != OperationType.ADD) {
            throw new RuntimeException("Invalid operation: " + op);
        }

        if (N.isNullOrEmpty(columnNames) && N.isNullOrEmpty(columnNameList) && N.isNullOrEmpty(props) && N.isNullOrEmpty(propsList)) {
            throw new RuntimeException("Column names or props must be set first by insert");
        }

        this.tableName = tableName;

        sb.append(_INSERT);
        sb.append(_SPACE_INTO_SPACE);

        sb.append(tableName);

        sb.append(WD._SPACE);
        sb.append(WD._PARENTHESES_L);

        final Map<String, String> propColumnNameMap = getPropColumnNameMap(entityClass, namingPolicy);

        if (N.notNullOrEmpty(columnNames)) {
            if (columnNames.length == 1 && columnNames[0].indexOf(WD._SPACE) > 0) {
                sb.append(columnNames[0]);
            } else {
                for (int i = 0, len = columnNames.length; i < len; i++) {
                    if (i > 0) {
                        sb.append(_COMMA_SPACE);
                    }

                    sb.append(formalizeColumnName(propColumnNameMap, columnNames[i]));
                }
            }
        } else if (N.notNullOrEmpty(columnNameList)) {
            int i = 0;
            for (String columnName : columnNameList) {
                if (i++ > 0) {
                    sb.append(_COMMA_SPACE);
                }

                sb.append(formalizeColumnName(propColumnNameMap, columnName));
            }
        } else {
            final Map<String, Object> props = N.isNullOrEmpty(this.props) ? propsList.iterator().next() : this.props;

            int i = 0;
            for (String columnName : props.keySet()) {
                if (i++ > 0) {
                    sb.append(_COMMA_SPACE);
                }

                sb.append(formalizeColumnName(propColumnNameMap, columnName));
            }
        }

        sb.append(WD._PARENTHESES_R);

        sb.append(_SPACE_VALUES_SPACE);

        sb.append(WD._PARENTHESES_L);

        if (N.notNullOrEmpty(columnNames)) {
            switch (cqlPolicy) {
                case CQL:
                case PARAMETERIZED_CQL: {
                    for (int i = 0, len = columnNames.length; i < len; i++) {
                        if (i > 0) {
                            sb.append(_COMMA_SPACE);
                        }

                        sb.append(WD._QUESTION_MARK);
                    }

                    break;
                }

                case NAMED_CQL: {
                    for (int i = 0, len = columnNames.length; i < len; i++) {
                        if (i > 0) {
                            sb.append(_COMMA_SPACE);
                        }

                        sb.append(":");
                        sb.append(columnNames[i]);
                    }

                    break;
                }

                default:
                    throw new RuntimeException("Not supported CQL policy: " + cqlPolicy);
            }
        } else if (N.notNullOrEmpty(columnNameList)) {
            switch (cqlPolicy) {
                case CQL:
                case PARAMETERIZED_CQL: {
                    for (int i = 0, size = columnNameList.size(); i < size; i++) {
                        if (i > 0) {
                            sb.append(_COMMA_SPACE);
                        }

                        sb.append(WD._QUESTION_MARK);
                    }

                    break;
                }

                case NAMED_CQL: {
                    int i = 0;
                    for (String columnName : columnNameList) {
                        if (i++ > 0) {
                            sb.append(_COMMA_SPACE);
                        }

                        sb.append(":");
                        sb.append(columnName);
                    }

                    break;
                }

                default:
                    throw new RuntimeException("Not supported CQL policy: " + cqlPolicy);
            }
        } else if (N.notNullOrEmpty(props)) {
            appendInsertProps(props);
        } else {
            int i = 0;
            for (Map<String, Object> props : propsList) {
                if (i++ > 0) {
                    sb.append(WD._PARENTHESES_R);
                    sb.append(_COMMA_SPACE);
                    sb.append(WD._PARENTHESES_L);
                }

                appendInsertProps(props);
            }
        }

        sb.append(WD._PARENTHESES_R);

        return this;
    }

    /**
     *
     * @param entityClass
     * @return
     */
    public CQLBuilder into(final Class<?> entityClass) {
        this.entityClass = entityClass;

        return into(getTableName(entityClass, namingPolicy));
    }

    /**
     *
     * @param expr
     * @return
     */
    public CQLBuilder from(String expr) {
        expr = expr.trim();
        String tableName = expr.indexOf(WD._COMMA) > 0 ? commaSplitter.split(expr).get(0) : expr;

        if (tableName.indexOf(WD.SPACE) > 0) {
            tableName = spaceSplitter.split(tableName).get(0);
        }

        return from(tableName, expr);
    }

    /**
     *
     * @param tableNames
     * @return
     */
    @SafeVarargs
    public final CQLBuilder from(final String... tableNames) {
        if (tableNames.length == 1) {
            return from(tableNames[0]);
        } else {
            String tableName = tableNames[0].trim();

            if (tableName.indexOf(WD.SPACE) > 0) {
                tableName = spaceSplitter.split(tableName).get(0);
            }

            return from(tableName, StringUtil.join(tableNames, WD.COMMA_SPACE));
        }
    }

    /**
     *
     * @param tableNames
     * @return
     */
    public CQLBuilder from(final Collection<String> tableNames) {
        String tableName = tableNames.iterator().next().trim();

        if (tableName.indexOf(WD.SPACE) > 0) {
            tableName = spaceSplitter.split(tableName).get(0);
        }

        return from(tableName, StringUtil.join(tableNames, WD.SPACE));
    }

    /**
     *
     * @param tableAliases
     * @return
     */
    public CQLBuilder from(final Map<String, String> tableAliases) {
        String tableName = tableAliases.keySet().iterator().next().trim();

        if (tableName.indexOf(WD.SPACE) > 0) {
            tableName = spaceSplitter.split(tableName).get(0);
        }

        String expr = "";

        int i = 0;
        for (Map.Entry<String, String> entry : tableAliases.entrySet()) {
            if (i++ > 0) {
                expr += WD.COMMA_SPACE;
            }

            expr += (entry.getKey() + " " + entry.getValue());
        }

        return from(tableName, expr);
    }

    /**
     *
     * @param tableName
     * @param fromCause
     * @return
     */
    private CQLBuilder from(final String tableName, final String fromCause) {
        if (op != OperationType.QUERY && op != OperationType.DELETE) {
            throw new RuntimeException("Invalid operation: " + op);
        }

        if (op == OperationType.QUERY && N.isNullOrEmpty(columnNames) && N.isNullOrEmpty(columnNameList) && N.isNullOrEmpty(columnAliases)) {
            throw new RuntimeException("Column names or props must be set first by select");
        }

        this.tableName = tableName;

        sb.append(op == OperationType.QUERY ? _SELECT : _DELETE);
        sb.append(WD._SPACE);

        if (N.notNullOrEmpty(predicates)) {
            sb.append(predicates);
            sb.append(WD._SPACE);
        }

        final Map<String, String> propColumnNameMap = getPropColumnNameMap(entityClass, namingPolicy);

        if (N.notNullOrEmpty(columnNames)) {
            if (columnNames.length == 1) {
                final String columnName = StringUtil.trim(columnNames[0]);
                int idx = columnName.indexOf(' ');

                if (idx < 0) {
                    idx = columnName.indexOf(',');
                }

                if (idx > 0) {
                    sb.append(columnName);
                } else {
                    sb.append(formalizeColumnName(propColumnNameMap, columnName));

                    if (op == OperationType.QUERY && namingPolicy != NamingPolicy.LOWER_CAMEL_CASE && !WD.ASTERISK.equals(columnName)) {
                        sb.append(_SPACE_AS_SPACE);

                        sb.append(WD._QUOTATION_D);
                        sb.append(columnName);
                        sb.append(WD._QUOTATION_D);
                    }
                }
            } else {
                String columnName = null;

                for (int i = 0, len = columnNames.length; i < len; i++) {
                    columnName = StringUtil.trim(columnNames[i]);

                    if (i > 0) {
                        sb.append(_COMMA_SPACE);
                    }

                    int idx = columnName.indexOf(' ');

                    if (idx > 0) {
                        int idx2 = columnName.indexOf(" AS ", idx);

                        if (idx2 < 0) {
                            idx2 = columnName.indexOf(" as ", idx);
                        }

                        sb.append(formalizeColumnName(propColumnNameMap, columnName.substring(0, idx).trim()));

                        sb.append(_SPACE_AS_SPACE);

                        sb.append(WD._QUOTATION_D);
                        sb.append(columnName.substring(idx2 > 0 ? idx2 + 4 : idx + 1).trim());
                        sb.append(WD._QUOTATION_D);
                    } else {
                        sb.append(formalizeColumnName(propColumnNameMap, columnName));

                        if (op == OperationType.QUERY && namingPolicy != NamingPolicy.LOWER_CAMEL_CASE && !WD.ASTERISK.equals(columnName)) {
                            sb.append(_SPACE_AS_SPACE);

                            sb.append(WD._QUOTATION_D);
                            sb.append(columnName);
                            sb.append(WD._QUOTATION_D);
                        }
                    }
                }
            }
        } else if (N.notNullOrEmpty(columnNameList)) {
            int i = 0;
            for (String columnName : columnNameList) {
                if (i++ > 0) {
                    sb.append(_COMMA_SPACE);
                }

                sb.append(formalizeColumnName(propColumnNameMap, columnName));

                if (op == OperationType.QUERY && namingPolicy != NamingPolicy.LOWER_CAMEL_CASE && !WD.ASTERISK.equals(columnName)) {
                    sb.append(_SPACE_AS_SPACE);

                    sb.append(WD._QUOTATION_D);
                    sb.append(columnName);
                    sb.append(WD._QUOTATION_D);
                }
            }
        } else if (N.notNullOrEmpty(columnAliases)) {
            int i = 0;
            for (Map.Entry<String, String> entry : columnAliases.entrySet()) {
                if (i++ > 0) {
                    sb.append(_COMMA_SPACE);
                }

                sb.append(formalizeColumnName(propColumnNameMap, entry.getKey()));

                if (N.notNullOrEmpty(entry.getValue())) {
                    sb.append(_SPACE_AS_SPACE);

                    sb.append(WD._QUOTATION_D);
                    sb.append(entry.getValue());
                    sb.append(WD._QUOTATION_D);
                }
            }
        }

        if (sb.charAt(sb.length() - 1) == ' ') {
            sb.setLength(sb.length() - 1);
        }

        sb.append(_SPACE_FROM_SPACE);

        sb.append(fromCause);

        return this;
    }

    /**
     *
     * @param entityClass
     * @return
     */
    public CQLBuilder from(final Class<?> entityClass) {
        this.entityClass = entityClass;

        return from(getTableName(entityClass, namingPolicy));
    }

    /**
     *
     * @param expr
     * @return
     */
    public CQLBuilder where(final String expr) {
        init(true);

        sb.append(_SPACE_WHERE_SPACE);

        appendStringExpr(expr);

        return this;
    }

    /**
     *
     * @param cond any literal written in <code>Expression</code> condition won't be formalized
     * @return
     */
    public CQLBuilder where(final Condition cond) {
        init(true);

        sb.append(_SPACE_WHERE_SPACE);

        appendCondition(cond);

        return this;
    }

    /**
     * Append string expr.
     *
     * @param expr
     */
    private void appendStringExpr(final String expr) {
        final Map<String, String> propColumnNameMap = getPropColumnNameMap(entityClass, namingPolicy);
        final List<String> words = SQLParser.parse(expr);

        String word = null;
        for (int i = 0, len = words.size(); i < len; i++) {
            word = words.get(i);

            if (!StringUtil.isAsciiAlpha(word.charAt(0))) {
                sb.append(word);
            } else if (SQLParser.isFunctionName(words, len, i)) {
                sb.append(word);
            } else {
                sb.append(formalizeColumnName(propColumnNameMap, word));
            }
        }
    }

    /**
     *
     * @param expr
     * @return
     */
    public CQLBuilder orderBy(final String expr) {
        sb.append(_SPACE_ORDER_BY_SPACE);

        if (expr.indexOf(WD._SPACE) > 0) {
            // sb.append(columnNames[0]);
            appendStringExpr(expr);
        } else {
            sb.append(formalizeColumnName(expr));
        }

        return this;
    }

    /**
     *
     * @param columnNames
     * @return
     */
    @SafeVarargs
    public final CQLBuilder orderBy(final String... columnNames) {
        sb.append(_SPACE_ORDER_BY_SPACE);

        if (columnNames.length == 1) {
            if (columnNames[0].indexOf(WD._SPACE) > 0) {
                // sb.append(columnNames[0]);
                appendStringExpr(columnNames[0]);
            } else {
                sb.append(formalizeColumnName(columnNames[0]));
            }
        } else {
            final Map<String, String> propColumnNameMap = getPropColumnNameMap(entityClass, namingPolicy);

            for (int i = 0, len = columnNames.length; i < len; i++) {
                if (i > 0) {
                    sb.append(_COMMA_SPACE);
                }

                sb.append(formalizeColumnName(propColumnNameMap, columnNames[i]));
            }
        }

        return this;
    }

    /**
     *
     * @param columnName
     * @param direction
     * @return
     */
    public CQLBuilder orderBy(final String columnName, final SortDirection direction) {
        orderBy(columnName);

        sb.append(WD._SPACE);
        sb.append(direction.toString());

        return this;
    }

    /**
     *
     * @param columnNames
     * @return
     */
    public CQLBuilder orderBy(final Collection<String> columnNames) {
        sb.append(_SPACE_ORDER_BY_SPACE);

        final Map<String, String> propColumnNameMap = getPropColumnNameMap(entityClass, namingPolicy);
        int i = 0;
        for (String columnName : columnNames) {
            if (i++ > 0) {
                sb.append(_COMMA_SPACE);
            }

            sb.append(formalizeColumnName(propColumnNameMap, columnName));
        }

        return this;
    }

    /**
     *
     * @param columnNames
     * @param direction
     * @return
     */
    public CQLBuilder orderBy(final Collection<String> columnNames, final SortDirection direction) {
        orderBy(columnNames);

        sb.append(WD._SPACE);
        sb.append(direction.toString());

        return this;
    }

    /**
     *
     * @param orders
     * @return
     */
    public CQLBuilder orderBy(final Map<String, SortDirection> orders) {
        sb.append(_SPACE_ORDER_BY_SPACE);

        final Map<String, String> propColumnNameMap = getPropColumnNameMap(entityClass, namingPolicy);
        int i = 0;
        for (Map.Entry<String, SortDirection> entry : orders.entrySet()) {
            if (i++ > 0) {
                sb.append(_COMMA_SPACE);
            }

            sb.append(formalizeColumnName(propColumnNameMap, entry.getKey()));

            sb.append(WD._SPACE);
            sb.append(entry.getValue().toString());
        }

        return this;
    }

    /**
     *
     * @param count
     * @return
     */
    public CQLBuilder limit(final int count) {
        sb.append(_SPACE_LIMIT_SPACE);

        sb.append(count);

        return this;
    }

    /**
     *
     * @param expr
     * @return
     */
    public CQLBuilder set(final String expr) {
        return set(N.asArray(expr));
    }

    /**
     *
     * @param columnNames
     * @return
     */
    @SafeVarargs
    public final CQLBuilder set(final String... columnNames) {
        init(false);

        sb.append(_SPACE_SET_SPACE);

        if (columnNames.length == 1 && SQLParser.parse(columnNames[0]).contains(WD.EQUAL)) {
            appendStringExpr(columnNames[0]);
        } else {
            final Map<String, String> propColumnNameMap = getPropColumnNameMap(entityClass, namingPolicy);

            switch (cqlPolicy) {
                case CQL:
                case PARAMETERIZED_CQL: {
                    for (int i = 0, len = columnNames.length; i < len; i++) {
                        if (i > 0) {
                            sb.append(_COMMA_SPACE);
                        }

                        sb.append(formalizeColumnName(propColumnNameMap, columnNames[i]));

                        sb.append(_SPACE_EQUAL_SPACE);

                        sb.append(WD._QUESTION_MARK);
                    }

                    break;
                }

                case NAMED_CQL: {
                    for (int i = 0, len = columnNames.length; i < len; i++) {
                        if (i > 0) {
                            sb.append(_COMMA_SPACE);
                        }

                        sb.append(formalizeColumnName(propColumnNameMap, columnNames[i]));

                        sb.append(_SPACE_EQUAL_SPACE);

                        sb.append(":");
                        sb.append(columnNames[i]);
                    }

                    break;
                }

                default:
                    throw new RuntimeException("Not supported CQL policy: " + cqlPolicy);
            }
        }

        this.columnNameList = null;

        return this;
    }

    /**
     *
     * @param columnNames
     * @return
     */
    public CQLBuilder set(final Collection<String> columnNames) {
        init(false);

        sb.append(_SPACE_SET_SPACE);

        final Map<String, String> propColumnNameMap = getPropColumnNameMap(entityClass, namingPolicy);

        switch (cqlPolicy) {
            case CQL:
            case PARAMETERIZED_CQL: {
                int i = 0;
                for (String columnName : columnNames) {
                    if (i++ > 0) {
                        sb.append(_COMMA_SPACE);
                    }

                    sb.append(formalizeColumnName(propColumnNameMap, columnName));

                    sb.append(_SPACE_EQUAL_SPACE);

                    sb.append(WD._QUESTION_MARK);
                }

                break;
            }

            case NAMED_CQL: {
                int i = 0;
                for (String columnName : columnNames) {
                    if (i++ > 0) {
                        sb.append(_COMMA_SPACE);
                    }

                    sb.append(formalizeColumnName(propColumnNameMap, columnName));

                    sb.append(_SPACE_EQUAL_SPACE);

                    sb.append(":");
                    sb.append(columnName);
                }

                break;
            }

            default:
                throw new RuntimeException("Not supported CQL policy: " + cqlPolicy);
        }

        this.columnNameList = null;

        return this;
    }

    /**
     *
     * @param props
     * @return
     */
    public CQLBuilder set(final Map<String, Object> props) {
        init(false);

        sb.append(_SPACE_SET_SPACE);

        final Map<String, String> propColumnNameMap = getPropColumnNameMap(entityClass, namingPolicy);

        switch (cqlPolicy) {
            case CQL: {
                int i = 0;
                for (Map.Entry<String, Object> entry : props.entrySet()) {
                    if (i++ > 0) {
                        sb.append(_COMMA_SPACE);
                    }

                    sb.append(formalizeColumnName(propColumnNameMap, entry.getKey()));

                    sb.append(_SPACE_EQUAL_SPACE);

                    setParameterForCQL(entry.getValue());
                }

                break;
            }

            case PARAMETERIZED_CQL: {
                int i = 0;
                for (Map.Entry<String, Object> entry : props.entrySet()) {
                    if (i++ > 0) {
                        sb.append(_COMMA_SPACE);
                    }

                    sb.append(formalizeColumnName(propColumnNameMap, entry.getKey()));

                    sb.append(_SPACE_EQUAL_SPACE);

                    setParameterForRawCQL(entry.getValue());
                }

                break;
            }

            case NAMED_CQL: {
                int i = 0;
                for (Map.Entry<String, Object> entry : props.entrySet()) {
                    if (i++ > 0) {
                        sb.append(_COMMA_SPACE);
                    }

                    sb.append(formalizeColumnName(propColumnNameMap, entry.getKey()));

                    sb.append(_SPACE_EQUAL_SPACE);

                    setParameterForNamedCQL(entry.getKey(), entry.getValue());
                }

                break;
            }
            default:
                throw new RuntimeException("Not supported CQL policy: " + cqlPolicy);
        }

        this.columnNameList = null;

        return this;
    }

    /**
     * Only the dirty properties will be set into the result CQL if the specified entity is a dirty marker entity.
     *
     * @param entity
     * @return
     */
    public CQLBuilder set(final Object entity) {
        return set(entity, null);
    }

    /**
     * Only the dirty properties will be set into the result SQL if the specified entity is a dirty marker entity.
     *
     * @param entity
     * @param excludedPropNames
     * @return
     */
    @SuppressWarnings("deprecation")
    public CQLBuilder set(final Object entity, final Set<String> excludedPropNames) {
        if (entity instanceof String) {
            return set(N.asArray((String) entity));
        } else if (entity instanceof Map) {
            if (N.isNullOrEmpty(excludedPropNames)) {
                return set((Map<String, Object>) entity);
            } else {
                final Map<String, Object> props = new LinkedHashMap<>((Map<String, Object>) entity);
                Maps.removeKeys(props, excludedPropNames);
                return set(props);
            }
        } else {
            final Class<?> entityClass = entity.getClass();
            this.entityClass = entityClass;
            final Collection<String> propNames = getUpdatePropNamesByClass(entityClass, excludedPropNames);
            final Set<String> dirtyPropNames = DirtyMarkerUtil.isDirtyMarker(entityClass) ? ((DirtyMarker) entity).dirtyPropNames() : null;
            final Map<String, Object> props = N.newHashMap(N.initHashCapacity(N.isNullOrEmpty(dirtyPropNames) ? propNames.size() : dirtyPropNames.size()));
            final EntityInfo entityInfo = ParserUtil.getEntityInfo(entityClass);

            for (String propName : propNames) {
                if (dirtyPropNames == null || dirtyPropNames.contains(propName)) {
                    props.put(propName, entityInfo.getPropValue(entity, propName));
                }
            }

            return set(props);
        }
    }

    /**
     *
     * @param entityClass
     * @return
     */
    public CQLBuilder set(Class<?> entityClass) {
        return set(entityClass, null);
    }

    /**
     *
     * @param entityClass
     * @param excludedPropNames
     * @return
     */
    public CQLBuilder set(Class<?> entityClass, final Set<String> excludedPropNames) {
        this.entityClass = entityClass;

        return set(getUpdatePropNamesByClass(entityClass, excludedPropNames));
    }

    /**
     *
     * @param options
     * @return
     */
    CQLBuilder using(String... options) {
        init(false);

        sb.append(_SPACE_USING_SPACE);

        for (int i = 0, len = options.length; i < len; i++) {
            if (i > 0) {
                sb.append(_SPACE_AND_SPACE);
            }

            sb.append(options[i]);
        }

        return this;
    }

    /**
     *
     * @param timestamp
     * @return
     */
    public CQLBuilder usingTTL(long timestamp) {
        return usingTTL(String.valueOf(timestamp));
    }

    /**
     *
     * @param timestamp
     * @return
     */
    public CQLBuilder usingTTL(String timestamp) {
        init(false);

        sb.append(_SPACE_USING_TTL_SPACE);
        sb.append(timestamp);

        return this;
    }

    /**
     *
     * @param timestamp
     * @return
     */
    public CQLBuilder usingTimestamp(Date timestamp) {
        return usingTimestamp(timestamp.getTime());
    }

    /**
     *
     * @param timestamp
     * @return
     */
    public CQLBuilder usingTimestamp(long timestamp) {
        return usingTimestamp(String.valueOf(timestamp));
    }

    /**
     *
     * @param timestamp
     * @return
     */
    public CQLBuilder usingTimestamp(String timestamp) {
        init(false);

        sb.append(_SPACE_USING_TIMESTAMP_SPACE);
        sb.append(timestamp);

        return this;
    }

    /**
     *
     * @param expr
     * @return
     */
    public CQLBuilder iF(final String expr) {
        init(true);

        sb.append(_SPACE_IF_SPACE);

        appendStringExpr(expr);

        return this;
    }

    /**
     *
     * @param cond any literal written in <code>Expression</code> condition won't be formalized
     * @return
     */
    public CQLBuilder iF(final Condition cond) {
        init(true);

        sb.append(_SPACE_IF_SPACE);

        appendCondition(cond);

        return this;
    }

    /**
     *
     * @return
     */
    public CQLBuilder ifExists() {
        init(true);

        sb.append(_SPACE_IF_EXISTS);

        return this;
    }

    /**
     * If not exists.
     *
     * @return
     */
    public CQLBuilder ifNotExists() {
        init(true);

        sb.append(_SPACE_IF_NOT_EXISTS);

        return this;
    }

    /**
     *
     * @return
     */
    public CQLBuilder allowFiltering() {
        init(true);

        sb.append(_SPACE_ALLOW_FILTERING);

        return this;
    }

    /**
     * This CQLBuilder will be closed after <code>cql()</code> is called.
     *
     * @return
     */
    public String cql() {
        if (sb == null) {
            throw new RuntimeException("This CQLBuilder has been closed after cql() was called previously");
        }

        init(true);

        String cql = null;

        try {
            cql = sb.toString();
        } finally {
            Objectory.recycle(sb);
            sb = null;

            activeStringBuilderCounter.decrementAndGet();
        }

        // N.println(cql);

        if (logger.isDebugEnabled()) {
            logger.debug(cql);
        }

        return cql;
    }

    /**
     *
     * @return
     */
    public List<Object> parameters() {
        return parameters;
    }

    /**
     *  This CQLBuilder will be closed after <code>pair()</code> is called.
     *
     * @return
     */
    public CP pair() {
        return new CP(cql(), parameters);
    }

    /**
     *
     * @param setForUpdate
     */
    void init(boolean setForUpdate) {
        if (sb.length() > 0) {

            if (op == OperationType.UPDATE && setForUpdate && N.notNullOrEmpty(columnNameList)) {
                set(columnNameList);
            }

            return;
        }

        if (op == OperationType.UPDATE) {
            sb.append(_UPDATE);

            sb.append(WD._SPACE);
            sb.append(tableName);

            if (setForUpdate && N.notNullOrEmpty(columnNameList)) {
                set(columnNameList);
            }
        } else if (op == OperationType.DELETE) {
            char[] deleteFromTableChars = tableDeleteFrom.get(tableName);

            if (deleteFromTableChars == null) {
                deleteFromTableChars = (WD.DELETE + WD.SPACE + WD.FROM + WD.SPACE + tableName).toCharArray();
                tableDeleteFrom.put(tableName, deleteFromTableChars);
            }

            sb.append(deleteFromTableChars);
        }
    }

    /**
     * Sets the parameter for CQL.
     *
     * @param propValue the new parameter for CQL
     */
    private void setParameterForCQL(final Object propValue) {
        if (CF.QME.equals(propValue)) {
            sb.append(WD._QUESTION_MARK);
        } else if (propValue instanceof Condition) {
            appendCondition((Condition) propValue);
        } else {
            sb.append(Expression.formalize(propValue));
        }
    }

    /**
     * Sets the parameter for raw CQL.
     *
     * @param propValue the new parameter for raw CQL
     */
    private void setParameterForRawCQL(final Object propValue) {
        if (CF.QME.equals(propValue)) {
            sb.append(WD._QUESTION_MARK);
        } else if (propValue instanceof Condition) {
            appendCondition((Condition) propValue);
        } else {
            sb.append(WD._QUESTION_MARK);

            parameters.add(propValue);
        }
    }

    /**
     * Sets the parameter for named CQL.
     *
     * @param propName
     * @param propValue
     */
    private void setParameterForNamedCQL(final String propName, final Object propValue) {
        if (CF.QME.equals(propValue)) {
            sb.append(":");
            sb.append(propName);
        } else if (propValue instanceof Condition) {
            appendCondition((Condition) propValue);
        } else {
            sb.append(":");
            sb.append(propName);

            parameters.add(propValue);
        }
    }

    /**
     * Sets the parameter.
     *
     * @param propName
     * @param propValue
     */
    private void setParameter(final String propName, final Object propValue) {
        switch (cqlPolicy) {
            case CQL: {
                setParameterForCQL(propValue);

                break;
            }

            case PARAMETERIZED_CQL: {
                setParameterForRawCQL(propValue);

                break;
            }

            case NAMED_CQL: {
                setParameterForNamedCQL(propName, propValue);

                break;
            }

            default:
                throw new RuntimeException("Not supported CQL policy: " + cqlPolicy);
        }
    }

    /**
     * Append insert props.
     *
     * @param props
     */
    private void appendInsertProps(final Map<String, Object> props) {
        switch (cqlPolicy) {
            case CQL: {
                int i = 0;
                Object propValue = null;
                for (String propName : props.keySet()) {
                    if (i++ > 0) {
                        sb.append(_COMMA_SPACE);
                    }

                    propValue = props.get(propName);

                    setParameterForCQL(propValue);
                }

                break;
            }

            case PARAMETERIZED_CQL: {
                int i = 0;
                Object propValue = null;
                for (String propName : props.keySet()) {
                    if (i++ > 0) {
                        sb.append(_COMMA_SPACE);
                    }

                    propValue = props.get(propName);

                    setParameterForRawCQL(propValue);
                }

                break;
            }

            case NAMED_CQL: {
                int i = 0;
                Object propValue = null;
                for (String propName : props.keySet()) {
                    if (i++ > 0) {
                        sb.append(_COMMA_SPACE);
                    }

                    propValue = props.get(propName);

                    setParameterForNamedCQL(propName, propValue);
                }

                break;
            }

            default:
                throw new RuntimeException("Not supported CQL policy: " + cqlPolicy);
        }
    }

    /**
     *
     * @param cond
     */
    private void appendCondition(final Condition cond) {
        if (cond instanceof Binary) {
            final Binary binary = (Binary) cond;
            final String propName = binary.getPropName();

            sb.append(formalizeColumnName(propName));

            sb.append(WD._SPACE);
            sb.append(binary.getOperator().toString());
            sb.append(WD._SPACE);

            Object propValue = binary.getPropValue();
            setParameter(propName, propValue);
        } else if (cond instanceof Between) {
            final Between bt = (Between) cond;
            final String propName = bt.getPropName();

            sb.append(formalizeColumnName(propName));

            sb.append(WD._SPACE);
            sb.append(bt.getOperator().toString());
            sb.append(WD._SPACE);

            Object minValue = bt.getMinValue();
            if (cqlPolicy == CQLPolicy.NAMED_CQL) {
                setParameter("min" + StringUtil.capitalize(propName), minValue);
            } else {
                setParameter(propName, minValue);
            }

            sb.append(WD._SPACE);
            sb.append(WD.AND);
            sb.append(WD._SPACE);

            Object maxValue = bt.getMaxValue();
            if (cqlPolicy == CQLPolicy.NAMED_CQL) {
                setParameter("max" + StringUtil.capitalize(propName), maxValue);
            } else {
                setParameter(propName, maxValue);
            }
        } else if (cond instanceof In) {
            final In in = (In) cond;
            final String propName = in.getPropName();
            final List<Object> parameters = in.getParameters();

            sb.append(formalizeColumnName(propName));

            sb.append(WD._SPACE);
            sb.append(in.getOperator().toString());
            sb.append(WD.SPACE_PARENTHESES_L);

            for (int i = 0, len = parameters.size(); i < len; i++) {
                if (i > 0) {
                    sb.append(WD.COMMA_SPACE);
                }

                if (cqlPolicy == CQLPolicy.NAMED_CQL) {
                    setParameter(propName + (i + 1), parameters.get(i));
                } else {
                    setParameter(propName, parameters.get(i));
                }
            }

            sb.append(WD._PARENTHESES_R);
        } else if (cond instanceof Cell) {
            final Cell cell = (Cell) cond;

            sb.append(WD._SPACE);
            sb.append(cell.getOperator().toString());
            sb.append(WD._SPACE);

            sb.append(_PARENTHESES_L);
            appendCondition(cell.getCondition());
            sb.append(_PARENTHESES_R);
        } else if (cond instanceof Junction) {
            final Junction junction = (Junction) cond;
            final List<Condition> conditionList = junction.getConditions();

            if (N.isNullOrEmpty(conditionList)) {
                throw new IllegalArgumentException("The junction condition(" + junction.getOperator().toString() + ") doesn't include any element.");
            }

            if (conditionList.size() == 1) {
                appendCondition(conditionList.get(0));
            } else {
                // TODO ((id = :id) AND (gui = :gui)) is not support.
                // only (id = :id) AND (gui = :gui) works.
                // sb.append(_PARENTHESES_L);

                for (int i = 0, size = conditionList.size(); i < size; i++) {
                    if (i > 0) {
                        sb.append(_SPACE);
                        sb.append(junction.getOperator().toString());
                        sb.append(_SPACE);
                    }

                    sb.append(_PARENTHESES_L);

                    appendCondition(conditionList.get(i));

                    sb.append(_PARENTHESES_R);
                }

                // sb.append(_PARENTHESES_R);
            }
        } else if (cond instanceof SubQuery) {
            final SubQuery subQuery = (SubQuery) cond;

            if (N.notNullOrEmpty(subQuery.getSql())) {
                sb.append(subQuery.getSql());
            } else {

                if (this instanceof SCCB) {
                    sb.append(SCCB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).where(subQuery.getCondition()).cql());
                } else if (this instanceof PSC) {
                    sb.append(PSC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).where(subQuery.getCondition()).cql());
                } else if (this instanceof NSC) {
                    sb.append(NSC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).where(subQuery.getCondition()).cql());
                } else if (this instanceof ACCB) {
                    sb.append(ACCB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).where(subQuery.getCondition()).cql());
                } else if (this instanceof PAC) {
                    sb.append(PAC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).where(subQuery.getCondition()).cql());
                } else if (this instanceof NAC) {
                    sb.append(NAC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).where(subQuery.getCondition()).cql());
                } else if (this instanceof LCCB) {
                    sb.append(LCCB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).where(subQuery.getCondition()).cql());
                } else if (this instanceof PLC) {
                    sb.append(PLC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).where(subQuery.getCondition()).cql());
                } else if (this instanceof NLC) {
                    sb.append(NLC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).where(subQuery.getCondition()).cql());
                } else {
                    throw new RuntimeException("Unsupproted subQuery condition: " + cond);
                }
            }
        } else if (cond instanceof Expression) {
            sb.append(cond.toString());
        } else {
            throw new IllegalArgumentException("Unsupported condtion: " + cond.toString());
        }
    }

    /**
     * Formalize column name.
     *
     * @param propName
     * @return
     */
    private String formalizeColumnName(final String propName) {
        return formalizeColumnName(getPropColumnNameMap(entityClass, namingPolicy), propName);
    }

    /**
     * Formalize column name.
     *
     * @param propColumnNameMap
     * @param propName
     * @return
     */
    private String formalizeColumnName(final Map<String, String> propColumnNameMap, final String propName) {
        final String columnName = propColumnNameMap == null ? null : propColumnNameMap.get(propName);

        if (columnName != null) {
            return columnName;
        }

        return formalizeColumnName(propName, namingPolicy);
    }

    /**
     * Gets the prop column name map.
     *
     * @return
     */
    private static Map<String, String> getPropColumnNameMap(final Class<?> entityClass, final NamingPolicy namingPolicy) {
        if (entityClass == null || Map.class.isAssignableFrom(entityClass)) {
            return N.emptyMap();
        }

        final Map<NamingPolicy, Map<String, String>> namingColumnNameMap = entityTablePropColumnNameMap.get(entityClass);
        Map<String, String> result = null;

        if (namingColumnNameMap == null || (result = namingColumnNameMap.get(namingPolicy)) == null) {
            result = registerEntityPropColumnNameMap(entityClass, namingPolicy, null);
        }

        return result;
    }

    /**
     * Register entity prop column name map.
     *
     * @param entityClass annotated with @Table, @Column
     */
    private static Map<String, String> registerEntityPropColumnNameMap(final Class<?> entityClass, final NamingPolicy namingPolicy,
            final Set<Class<?>> registeringClasses) {
        N.checkArgNotNull(entityClass);

        if (registeringClasses != null) {
            if (registeringClasses.contains(entityClass)) {
                throw new RuntimeException("Cycling references found among: " + registeringClasses);
            } else {
                registeringClasses.add(entityClass);
            }
        }

        Map<String, String> propColumnNameMap = new HashMap<>();
        final EntityInfo entityInfo = ParserUtil.getEntityInfo(entityClass);
        String columnName = null;

        for (PropInfo propInfo : entityInfo.propInfoList) {
            if (propInfo.isAnnotationPresent(Column.class)) {
                columnName = propInfo.getAnnotation(Column.class).value();
            } else {
                try {
                    if (propInfo.isAnnotationPresent(javax.persistence.Column.class)) {
                        columnName = propInfo.getAnnotation(javax.persistence.Column.class).name();
                    }
                } catch (Throwable e) {
                    logger.warn("To support javax.persistence.Table/Column, please add dependence javax.persistence:persistence-api");
                }
            }

            if (N.notNullOrEmpty(columnName)) {
                propColumnNameMap.put(propInfo.name, columnName);
            } else {
                propColumnNameMap.put(propInfo.name, formalizeColumnName(propInfo.name, namingPolicy));

                final Type<?> propType = propInfo.type.isCollection() ? propInfo.type.getElementType() : propInfo.type;

                if (propType.isEntity()) {
                    final Map<String, String> subPropColumnNameMap = registerEntityPropColumnNameMap(propType.clazz(), namingPolicy,
                            N.<Class<?>> asLinkedHashSet(entityClass));

                    if (N.notNullOrEmpty(subPropColumnNameMap)) {
                        final String subTableName = getTableName(propType.clazz(), namingPolicy);

                        for (Map.Entry<String, String> entry : subPropColumnNameMap.entrySet()) {
                            propColumnNameMap.put(propInfo.name + WD.PERIOD + entry.getKey(), subTableName + WD.PERIOD + entry.getValue());
                        }
                    }
                }
            }
        }

        //    final Map<String, String> tmp = entityTablePropColumnNameMap.get(entityClass);
        //
        //    if (N.notNullOrEmpty(tmp)) {
        //        propColumnNameMap.putAll(tmp);
        //    }

        if (N.isNullOrEmpty(propColumnNameMap)) {
            propColumnNameMap = N.<String, String> emptyMap();
        }

        Map<NamingPolicy, Map<String, String>> namingPropColumnMap = entityTablePropColumnNameMap.get(entityClass);

        if (namingPropColumnMap == null) {
            namingPropColumnMap = new EnumMap<>(NamingPolicy.class);
            // TODO not necessary?
            // namingPropColumnMap = Collections.synchronizedMap(namingPropColumnMap)
            entityTablePropColumnNameMap.put(entityClass, namingPropColumnMap);
        }

        namingPropColumnMap.put(namingPolicy, propColumnNameMap);

        return propColumnNameMap;
    }

    private static String formalizeColumnName(final String propName, final NamingPolicy namingPolicy) {
        if (namingPolicy == NamingPolicy.LOWER_CAMEL_CASE) {
            return ClassUtil.formalizePropName(propName);
        } else {
            return namingPolicy.convert(propName);
        }
    }

    /**
     *
     * @param <T>
     * @param <EX>
     * @param func
     * @return
     * @throws EX the ex
     */
    public <T, EX extends Exception> T apply(final Throwables.Function<? super CP, T, EX> func) throws EX {
        return func.apply(this.pair());
    }

    /**
     *
     * @param <EX>
     * @param consumer
     * @throws EX the ex
     */
    public <EX extends Exception> void accept(final Throwables.Consumer<? super CP, EX> consumer) throws EX {
        consumer.accept(this.pair());
    }

    //    @Override
    //    public int hashCode() {
    //        return sb.hashCode();
    //    }
    //
    //    @Override
    //    public boolean equals(Object obj) {
    //        if (obj == this) {
    //            return true;
    //        }
    //
    //        if (obj instanceof CQLBuilder) {
    //            final CQLBuilder other = (CQLBuilder) obj;
    //
    //            return N.equals(this.sb, other.sb) && N.equals(this.parameters, other.parameters);
    //        }
    //
    //        return false;
    //    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return cql();
    }

    /**
     * Parses the insert entity.
     *
     * @param instance
     * @param entity
     * @param excludedPropNames
     */
    private static void parseInsertEntity(final CQLBuilder instance, final Object entity, final Set<String> excludedPropNames) {
        if (entity instanceof String) {
            instance.columnNames = N.asArray((String) entity);
        } else if (entity instanceof Map) {
            if (N.isNullOrEmpty(excludedPropNames)) {
                instance.props = (Map<String, Object>) entity;
            } else {
                instance.props = new LinkedHashMap<>((Map<String, Object>) entity);
                Maps.removeKeys(instance.props, excludedPropNames);
            }
        } else {
            final Class<?> entityClass = entity.getClass();
            final Collection<String> propNames = getInsertPropNamesByClass(entityClass, excludedPropNames);
            final Map<String, Object> map = N.newHashMap(N.initHashCapacity(propNames.size()));
            final EntityInfo entityInfo = ParserUtil.getEntityInfo(entityClass);

            for (String propName : propNames) {
                map.put(propName, entityInfo.getPropValue(entity, propName));
            }

            instance.props = map;
        }
    }

    /**
     * The Enum CQLPolicy.
     */
    enum CQLPolicy {

        /** The cql. */
        CQL,
        /** The parameterized cql. */
        PARAMETERIZED_CQL,
        /** The named cql. */
        NAMED_CQL;
    }

    /**
     * Un-parameterized CQL builder with snake case (lower case with underscore) field/column naming strategy.
     *
     * For example:
     * <pre>
     * <code>
     * SCCB.select("firstName", "lastName").from("account").where(L.eq("id", 1)).sql();
     * // Output: SELECT first_name AS "firstName", last_name AS "lastName" FROM account WHERE id = 1
     * </code>
     * </pre>
     *
     * @deprecated {@code PSC or NSC} is preferred.
     */
    @Deprecated
    public static final class SCCB extends CQLBuilder {

        /**
         * Instantiates a new sccb.
         */
        SCCB() {
            super(NamingPolicy.LOWER_CASE_WITH_UNDERSCORE, CQLPolicy.CQL);
        }

        /**
         * Creates the instance.
         *
         * @return
         */
        static SCCB createInstance() {
            return new SCCB();
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder insert(final String expr) {
            return insert(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder insert(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.props = props;

            return instance;
        }

        /**
         *
         * @param entity
         * @return
         */
        public static CQLBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         *
         * @param entity
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entity.getClass();

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entityClass;
            instance.columnNameList = getInsertPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder select(final String expr) {
            return select(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final String expr, final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final String expr, final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass) {
            return select(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.entityClass = entityClass;
            instance.columnNameList = getSelectPropNamesByClass(entityClass, true, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass);
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.entityClass = entityClass;
            instance.tableName = getTableName(entityClass, NamingPolicy.LOWER_CASE_WITH_UNDERSCORE);
            instance.columnNameList = getUpdatePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder delete(final String expr) {
            return delete(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder delete(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.entityClass = entityClass;
            instance.columnNameList = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass) {
            return deleteFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return delete(entityClass, excludedPropNames).from(entityClass);
        }
    }

    /**
     * Un-parameterized CQL builder with all capitals case (upper case with underscore) field/column naming strategy.
     *
     * For example:
     * <pre>
     * <code>
     * N.println(ACCB.select("firstName", "lastName").from("account").where(L.eq("id", 1)).sql());
     * // Output: SELECT FIRST_NAME AS "firstName", LAST_NAME AS "lastName" FROM ACCOUNT WHERE ID = 1
     * </code>
     * </pre>
     *
     * @deprecated {@code PAC or NAC} is preferred.
     */
    @Deprecated
    public static final class ACCB extends CQLBuilder {

        /**
         * Instantiates a new accb.
         */
        ACCB() {
            super(NamingPolicy.UPPER_CASE_WITH_UNDERSCORE, CQLPolicy.CQL);
        }

        /**
         * Creates the instance.
         *
         * @return
         */
        static ACCB createInstance() {
            return new ACCB();
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder insert(final String expr) {
            return insert(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder insert(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.props = props;

            return instance;
        }

        /**
         *
         * @param entity
         * @return
         */
        public static CQLBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         *
         * @param entity
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entity.getClass();

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entityClass;
            instance.columnNameList = getInsertPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder select(final String expr) {
            return select(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final String expr, final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final String expr, final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass) {
            return select(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.entityClass = entityClass;
            instance.columnNameList = getSelectPropNamesByClass(entityClass, true, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass);
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.entityClass = entityClass;
            instance.tableName = getTableName(entityClass, NamingPolicy.UPPER_CASE_WITH_UNDERSCORE);
            instance.columnNameList = getUpdatePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder delete(final String expr) {
            return delete(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder delete(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.entityClass = entityClass;
            instance.columnNameList = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass) {
            return deleteFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return delete(entityClass, excludedPropNames).from(entityClass);
        }
    }

    /**
     * Un-parameterized CQL builder with lower camel case field/column naming strategy.
     *
     * For example:
     * <pre>
     * <code>
     * N.println(LCCB.select("firstName", "lastName").from("account").where(L.eq("id", 1)).sql());
     * // SELECT firstName, lastName FROM account WHERE id = 1
     * </code>
     * </pre>
     *
     * @deprecated {@code PLC or NLC} is preferred.
     */
    @Deprecated
    public static final class LCCB extends CQLBuilder {

        /**
         * Instantiates a new lccb.
         */
        LCCB() {
            super(NamingPolicy.LOWER_CAMEL_CASE, CQLPolicy.CQL);
        }

        /**
         * Creates the instance.
         *
         * @return
         */
        static LCCB createInstance() {
            return new LCCB();
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder insert(final String expr) {
            return insert(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder insert(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.props = props;

            return instance;
        }

        /**
         *
         * @param entity
         * @return
         */
        public static CQLBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         *
         * @param entity
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entity.getClass();

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entityClass;
            instance.columnNameList = getInsertPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder select(final String expr) {
            return select(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final String expr, final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final String expr, final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass) {
            return select(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.entityClass = entityClass;
            instance.columnNameList = getSelectPropNamesByClass(entityClass, true, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass);
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.entityClass = entityClass;
            instance.tableName = getTableName(entityClass, NamingPolicy.LOWER_CAMEL_CASE);
            instance.columnNameList = getUpdatePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder delete(final String expr) {
            return delete(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder delete(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.entityClass = entityClass;
            instance.columnNameList = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass) {
            return deleteFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return delete(entityClass, excludedPropNames).from(entityClass);
        }
    }

    /**
     * Parameterized('?') CQL builder with snake case (lower case with underscore) field/column naming strategy.
     *
     * For example:
     * <pre>
     * <code>
     * N.println(PSC.select("firstName", "lastName").from("account").where(L.eq("id", 1)).sql());
     * // SELECT first_name AS "firstName", last_name AS "lastName" FROM account WHERE id = ?
     * </code>
     * </pre>
     */
    public static final class PSC extends CQLBuilder {

        /**
         * Instantiates a new psc.
         */
        PSC() {
            super(NamingPolicy.LOWER_CASE_WITH_UNDERSCORE, CQLPolicy.PARAMETERIZED_CQL);
        }

        /**
         * Creates the instance.
         *
         * @return
         */
        static PSC createInstance() {
            return new PSC();
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder insert(final String expr) {
            return insert(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder insert(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.props = props;

            return instance;
        }

        /**
         *
         * @param entity
         * @return
         */
        public static CQLBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         *
         * @param entity
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entity.getClass();

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entityClass;
            instance.columnNameList = getInsertPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder select(final String expr) {
            return select(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final String expr, final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final String expr, final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass) {
            return select(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.entityClass = entityClass;
            instance.columnNameList = getSelectPropNamesByClass(entityClass, true, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass);
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.entityClass = entityClass;
            instance.tableName = getTableName(entityClass, NamingPolicy.LOWER_CASE_WITH_UNDERSCORE);
            instance.columnNameList = getUpdatePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder delete(final String expr) {
            return delete(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder delete(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.entityClass = entityClass;
            instance.columnNameList = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass) {
            return deleteFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return delete(entityClass, excludedPropNames).from(entityClass);
        }
    }

    /**
     * Parameterized('?') CQL builder with all capitals case (upper case with underscore) field/column naming strategy.
     *
     * For example:
     * <pre>
     * <code>
     * N.println(PAC.select("firstName", "lastName").from("account").where(L.eq("id", 1)).sql());
     * // SELECT FIRST_NAME AS "firstName", LAST_NAME AS "lastName" FROM ACCOUNT WHERE ID = ?
     * </code>
     * </pre>
     */
    public static final class PAC extends CQLBuilder {

        /**
         * Instantiates a new pac.
         */
        PAC() {
            super(NamingPolicy.UPPER_CASE_WITH_UNDERSCORE, CQLPolicy.PARAMETERIZED_CQL);
        }

        /**
         * Creates the instance.
         *
         * @return
         */
        static PAC createInstance() {
            return new PAC();
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder insert(final String expr) {
            return insert(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder insert(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.props = props;

            return instance;
        }

        /**
         *
         * @param entity
         * @return
         */
        public static CQLBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         *
         * @param entity
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entity.getClass();

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entityClass;
            instance.columnNameList = getInsertPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder select(final String expr) {
            return select(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final String expr, final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final String expr, final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass) {
            return select(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.entityClass = entityClass;
            instance.columnNameList = getSelectPropNamesByClass(entityClass, true, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass);
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.entityClass = entityClass;
            instance.tableName = getTableName(entityClass, NamingPolicy.UPPER_CASE_WITH_UNDERSCORE);
            instance.columnNameList = getUpdatePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder delete(final String expr) {
            return delete(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder delete(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.entityClass = entityClass;
            instance.columnNameList = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass) {
            return deleteFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return delete(entityClass, excludedPropNames).from(entityClass);
        }
    }

    /**
     * Parameterized('?') CQL builder with lower camel case field/column naming strategy.
     *
     * For example:
     * <pre>
     * <code>
     * N.println(PLC.select("firstName", "lastName").from("account").where(L.eq("id", 1)).sql());
     * // SELECT firstName, lastName FROM account WHERE id = ?
     * </code>
     * </pre>
     */
    public static final class PLC extends CQLBuilder {

        /**
         * Instantiates a new plc.
         */
        PLC() {
            super(NamingPolicy.LOWER_CAMEL_CASE, CQLPolicy.PARAMETERIZED_CQL);
        }

        /**
         * Creates the instance.
         *
         * @return
         */
        static PLC createInstance() {
            return new PLC();
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder insert(final String expr) {
            return insert(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder insert(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.props = props;

            return instance;
        }

        /**
         *
         * @param entity
         * @return
         */
        public static CQLBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         *
         * @param entity
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entity.getClass();

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entityClass;
            instance.columnNameList = getInsertPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder select(final String expr) {
            return select(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final String expr, final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final String expr, final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass) {
            return select(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.entityClass = entityClass;
            instance.columnNameList = getSelectPropNamesByClass(entityClass, true, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass);
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.entityClass = entityClass;
            instance.tableName = getTableName(entityClass, NamingPolicy.LOWER_CAMEL_CASE);
            instance.columnNameList = getUpdatePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder delete(final String expr) {
            return delete(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder delete(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.entityClass = entityClass;
            instance.columnNameList = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass) {
            return deleteFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return delete(entityClass, excludedPropNames).from(entityClass);
        }
    }

    /**
     * Named CQL builder with snake case (lower case with underscore) field/column naming strategy.
     *
     * For example:
     * <pre>
     * <code>
     * N.println(NSC.select("firstName", "lastName").from("account").where(L.eq("id", 1)).sql());
     * // SELECT first_name AS "firstName", last_name AS "lastName" FROM account WHERE id = :id
     * </code>
     * </pre>
     */
    public static final class NSC extends CQLBuilder {

        /**
         * Instantiates a new nsc.
         */
        NSC() {
            super(NamingPolicy.LOWER_CASE_WITH_UNDERSCORE, CQLPolicy.NAMED_CQL);
        }

        /**
         * Creates the instance.
         *
         * @return
         */
        static NSC createInstance() {
            return new NSC();
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder insert(final String expr) {
            return insert(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder insert(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.props = props;

            return instance;
        }

        /**
         *
         * @param entity
         * @return
         */
        public static CQLBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         *
         * @param entity
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entity.getClass();

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entityClass;
            instance.columnNameList = getInsertPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder select(final String expr) {
            return select(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final String expr, final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final String expr, final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass) {
            return select(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.entityClass = entityClass;
            instance.columnNameList = getSelectPropNamesByClass(entityClass, true, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass);
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.entityClass = entityClass;
            instance.tableName = getTableName(entityClass, NamingPolicy.LOWER_CASE_WITH_UNDERSCORE);
            instance.columnNameList = getUpdatePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder delete(final String expr) {
            return delete(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder delete(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.entityClass = entityClass;
            instance.columnNameList = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass) {
            return deleteFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return delete(entityClass, excludedPropNames).from(entityClass);
        }
    }

    /**
     * Named CQL builder with all capitals case (upper case with underscore) field/column naming strategy.
     *
     * For example:
     * <pre>
     * <code>
     * N.println(NAC.select("firstName", "lastName").from("account").where(L.eq("id", 1)).sql());
     * // SELECT FIRST_NAME AS "firstName", LAST_NAME AS "lastName" FROM ACCOUNT WHERE ID = :id
     * </code>
     * </pre>
     */
    public static final class NAC extends CQLBuilder {

        /**
         * Instantiates a new nac.
         */
        NAC() {
            super(NamingPolicy.UPPER_CASE_WITH_UNDERSCORE, CQLPolicy.NAMED_CQL);
        }

        /**
         * Creates the instance.
         *
         * @return
         */
        static NAC createInstance() {
            return new NAC();
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder insert(final String expr) {
            return insert(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder insert(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.props = props;

            return instance;
        }

        /**
         *
         * @param entity
         * @return
         */
        public static CQLBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         *
         * @param entity
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entity.getClass();

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entityClass;
            instance.columnNameList = getInsertPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder select(final String expr) {
            return select(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final String expr, final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final String expr, final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass) {
            return select(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.entityClass = entityClass;
            instance.columnNameList = getSelectPropNamesByClass(entityClass, true, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass);
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.entityClass = entityClass;
            instance.tableName = getTableName(entityClass, NamingPolicy.UPPER_CASE_WITH_UNDERSCORE);
            instance.columnNameList = getUpdatePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder delete(final String expr) {
            return delete(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder delete(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.entityClass = entityClass;
            instance.columnNameList = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass) {
            return deleteFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return delete(entityClass, excludedPropNames).from(entityClass);
        }
    }

    /**
     * Named SQL builder with lower camel case field/column naming strategy.
     *
     * For example:
     * <pre>
     * <code>
     * N.println(NLC.select("firstName", "lastName").from("account").where(L.eq("id", 1)).sql());
     * // SELECT firstName, lastName FROM account WHERE id = :id
     * </code>
     * </pre>
     */
    public static final class NLC extends CQLBuilder {

        /**
         * Instantiates a new nlc.
         */
        NLC() {
            super(NamingPolicy.LOWER_CAMEL_CASE, CQLPolicy.NAMED_CQL);
        }

        /**
         * Creates the instance.
         *
         * @return
         */
        static NLC createInstance() {
            return new NLC();
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder insert(final String expr) {
            return insert(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder insert(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.props = props;

            return instance;
        }

        /**
         *
         * @param entity
         * @return
         */
        public static CQLBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         *
         * @param entity
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entity.getClass();

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.ADD;
            instance.entityClass = entityClass;
            instance.columnNameList = getInsertPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder select(final String expr) {
            return select(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final String expr, final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param expr <code>ALL | DISTINCT | DISTINCTROW...</code>
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final String expr, final Map<String, String> columnAliases) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.predicates = expr;
            instance.columnAliases = columnAliases;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass) {
            return select(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.QUERY;
            instance.entityClass = entityClass;
            instance.columnNameList = getSelectPropNamesByClass(entityClass, true, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass);
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.UPDATE;
            instance.entityClass = entityClass;
            instance.tableName = getTableName(entityClass, NamingPolicy.LOWER_CAMEL_CASE);
            instance.columnNameList = getUpdatePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param expr
         * @return
         */
        public static CQLBuilder delete(final String expr) {
            return delete(N.asArray(expr));
        }

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder delete(final String... columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.columnNameList = columnNames;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.entityClass = entityClass;
            instance.columnNameList = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance.op = OperationType.DELETE;
            instance.tableName = tableName;

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass) {
            return deleteFrom(entityClass, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder deleteFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return delete(entityClass, excludedPropNames).from(entityClass);
        }
    }

    /**
     * The Class CP.
     */
    public static final class CP {

        /** The cql. */
        public final String cql;

        /** The parameters. */
        public final List<Object> parameters;

        /**
         * Instantiates a new cp.
         *
         * @param cql
         * @param parameters
         */
        CP(final String cql, final List<Object> parameters) {
            this.cql = cql;
            this.parameters = ImmutableList.of(parameters);
        }

        /**
         *
         * @return
         */
        public Pair<String, List<Object>> __() {
            return Pair.of(cql, parameters);
        }

        /**
         *
         * @return
         */
        @Override
        public int hashCode() {
            return N.hashCode(cql) * 31 + N.hashCode(parameters);
        }

        /**
         *
         * @param obj
         * @return true, if successful
         */
        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj instanceof CP) {
                CP other = (CP) obj;

                return N.equals(other.cql, cql) && N.equals(other.parameters, parameters);
            }

            return false;
        }

        /**
         *
         * @return
         */
        @Override
        public String toString() {
            return "{cql=" + cql + ", parameters=" + N.toString(parameters) + "}";
        }
    }
}
