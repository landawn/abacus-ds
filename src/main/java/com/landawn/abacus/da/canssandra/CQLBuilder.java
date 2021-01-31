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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.landawn.abacus.DirtyMarker;
import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.annotation.NonUpdatable;
import com.landawn.abacus.annotation.NotColumn;
import com.landawn.abacus.annotation.ReadOnly;
import com.landawn.abacus.annotation.ReadOnlyId;
import com.landawn.abacus.condition.Between;
import com.landawn.abacus.condition.Binary;
import com.landawn.abacus.condition.Cell;
import com.landawn.abacus.condition.Clause;
import com.landawn.abacus.condition.Condition;
import com.landawn.abacus.condition.ConditionFactory.CF;
import com.landawn.abacus.condition.Criteria;
import com.landawn.abacus.condition.Expression;
import com.landawn.abacus.condition.Having;
import com.landawn.abacus.condition.In;
import com.landawn.abacus.condition.InSubQuery;
import com.landawn.abacus.condition.Join;
import com.landawn.abacus.condition.Junction;
import com.landawn.abacus.condition.Limit;
import com.landawn.abacus.condition.SubQuery;
import com.landawn.abacus.condition.Where;
import com.landawn.abacus.core.DirtyMarkerUtil;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.EntityInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.util.Array;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.ImmutableMap;
import com.landawn.abacus.util.ImmutableSet;
import com.landawn.abacus.util.Maps;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.ObjectPool;
import com.landawn.abacus.util.Objectory;
import com.landawn.abacus.util.OperationType;
import com.landawn.abacus.util.SQLBuilder;
import com.landawn.abacus.util.SQLParser;
import com.landawn.abacus.util.SortDirection;
import com.landawn.abacus.util.StringUtil;
import com.landawn.abacus.util.Throwables;
import com.landawn.abacus.util.Tuple.Tuple2;
import com.landawn.abacus.util.WD;

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

    // TODO performance goal: 80% cases (or maybe sql.length < 1024?) can be composed in 0.1 millisecond. 0.01 millisecond will be fantastic if possible.

    private static final Logger logger = LoggerFactory.getLogger(CQLBuilder.class);

    public static final String DISTINCT = WD.DISTINCT;

    public static final String COUNT_ALL = "count(*)";

    static final int POOL_SIZE;

    static {
        int multi = (int) (Runtime.getRuntime().maxMemory() / ((1024 * 1024) * 256));

        POOL_SIZE = Math.max(1000, Math.min(1000 * multi, 8192));
    }

    private static final Map<Class<?>, Set<String>[]> defaultPropNamesPool = new ObjectPool<>(POOL_SIZE);

    private static final Map<NamingPolicy, Map<Class<?>, String>> fullSelectPartsPool = new HashMap<>(NamingPolicy.values().length);

    static {
        for (NamingPolicy np : NamingPolicy.values()) {
            fullSelectPartsPool.put(np, new ConcurrentHashMap<>());
        }
    }

    private static final Map<Class<?>, String[]> classTableNameMap = new ConcurrentHashMap<>();

    private static final Map<String, char[]> tableDeleteFrom = new ConcurrentHashMap<>();

    private static final AtomicInteger activeStringBuilderCounter = new AtomicInteger();

    static final char[] _SPACE_USING_TIMESTAMP_SPACE = " USING TIMESTAMP ".toCharArray();

    static final char[] _SPACE_USING_TTL_SPACE = " USING TTL ".toCharArray();

    static final char[] _SPACE_IF_SPACE = " IF ".toCharArray();

    static final char[] _SPACE_IF_EXISTS = " IF EXISTS".toCharArray();

    static final char[] _SPACE_IF_NOT_EXISTS = " IF NOT EXISTS".toCharArray();

    static final char[] _SPACE_ALLOW_FILTERING = " ALLOW FILTERING".toCharArray();

    static final char[] _INSERT = WD.INSERT.toCharArray();

    static final char[] _SPACE_INSERT_SPACE = (WD.SPACE + WD.INSERT + WD.SPACE).toCharArray();

    static final char[] _INTO = WD.INTO.toCharArray();

    static final char[] _SPACE_INTO_SPACE = (WD.SPACE + WD.INTO + WD.SPACE).toCharArray();

    static final char[] _VALUES = WD.VALUES.toCharArray();

    static final char[] _SPACE_VALUES_SPACE = (WD.SPACE + WD.VALUES + WD.SPACE).toCharArray();

    static final char[] _SELECT = WD.SELECT.toCharArray();

    static final char[] _SPACE_SELECT_SPACE = (WD.SPACE + WD.SELECT + WD.SPACE).toCharArray();

    static final char[] _FROM = WD.FROM.toCharArray();

    static final char[] _SPACE_FROM_SPACE = (WD.SPACE + WD.FROM + WD.SPACE).toCharArray();

    static final char[] _UPDATE = WD.UPDATE.toCharArray();

    static final char[] _SPACE_UPDATE_SPACE = (WD.SPACE + WD.UPDATE + WD.SPACE).toCharArray();

    static final char[] _SET = WD.SET.toCharArray();

    static final char[] _SPACE_SET_SPACE = (WD.SPACE + WD.SET + WD.SPACE).toCharArray();

    static final char[] _DELETE = WD.DELETE.toCharArray();

    static final char[] _SPACE_DELETE_SPACE = (WD.SPACE + WD.DELETE + WD.SPACE).toCharArray();

    static final char[] _USING = WD.USING.toCharArray();

    static final char[] _SPACE_USING_SPACE = (WD.SPACE + WD.USING + WD.SPACE).toCharArray();

    static final char[] _WHERE = WD.WHERE.toCharArray();

    static final char[] _SPACE_WHERE_SPACE = (WD.SPACE + WD.WHERE + WD.SPACE).toCharArray();

    static final char[] _GROUP_BY = WD.GROUP_BY.toCharArray();

    static final char[] _SPACE_GROUP_BY_SPACE = (WD.SPACE + WD.GROUP_BY + WD.SPACE).toCharArray();

    static final char[] _HAVING = WD.HAVING.toCharArray();

    static final char[] _SPACE_HAVING_SPACE = (WD.SPACE + WD.HAVING + WD.SPACE).toCharArray();

    static final char[] _ORDER_BY = WD.ORDER_BY.toCharArray();

    static final char[] _SPACE_ORDER_BY_SPACE = (WD.SPACE + WD.ORDER_BY + WD.SPACE).toCharArray();

    static final char[] _LIMIT = (WD.SPACE + WD.LIMIT + WD.SPACE).toCharArray();

    static final char[] _SPACE_LIMIT_SPACE = (WD.SPACE + WD.LIMIT + WD.SPACE).toCharArray();

    static final char[] _OFFSET = WD.OFFSET.toCharArray();

    static final char[] _SPACE_OFFSET_SPACE = (WD.SPACE + WD.OFFSET + WD.SPACE).toCharArray();

    static final char[] _AND = WD.AND.toCharArray();

    static final char[] _SPACE_AND_SPACE = (WD.SPACE + WD.AND + WD.SPACE).toCharArray();

    static final char[] _OR = WD.OR.toCharArray();

    static final char[] _SPACE_OR_SPACE = (WD.SPACE + WD.OR + WD.SPACE).toCharArray();

    static final char[] _AS = WD.AS.toCharArray();

    static final char[] _SPACE_AS_SPACE = (WD.SPACE + WD.AS + WD.SPACE).toCharArray();

    static final char[] _SPACE_EQUAL_SPACE = (WD.SPACE + WD.EQUAL + WD.SPACE).toCharArray();

    static final char[] _SPACE_FOR_UPDATE = (WD.SPACE + WD.FOR_UPDATE).toCharArray();

    static final char[] _COMMA_SPACE = WD.COMMA_SPACE.toCharArray();

    static final String SPACE_AS_SPACE = WD.SPACE + WD.AS + WD.SPACE;

    private static final Set<String> sqlKeyWords = new HashSet<>(1024);

    static {
        final Field[] fields = WD.class.getDeclaredFields();
        int m = 0;

        for (Field field : fields) {
            m = field.getModifiers();

            if (Modifier.isPublic(m) && Modifier.isStatic(m) && Modifier.isFinal(m) && field.getType().equals(String.class)) {
                try {
                    final String value = (String) field.get(null);

                    for (String e : StringUtil.split(value, ' ', true)) {
                        sqlKeyWords.add(e);
                        sqlKeyWords.add(e.toUpperCase());
                        sqlKeyWords.add(e.toLowerCase());
                    }
                } catch (Exception e) {
                    // ignore, should never happen.
                }
            }
        }
    }

    private final NamingPolicy _namingPolicy;

    private final CQLPolicy _cqlPolicy;

    private final List<Object> _parameters = new ArrayList<>();

    private StringBuilder _sb;

    private Class<?> _entityClass;

    private ImmutableMap<String, Tuple2<String, Boolean>> _propColumnNameMap;

    private String _alias;

    private OperationType _op;

    private String _tableName;

    private String _preselect;

    private Collection<String> _columnNames;

    private Map<String, String> _columnAliases;

    private Map<String, Object> _props;

    private Collection<Map<String, Object>> _propsList;

    private boolean _isForConditionOnly = false;

    CQLBuilder(final NamingPolicy namingPolicy, final CQLPolicy cqlPolicy) {
        if (activeStringBuilderCounter.incrementAndGet() > 1024) {
            logger.error("Too many(" + activeStringBuilderCounter.get()
                    + ") StringBuilder instances are created in CQLBuilder. The method cql()/pair() must be called to release resources and close CQLBuilder");
        }

        this._sb = Objectory.createStringBuilder();

        this._namingPolicy = namingPolicy == null ? NamingPolicy.LOWER_CASE_WITH_UNDERSCORE : namingPolicy;
        this._cqlPolicy = cqlPolicy == null ? CQLPolicy.CQL : cqlPolicy;
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
            final EntityInfo entityInfo = ParserUtil.getEntityInfo(entityClass);

            if (entityInfo.tableName.isPresent()) {
                entityTableNames = Array.repeat(entityInfo.tableName.get(), 3);
            } else {
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
     * @param excludedPropNames
     * @return
     */
    static Collection<String> getSelectPropNamesByClass(final Class<?> entityClass, final Set<String> excludedPropNames) {
        final Collection<String>[] val = loadPropNamesByClass(entityClass);
        final Collection<String> propNames = val[0]; // includeSubEntityProperties ? val[0] : val[1];

        if (N.isNullOrEmpty(excludedPropNames)) {
            return propNames;
        } else {
            final List<String> tmp = new ArrayList<>(N.max(0, propNames.size() - excludedPropNames.size()));
            int idx = 0;

            for (String propName : propNames) {
                if (!(excludedPropNames.contains(propName)
                        || ((idx = propName.indexOf(WD._PERIOD)) > 0 && excludedPropNames.contains(propName.substring(0, idx))))) {
                    tmp.add(propName);
                }
            }

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

                final Set<String> nonUpdatableNonWritablePropNames = N.newHashSet();
                final Set<String> nonUpdatablePropNames = N.newHashSet();
                final Set<String> transientPropNames = N.newHashSet();

                for (PropInfo propInfo : entityInfo.propInfoList) {
                    if (propInfo.isAnnotationPresent(ReadOnly.class) || propInfo.isAnnotationPresent(ReadOnlyId.class)) {
                        nonUpdatableNonWritablePropNames.add(propInfo.name);
                    }

                    if (propInfo.isAnnotationPresent(NonUpdatable.class)) {
                        nonUpdatablePropNames.add(propInfo.name);
                    }

                    if (propInfo.isTransient || propInfo.isAnnotationPresent(NotColumn.class)) {
                        nonUpdatableNonWritablePropNames.add(propInfo.name);
                        transientPropNames.add(propInfo.name);
                    }
                }

                nonUpdatablePropNames.addAll(nonUpdatableNonWritablePropNames);

                val[0].removeAll(transientPropNames);
                val[1].removeAll(transientPropNames);
                val[2].removeAll(nonUpdatableNonWritablePropNames);
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
        final Map<String, Expression> m = N.newLinkedHashMap(propNames.length);

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
        final Map<String, Expression> m = new LinkedHashMap<>(propNames.size());

        for (String propName : propNames) {
            m.put(propName, CF.QME);
        }

        return m;
    }

    private static final Map<Integer, String> QM_CACHE = new HashMap<>();

    static {
        for (int i = 0; i <= 30; i++) {
            QM_CACHE.put(i, StringUtil.repeat("?", i, ", "));
        }

        QM_CACHE.put(100, StringUtil.repeat("?", 100, ", "));
        QM_CACHE.put(200, StringUtil.repeat("?", 200, ", "));
        QM_CACHE.put(300, StringUtil.repeat("?", 300, ", "));
        QM_CACHE.put(500, StringUtil.repeat("?", 500, ", "));
        QM_CACHE.put(1000, StringUtil.repeat("?", 1000, ", "));
    }

    /**
     * Repeat question mark({@code ?}) {@code n} times with delimiter {@code ", "}.
     * <br />
     * It's designed for batch SQL builder.
     *
     * @param n
     * @return
     */
    public static String repeatQM(int n) {
        N.checkArgNotNegative(n, "count");

        String result = QM_CACHE.get(n);

        if (result == null) {
            result = StringUtil.repeat("?", n, ", ");
        }

        return result;
    }

    /**
     *
     * @param tableName
     * @return
     */
    public CQLBuilder into(final String tableName) {
        if (_op != OperationType.ADD) {
            throw new RuntimeException("Invalid operation: " + _op);
        }

        if (N.isNullOrEmpty(_columnNames) && N.isNullOrEmpty(_props) && N.isNullOrEmpty(_propsList)) {
            throw new RuntimeException("Column names or props must be set first by insert");
        }

        this._tableName = tableName;

        _sb.append(_INSERT);
        _sb.append(_SPACE_INTO_SPACE);

        _sb.append(tableName);

        _sb.append(_SPACE);
        _sb.append(WD._PARENTHESES_L);

        if (N.notNullOrEmpty(_columnNames)) {
            int i = 0;
            for (String columnName : _columnNames) {
                if (i++ > 0) {
                    _sb.append(_COMMA_SPACE);
                }

                appendColumnName(_propColumnNameMap, columnName);
            }
        } else {
            final Map<String, Object> props = N.isNullOrEmpty(this._props) ? _propsList.iterator().next() : this._props;

            int i = 0;
            for (String columnName : props.keySet()) {
                if (i++ > 0) {
                    _sb.append(_COMMA_SPACE);
                }

                appendColumnName(_propColumnNameMap, columnName);
            }
        }

        _sb.append(WD._PARENTHESES_R);

        _sb.append(_SPACE_VALUES_SPACE);

        _sb.append(WD._PARENTHESES_L);

        if (N.notNullOrEmpty(_columnNames)) {
            switch (_cqlPolicy) {
                case CQL:
                case PARAMETERIZED_CQL: {
                    for (int i = 0, size = _columnNames.size(); i < size; i++) {
                        if (i > 0) {
                            _sb.append(_COMMA_SPACE);
                        }

                        _sb.append(WD._QUESTION_MARK);
                    }

                    break;
                }

                case NAMED_CQL: {
                    int i = 0;
                    for (String columnName : _columnNames) {
                        if (i++ > 0) {
                            _sb.append(_COMMA_SPACE);
                        }

                        _sb.append(":");
                        _sb.append(columnName);
                    }

                    break;
                }

                default:
                    throw new RuntimeException("Not supported SQL policy: " + _cqlPolicy);
            }
        } else if (N.notNullOrEmpty(_props)) {
            appendInsertProps(_props);
        } else {
            int i = 0;
            for (Map<String, Object> props : _propsList) {
                if (i++ > 0) {
                    _sb.append(WD._PARENTHESES_R);
                    _sb.append(_COMMA_SPACE);
                    _sb.append(WD._PARENTHESES_L);
                }

                appendInsertProps(props);
            }
        }

        _sb.append(WD._PARENTHESES_R);

        return this;
    }

    /**
     *
     * @param entityClass
     * @return
     */
    public CQLBuilder into(final Class<?> entityClass) {
        if (this._entityClass == null) {
            this._entityClass = entityClass;
            this._propColumnNameMap = getProp2ColumnNameMap(this._entityClass, this._namingPolicy);
        }

        return into(getTableName(entityClass, _namingPolicy));
    }

    /**
     * 
     * @return
     */
    public CQLBuilder distinct() {
        return preselect(DISTINCT);
    }

    /**
     * 
     * @param preselect <code>ALL | DISTINCT | DISTINCTROW...</code>
     * @return
     */
    public CQLBuilder preselect(final String preselect) {
        N.checkArgNotNull(preselect, "preselect");

        if (_sb.length() > 0) {
            throw new IllegalStateException("'distinct|preselect' must be called before 'from' operation");
        }

        if (N.isNullOrEmpty(this._preselect)) {
            this._preselect = preselect;
        } else {
            this._preselect += preselect;
        }

        return this;
    }

    /**
     *
     * @param expr
     * @return
     */
    public CQLBuilder from(String expr) {
        expr = expr.trim();

        final int idx = expr.indexOf(WD._COMMA);
        final String tableName = idx > 0 ? expr.substring(0, idx) : expr;

        return from(tableName.trim(), expr);
    }

    /**
     *
     * @param tableNames
     * @return
     */
    @SafeVarargs
    public final CQLBuilder from(final String... tableNames) {
        if (tableNames.length == 1) {
            return from(tableNames[0].trim());
        }

        final String tableName = tableNames[0].trim();
        return from(tableName, StringUtil.join(tableNames, WD.COMMA_SPACE));
    }

    /**
     *
     * @param tableNames
     * @return
     */
    public CQLBuilder from(final Collection<String> tableNames) {
        if (tableNames.size() == 1) {
            return from(tableNames.iterator().next().trim());
        }

        final String tableName = tableNames.iterator().next().trim();
        return from(tableName, StringUtil.join(tableNames, WD.COMMA_SPACE));
    }

    /**
     *
     * @param entityClass
     * @return
     */
    public CQLBuilder from(final Class<?> entityClass) {
        if (this._entityClass == null) {
            this._entityClass = entityClass;
            this._propColumnNameMap = getProp2ColumnNameMap(this._entityClass, this._namingPolicy);
        }

        return from(getTableName(entityClass, _namingPolicy));
    }

    /**
     *
     * @param entityClass
     * @param alias
     * @return
     */
    public CQLBuilder from(final Class<?> entityClass, final String alias) {
        if (this._entityClass == null) {
            this._entityClass = entityClass;
            this._propColumnNameMap = getProp2ColumnNameMap(this._entityClass, this._namingPolicy);
        }

        if (N.isNullOrEmpty(alias)) {
            return from(getTableName(entityClass, _namingPolicy));
        } else {
            return from(getTableName(entityClass, _namingPolicy) + " " + alias);
        }
    }

    /**
     *
     * @param tableName
     * @param fromCause
     * @return
     */
    private CQLBuilder from(final String tableName, final String fromCause) {
        if (_op != OperationType.QUERY && _op != OperationType.DELETE) {
            throw new RuntimeException("Invalid operation: " + _op);
        }

        if (_op == OperationType.QUERY && N.isNullOrEmpty(_columnNames) && N.isNullOrEmpty(_columnAliases)) {
            throw new RuntimeException("Column names or props must be set first by select");
        }

        this._tableName = tableName;

        int idx = tableName.indexOf(' ');

        if (idx > 0) {
            this._tableName = tableName.substring(0, idx).trim();
            _alias = tableName.substring(idx + 1).trim();
        } else {
            this._tableName = tableName.trim();
        }

        _sb.append(_op == OperationType.QUERY ? _SELECT : _DELETE);
        _sb.append(WD._SPACE);

        if (N.notNullOrEmpty(_preselect)) {
            _sb.append(_preselect);
            _sb.append(_SPACE);
        }

        final boolean isForSelect = _op == OperationType.QUERY;
        final boolean withAlias = N.notNullOrEmpty(_alias);

        if (N.notNullOrEmpty(_columnNames)) {
            if (_entityClass != null && withAlias == false && _columnNames == getSelectPropNamesByClass(_entityClass, null)) {
                String fullSelectParts = fullSelectPartsPool.get(_namingPolicy).get(_entityClass);

                if (N.isNullOrEmpty(fullSelectParts)) {
                    fullSelectParts = "";

                    int i = 0;
                    for (String columnName : _columnNames) {
                        if (i++ > 0) {
                            fullSelectParts += WD.COMMA_SPACE;
                        }

                        fullSelectParts += formalizeColumnName(_propColumnNameMap, columnName);

                        if (_namingPolicy != NamingPolicy.LOWER_CAMEL_CASE && !WD.ASTERISK.equals(columnName)) {
                            fullSelectParts += " AS ";

                            fullSelectParts += WD.QUOTATION_D;
                            fullSelectParts += columnName;
                            fullSelectParts += WD.QUOTATION_D;
                        }
                    }

                    fullSelectPartsPool.get(_namingPolicy).put(_entityClass, fullSelectParts);
                }

                _sb.append(fullSelectParts);
            } else {
                int i = 0;
                for (String columnName : _columnNames) {
                    if (i++ > 0) {
                        _sb.append(_COMMA_SPACE);
                    }

                    appendColumnName(_propColumnNameMap, columnName, null, false, null, isForSelect);
                }
            }
        } else if (N.notNullOrEmpty(_columnAliases)) {
            int i = 0;
            for (Map.Entry<String, String> entry : _columnAliases.entrySet()) {
                if (i++ > 0) {
                    _sb.append(_COMMA_SPACE);
                }

                appendColumnName(_propColumnNameMap, entry.getKey(), entry.getValue(), false, null, isForSelect);
            }
        } else {
            throw new UnsupportedOperationException("No select part specified");
        }

        _sb.append(_SPACE_FROM_SPACE);

        _sb.append(fromCause);

        return this;
    }

    /**
     *
     * @param expr
     * @return
     */
    public CQLBuilder where(final String expr) {
        init(true);

        _sb.append(_SPACE_WHERE_SPACE);

        appendStringExpr(expr, false);

        return this;
    }

    public CQLBuilder where(final Condition cond) {
        init(true);

        _sb.append(_SPACE_WHERE_SPACE);

        appendCondition(cond);

        return this;
    }

    /**
     *
     * @param expr
     * @return
     */
    public CQLBuilder orderBy(final String expr) {
        _sb.append(_SPACE_ORDER_BY_SPACE);

        appendColumnName(expr);

        return this;
    }

    /**
     *
     * @param columnNames
     * @return
     */
    @SafeVarargs
    public final CQLBuilder orderBy(final String... columnNames) {
        _sb.append(_SPACE_ORDER_BY_SPACE);

        for (int i = 0, len = columnNames.length; i < len; i++) {
            if (i > 0) {
                _sb.append(_COMMA_SPACE);
            }

            appendColumnName(_propColumnNameMap, columnNames[i]);
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

        _sb.append(_SPACE);
        _sb.append(direction.toString());

        return this;
    }

    /**
     *
     * @param columnNames
     * @return
     */
    public CQLBuilder orderBy(final Collection<String> columnNames) {
        _sb.append(_SPACE_ORDER_BY_SPACE);

        int i = 0;
        for (String columnName : columnNames) {
            if (i++ > 0) {
                _sb.append(_COMMA_SPACE);
            }

            appendColumnName(_propColumnNameMap, columnName);
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

        _sb.append(_SPACE);
        _sb.append(direction.toString());

        return this;
    }

    /**
     *
     * @param orders
     * @return
     */
    public CQLBuilder orderBy(final Map<String, SortDirection> orders) {
        _sb.append(_SPACE_ORDER_BY_SPACE);

        int i = 0;

        for (Map.Entry<String, SortDirection> entry : orders.entrySet()) {
            if (i++ > 0) {
                _sb.append(_COMMA_SPACE);
            }

            appendColumnName(_propColumnNameMap, entry.getKey());

            _sb.append(_SPACE);
            _sb.append(entry.getValue().toString());
        }

        return this;
    }

    /**
     *
     * @param count
     * @return
     */
    public CQLBuilder limit(final int count) {
        _sb.append(_SPACE_LIMIT_SPACE);

        _sb.append(count);

        return this;
    }

    public CQLBuilder append(final Condition cond) {
        init(true);

        if (cond instanceof Criteria) {
            final Criteria criteria = (Criteria) cond;

            final Collection<Join> joins = criteria.getJoins();

            if (N.notNullOrEmpty(joins)) {
                for (Join join : joins) {
                    _sb.append(_SPACE).append(join.getOperator()).append(_SPACE);

                    if (join.getJoinEntities().size() == 1) {
                        _sb.append(join.getJoinEntities().get(0));
                    } else {
                        _sb.append(WD._PARENTHESES_L);
                        int idx = 0;

                        for (String joinTableName : join.getJoinEntities()) {
                            if (idx++ > 0) {
                                _sb.append(_COMMA_SPACE);
                            }

                            _sb.append(joinTableName);
                        }

                        _sb.append(WD._PARENTHESES_R);
                    }

                    appendCondition(join.getCondition());
                }
            }

            final Cell where = criteria.getWhere();

            if ((where != null)) {
                _sb.append(_SPACE_WHERE_SPACE);
                appendCondition(where.getCondition());
            }

            final Cell groupBy = criteria.getGroupBy();

            if (groupBy != null) {
                _sb.append(_SPACE_GROUP_BY_SPACE);
                appendCondition(groupBy.getCondition());
            }

            final Cell having = criteria.getHaving();

            if (having != null) {
                _sb.append(_SPACE_HAVING_SPACE);
                appendCondition(having.getCondition());
            }

            List<Cell> aggregations = criteria.getAggregation();

            if (N.notNullOrEmpty(aggregations)) {
                for (Cell aggregation : aggregations) {
                    _sb.append(_SPACE).append(aggregation.getOperator()).append(_SPACE);
                    appendCondition(aggregation.getCondition());
                }
            }

            final Cell orderBy = criteria.getOrderBy();

            if (orderBy != null) {
                _sb.append(_SPACE_ORDER_BY_SPACE);
                appendCondition(orderBy.getCondition());
            }

            final Limit limit = criteria.getLimit();

            if (limit != null) {
                if (N.notNullOrEmpty(limit.getExpr())) {
                    _sb.append(_SPACE).append(limit.getExpr());
                } else {
                    limit(limit.getCount());
                }
            }
        } else if (cond instanceof Clause) {
            _sb.append(_SPACE).append(cond.getOperator()).append(_SPACE);
            appendCondition(((Clause) cond).getCondition());
        } else {
            if (!_isForConditionOnly) {
                _sb.append(_SPACE_WHERE_SPACE);
            }

            appendCondition(cond);
        }

        return this;
    }

    public CQLBuilder append(final String expr) {
        _sb.append(expr);

        return this;
    }

    /**
     *
     * @param expr
     * @return
     */
    public CQLBuilder set(final String expr) {
        return set(Array.asList(expr));
    }

    /**
     *
     * @param columnNames
     * @return
     */
    @SafeVarargs
    public final CQLBuilder set(final String... columnNames) {
        return set(Array.asList(columnNames));
    }

    /**
     *
     * @param columnNames
     * @return
     */
    public CQLBuilder set(final Collection<String> columnNames) {
        init(false);

        switch (_cqlPolicy) {
            case CQL:
            case PARAMETERIZED_CQL: {
                int i = 0;
                for (String columnName : columnNames) {
                    if (i++ > 0) {
                        _sb.append(_COMMA_SPACE);
                    }

                    appendColumnName(_propColumnNameMap, columnName);

                    if (columnName.indexOf('=') < 0) {
                        _sb.append(" = ?");
                    }
                }

                break;
            }

            case NAMED_CQL: {
                int i = 0;
                for (String columnName : columnNames) {
                    if (i++ > 0) {
                        _sb.append(_COMMA_SPACE);
                    }

                    appendColumnName(_propColumnNameMap, columnName);

                    if (columnName.indexOf('=') < 0) {
                        _sb.append(" = :");
                        _sb.append(columnName);
                    }
                }

                break;
            }

            default:
                throw new RuntimeException("Not supported SQL policy: " + _cqlPolicy);
        }

        this._columnNames = null;

        return this;
    }

    /**
     *
     * @param props
     * @return
     */
    public CQLBuilder set(final Map<String, Object> props) {
        init(false);

        switch (_cqlPolicy) {
            case CQL: {
                int i = 0;
                for (Map.Entry<String, Object> entry : props.entrySet()) {
                    if (i++ > 0) {
                        _sb.append(_COMMA_SPACE);
                    }

                    appendColumnName(_propColumnNameMap, entry.getKey());

                    _sb.append(_SPACE_EQUAL_SPACE);

                    setParameterForSQL(entry.getValue());
                }

                break;
            }

            case PARAMETERIZED_CQL: {
                int i = 0;
                for (Map.Entry<String, Object> entry : props.entrySet()) {
                    if (i++ > 0) {
                        _sb.append(_COMMA_SPACE);
                    }

                    appendColumnName(_propColumnNameMap, entry.getKey());

                    _sb.append(_SPACE_EQUAL_SPACE);

                    setParameterForRawSQL(entry.getValue());
                }

                break;
            }

            case NAMED_CQL: {
                int i = 0;
                for (Map.Entry<String, Object> entry : props.entrySet()) {
                    if (i++ > 0) {
                        _sb.append(_COMMA_SPACE);
                    }

                    appendColumnName(_propColumnNameMap, entry.getKey());

                    _sb.append(_SPACE_EQUAL_SPACE);

                    setParameterForNamedSQL(entry.getKey(), entry.getValue());
                }

                break;
            }

            default:
                throw new RuntimeException("Not supported SQL policy: " + _cqlPolicy);
        }

        this._columnNames = null;

        return this;
    }

    /**
     * Only the dirty properties will be set into the result SQL if the specified entity is a dirty marker entity.
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
    @SuppressWarnings("null")
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
            this._entityClass = entityClass;
            this._propColumnNameMap = getProp2ColumnNameMap(this._entityClass, this._namingPolicy);
            final Collection<String> propNames = getUpdatePropNamesByClass(entityClass, excludedPropNames);
            final Set<String> dirtyPropNames = DirtyMarkerUtil.isDirtyMarker(entityClass) ? DirtyMarkerUtil.dirtyPropNames((DirtyMarker) entity) : null;
            final boolean isEmptyDirtyPropNames = N.isNullOrEmpty(dirtyPropNames);
            final Map<String, Object> props = N.newHashMap(N.isNullOrEmpty(dirtyPropNames) ? propNames.size() : dirtyPropNames.size());
            final EntityInfo entityInfo = ParserUtil.getEntityInfo(entityClass);

            for (String propName : propNames) {
                if (isEmptyDirtyPropNames || dirtyPropNames.contains(propName)) {
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
        this._entityClass = entityClass;
        this._propColumnNameMap = getProp2ColumnNameMap(this._entityClass, this._namingPolicy);

        return set(entityClass, null);
    }

    /**
     *
     * @param entityClass
     * @param excludedPropNames
     * @return
     */
    public CQLBuilder set(Class<?> entityClass, final Set<String> excludedPropNames) {
        this._entityClass = entityClass;
        this._propColumnNameMap = getProp2ColumnNameMap(this._entityClass, this._namingPolicy);

        return set(getUpdatePropNamesByClass(entityClass, excludedPropNames));
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

        _sb.append(_SPACE_USING_TTL_SPACE);
        _sb.append(timestamp);

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

        _sb.append(_SPACE_USING_TIMESTAMP_SPACE);
        _sb.append(timestamp);

        return this;
    }

    /**
     *
     * @param expr
     * @return
     */
    public CQLBuilder iF(final String expr) {
        init(true);

        _sb.append(_SPACE_IF_SPACE);

        appendStringExpr(expr, false);

        return this;
    }

    /**
     *
     * @param cond any literal written in <code>Expression</code> condition won't be formalized
     * @return
     */
    public CQLBuilder iF(final Condition cond) {
        init(true);

        _sb.append(_SPACE_IF_SPACE);

        appendCondition(cond);

        return this;
    }

    public CQLBuilder ifExists() {
        init(true);

        _sb.append(_SPACE_IF_EXISTS);

        return this;
    }

    /**
     * If not exists.
     *
     * @return
     */
    public CQLBuilder ifNotExists() {
        init(true);

        _sb.append(_SPACE_IF_NOT_EXISTS);

        return this;
    }

    public CQLBuilder allowFiltering() {
        init(true);

        _sb.append(_SPACE_ALLOW_FILTERING);

        return this;
    }

    /**
     * This CQLBuilder will be closed after <code>cql()</code> is called.
     *
     * @return
     */
    public String cql() {
        if (_sb == null) {
            throw new RuntimeException("This CQLBuilder has been closed after cql() was called previously");
        }

        init(true);

        String cql = null;

        try {
            cql = _sb.charAt(0) == ' ' ? _sb.substring(1) : _sb.toString();
        } finally {
            Objectory.recycle(_sb);
            _sb = null;

            activeStringBuilderCounter.decrementAndGet();
        }

        if (logger.isDebugEnabled()) {
            logger.debug(cql);
        }

        return cql;
    }

    public List<Object> parameters() {
        return _parameters;
    }

    /**
     *  This CQLBuilder will be closed after <code>pair()</code> is called.
     *
     * @return
     */
    public CP pair() {
        final String cql = cql();

        return new CP(cql, _parameters);
    }

    public <T, E extends Exception> T apply(final Throwables.Function<? super CP, T, E> func) throws E {
        return func.apply(this.pair());
    }

    public <T, E extends Exception> T apply(final Throwables.BiFunction<? super String, ? super List<Object>, T, E> func) throws E {
        final CP cp = this.pair();

        return func.apply(cp.cql, cp.parameters);
    }

    public <E extends Exception> void accept(final Throwables.Consumer<? super CP, E> consumer) throws E {
        consumer.accept(this.pair());
    }

    public <E extends Exception> void accept(final Throwables.BiConsumer<? super String, ? super List<Object>, E> consumer) throws E {
        final CP cp = this.pair();

        consumer.accept(cp.cql, cp.parameters);
    }

    /**
     *
     * @param setForUpdate
     */
    void init(boolean setForUpdate) {
        // Note: any change, please take a look at: parse(final Class<?> entityClass, final Condition cond) first.

        if (_sb.length() > 0) {
            return;
        }

        if (_op == OperationType.UPDATE) {
            _sb.append(_UPDATE);

            _sb.append(_SPACE);
            _sb.append(_tableName);

            _sb.append(_SPACE_SET_SPACE);

            if (setForUpdate && N.notNullOrEmpty(_columnNames)) {
                set(_columnNames);
            }
        } else if (_op == OperationType.DELETE) {
            final String newTableName = _tableName;

            char[] deleteFromTableChars = tableDeleteFrom.get(newTableName);

            if (deleteFromTableChars == null) {
                deleteFromTableChars = (WD.DELETE + WD.SPACE + WD.FROM + WD.SPACE + newTableName).toCharArray();
                tableDeleteFrom.put(newTableName, deleteFromTableChars);
            }

            _sb.append(deleteFromTableChars);
        }
    }

    /**
     * Sets the parameter for SQL.
     *
     * @param propValue the new parameter for SQL
     */
    private void setParameterForSQL(final Object propValue) {
        if (CF.QME.equals(propValue)) {
            _sb.append(WD._QUESTION_MARK);
        } else if (propValue instanceof Condition) {
            appendCondition((Condition) propValue);
        } else {
            _sb.append(Expression.formalize(propValue));
        }
    }

    /**
     * Sets the parameter for raw SQL.
     *
     * @param propValue the new parameter for raw SQL
     */
    private void setParameterForRawSQL(final Object propValue) {
        if (CF.QME.equals(propValue)) {
            _sb.append(WD._QUESTION_MARK);
        } else if (propValue instanceof Condition) {
            appendCondition((Condition) propValue);
        } else {
            _sb.append(WD._QUESTION_MARK);

            _parameters.add(propValue);
        }
    }

    /**
     * Sets the parameter for named SQL.
     *
     * @param propName
     * @param propValue
     */
    private void setParameterForNamedSQL(final String propName, final Object propValue) {
        if (CF.QME.equals(propValue)) {
            _sb.append(":");
            _sb.append(propName);
        } else if (propValue instanceof Condition) {
            appendCondition((Condition) propValue);
        } else {
            _sb.append(":");
            _sb.append(propName);

            _parameters.add(propValue);
        }
    }

    /**
     * Sets the parameter.
     *
     * @param propName
     * @param propValue
     */
    private void setParameter(final String propName, final Object propValue) {
        switch (_cqlPolicy) {
            case CQL: {
                setParameterForSQL(propValue);

                break;
            }

            case PARAMETERIZED_CQL: {
                setParameterForRawSQL(propValue);

                break;
            }

            case NAMED_CQL: {
                setParameterForNamedSQL(propName, propValue);

                break;
            }

            default:
                throw new RuntimeException("Not supported SQL policy: " + _cqlPolicy);
        }
    }

    /**
     * Append insert props.
     *
     * @param props
     */
    private void appendInsertProps(final Map<String, Object> props) {
        switch (_cqlPolicy) {
            case CQL: {
                int i = 0;
                Object propValue = null;
                for (String propName : props.keySet()) {
                    if (i++ > 0) {
                        _sb.append(_COMMA_SPACE);
                    }

                    propValue = props.get(propName);

                    setParameterForSQL(propValue);
                }

                break;
            }

            case PARAMETERIZED_CQL: {
                int i = 0;
                Object propValue = null;
                for (String propName : props.keySet()) {
                    if (i++ > 0) {
                        _sb.append(_COMMA_SPACE);
                    }

                    propValue = props.get(propName);

                    setParameterForRawSQL(propValue);
                }

                break;
            }

            case NAMED_CQL: {
                int i = 0;
                Object propValue = null;
                for (String propName : props.keySet()) {
                    if (i++ > 0) {
                        _sb.append(_COMMA_SPACE);
                    }

                    propValue = props.get(propName);

                    setParameterForNamedSQL(propName, propValue);
                }

                break;
            }

            default:
                throw new RuntimeException("Not supported SQL policy: " + _cqlPolicy);
        }
    }

    /**
     *
     * @param cond
     */
    private void appendCondition(final Condition cond) {
        //    if (sb.charAt(sb.length() - 1) != _SPACE) {
        //        sb.append(_SPACE);
        //    }

        if (cond instanceof Binary) {
            final Binary binary = (Binary) cond;
            final String propName = binary.getPropName();

            appendColumnName(propName);

            _sb.append(_SPACE);
            _sb.append(binary.getOperator().toString());
            _sb.append(_SPACE);

            Object propValue = binary.getPropValue();
            setParameter(propName, propValue);
        } else if (cond instanceof Between) {
            final Between bt = (Between) cond;
            final String propName = bt.getPropName();

            appendColumnName(propName);

            _sb.append(_SPACE);
            _sb.append(bt.getOperator().toString());
            _sb.append(_SPACE);

            Object minValue = bt.getMinValue();
            if (_cqlPolicy == CQLPolicy.NAMED_CQL) {
                setParameter("min" + StringUtil.capitalize(propName), minValue);
            } else {
                setParameter(propName, minValue);
            }

            _sb.append(_SPACE);
            _sb.append(WD.AND);
            _sb.append(_SPACE);

            Object maxValue = bt.getMaxValue();
            if (_cqlPolicy == CQLPolicy.NAMED_CQL) {
                setParameter("max" + StringUtil.capitalize(propName), maxValue);
            } else {
                setParameter(propName, maxValue);
            }
        } else if (cond instanceof In) {
            final In in = (In) cond;
            final String propName = in.getPropName();
            final List<Object> parameters = in.getParameters();

            appendColumnName(propName);

            _sb.append(_SPACE);
            _sb.append(in.getOperator().toString());
            _sb.append(WD.SPACE_PARENTHESES_L);

            for (int i = 0, len = parameters.size(); i < len; i++) {
                if (i > 0) {
                    _sb.append(WD.COMMA_SPACE);
                }

                if (_cqlPolicy == CQLPolicy.NAMED_CQL) {
                    setParameter(propName + (i + 1), parameters.get(i));
                } else {
                    setParameter(propName, parameters.get(i));
                }
            }

            _sb.append(WD._PARENTHESES_R);
        } else if (cond instanceof InSubQuery) {
            final InSubQuery inSubQuery = (InSubQuery) cond;
            final String propName = inSubQuery.getPropName();

            appendColumnName(propName);

            _sb.append(_SPACE);
            _sb.append(inSubQuery.getOperator().toString());
            _sb.append(WD.SPACE_PARENTHESES_L);

            appendCondition(inSubQuery.getSubQuery());

            _sb.append(WD._PARENTHESES_R);
        } else if (cond instanceof Where || cond instanceof Having) {
            final Cell cell = (Cell) cond;

            _sb.append(_SPACE);
            _sb.append(cell.getOperator().toString());
            _sb.append(_SPACE);

            appendCondition(cell.getCondition());
        } else if (cond instanceof Cell) {
            final Cell cell = (Cell) cond;

            _sb.append(_SPACE);
            _sb.append(cell.getOperator().toString());
            _sb.append(_SPACE);

            _sb.append(_PARENTHESES_L);
            appendCondition(cell.getCondition());
            _sb.append(_PARENTHESES_R);
        } else if (cond instanceof Junction) {
            final Junction junction = (Junction) cond;
            final List<Condition> conditionList = junction.getConditions();

            if (N.isNullOrEmpty(conditionList)) {
                throw new IllegalArgumentException("The junction condition(" + junction.getOperator().toString() + ") doesn't include any element.");
            }

            if (conditionList.size() == 1) {
                appendCondition(conditionList.get(0));
            } else {
                // TODO ((id = :id) AND (gui = :gui)) is not support in Cassandra.
                // only (id = :id) AND (gui = :gui) works.
                // sb.append(_PARENTHESES_L);

                for (int i = 0, size = conditionList.size(); i < size; i++) {
                    if (i > 0) {
                        _sb.append(_SPACE);
                        _sb.append(junction.getOperator().toString());
                        _sb.append(_SPACE);
                    }

                    _sb.append(_PARENTHESES_L);

                    appendCondition(conditionList.get(i));

                    _sb.append(_PARENTHESES_R);
                }

                // sb.append(_PARENTHESES_R);
            }
        } else if (cond instanceof SubQuery) {
            final SubQuery subQuery = (SubQuery) cond;
            final Condition subCond = subQuery.getCondition();

            if (N.notNullOrEmpty(subQuery.getSql())) {
                _sb.append(subQuery.getSql());
            } else {
                if (subQuery.getEntityClass() != null) {
                    if (this instanceof PSC) {
                        _sb.append(PSC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).cql());
                    } else if (this instanceof NSC) {
                        _sb.append(NSC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).cql());
                    } else if (this instanceof PAC) {
                        _sb.append(PAC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).cql());
                    } else if (this instanceof NAC) {
                        _sb.append(NAC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).cql());
                    } else if (this instanceof PLC) {
                        _sb.append(PLC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).cql());
                    } else if (this instanceof NLC) {
                        _sb.append(NLC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).cql());
                    } else {
                        throw new RuntimeException("Unsupproted subQuery condition: " + cond);
                    }
                } else {
                    if (this instanceof PSC) {
                        _sb.append(PSC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).cql());
                    } else if (this instanceof NSC) {
                        _sb.append(NSC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).cql());
                    } else if (this instanceof PAC) {
                        _sb.append(PAC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).cql());
                    } else if (this instanceof NAC) {
                        _sb.append(NAC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).cql());
                    } else if (this instanceof PLC) {
                        _sb.append(PLC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).cql());
                    } else if (this instanceof NLC) {
                        _sb.append(NLC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).cql());
                    } else {
                        throw new RuntimeException("Unsupproted subQuery condition: " + cond);
                    }
                }
            }
        } else if (cond instanceof Expression) {
            // ==== version 1
            // sb.append(cond.toString());

            // ==== version 2
            //    final List<String> words = SQLParser.parse(((Expression) cond).getLiteral());
            //    final Map<String, String> propColumnNameMap = getPropColumnNameMap(entityClass, namingPolicy);
            //
            //    String word = null;
            //
            //    for (int i = 0, size = words.size(); i < size; i++) {
            //        word = words.get(i);
            //
            //        if ((i > 2) && WD.AS.equalsIgnoreCase(words.get(i - 2))) {
            //            sb.append(word);
            //        } else if ((i > 1) && WD.SPACE.equalsIgnoreCase(words.get(i - 1))
            //                && (propColumnNameMap.containsKey(words.get(i - 2)) || propColumnNameMap.containsValue(words.get(i - 2)))) {
            //            sb.append(word);
            //        } else {
            //            sb.append(formalizeColumnName(propColumnNameMap, word));
            //        }
            //    }

            // ==== version 3
            appendStringExpr(((Expression) cond).getLiteral(), false);
        } else {
            throw new IllegalArgumentException("Unsupported condtion: " + cond.toString());
        }
    }

    private void appendStringExpr(final String expr, final boolean isFromAppendColumn) {
        // TODO performance improvement.

        if (expr.length() < 16) {
            boolean allChars = true;
            char ch = 0;

            for (int i = 0, len = expr.length(); i < len; i++) {
                ch = expr.charAt(i);

                // https://www.sciencebuddies.org/science-fair-projects/references/ascii-table
                if (ch < 'A' || (ch > 'Z' && ch < '_') || ch > 'z') {
                    allChars = false;
                    break;
                }
            }

            if (allChars) {
                if (isFromAppendColumn) {
                    _sb.append(formalizeColumnName(expr, _namingPolicy));
                } else {
                    _sb.append(formalizeColumnName(_propColumnNameMap, expr));
                }

                return;
            }
        }

        final List<String> words = SQLParser.parse(expr);

        String word = null;
        for (int i = 0, len = words.size(); i < len; i++) {
            word = words.get(i);

            if (!StringUtil.isAsciiAlpha(word.charAt(0))) {
                _sb.append(word);
            } else if (SQLParser.isFunctionName(words, len, i)) {
                _sb.append(word);
            } else {
                _sb.append(formalizeColumnName(_propColumnNameMap, word));
            }
        }
    }

    private void appendColumnName(final String propName) {
        appendColumnName(_propColumnNameMap, propName);
    }

    private void appendColumnName(final ImmutableMap<String, Tuple2<String, Boolean>> propColumnNameMap, final String propName) {
        appendColumnName(propColumnNameMap, propName, null, false, null, false);
    }

    private void appendColumnName(final ImmutableMap<String, Tuple2<String, Boolean>> propColumnNameMap, final String propName, final String propAlias,
            final boolean withClassAlias, final String classAlias, final boolean isForSelect) {
        final Tuple2<String, Boolean> tp = propColumnNameMap == null ? null : propColumnNameMap.get(propName);

        if (tp != null) {
            if (tp._2.booleanValue() && _alias != null && _alias.length() > 0) {
                _sb.append(_alias).append(WD._PERIOD);
            }

            _sb.append(tp._1);

            if (isForSelect) {
                _sb.append(_SPACE_AS_SPACE);
                _sb.append(WD._QUOTATION_D);

                if (withClassAlias) {
                    _sb.append(classAlias).append(WD._PERIOD);
                }

                _sb.append(N.notNullOrEmpty(propAlias) ? propAlias : propName);
                _sb.append(WD._QUOTATION_D);
            }

            return;
        }

        if (N.notNullOrEmpty(propAlias)) {
            appendStringExpr(propName, true);

            if (isForSelect) {
                _sb.append(_SPACE_AS_SPACE);
                _sb.append(WD._QUOTATION_D);

                if (withClassAlias) {
                    _sb.append(classAlias).append(WD._PERIOD);
                }

                _sb.append(propAlias);
                _sb.append(WD._QUOTATION_D);
            }
        } else {
            if (isForSelect) {
                int index = propName.indexOf(" AS ");

                if (index < 0) {
                    index = propName.indexOf(" as ");
                }

                if (index > 0) {
                    appendColumnName(propColumnNameMap, propName.substring(0, index).trim(), propName.substring(index + 4).trim(), withClassAlias, classAlias,
                            isForSelect);
                } else {
                    appendStringExpr(propName, true);

                    if (propName.charAt(propName.length() - 1) != '*') {
                        _sb.append(_SPACE_AS_SPACE);
                        _sb.append(WD._QUOTATION_D);

                        if (withClassAlias) {
                            _sb.append(classAlias).append(WD._PERIOD);
                        }

                        _sb.append(propName);
                        _sb.append(WD._QUOTATION_D);
                    }
                }
            } else {
                appendStringExpr(propName, true);
            }
        }
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

    public void println() {
        N.println(cql());
    }

    @Override
    public String toString() {
        return cql();
    }

    static String formalizeColumnName(final String word, final NamingPolicy namingPolicy) {
        if (sqlKeyWords.contains(word)) {
            return word;
        } else if (namingPolicy == NamingPolicy.LOWER_CAMEL_CASE) {
            return ClassUtil.formalizePropName(word);
        } else {
            return namingPolicy.convert(word);
        }
    }

    private String formalizeColumnName(final ImmutableMap<String, Tuple2<String, Boolean>> propColumnNameMap, final String propName) {
        final Tuple2<String, Boolean> tp = propColumnNameMap == null ? null : propColumnNameMap.get(propName);

        if (tp != null) {
            if (tp._2.booleanValue() && _alias != null && _alias.length() > 0) {
                return _alias + "." + tp._1;
            } else {
                return tp._1;
            }
        }

        return formalizeColumnName(propName, _namingPolicy);
    }

    private static void parseInsertEntity(final CQLBuilder instance, final Object entity, final Set<String> excludedPropNames) {
        if (entity instanceof String) {
            instance._columnNames = Array.asList(((String) entity));
        } else if (entity instanceof Map) {
            if (N.isNullOrEmpty(excludedPropNames)) {
                instance._props = (Map<String, Object>) entity;
            } else {
                instance._props = new LinkedHashMap<>((Map<String, Object>) entity);
                Maps.removeKeys(instance._props, excludedPropNames);
            }
        } else {
            final Class<?> entityClass = entity.getClass();
            final Collection<String> propNames = getInsertPropNamesByClass(entityClass, excludedPropNames);
            final Map<String, Object> map = N.newHashMap(propNames.size());
            final EntityInfo entityInfo = ParserUtil.getEntityInfo(entityClass);

            for (String propName : propNames) {
                map.put(propName, entityInfo.getPropValue(entity, propName));
            }

            instance._props = map;
        }
    }

    @SuppressWarnings("deprecation")
    static ImmutableMap<String, Tuple2<String, Boolean>> getProp2ColumnNameMap(final Class<?> entityClass, final NamingPolicy namingPolicy) {
        return SQLBuilder.getProp2ColumnNameMap(entityClass, namingPolicy);
    }

    enum CQLPolicy {
        CQL, PARAMETERIZED_CQL, NAMED_CQL;
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
         * To generate {@code cql} part for the specified {@code cond} only.
         *
         * @param cond
         * @param entityClass
         * @return
         */
        public static CQLBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CQLBuilder instance = createInstance();

            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
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

            instance._op = OperationType.ADD;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

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

            instance._op = OperationType.ADD;
            instance._entityClass = entity.getClass();
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);

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

            instance._op = OperationType.ADD;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getInsertPropNamesByClass(entityClass, excludedPropNames);

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
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

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

            instance._op = OperationType.UPDATE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._tableName = getTableName(entityClass, NamingPolicy.LOWER_CASE_WITH_UNDERSCORE);
            instance._columnNames = getUpdatePropNamesByClass(entityClass, excludedPropNames);

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

            instance._op = OperationType.DELETE;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._columnNames = columnNames;

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

            instance._op = OperationType.DELETE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

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

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            N.checkArgNotNullOrEmpty(columnAliases, "columnAliases");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnAliases = columnAliases;

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

            instance._op = OperationType.QUERY;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getSelectPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, (String) null);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, null, excludedPropNames);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass, alias);
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
         * To generate {@code cql} part for the specified {@code cond} only.
         *
         * @param cond
         * @param entityClass
         * @return
         */
        public static CQLBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CQLBuilder instance = createInstance();

            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
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

            instance._op = OperationType.ADD;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

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

            instance._op = OperationType.ADD;
            instance._entityClass = entity.getClass();
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);

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

            instance._op = OperationType.ADD;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getInsertPropNamesByClass(entityClass, excludedPropNames);

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
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

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

            instance._op = OperationType.UPDATE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._tableName = getTableName(entityClass, NamingPolicy.UPPER_CASE_WITH_UNDERSCORE);
            instance._columnNames = getUpdatePropNamesByClass(entityClass, excludedPropNames);

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

            instance._op = OperationType.DELETE;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._columnNames = columnNames;

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

            instance._op = OperationType.DELETE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

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

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            N.checkArgNotNullOrEmpty(columnAliases, "columnAliases");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnAliases = columnAliases;

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

            instance._op = OperationType.QUERY;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getSelectPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, (String) null);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, null, excludedPropNames);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass, alias);
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
         * To generate {@code cql} part for the specified {@code cond} only.
         *
         * @param cond
         * @param entityClass
         * @return
         */
        public static CQLBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CQLBuilder instance = createInstance();

            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
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

            instance._op = OperationType.ADD;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

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

            instance._op = OperationType.ADD;
            instance._entityClass = entity.getClass();
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);

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

            instance._op = OperationType.ADD;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getInsertPropNamesByClass(entityClass, excludedPropNames);

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
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

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

            instance._op = OperationType.UPDATE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._tableName = getTableName(entityClass, NamingPolicy.LOWER_CAMEL_CASE);
            instance._columnNames = getUpdatePropNamesByClass(entityClass, excludedPropNames);

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

            instance._op = OperationType.DELETE;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._columnNames = columnNames;

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

            instance._op = OperationType.DELETE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

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

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            N.checkArgNotNullOrEmpty(columnAliases, "columnAliases");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnAliases = columnAliases;

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

            instance._op = OperationType.QUERY;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getSelectPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, (String) null);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, null, excludedPropNames);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass, alias);
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
         * To generate {@code cql} part for the specified {@code cond} only.
         *
         * @param cond
         * @param entityClass
         * @return
         */
        public static CQLBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CQLBuilder instance = createInstance();

            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
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

            instance._op = OperationType.ADD;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

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

            instance._op = OperationType.ADD;
            instance._entityClass = entity.getClass();
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);

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

            instance._op = OperationType.ADD;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getInsertPropNamesByClass(entityClass, excludedPropNames);

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
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

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

            instance._op = OperationType.UPDATE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._tableName = getTableName(entityClass, NamingPolicy.LOWER_CASE_WITH_UNDERSCORE);
            instance._columnNames = getUpdatePropNamesByClass(entityClass, excludedPropNames);

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

            instance._op = OperationType.DELETE;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._columnNames = columnNames;

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

            instance._op = OperationType.DELETE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

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

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            N.checkArgNotNullOrEmpty(columnAliases, "columnAliases");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnAliases = columnAliases;

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

            instance._op = OperationType.QUERY;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getSelectPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, (String) null);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, null, excludedPropNames);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass, alias);
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
         * To generate {@code cql} part for the specified {@code cond} only.
         *
         * @param cond
         * @param entityClass
         * @return
         */
        public static CQLBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CQLBuilder instance = createInstance();

            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
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

            instance._op = OperationType.ADD;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

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

            instance._op = OperationType.ADD;
            instance._entityClass = entity.getClass();
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);

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

            instance._op = OperationType.ADD;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getInsertPropNamesByClass(entityClass, excludedPropNames);

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
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

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

            instance._op = OperationType.UPDATE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._tableName = getTableName(entityClass, NamingPolicy.UPPER_CASE_WITH_UNDERSCORE);
            instance._columnNames = getUpdatePropNamesByClass(entityClass, excludedPropNames);

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

            instance._op = OperationType.DELETE;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._columnNames = columnNames;

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

            instance._op = OperationType.DELETE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

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

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            N.checkArgNotNullOrEmpty(columnAliases, "columnAliases");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnAliases = columnAliases;

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

            instance._op = OperationType.QUERY;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getSelectPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, (String) null);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, null, excludedPropNames);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass, alias);
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
         * To generate {@code cql} part for the specified {@code cond} only.
         *
         * @param cond
         * @param entityClass
         * @return
         */
        public static CQLBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CQLBuilder instance = createInstance();

            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
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

            instance._op = OperationType.ADD;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

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

            instance._op = OperationType.ADD;
            instance._entityClass = entity.getClass();
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);

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

            instance._op = OperationType.ADD;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getInsertPropNamesByClass(entityClass, excludedPropNames);

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
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

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

            instance._op = OperationType.UPDATE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._tableName = getTableName(entityClass, NamingPolicy.LOWER_CAMEL_CASE);
            instance._columnNames = getUpdatePropNamesByClass(entityClass, excludedPropNames);

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

            instance._op = OperationType.DELETE;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._columnNames = columnNames;

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

            instance._op = OperationType.DELETE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

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

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            N.checkArgNotNullOrEmpty(columnAliases, "columnAliases");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnAliases = columnAliases;

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

            instance._op = OperationType.QUERY;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getSelectPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, (String) null);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, null, excludedPropNames);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass, alias);
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
         * To generate {@code cql} part for the specified {@code cond} only.
         *
         * @param cond
         * @param entityClass
         * @return
         */
        public static CQLBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CQLBuilder instance = createInstance();

            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
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

            instance._op = OperationType.ADD;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

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

            instance._op = OperationType.ADD;
            instance._entityClass = entity.getClass();
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);

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

            instance._op = OperationType.ADD;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getInsertPropNamesByClass(entityClass, excludedPropNames);

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
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

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

            instance._op = OperationType.UPDATE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._tableName = getTableName(entityClass, NamingPolicy.LOWER_CASE_WITH_UNDERSCORE);
            instance._columnNames = getUpdatePropNamesByClass(entityClass, excludedPropNames);

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

            instance._op = OperationType.DELETE;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._columnNames = columnNames;

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

            instance._op = OperationType.DELETE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

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

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            N.checkArgNotNullOrEmpty(columnAliases, "columnAliases");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnAliases = columnAliases;

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

            instance._op = OperationType.QUERY;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getSelectPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, (String) null);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, null, excludedPropNames);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass, alias);
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
         * To generate {@code cql} part for the specified {@code cond} only.
         *
         * @param cond
         * @param entityClass
         * @return
         */
        public static CQLBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CQLBuilder instance = createInstance();

            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
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

            instance._op = OperationType.ADD;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

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

            instance._op = OperationType.ADD;
            instance._entityClass = entity.getClass();
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);

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

            instance._op = OperationType.ADD;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getInsertPropNamesByClass(entityClass, excludedPropNames);

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
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

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

            instance._op = OperationType.UPDATE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._tableName = getTableName(entityClass, NamingPolicy.UPPER_CASE_WITH_UNDERSCORE);
            instance._columnNames = getUpdatePropNamesByClass(entityClass, excludedPropNames);

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

            instance._op = OperationType.DELETE;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._columnNames = columnNames;

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

            instance._op = OperationType.DELETE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

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

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            N.checkArgNotNullOrEmpty(columnAliases, "columnAliases");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnAliases = columnAliases;

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

            instance._op = OperationType.QUERY;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getSelectPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, (String) null);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, null, excludedPropNames);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass, alias);
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
         * To generate {@code cql} part for the specified {@code cond} only.
         *
         * @param cond
         * @param entityClass
         * @return
         */
        public static CQLBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CQLBuilder instance = createInstance();

            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
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

            instance._op = OperationType.ADD;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder insert(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param props
         * @return
         */
        public static CQLBuilder insert(final Map<String, Object> props) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

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

            instance._op = OperationType.ADD;
            instance._entityClass = entity.getClass();
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);

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

            instance._op = OperationType.ADD;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getInsertPropNamesByClass(entityClass, excludedPropNames);

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
         * @param tableName
         * @return
         */
        public static CQLBuilder update(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

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

            instance._op = OperationType.UPDATE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._tableName = getTableName(entityClass, NamingPolicy.LOWER_CAMEL_CASE);
            instance._columnNames = getUpdatePropNamesByClass(entityClass, excludedPropNames);

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

            instance._op = OperationType.DELETE;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder delete(final Collection<String> columnNames) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._columnNames = columnNames;

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

            instance._op = OperationType.DELETE;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param tableName
         * @return
         */
        public static CQLBuilder deleteFrom(final String tableName) {
            final CQLBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

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

        /**
         *
         * @param columnNames
         * @return
         */
        @SafeVarargs
        public static CQLBuilder select(final String... columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         *
         * @param columnNames
         * @return
         */
        public static CQLBuilder select(final Collection<String> columnNames) {
            N.checkArgNotNullOrEmpty(columnNames, "columnNames");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnNames = columnNames;

            return instance;
        }

        /**
         *
         * @param columnAliases
         * @return
         */
        public static CQLBuilder select(final Map<String, String> columnAliases) {
            N.checkArgNotNullOrEmpty(columnAliases, "columnAliases");

            final CQLBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._columnAliases = columnAliases;

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

            instance._op = OperationType.QUERY;
            instance._entityClass = entityClass;
            instance._propColumnNameMap = getProp2ColumnNameMap(instance._entityClass, instance._namingPolicy);
            instance._columnNames = getSelectPropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         *
         * @param entityClass
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, (String) null);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, null);
        }

        /**
         *
         * @param entityClass
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, null, excludedPropNames);
        }

        /**
         *
         * @param entityClass
         * @param alias
         * @param excludedPropNames
         * @return
         */
        public static CQLBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return select(entityClass, excludedPropNames).from(entityClass, alias);
        }
    }

    public static final class CP {
        public final String cql;
        public final List<Object> parameters;

        CP(final String cql, final List<Object> parameters) {
            this.cql = cql;
            this.parameters = ImmutableList.of(parameters);
        }

        @Override
        public int hashCode() {
            return N.hashCode(cql) * 31 + N.hashCode(parameters);
        }

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

        @Override
        public String toString() {
            return "{cql=" + cql + ", parameters=" + N.toString(parameters) + "}";
        }
    }
}
