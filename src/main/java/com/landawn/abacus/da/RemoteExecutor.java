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

package com.landawn.abacus.da;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.landawn.abacus.exception.AbacusException;
import com.landawn.abacus.http.ContentFormat;
import com.landawn.abacus.http.HttpClient;
import com.landawn.abacus.http.HttpSettings;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.PoolableWrapper;
import com.landawn.abacus.util.AddrUtil;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.DependencyFinder;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Pair;
import com.landawn.abacus.util.StringUtil;
import com.landawn.abacus.util.function.Predicate;

// TODO: Auto-generated Javadoc
/**
 * Execute the code/method on remote severs, without deploying changes to the target servers first.
 * It's required to deploy <code>com.landawn.abacus.da.http.JavaExecutionServlet</code> under Tomcat or other servlet containers on target servers first.
 * Here is the sample for web.xml to deploy the servlet under Tomcat:
 * 
 * <pre>
 *  {@code
 *     <servlet>
 *         <description>Hello javaExecution</description>
 *         <display-name>javaExecution</display-name>
 *         <servlet-name>javaExecution</servlet-name>
 *         <servlet-class>com.landawn.abacus.da.http.JavaExecutionServlet</servlet-class>
 *     </servlet>
 * 
 *     <servlet-mapping>
 *         <servlet-name>javaExecution</servlet-name>
 *         <url-pattern>/javaExecution/*</url-pattern>
 *     </servlet-mapping>
 * }
 * </pre>
 * Anonymous classes are supported except the ones created by lambda. For example:
 * <pre> 
 *    RemoteTask<?, Object> remoteTask = new RemoteTask<Object, Object>() {
 *      public Object run(Object t) {
 *          // doing something.
 *          return null;
 *      }
 *    };
 *    
 *    remoteExecutor.execute(remoteTask, param);
 * </pre>
 *
 * @author Haiyang Li
 * @since 0.8
 */
public final class RemoteExecutor {

    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(RemoteExecutor.class);

    /** The Constant filteredClassNameSet. */
    private static final Set<String> filteredClassNameSet = N.asSet();

    static {
        filteredClassNameSet.add("com.landawn.abacus.*");
        filteredClassNameSet.add("java.*");
        filteredClassNameSet.add("javax.*");
        filteredClassNameSet.add("org.apache.*");
        filteredClassNameSet.add("org.eclipse.*");
        filteredClassNameSet.add("org.junit.*");
        filteredClassNameSet.add("org.hamcrest.*");
        filteredClassNameSet.add("org.mockito.*");
        filteredClassNameSet.add("org.xerial.snappy.*");
        filteredClassNameSet.add("net.jpountz.lz4.*");
        filteredClassNameSet.add("org.joda.*");
        filteredClassNameSet.add("org.springframework.*");
        filteredClassNameSet.add("org.hibernate.*");
        filteredClassNameSet.add("org.elasticsearch.*");
        filteredClassNameSet.add("com.mysql.*");
        filteredClassNameSet.add("org.postgresql.*");
        filteredClassNameSet.add("oracle.jdbc.*");
        filteredClassNameSet.add("com.microsoft.sqlserver.*");
        filteredClassNameSet.add("com.ibm.db2.*");
        filteredClassNameSet.add("org.hsqldb.*");
        filteredClassNameSet.add("org.h2.*");
        filteredClassNameSet.add("com.mongodb.*");
        filteredClassNameSet.add("com.datastax.driver.*");
        filteredClassNameSet.add("com.couchbase.*");
        filteredClassNameSet.add("io.netty.*");
        filteredClassNameSet.add("com.fasterxml.*");
        filteredClassNameSet.add("net.spy.memcached.*");
        filteredClassNameSet.add("redis.clients.*");
        filteredClassNameSet.add("com.esotericsoftware.kryo.*");
        filteredClassNameSet.add("ch.qos.logback.*");
        filteredClassNameSet.add("org.slf4j.*");
        filteredClassNameSet.add("org.json.*");
        filteredClassNameSet.add("org.easymock.*");
        filteredClassNameSet.add("org.codehaus.*");
        filteredClassNameSet.add("org.aspectj.*");
        filteredClassNameSet.add("org.javassist.*");
        filteredClassNameSet.add("com.amazonaws.*");
        filteredClassNameSet.add("com.alibaba.fastjson.*");
        filteredClassNameSet.add("com.google.common.*"); // for Guava
        filteredClassNameSet.add("com.google.gson.*");
        filteredClassNameSet.add("com.google.appengine.*");
        filteredClassNameSet.add("com.google.inject.guice.*");
        filteredClassNameSet.add("com.google.protobuf.*");
    }

    /** The Constant classBytesMapPool. */
    private static final Map<Class<?>, LinkedHashMap<String, byte[]>> classBytesMapPool = new ConcurrentHashMap<>();

    /**
     * To set the global properties.
     */
    static final KeyedObjectPool<Object, PoolableWrapper<Object>> pool = PoolFactory.createKeyedObjectPool(8192);

    /** The Constant kryoParser. */
    static final KryoParser kryoParser = ParserFactory.isKryoAvailable() ? ParserFactory.createKryoParser() : null;

    /**
     * Get global property with the specified key.
     *
     * @param <T>
     * @param key
     * @return
     */
    public static <T> T getProperty(Object key) {
        final PoolableWrapper<Object> wrapper = pool.get(key);

        return wrapper == null ? null : (T) wrapper.value();
    }

    /**
     * Get global property with the specified key.
     *
     * @param <T>
     * @param targetClass
     * @param key
     * @return
     */
    public static <T> T getProperty(Class<T> targetClass, Object key) {
        final PoolableWrapper<Object> wrapper = pool.get(key);

        return N.convert(wrapper == null ? null : wrapper.value(), targetClass);
    }

    /**
     * Set global property with the specified key and value.
     *
     * @param key
     * @param value
     */
    public static void setProperty(Object key, Object value) {
        pool.put(key, PoolableWrapper.of(value));
    }

    /**
     * Set global property with the specified key and value.
     *
     * @param key
     * @param value
     * @param liveTime
     * @param maxIdleTime
     */
    public static void setProperty(Object key, Object value, long liveTime, long maxIdleTime) {
        pool.put(key, PoolableWrapper.of(value, liveTime, maxIdleTime));
    }

    /**
     * remove global property with the specified key and value.
     *
     * @param key
     * @return
     */
    public static Object removeProperty(Object key) {
        return pool.remove(key);
    }

    /** The http clients. */
    private final List<HttpClient> httpClients;

    /** The async executor. */
    private final AsyncExecutor asyncExecutor;

    /** The class namefilter. */
    private final Predicate<? super String> classNamefilter;

    /**
     * Instantiates a new remote executor.
     *
     * @param servers a string containing whitespace or comma separated host or IP addresses and port numbers of the form
     * "host:port host2:port" or "host:port, host2:port"
     */
    public RemoteExecutor(String servers) {
        this(servers, null);
    }

    /**
     * Instantiates a new remote executor.
     *
     * @param servers a string containing whitespace or comma separated host or IP addresses and port numbers of the form
     * "host:port host2:port" or "host:port, host2:port"
     * @param classNamefilter
     */
    public RemoteExecutor(String servers, Predicate<? super String> classNamefilter) {
        this(AddrUtil.getServerList(servers), classNamefilter);
    }

    /**
     * Instantiates a new remote executor.
     *
     * @param servers
     */
    public RemoteExecutor(Collection<String> servers) {
        this(servers, null);
    }

    /**
     * Instantiates a new remote executor.
     *
     * @param servers
     * @param classNamefilter
     */
    public RemoteExecutor(final Collection<String> servers, final Predicate<? super String> classNamefilter) {
        httpClients = new ArrayList<>(servers.size());

        for (String server : servers) {
            httpClients.add(HttpClient.create(server, HttpClient.DEFAULT_MAX_CONNECTION, Integer.MAX_VALUE, Integer.MAX_VALUE));
        }

        asyncExecutor = new AsyncExecutor(Math.min(8, servers.size()), servers.size(), 180L, TimeUnit.SECONDS);

        this.classNamefilter = new Predicate<String>() {
            @Override
            public boolean test(String value) {
                if (classNamefilter != null && classNamefilter.test(value)) {
                    return true;
                }

                for (String clsName : filteredClassNameSet) {
                    if (value.matches(clsName)) {
                        return true;
                    }
                }

                return false;
            }
        };
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @return
     */
    public <T> List<RemoteExecutionResponse> execute(final Class<? extends RemoteTask<?, T>> remoteTask) {
        return execute(remoteTask, (Object) null);
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param parameter it must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @return
     */
    public <T> List<RemoteExecutionResponse> execute(final Class<? extends RemoteTask<?, T>> remoteTask, final Object parameter) {
        return execute(remoteTask, parameter, null, Long.MAX_VALUE, null);
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param parameter it must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @param httpSettings refer to com.landawn.abacus.http.AbstractHttpClient.HttpSettings
     * @param totalExecutionTimeout
     * @param serverFilter filter the servers by the input parameter or other logic. There are two elements in the array. left is the target server url, right is the input parameter.
     * @return
     * @see com.landawn.abacus.http.HttpSettings
     */
    public <T> List<RemoteExecutionResponse> execute(final Class<? extends RemoteTask<?, T>> remoteTask, final Object parameter,
            final HttpSettings httpSettings, final long totalExecutionTimeout, final Predicate<Pair<String, Object>> serverFilter) {
        return execute(remoteTask, parameter, null, httpSettings, totalExecutionTimeout, serverFilter);
    }

    /**
     * Execute the task on different remote servers in parallel with different element in <code>parameters</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param serverParameterMapper the key is the target host/url and the value must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @return
     */
    public <T> List<RemoteExecutionResponse> execute(final Class<? extends RemoteTask<?, T>> remoteTask, final Map<String, ?> serverParameterMapper) {
        return execute(remoteTask, serverParameterMapper, null, Long.MAX_VALUE);
    }

    /**
     * Execute the task on different remote servers in parallel with different element in <code>parameters</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param serverParameterMapper the key is the target host/url and the value must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @param httpSettings refer to com.landawn.abacus.http.AbstractHttpClient.HttpSettings
     * @param totalExecutionTimeout
     * @return
     * @see com.landawn.abacus.http.HttpSettings
     */
    public <T> List<RemoteExecutionResponse> execute(final Class<? extends RemoteTask<?, T>> remoteTask, final Map<String, ?> serverParameterMapper,
            final HttpSettings httpSettings, final long totalExecutionTimeout) {
        return execute(remoteTask, null, serverParameterMapper, httpSettings, totalExecutionTimeout, null);
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask must be equals to the instance obtained by remoteTask.getClass().newInstance(). That's to say no extra properties has been set after the object was created.
     * @return
     */
    public <T> List<RemoteExecutionResponse> execute(final RemoteTask<?, T> remoteTask) {
        return execute(remoteTask, (Object) null);
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask must be equals to the instance obtained by remoteTask.getClass().newInstance(). That's to say no extra properties has been set after the object was created.
     * @param parameter it must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @return
     */
    public <T> List<RemoteExecutionResponse> execute(final RemoteTask<?, T> remoteTask, final Object parameter) {
        return execute(remoteTask, parameter, null, Long.MAX_VALUE, null);
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask must be equals to the instance obtained by remoteTask.getClass().newInstance(). That's to say no extra properties has been set after the object was created.
     * @param parameter it must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @param httpSettings refer to com.landawn.abacus.http.AbstractHttpClient.HttpSettings
     * @param totalExecutionTimeout
     * @param serverFilter filter the servers by the input parameter or other logic. There are two elements in the array. left is the target server url, right is the input parameter.
     * @return
     * @see com.landawn.abacus.http.HttpSettings
     */
    public <T> List<RemoteExecutionResponse> execute(final RemoteTask<?, T> remoteTask, final Object parameter, final HttpSettings httpSettings,
            final long totalExecutionTimeout, final Predicate<Pair<String, Object>> serverFilter) {
        return execute((Class<? extends RemoteTask<?, T>>) remoteTask.getClass(), parameter, httpSettings, totalExecutionTimeout, serverFilter);
    }

    /**
     * Execute the task on different remote servers in parallel with different element in <code>parameters</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask must be equals to the instance obtained by remoteTask.getClass().newInstance(). That's to say no extra properties has been set after the object was created.
     * @param serverParameterMapper the key is the target host/url and the value must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @return
     */
    public <T> List<RemoteExecutionResponse> execute(final RemoteTask<?, T> remoteTask, final Map<String, ?> serverParameterMapper) {
        return execute(remoteTask, serverParameterMapper, null, Long.MAX_VALUE);
    }

    /**
     * Execute the task on different remote servers in parallel with different element in <code>parameters</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask must be equals to the instance obtained by remoteTask.getClass().newInstance(). That's to say no extra properties has been set after the object was created.
     * @param serverParameterMapper the key is the target host/url and the value must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     * @param httpSettings refer to com.landawn.abacus.http.AbstractHttpClient.HttpSettings
     * @param totalExecutionTimeout
     * @return
     * @see com.landawn.abacus.http.HttpSettings
     */
    public <T> List<RemoteExecutionResponse> execute(final RemoteTask<?, T> remoteTask, final Map<String, ?> serverParameterMapper,
            final HttpSettings httpSettings, final long totalExecutionTimeout) {
        return execute((Class<? extends RemoteTask<?, T>>) remoteTask.getClass(), serverParameterMapper, httpSettings, totalExecutionTimeout);
    }

    /**
     * Execute.
     *
     * @param <T>
     * @param remoteTask
     * @param parameter
     * @param serverParameterMapper
     * @param httpSettings
     * @param totalExecutionTimeout
     * @param serverFilter
     * @return
     */
    private <T> List<RemoteExecutionResponse> execute(final Class<? extends RemoteTask<?, T>> remoteTask, final Object parameter,
            final Map<String, ?> serverParameterMapper, final HttpSettings httpSettings, final long totalExecutionTimeout,
            final Predicate<Pair<String, Object>> serverFilter) {
        final long startTime = System.currentTimeMillis();

        final List<ContinuableFuture<RemoteExecutionResponse>> futureResponses = asyncExecute(remoteTask, parameter, serverParameterMapper, httpSettings,
                serverFilter);

        final List<RemoteExecutionResponse> result = new ArrayList<>(futureResponses.size());
        RemoteExecutionResponse response = null;

        for (Future<RemoteExecutionResponse> futureResponse : futureResponses) {
            final long remainingExecutionTime = totalExecutionTimeout - (System.currentTimeMillis() - startTime);

            try {
                response = futureResponse.get(remainingExecutionTime > 0 ? remainingExecutionTime : 1, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                response = new RemoteExecutionResponse();
                response.setErrorCode(getClassName(e.getClass()));
                response.setErrorMessage(e.getMessage());

                response.setRequestHost(IOUtil.HOST_NAME);
                response.setRequestTime(startTime);
                response.setElapsedTime(System.currentTimeMillis() - startTime);
            }

            result.add(response);
        }

        return result;
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @return
     */
    public <T> List<ContinuableFuture<RemoteExecutionResponse>> asyncExecute(final Class<? extends RemoteTask<?, T>> remoteTask) {
        return asyncExecute(remoteTask, (Object) null);
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param parameter it must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @return
     */
    public <T> List<ContinuableFuture<RemoteExecutionResponse>> asyncExecute(final Class<? extends RemoteTask<?, T>> remoteTask, final Object parameter) {
        return asyncExecute(remoteTask, parameter, null, null);
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param parameter it must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @param httpSettings refer to com.landawn.abacus.http.AbstractHttpClient.HttpSettings
     * @param serverFilter filter the servers by the input parameter or other logic. There are two elements in the array. left is the target server url, right is the input parameter.
     * @return
     * @see com.landawn.abacus.http.HttpSettings
     */
    public <T> List<ContinuableFuture<RemoteExecutionResponse>> asyncExecute(final Class<? extends RemoteTask<?, T>> remoteTask, final Object parameter,
            final HttpSettings httpSettings, final Predicate<Pair<String, Object>> serverFilter) {
        return asyncExecute(remoteTask, parameter, null, httpSettings, serverFilter);
    }

    /**
     * Execute the task on different remote servers in parallel with different element in <code>parameters</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param serverParameterMapper the key is the target host/url and the value must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @return
     */
    public <T> List<ContinuableFuture<RemoteExecutionResponse>> asyncExecute(final Class<? extends RemoteTask<?, T>> remoteTask,
            final Map<String, ?> serverParameterMapper) {
        return asyncExecute(remoteTask, serverParameterMapper, null);
    }

    /**
     * Execute the task on different remote servers in parallel with different element in <code>parameters</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param serverParameterMapper the key is the target host/url and the value must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @param httpSettings refer to com.landawn.abacus.http.AbstractHttpClient.HttpSettings
     * @return
     * @see com.landawn.abacus.http.HttpSettings
     */
    public <T> List<ContinuableFuture<RemoteExecutionResponse>> asyncExecute(final Class<? extends RemoteTask<?, T>> remoteTask,
            final Map<String, ?> serverParameterMapper, final HttpSettings httpSettings) {
        return asyncExecute(remoteTask, null, serverParameterMapper, httpSettings, null);
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @return
     */
    public <T> List<ContinuableFuture<RemoteExecutionResponse>> asyncExecute(final RemoteTask<?, T> remoteTask) {
        return asyncExecute(remoteTask, (Object) null);
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param parameter it must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @return
     */
    public <T> List<ContinuableFuture<RemoteExecutionResponse>> asyncExecute(final RemoteTask<?, T> remoteTask, final Object parameter) {
        return asyncExecute(remoteTask, parameter, null, null);
    }

    /**
     * Execute the task on different remote servers in parallel with <code>parameter</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param parameter it must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @param httpSettings refer to com.landawn.abacus.http.AbstractHttpClient.HttpSettings
     * @param serverFilter filter the servers by the input parameter or other logic. There are two elements in the array. left is the target server url, right is the input parameter.
     * @return
     * @see com.landawn.abacus.http.HttpSettings
     */
    public <T> List<ContinuableFuture<RemoteExecutionResponse>> asyncExecute(final RemoteTask<?, T> remoteTask, final Object parameter,
            final HttpSettings httpSettings, final Predicate<Pair<String, Object>> serverFilter) {
        return asyncExecute((Class<? extends RemoteTask<?, T>>) remoteTask.getClass(), parameter, httpSettings, serverFilter);
    }

    /**
     * Execute the task on different remote servers in parallel with different element in <code>parameters</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param serverParameterMapper the key is the target host/url and the value must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @return
     */
    public <T> List<ContinuableFuture<RemoteExecutionResponse>> asyncExecute(final RemoteTask<?, T> remoteTask, final Map<String, ?> serverParameterMapper) {
        return asyncExecute(remoteTask, serverParameterMapper, null);
    }

    /**
     * Execute the task on different remote servers in parallel with different element in <code>parameters</code>.
     * The types of parameter/return value only can be the classes exists in JDK or common libraries deployed on target servers.
     * It's one of the best practices to transfer big request parameter or response result by <code>DataSet</code>
     *
     * @param <T>
     * @param remoteTask which has and only has the default constructor
     * @param serverParameterMapper the key is the target host/url and the value must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     * @param httpSettings refer to com.landawn.abacus.http.AbstractHttpClient.HttpSettings
     * @return
     * @see com.landawn.abacus.http.HttpSettings
     */
    public <T> List<ContinuableFuture<RemoteExecutionResponse>> asyncExecute(final RemoteTask<?, T> remoteTask, final Map<String, ?> serverParameterMapper,
            final HttpSettings httpSettings) {
        return asyncExecute((Class<? extends RemoteTask<?, T>>) remoteTask.getClass(), serverParameterMapper, httpSettings);
    }

    /**
     * Async execute.
     *
     * @param <T>
     * @param remoteTask
     * @param parameter
     * @param serverParameterMapper
     * @param httpSettings
     * @param serverFilter
     * @return
     */
    @SuppressWarnings("deprecation")
    private <T> List<ContinuableFuture<RemoteExecutionResponse>> asyncExecute(final Class<? extends RemoteTask<?, T>> remoteTask, final Object parameter,
            final Map<String, ?> serverParameterMapper, final HttpSettings httpSettings, final Predicate<Pair<String, Object>> serverFilter) {
        if (kryoParser == null) {
            throw new AbacusException("Kryo libraries are required");
        }

        final HttpSettings newHttpSettings = httpSettings == null ? HttpSettings.create() : httpSettings.copy();

        if (newHttpSettings.getContentFormat() == null || newHttpSettings.getContentFormat() == ContentFormat.NONE) {
            newHttpSettings.setContentFormat(ContentFormat.JSON);
        }

        if (!(newHttpSettings.getContentFormat() == ContentFormat.JSON || newHttpSettings.getContentFormat() == ContentFormat.KRYO)) {
            throw new IllegalArgumentException("Only format JSON/Kryo is supported");
        }

        final RemoteExecutionRequest request0 = createRemoteRequest(remoteTask);
        if (serverParameterMapper == null) {
            request0.setParameter(parameter);
        }

        if (logger.isInfoEnabled()) {
            logger.info("Classes: " + request0.getClassBytesMap().keySet() + " will be transferred to target servers for execution");
        }

        final List<ContinuableFuture<RemoteExecutionResponse>> futureResponses = new ArrayList<>(httpClients.size());

        String paramKey = null;
        for (int i = 0, len = httpClients.size(); i < len; i++) {
            final HttpClient httpClient = httpClients.get(i);

            if (serverParameterMapper == null) {
                if (serverFilter != null && serverFilter.test(Pair.of(httpClient.url(), parameter)) == false) {
                    continue;
                }

            } else {
                for (String key : serverParameterMapper.keySet()) {
                    if (httpClient.url().equals(key) || httpClient.url().contains(key) || StringUtil.containsIgnoreCase(httpClient.url(), key)) {
                        paramKey = key;

                        break;
                    }
                }

                if (paramKey == null) {
                    continue;
                }
            }

            final RemoteExecutionRequest request = paramKey == null ? request0 : request0.copy().setParameter(serverParameterMapper.get(paramKey));

            final Callable<RemoteExecutionResponse> cmd = new Callable<RemoteExecutionResponse>() {
                @Override
                public RemoteExecutionResponse call() throws Exception {
                    final RemoteExecutionResponse result = httpClient.post(RemoteExecutionResponse.class, request, newHttpSettings);

                    result.setRequestHost(request.getRequestHost());
                    result.setRequestTime(request.getRequestTime());
                    result.setElapsedTime(System.currentTimeMillis() - request.getRequestTime());

                    return result;
                }
            };

            if (logger.isInfoEnabled()) {
                logger.info("Starting task on remote server: " + httpClient.url());
            }

            futureResponses.add(asyncExecutor.execute(cmd));
        }

        return futureResponses;
    }

    /**
     * Creates the remote request.
     *
     * @param remoteTask
     * @return
     */
    @SuppressWarnings("deprecation")
    private RemoteExecutionRequest createRemoteRequest(final Class<? extends RemoteTask<?, ?>> remoteTask) {
        final RemoteExecutionRequest request = new RemoteExecutionRequest();

        request.setClassName(getClassName(remoteTask));

        request.setRequestHost(IOUtil.HOST_NAME);
        request.setRequestTime(System.currentTimeMillis());

        request.setClassBytesMap(getClassDependency(remoteTask));

        return request;
    }

    //    @SuppressWarnings("deprecation")
    //    private RemoteExecutionRequest<Object> setRequestParameter(final RemoteExecutionRequest<Object> request, Object parameter) {
    //        if (parameter == null) {
    //            request.setParameterType(null);
    //            request.setParameterString(null);
    //        } else {
    //            final Type<Object> paramType = getType(parameter);
    //
    //            request.setParameterType(paramType.getName());
    //            request.setParameterString(paramType.stringOf(parameter));
    //        }
    //
    //        return request;
    //    }

    /**
     * Gets the class dependency.
     *
     * @param classToCheck
     * @return
     */
    private LinkedHashMap<String, byte[]> getClassDependency(final Class<?> classToCheck) {
        LinkedHashMap<String, byte[]> classBytesMap = classBytesMapPool.get(classToCheck);

        if (classBytesMap == null) {
            final List<Class<?>> dependencies = new ArrayList<>(DependencyFinder.getDependenciesRecursively(classToCheck, classNamefilter));
            N.reverse(dependencies);
            classBytesMap = new LinkedHashMap<>(dependencies.size() + 1);

            for (Class<?> cls : dependencies) {
                classBytesMap.put(getClassName(cls), readClass(cls));
            }

            final String clsName = getClassName(classToCheck);

            if (classNamefilter.test(clsName) == false) {
                classBytesMap.put(getClassName(classToCheck), readClass(classToCheck));
            }

            classBytesMapPool.put(classToCheck, classBytesMap);
        }

        return classBytesMap;
    }

    /**
     * Read class.
     *
     * @param cls
     * @return
     */
    private static byte[] readClass(Class<?> cls) {
        InputStream is = null;

        try {
            String clsName = getClassName(cls);
            int lastIndex = clsName.lastIndexOf('.');
            is = cls.getResourceAsStream((lastIndex < 0 ? clsName : clsName.substring(lastIndex + 1)) + ".class");

            return IOUtil.readBytes(is);
        } finally {
            IOUtil.close(is);
        }
    }

    /**
     * Gets the class name.
     *
     * @param cls
     * @return
     */
    private static String getClassName(final Class<?> cls) {
        return cls.getName();
    }
}
