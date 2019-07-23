/*
 * Copyright (C) 2016 HaiYang Li
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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.neo4j.ogm.cypher.Filter;
import org.neo4j.ogm.cypher.Filters;
import org.neo4j.ogm.cypher.query.Pagination;
import org.neo4j.ogm.cypher.query.SortOrder;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;

import com.landawn.abacus.util.stream.Stream;

// TODO: Auto-generated Javadoc
/**
 * It's a simple wrapper of Neo4j Java client.
 * Refer to: http://neo4j.com/docs/ogm/java/stable/ 
 *
 * @author HaiYang Li
 * @since 0.8
 */
public final class Neo4jExecutor {

    /** The session pool. */
    private final LinkedBlockingQueue<Session> sessionPool = new LinkedBlockingQueue<Session>(8192);

    /** The session factory. */
    private final SessionFactory sessionFactory;

    /**
     * Instantiates a new neo 4 j executor.
     *
     * @param sessionFactory the session factory
     */
    public Neo4jExecutor(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    /**
     * Session factory.
     *
     * @return the session factory
     */
    public SessionFactory sessionFactory() {
        return sessionFactory;
    }

    /**
     * Open session.
     *
     * @return the session
     */
    public Session openSession() {
        return sessionFactory.openSession();
    }

    /**
     * Load.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @return the t
     */
    public <T> T load(Class<T> targetClass, Long id) {
        final Session session = getSession();

        try {
            return session.load(targetClass, id);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param id the id
     * @param depth the depth
     * @return the t
     */
    public <T> T load(Class<T> targetClass, Long id, int depth) {
        final Session session = getSession();

        try {
            return session.load(targetClass, id, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param ids the ids
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Collection<Long> ids) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param ids the ids
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Collection<Long> ids, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param ids the ids
     * @param sortOrder the sort order
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Collection<Long> ids, SortOrder sortOrder) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, sortOrder);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param ids the ids
     * @param sortOrder the sort order
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Collection<Long> ids, SortOrder sortOrder, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, sortOrder, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param ids the ids
     * @param pagination the pagination
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Collection<Long> ids, Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param ids the ids
     * @param pagination the pagination
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Collection<Long> ids, Pagination pagination, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param ids the ids
     * @param sortOrder the sort order
     * @param pagination the pagination
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Collection<Long> ids, SortOrder sortOrder, Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, sortOrder, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param ids the ids
     * @param sortOrder the sort order
     * @param pagination the pagination
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Collection<Long> ids, SortOrder sortOrder, Pagination pagination, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, ids, sortOrder, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param objects the objects
     * @return the collection
     */
    public <T> Collection<T> loadAll(Collection<T> objects) {
        final Session session = getSession();

        try {
            return session.loadAll(objects);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param objects the objects
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Collection<T> objects, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param objects the objects
     * @param sortOrder the sort order
     * @return the collection
     */
    public <T> Collection<T> loadAll(Collection<T> objects, SortOrder sortOrder) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, sortOrder);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param objects the objects
     * @param sortOrder the sort order
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Collection<T> objects, SortOrder sortOrder, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, sortOrder, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param objects the objects
     * @param pagination the pagination
     * @return the collection
     */
    public <T> Collection<T> loadAll(Collection<T> objects, Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param objects the objects
     * @param pagination the pagination
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Collection<T> objects, Pagination pagination, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param objects the objects
     * @param sortOrder the sort order
     * @param pagination the pagination
     * @return the collection
     */
    public <T> Collection<T> loadAll(Collection<T> objects, SortOrder sortOrder, Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, sortOrder, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param objects the objects
     * @param sortOrder the sort order
     * @param pagination the pagination
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Collection<T> objects, SortOrder sortOrder, Pagination pagination, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(objects, sortOrder, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param sortOrder the sort order
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, SortOrder sortOrder) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, sortOrder);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param sortOrder the sort order
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, SortOrder sortOrder, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, sortOrder, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param pagination the pagination
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param pagination the pagination
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Pagination pagination, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param sortOrder the sort order
     * @param pagination the pagination
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, SortOrder sortOrder, Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, sortOrder, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param sortOrder the sort order
     * @param pagination the pagination
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, SortOrder sortOrder, Pagination pagination, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, sortOrder, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filter filter) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filter filter, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sortOrder the sort order
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filter filter, SortOrder sortOrder) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, sortOrder);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sortOrder the sort order
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filter filter, SortOrder sortOrder, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, sortOrder, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param pagination the pagination
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filter filter, Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param pagination the pagination
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filter filter, Pagination pagination, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sortOrder the sort order
     * @param pagination the pagination
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filter filter, SortOrder sortOrder, Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, sortOrder, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filter the filter
     * @param sortOrder the sort order
     * @param pagination the pagination
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filter filter, SortOrder sortOrder, Pagination pagination, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filter, sortOrder, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filters the filters
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filters filters) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filters the filters
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filters filters, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filters the filters
     * @param sortOrder the sort order
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filters filters, SortOrder sortOrder) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, sortOrder);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filters the filters
     * @param sortOrder the sort order
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filters filters, SortOrder sortOrder, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, sortOrder, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filters the filters
     * @param pagination the pagination
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filters filters, Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filters the filters
     * @param pagination the pagination
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filters filters, Pagination pagination, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filters the filters
     * @param sortOrder the sort order
     * @param pagination the pagination
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filters filters, SortOrder sortOrder, Pagination pagination) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, sortOrder, pagination);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Load all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     * @param filters the filters
     * @param sortOrder the sort order
     * @param pagination the pagination
     * @param depth the depth
     * @return the collection
     */
    public <T> Collection<T> loadAll(Class<T> targetClass, Filters filters, SortOrder sortOrder, Pagination pagination, int depth) {
        final Session session = getSession();

        try {
            return session.loadAll(targetClass, filters, sortOrder, pagination, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Save.
     *
     * @param <T> the generic type
     * @param object the object
     */
    public <T> void save(T object) {
        final Session session = getSession();

        try {
            session.save(object);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Save.
     *
     * @param <T> the generic type
     * @param object the object
     * @param depth the depth
     */
    public <T> void save(T object, int depth) {
        final Session session = getSession();

        try {
            session.save(object, depth);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Delete.
     *
     * @param <T> the generic type
     * @param object the object
     */
    public <T> void delete(T object) {
        final Session session = getSession();

        try {
            session.delete(object);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Delete all.
     *
     * @param <T> the generic type
     * @param targetClass the target class
     */
    public <T> void deleteAll(Class<T> targetClass) {
        final Session session = getSession();

        try {
            session.deleteAll(targetClass);
        } finally {
            closeSession(session);
        }

    }

    /**
     * Query for object.
     *
     * @param <T> the generic type
     * @param objectType the object type
     * @param cypher the cypher
     * @param parameters the parameters
     * @return the t
     */
    public <T> T queryForObject(Class<T> objectType, String cypher, Map<String, ?> parameters) {
        final Session session = getSession();

        try {
            return session.queryForObject(objectType, cypher, parameters);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Query.
     *
     * @param cypher the cypher
     * @param parameters the parameters
     * @return the stream
     */
    public Stream<Map<String, Object>> query(String cypher, Map<String, ?> parameters) {
        final Session session = getSession();

        return Stream.of(session.query(cypher, parameters).iterator()).onClose(newCloseHandle(session));
    }

    /**
     * Query.
     *
     * @param cypher the cypher
     * @param parameters the parameters
     * @param readOnly the read only
     * @return the stream
     */
    public Stream<Map<String, Object>> query(String cypher, Map<String, ?> parameters, boolean readOnly) {
        final Session session = getSession();

        return Stream.of(session.query(cypher, parameters, readOnly).iterator()).onClose(newCloseHandle(session));
    }

    /**
     * Query.
     *
     * @param <T> the generic type
     * @param objectType the object type
     * @param cypher the cypher
     * @param parameters the parameters
     * @return the stream
     */
    public <T> Stream<T> query(Class<T> objectType, String cypher, Map<String, ?> parameters) {
        final Session session = getSession();

        return Stream.of(session.query(objectType, cypher, parameters).iterator()).onClose(newCloseHandle(session));
    }

    /**
     * New close handle.
     *
     * @param session the session
     * @return the runnable
     */
    private Runnable newCloseHandle(final Session session) {
        return new Runnable() {
            @Override
            public void run() {
                closeSession(session);
            }
        };
    }

    /**
     * Count entities of type.
     *
     * @param entity the entity
     * @return the long
     */
    public long countEntitiesOfType(Class<?> entity) {
        final Session session = getSession();

        try {
            return session.countEntitiesOfType(entity);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Resolve graph id for.
     *
     * @param possibleEntity the possible entity
     * @return the long
     */
    public Long resolveGraphIdFor(Object possibleEntity) {
        final Session session = getSession();

        try {
            return session.resolveGraphIdFor(possibleEntity);
        } finally {
            closeSession(session);
        }
    }

    /**
     * Gets the session.
     *
     * @return the session
     */
    private Session getSession() {
        Session session = null;

        try {
            session = sessionPool.poll(100, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // ignore.
        }

        if (session == null) {
            session = openSession();
        }

        return session;
    }

    /**
     * Close session.
     *
     * @param session the session
     */
    private void closeSession(Session session) {
        if (session != null) {
            try {
                sessionPool.offer(session, 100, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                // ignore.
            }
        }
    }

}
