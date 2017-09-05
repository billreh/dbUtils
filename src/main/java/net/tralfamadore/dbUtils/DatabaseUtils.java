package net.tralfamadore.dbUtils;

import net.tralfamadore.*;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import javax.persistence.Entity;
import javax.persistence.MappedSuperclass;
import javax.persistence.Query;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Class: DbUtils
 * Created by billreh on 7/22/17.
 * @author wreh
 */
public class DatabaseUtils {
    /** The query cache */
    private static final Map<String,Tuple4<Long,Long,TimeUnit,Object>> cache = new HashMap<>();
    /** The session factory */
    private static SessionFactory sessionFactory;
    /** The type of time unit to use for caching */
    private TimeUnit cacheTimeUnit;
    /** The number of time units to cache for */
    private long cacheTime;
    /** Set to tru to ignore cache */
    private boolean nocache = false;
    /** The sql */
    private String sql;
    /** THe bind variables for the sql */
    private List<Object> bindVars = new ArrayList<>();
    /** The session */
    private Session session;
    /** Whether or not we're in a transaction */
    private boolean inTransaction;

    /**
     * Get the session factory.
     * @return The session factory.
     */
    private static SessionFactory sessionFactory() {
        if(sessionFactory == null)
            bootstrap();
        return sessionFactory;
    }

    /**
     * Get the session.
     * @return The session.
     */
    private Session session() {
        if(session == null)
            session = sessionFactory().openSession();
        return session;
    }

    /**
     * Bootstrap the db connection.
     */
    private static void bootstrap() {
        if(sessionFactory != null)
            return;
        synchronized (Bootstrap.class) {
            Configuration cfg = new Configuration();
            cfg.setProperty("hibernate.connection.username", "root");
            cfg.setProperty("hibernate.connection.password", "root");
            cfg.setProperty("hibernate.connection.url", "jdbc:mysql://localhost:3306/test");
            hibernateAnnotatedClasses().forEach(cfg::addAnnotatedClass);
            sessionFactory = cfg.buildSessionFactory(
                    new StandardServiceRegistryBuilder()
                            .applySettings(cfg.getProperties())
                            .build());
        }
    }

    /**
     * Get a list of hibernate annotated classes.
     * @return A list of hibernate annotated classes.
     */
    private static synchronized List<Class<?>> hibernateAnnotatedClasses(){
        List<Class <?>> hibernateAnnotatedClasses = new ArrayList<>();

        List<ClassLoader> classLoadersList = new LinkedList<>();
        classLoadersList.add(ClasspathHelper.contextClassLoader());
        classLoadersList.add(ClasspathHelper.staticClassLoader());
        classLoadersList.add(Bootstrap.class.getClassLoader());


        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(new SubTypesScanner(false /* don't exclude Object.class */), new ResourcesScanner())
                .setUrls(ClasspathHelper.forClassLoader(classLoadersList.toArray(new ClassLoader[0])))
                // TODO if we need to go down to the beans folder we have to figure out how to get the project name,
                .filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix("net.tralfamadore"))));

        Set<Class<?>> allClasses = reflections.getSubTypesOf(Object.class);

        for(Class<?> clazz : allClasses) {
            if (clazz.isAnnotationPresent(Entity.class) || clazz.isAnnotationPresent(MappedSuperclass.class)){
                hibernateAnnotatedClasses.add(clazz);
            }
        }

        return hibernateAnnotatedClasses;
    }

    /**
     * Shut it down.
     */
    synchronized static public void shutdown() {
        sessionFactory().close();
        sessionFactory = null;
    }

    /**
     * Get a {@link TableDescription} for the given table.
     * @param tableName The table name.
     * @return The {@link TableDescription} for the given table name.
     */
    public TableDescription getTableDescription(String tableName) {
        return getTableDescription(tableName, null);
    }

    /**
     * Get a {@link TableDescription} for the given table.
     * @param tableName The table name.
     * @param schemaName The schema name.
     * @return The {@link TableDescription} for the given table and schema names.
     */
    public TableDescription getTableDescription(String tableName, String schemaName) {
        try (Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> TableDescription.getTableDescription(connection, tableName, schemaName));
        }
    }

    /**
     * Get a list of table names matching <code>tableNamePattern</code>.
     * @param tableNamePattern The table name pattern to search on.  Table name pattern is in the form of a SQL LIKE
     *                         statement i.e. "%_LOG" matches all tables ending in _LOG.  May be null (searches all tables).
     * @return A list of table names that match the table name pattern.
     */
    public List<String> getTableNames(String tableNamePattern) {
        return getTableNames(null, tableNamePattern);
    }

    /**
     * Get a list of table names matching <code>tableNamePattern</code>.
     * @param schemaName The schema name to search in.  May be null (searches all schemas).
     * @param tableNamePattern The table name pattern to search on.  Table name pattern is in the form of a SQL LIKE
     *                         statement i.e. "%_LOG" matches all tables ending in _LOG.  May be null (searches all tables).
     * @return A list of table names that match the table name pattern.
     */
    public List<String> getTableNames(String schemaName, String tableNamePattern) {
        try(Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> getTableNames(connection, tableNamePattern, schemaName));
        }
    }

    /**
     * Get a list of schema names matching the name pattern.
     * @param schemaNamePattern A pattern in the style of sql LIKE, ie '%TEST%' would get all schemas with the word
     *                          TEST in them.  Can be null (lists all schemas).
     * @return A list of schema names.
     */
    public List<String> getSchemaNames(String schemaNamePattern) {
        try(Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> getSchemaNames(connection, schemaNamePattern));
        }
    }

    /**
     * Get a list of all catalog names.
     * @return A list of all catalog names.
     */
    public List<String> getCatalogNames() {
        try(Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(DatabaseUtils::getCatalogNames);
        }
    }

    /**
     * Cache this query and its results.
     * @param cacheTime The anount of time units to cache for.
     * @param cacheTimeUnit The time unit (millis, seconds, etc) to use.
     * @return The calling object.
     */
    public DatabaseUtils cache(long cacheTime, TimeUnit cacheTimeUnit) {
        nocache = false;
        this.cacheTime = cacheTime;
        this.cacheTimeUnit = cacheTimeUnit;
        return this;
    }

    /**
     * Ignore cached results.
     * @return The calling object.
     */
    public DatabaseUtils nocache() {
        nocache = true;
        cacheTime = 0;
        cacheTimeUnit = null;
        return this;
    }

    /**
     * Set the sql to use.
     * @param sql The sql to use.
     * @return The calling object.
     */
    public DatabaseUtils sql(String sql) {
        this.sql = sql;
        return this;
    }

    /**
     * Add a bind variable.
     * @param bindVar The bind variable to add.
     * @return The calling object.
     */
    public DatabaseUtils bindVar(Object bindVar) {
        bindVars.add(bindVar);
        return this;
    }

    /**
     * Add bind variables.
     * @param bindVars A list of bind variables.
     * @return The calling object.
     */
    public DatabaseUtils bindVars(List<Object> bindVars) {
        this.bindVars = bindVars;
        return this;
    }

    /**
     * Add bind variables.
     * @param bindVars A list of bind variables.
     * @return The calling object.
     */
    public DatabaseUtils bindVars(Object... bindVars) {
        return bindVars(bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars));
    }

    /**
     * Execute the sql.
     * @return The number of rows updated or inserted.
     */
    public int execute() {
        Throwable t = null;
        try {
            if (!inTransaction)
                session().beginTransaction();
            Query query = session().createNativeQuery(sql);
            for (int i = 0; i < bindVars.size(); i++) {
                query.setParameter(i + 1, bindVars.get(i));
            }
            return query.executeUpdate();
        } catch (Exception e) {
            t = e;
            if(!inTransaction)
                session().getTransaction().rollback();
            throw new RuntimeException(e);
        } finally {
            if(!inTransaction) {
                if(t == null)
                    session().getTransaction().commit();
                session().close();
            }
        }
    }

    /**
     * Insert or update an entity.
     * @param entity The entity to store.
     */
    public void store(Object entity) {
        store(Collections.singletonList(entity));
    }

    /**
     * Insert or update a list of entity.
     * @param entities The list of entities to store.
     */
    public void store(List<Object> entities) {
        Throwable t = null;
        try {
            if(!inTransaction)
                session().beginTransaction();
            entities.forEach(session::saveOrUpdate);
        } catch(Exception e) {
            t = e;
            if(!inTransaction)
                session().getTransaction().rollback();
            throw new RuntimeException(e);
        } finally {
            if(!inTransaction) {
                if (t == null)
                    session().getTransaction().commit();
                session().close();
            }
        }
    }

    /**
     * Flush the connection.
     */
    public void flush() {
        session().flush();
    }

    /**
     * Commit the current transaction.
     */
    public void commit() {
        if(session().getTransaction().isActive())
            session().getTransaction().commit();
    }

    /**
     * Rollback the current transaction.
     */
    public void rollback() {
        if(session().getTransaction().isActive())
            session().getTransaction().rollback();
    }

    /**
     * Execute the callback within a transaction.
     * @param transactionCallback The callback to execute.  Given an argument og DatabaseUtilities.
     * @return The return of transactionCallback.
     */
    public <T> T transactionCallback(TransactionCallback<T> transactionCallback) {
        try {
            inTransaction = true;
            session().beginTransaction();
            return transactionCallback.apply(this);
        } catch (Exception e) {
            if(session().getTransaction().isActive())
                session().getTransaction().rollback();
            throw new RuntimeException(e);
        } finally {
            if(session().getTransaction().isActive())
                session().getTransaction().commit();
            session().close();
            inTransaction = false;
        }
    }

    /**
     * Execute the result set callback.
     * @param resultSetCallback The callback to execute.
     * @param <T> The type returned.
     * @return The return of resultSetCallback.
     */
    public <T> T resultSetCallback(ResultSetCallback<T> resultSetCallback) {
        try {
            return session().doReturningWork(connection -> {
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                bindVariables(bindVars, preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                return resultSetCallback.apply(resultSet);
            });
        } finally {
            session().close();
        }
    }

    /**
     * Execute a callback with a connection.
     * @param connectionCallback The callback to execute.
     * @param <T> The type returned from the callback.
     * @return The return of connectionCallback.
     */
    public <T> T connectionCallback(ConnectionCallback<T> connectionCallback) {
            return session().doReturningWork(connectionCallback::apply);
    }

    /**
     * Select a row from the database as an array of objects.
     * @return An array of objects.
     */
    public Optional<Object[]> select() {
        List<Object[]> results = selectList();
        if(results.size() > 1) {
            throw new RuntimeException("Expected 0 or 1 results but got " + results.size());
        }
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    /**
     * Select a list of rows from the database as a arrays of objects.
     * @return An list of arrays of objects.
     */
    public List<Object[]> selectList() {
        if(sql == null) {
            throw new RuntimeException("No sql set");
        }
        if(!nocache && cache.containsKey(sql)) {
            Tuple4<Long, Long, TimeUnit, Object> result = cache.get(sql);
            if (result.getValue1() >= new Date().getTime()) {
                //noinspection unchecked
                return (List<Object[]>) result.getValue4();
            }
        }
        try (Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> {
                PreparedStatement statement = connection.prepareStatement(sql);
                bindVariables(bindVars, statement);
                ResultSet resultSet = statement.executeQuery();
                List<Object[]> rows = new ArrayList<>();
                while (resultSet.next()) {
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    Object[] row = new Object[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        row[i] = resultSet.getObject(i + 1);
                    }
                    rows.add(row);
                }
                if(cacheTime > 0 && cacheTimeUnit != null) {
                    cache.put(sql, new Tuple4<>(new Date().getTime() + cacheTimeUnit.toMillis(cacheTime), cacheTime, cacheTimeUnit, rows));
                }
                return rows;
            });
        }
    }

    /**
     * Select an object of type T from the database.
     * @param type The type class.
     * @param <T> The type.
     * @return An optional object of type T.
     */
    public <T> Optional<T> select(Class<T> type) {
        if(!nocache && cache.containsKey(sql)) {
            Tuple4<Long, Long, TimeUnit, Object> result = cache.get(sql);
            if (result.getValue1() >= new Date().getTime()) {
                //noinspection unchecked
                return result == null ? Optional.empty() : Optional.of((T) result.getValue4());
            }
        }
        try (Session hibernateSession = sessionFactory().openSession()) {
            T result;
            if(!type.isAnnotationPresent(Entity.class) ) {
                result = hibernateSession.doReturningWork(connection -> {
                    PreparedStatement preparedStatement = connection.prepareStatement(sql);
                    bindVariables(bindVars, preparedStatement);
                    try(ResultSet resultSet = preparedStatement.executeQuery()) {
                        T t = null;
                        if (resultSet.next()) {
                            //noinspection unchecked
                            t = (T) resultSet.getObject(1);
                        }
                        if (resultSet.next()) {
                            throw new RuntimeException("Expected 1 result but multiple results returned");
                        }
                        return t;
                    }
                });
            } else {
                Query query = hibernateSession.createNativeQuery(sql, type);
                for (int i = 0; i < bindVars.size(); i++) {
                    query.setParameter(i + 1, bindVars.get(i));
                }
                //noinspection unchecked
                result = (T) query.getSingleResult();
            }
            //noinspection unchecked
            if(cacheTime > 0 && cacheTimeUnit != null) {
                cache.put(sql, new Tuple4<>(new Date().getTime() + cacheTimeUnit.toMillis(cacheTime), cacheTime, cacheTimeUnit, result));
            }
            return result == null ? Optional.empty() : Optional.of(result);
        }
    }

    /**
     * Select a list of objects of type T from the database.
     * @param type The type class.
     * @param <T> The type.
     * @return A List of objects of type T.
     */
    public <T> List<T> selectList(Class<T> type) {
        if(!nocache && cache.containsKey(sql)) {
            Tuple4<Long, Long, TimeUnit, Object> result = cache.get(sql);
            if (result.getValue1() >= new Date().getTime()) {
                //noinspection unchecked
                return (List<T>) result.getValue4();
            }
        }
        try (Session hibernateSession = sessionFactory().openSession()) {
            List<T> result;
            if(!type.isAnnotationPresent(Entity.class) ) {
                result = hibernateSession.doReturningWork(connection -> {
                    List<T> results = new ArrayList<>();
                    PreparedStatement preparedStatement = connection.prepareStatement(sql);
                    bindVariables(bindVars, preparedStatement);
                    try(ResultSet resultSet = preparedStatement.executeQuery()) {
                        while(resultSet.next()) {
                            //noinspection unchecked
                            results.add((T) resultSet.getObject(1));
                        }
                        return results;
                    }
                });
            } else {
                Query query = hibernateSession.createNativeQuery(sql, type);
                for (int i = 0; i < bindVars.size(); i++) {
                    query.setParameter(i + 1, bindVars.get(i));
                }
                //noinspection unchecked
                result = query.getResultList();
            }
            if(cacheTime > 0 && cacheTimeUnit != null) {
                cache.put(sql, new Tuple4<>(new Date().getTime() + cacheTimeUnit.toMillis(cacheTime), cacheTime, cacheTimeUnit, result));
            }
            return result;
        }
    }

    /**
     * Select a result set as a map of column name to object values.
     * @return A map of column name to object values.
     */
    public Optional<Map<String,Object>> selectMap() {
        List<Map<String,Object>> results = selectMapList();
        if(results.size() > 1) {
            throw new RuntimeException("Expected 0 or 1 results but got " + results.size());
        }
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    /**
     * Select a list of maps of column name to object values.
     * @return A list of maps of column name to object values.
     */
    public List<Map<String,Object>> selectMapList() {
        if(!nocache && cache.containsKey(sql)) {
            Tuple4<Long, Long, TimeUnit, Object> result = cache.get(sql);
            if (result.getValue1() >= new Date().getTime()) {
                //noinspection unchecked
                return (List<Map<String,Object>>) result.getValue4();
            }
        }
        try (Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> {
                PreparedStatement statement = connection.prepareStatement(sql);
                bindVariables(bindVars, statement);
                ResultSet resultSet = statement.executeQuery();
                List<Map<String, Object>> rows = new ArrayList<>();
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    for (int i = 0; i < columnCount; i++) {
                        String colName = resultSet.getMetaData().getColumnName(i + 1);
                        Object value = resultSet.getObject(i + 1);
                        map.put(colName, value);
                    }
                    rows.add(map);
                }
                if(cacheTime > 0 && cacheTimeUnit != null) {
                    cache.put(sql, new Tuple4<>(new Date().getTime() + cacheTimeUnit.toMillis(cacheTime), cacheTime, cacheTimeUnit, rows));
                }
                return rows;
            });
        }
    }


    /**
     * Select an Optional Tuple2<U,V> from a query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @return An Optional Tuple2<U,V>.
     */
    public <T,U> Optional<Tuple2<T,U>> selectTuple(Class<T> class1, Class<U> class2) {
        if(!nocache && cache.containsKey(sql)) {
            Tuple4<Long, Long, TimeUnit, Object> result = cache.get(sql);
            if (result.getValue1() >= new Date().getTime()) {
                //noinspection unchecked
                return (Optional<Tuple2<T,U>>) result.getValue4();
            }
        }
        Optional<Tuple2<T,U>> result = resultSetCallback(resultSet -> TupleQuery.select(resultSet, class1, class2));
        if(cacheTime > 0 && cacheTimeUnit != null) {
            cache.put(sql, new Tuple4<>(new Date().getTime() + cacheTimeUnit.toMillis(cacheTime), cacheTime, cacheTimeUnit, result));
        }
        return result;
    }

    /**
     * Select a List of Tuple2<U,V> objects from a query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @return A List of  Tuple2<T,U> objects.
     */
    public <T,U> List<Tuple2<T,U>> selectTupleList(Class<T> class1, Class<U> class2) {
        if(!nocache && cache.containsKey(sql)) {
            Tuple4<Long, Long, TimeUnit, Object> result = cache.get(sql);
            if (result.getValue1() >= new Date().getTime()) {
                //noinspection unchecked
                return (List<Tuple2<T,U>>) result.getValue4();
            }
        }
        List<Tuple2<T,U>> result = resultSetCallback(resultSet -> TupleQuery.selectList(resultSet, class1, class2));
        if(cacheTime > 0 && cacheTimeUnit != null) {
            cache.put(sql, new Tuple4<>(new Date().getTime() + cacheTimeUnit.toMillis(cacheTime), cacheTime, cacheTimeUnit, result));
        }
        return result;
    }

    /**
     * Select an Optional Tuple3<U,V,W> from a query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @return An Optional Tuple3<T,U,V> object.
     */
    public <T,U,V> Optional<Tuple3<T,U,V>> selectTuple(Class<T> class1, Class<U> class2, Class<V> class3) {
        if(!nocache && cache.containsKey(sql)) {
            Tuple4<Long, Long, TimeUnit, Object> result = cache.get(sql);
            if (result.getValue1() >= new Date().getTime()) {
                //noinspection unchecked
                return (Optional<Tuple3<T,U,V>>) result.getValue4();
            }
        }
        Optional<Tuple3<T,U,V>> result = resultSetCallback(resultSet -> TupleQuery.select(resultSet, class1, class2, class3));
        if(cacheTime > 0 && cacheTimeUnit != null) {
            cache.put(sql, new Tuple4<>(new Date().getTime() + cacheTimeUnit.toMillis(cacheTime), cacheTime, cacheTimeUnit, result));
        }
        return result;
    }

    /**
     * Select a List of Tuple3<U,V,W> objects from a query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @return A List of  Tuple3<T,U,V> objects.
     */
    public <T,U,V> List<Tuple3<T,U,V>> selectTupleList(Class<T> class1, Class<U> class2, Class<V> class3) {
        if(!nocache && cache.containsKey(sql)) {
            Tuple4<Long, Long, TimeUnit, Object> result = cache.get(sql);
            if (result.getValue1() >= new Date().getTime()) {
                //noinspection unchecked
                return (List<Tuple3<T,U,V>>) result.getValue4();
            }
        }
        List<Tuple3<T,U,V>> result = resultSetCallback(resultSet -> TupleQuery.selectList(resultSet, class1, class2, class3));
        if(cacheTime > 0 && cacheTimeUnit != null) {
            cache.put(sql, new Tuple4<>(new Date().getTime() + cacheTimeUnit.toMillis(cacheTime), cacheTime, cacheTimeUnit, result));
        }
        return result;
    }

    /**
     * Select an Optional Tuple4<U,V,W,X> from a query.
     * @param query The sql query. Should yield 0 ... 1 four column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @return An Optional Tuple4<T,U,V,W> object.
     */
    public <T,U,V,W> Optional<Tuple4<T,U,V,W>> selectTuple(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4) {
        try (Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> {
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                bindVariables(bindVars, preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                return TupleQuery.select(resultSet, class1, class2, class3, class4);
            });
        }
    }

    /**
     * Select an Optional Tuple4<U,V,W,X> from a query.
     * @param query The sql query. Should yield 0 ... 1 four column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @return An Optional Tuple4<T,U,V,W> object.
     */
    public <T,U,V,W> Optional<Tuple4<T,U,V,W>> selectTuple(String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Object... bindVars) {
        return selectTuple(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2, class3, class4);
    }

    /**
     * Select a List of Tuple4<U,V,W,X> objects from a query.
     * @param query The sql query.  Should yield 0 ... * four column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @return A List of  Tuple4<T,U,V,W> objects.
     */
    public <T,U,V,W> List<Tuple4<T,U,V,W>> selectTupleList(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4) {
        try (Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> {
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                bindVariables(bindVars, preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                return TupleQuery.selectList(resultSet, class1, class2, class3, class4);
            });
        }
    }

    /**
     * Select a List of Tuple4<U,V,W,X> objects from a query.
     * @param query The sql query.  Should yield 0 ... * four column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @return A List of  Tuple4<T,U,V,W> objects.
     */
    public <T,U,V,W> List<Tuple4<T,U,V,W>> selectTupleList(String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Object... bindVars) {
        return selectTupleList(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2, class3, class4);
    }

    /**
     * Select an Optional Tuple5<U,V,W,X,Y> from a query.
     * Get a Tuple from a query.
     * @param query The sql query. Should yield 0 ... 1 five column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @return An Optional Tuple5<T,U,V,W,X> object.
     */
    public <T,U,V,W,X> Optional<Tuple5<T,U,V,W,X>> selectTuple(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5) {
        try (Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> {
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                bindVariables(bindVars, preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                return TupleQuery.select(resultSet, class1, class2, class3, class4, class5);
            });
        }
    }

    /**
     * Select an Optional Tuple5<U,V,W,X,Y> from a query.
     * Get a Tuple from a query.
     * @param query The sql query. Should yield 0 ... 1 five column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @return An Optional Tuple5<T,U,V,W,X> object.
     */
    public <T,U,V,W,X> Optional<Tuple5<T,U,V,W,X>> selectTuple(String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Object... bindVars) {
        return selectTuple(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2, class3, class4, class5);
    }

    /**
     * Select a List of Tuple5<U,V,W,X,Y> objects from a query.
     * @param query The sql query. Should yield 0 ... * five column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @return A List of  Tuple4<T,U,V,W,X> objects.
     */
    public <T,U,V,W,X> List<Tuple5<T,U,V,W,X>> selectTupleList(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5) {
        try (Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> {
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                bindVariables(bindVars, preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                return TupleQuery.selectList(resultSet, class1, class2, class3, class4, class5);
            });
        }
    }

    /**
     * Select a List of Tuple5<U,V,W,X,Y> objects from a query.
     * @param query The sql query. Should yield 0 ... * five column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @return A List of  Tuple4<T,U,V,W,X> objects.
     */
    public <T,U,V,W,X> List<Tuple5<T,U,V,W,X>> selectTupleList(String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Object... bindVars) {
        return selectTupleList(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2, class3, class4, class5);
    }

    /**
     * Select an Optional Tuple6<U,V,W,X,Y> from a query.
     * Get a Tuple from a query.
     * @param query The sql query. Should yield 0 ... 1 six column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param class6 The sixth class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @param <Y> The sixth type.
     * @return An Optional Tuple6<T,U,V,W,X,Y> object.
     */
    public <T,U,V,W,X,Y> Optional<Tuple6<T,U,V,W,X,Y>> selectTuple(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6) {
        try (Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> {
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                bindVariables(bindVars, preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                return TupleQuery.select(resultSet, class1, class2, class3, class4, class5, class6);
            });
        }
    }

    /**
     * Select an Optional Tuple6<U,V,W,X,Y> from a query.
     * Get a Tuple from a query.
     * @param query The sql query. Should yield 0 ... 1 six column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param class6 The sixth class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @param <Y> The sixth type.
     * @return An Optional Tuple6<T,U,V,W,X,Y> object.
     */
    public <T,U,V,W,X,Y> Optional<Tuple6<T,U,V,W,X,Y>> selectTuple(String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Object... bindVars) {
        return selectTuple(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2, class3, class4, class5, class6);
    }

    /**
     * Select a List of Tuple6<T,U,V,W,X,Y> objects from a query.
     * @param query The sql query. Should yield 0 ... * six column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param class6 The sixth class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @param <Y> The sixth type.
     * @return A List of  Tuple6<T,U,V,W,X,Y> objects.
     */
    public <T,U,V,W,X,Y> List<Tuple6<T,U,V,W,X,Y>> selectTupleList(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6) {
        try (Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> {
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                bindVariables(bindVars, preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                return TupleQuery.selectList(resultSet, class1, class2, class3, class4, class5, class6);
            });
        }
    }

    /**
     * Select a List of Tuple6<T,U,V,W,X,Y> objects from a query.
     * @param query The sql query. Should yield 0 ... * six column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param class6 The sixth class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @param <Y> The sixth type.
     * @return A List of  Tuple6<T,U,V,W,X,Y> objects.
     */
    public <T,U,V,W,X,Y> List<Tuple6<T,U,V,W,X,Y>> selectTupleList(String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Object... bindVars) {
        return selectTupleList(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2, class3, class4, class5, class6);
    }

    /**
     * Select an Optional Tuple7<U,V,W,X,Y<Z> from a query.
     * Get a Tuple from a query.
     * @param query The sql query. Should yield 0 ... 1 five column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param class6 The sixth class to bind to.
     * @param class7 The seventh class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @param <Y> The sixth type.
     * @param <Z> The seventh type.
     * @return An Optional Tuple7<T,U,V,W,X,Y,Z> object.
     */
    public <T,U,V,W,X,Y,Z> Optional<Tuple7<T,U,V,W,X,Y,Z>> selectTuple(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Class<Z> class7) {
        try (Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> {
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                bindVariables(bindVars, preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                return TupleQuery.select(resultSet, class1, class2, class3, class4, class5, class6, class7);
            });
        }
    }

    /**
     * Select an Optional Tuple7<U,V,W,X,Y<Z> from a query.
     * Get a Tuple from a query.
     * @param query The sql query. Should yield 0 ... 1 five column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param class6 The sixth class to bind to.
     * @param class7 The seventh class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @param <Y> The sixth type.
     * @param <Z> The seventh type.
     * @return An Optional Tuple7<T,U,V,W,X,Y,Z> object.
     */
    public <T,U,V,W,X,Y,Z> Optional<Tuple7<T,U,V,W,X,Y,Z>> selectTuple(String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Class<Z> class7, Object... bindVars) {
        return selectTuple(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2, class3, class4, class5, class6, class7);
    }

    /**
     * Select a List of Tuple7<T,U,V,W,X,Y,Z> objects from a query.
     * @param query The sql query. Should yield 0 ... * five column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param class6 The sixth class to bind to.
     * @param class7 The seventh class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @param <Y> The sixth type.
     * @param <Z> The seventh type.
     * @return A List of  Tuple7<T,U,V,W,X,Y,Z> objects.
     */
    public <T,U,V,W,X,Y,Z> List<Tuple7<T,U,V,W,X,Y,Z>> selectTupleList(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Class<Z> class7) {
        try (Session hibernateSession = sessionFactory().openSession()) {
            return hibernateSession.doReturningWork(connection -> {
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                bindVariables(bindVars, preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                return TupleQuery.selectList(resultSet, class1, class2, class3, class4, class5, class6, class7);
            });
        }
    }

    /**
     * Select a List of Tuple7<T,U,V,W,X,Y,Z> objects from a query.
     * @param query The sql query. Should yield 0 ... * five column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param class4 The fourth class to bind to.
     * @param class5 The fifth class to bind to.
     * @param class6 The sixth class to bind to.
     * @param class7 The seventh class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @param <W> The fourth type.
     * @param <X> The fifth type.
     * @param <Y> The sixth type.
     * @param <Z> The seventh type.
     * @return A List of  Tuple7<T,U,V,W,X,Y,Z> objects.
     */
    public <T,U,V,W,X,Y,Z> List<Tuple7<T,U,V,W,X,Y,Z>> selectTupleList(String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Class<Z> class7, Object... bindVars) {
        return selectTupleList(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2, class3, class4, class5, class6, class7);
    }

    /**
     * Bind the variables to the prepared statement.
     * @param bindVars The variables to bind.
     * @param preparedStatement The prepared statement to bind them to.
     * @throws SQLException When preparedStatement.setObject throws a sql exception.
     */
    private static void bindVariables(List<Object> bindVars, PreparedStatement preparedStatement) throws SQLException {
        int i = 1;
        for(Object bindVar : bindVars) {
            preparedStatement.setObject(i++, bindVar);
        }
    }

    /**
     * Get a list of table names matching <code>tableNamePattern</code>.
     * @param connection The {@link Connection}.
     * @param tableNamePattern The table name pattern, in the style of a sql LIKE pattern.  May be null (all tables).
     * @param schemaName The schema name.  May be null (all schemas).
     * @return A list of table names matching the pattern.
     */
    private static List<String> getTableNames(Connection connection, String tableNamePattern, String schemaName) {
        try {
            ResultSet resultSet = connection.getMetaData().getTables(null, schemaName, tableNamePattern, null);
            List<String> tableNames = new ArrayList<>();
            while(resultSet.next()) {
                tableNames.add(resultSet.getString("TABLE_NAME"));
            }
            return tableNames;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a list of schema names matching the name pattern.
     * @param connection The {@link Connection}.
     * @param schemaNamePattern A pattern in the style of sql LIKE, ie '%TEST%' would get all schemas with the word
     *                          TEST in them.  Can be null (lists all schemas).
     * @return A list of schema names.
     */
    private static List<String> getSchemaNames(Connection connection, String schemaNamePattern) {
        try {
            ResultSet resultSet = connection.getMetaData().getSchemas(null, schemaNamePattern);
            List<String> schemsNames = new ArrayList<>();
            while(resultSet.next()) {
                schemsNames.add(resultSet.getString("TABLE_SCHEM"));
            }
            return schemsNames;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a list of all catalog names.
     * @param connection The {@link Connection}.
     * @return A list of all catalog names.
     */
    private static List<String> getCatalogNames(Connection connection) {
        try {
            ResultSet resultSet = connection.getMetaData().getCatalogs();
            List<String> catalogNames = new ArrayList<>();
            while(resultSet.next()) {
                catalogNames.add(resultSet.getString("TABLE_CAT"));
            }
            return catalogNames;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
