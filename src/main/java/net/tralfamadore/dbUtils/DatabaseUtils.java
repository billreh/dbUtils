package net.tralfamadore.dbUtils;

import net.tralfamadore.*;
import org.hibernate.Session;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Class: DbUtils
 * Created by billreh on 7/22/17.
 * @author wreh
 */
@Repository
public class DatabaseUtils {
    /** The entity manager */
    @PersistenceContext
    private EntityManager em;

    /**
     * Get the entity manager.
     * @return The entity manager.
     */
    public EntityManager getEm() {
        return em;
    }

    /**
     * Set the entity manager.
     * @param em The entity manager.
     */
    public void setEm(EntityManager em) {
        this.em = em;
    }

    /**
     * Get a {@link TableDescription} for the given table.
     * @param tableName The table name.
     * @return The {@link TableDescription} for the given table name.
     */
    @Transactional
    public TableDescription getTableDescription(String tableName) {
        return getTableDescription(tableName, null);
    }

    /**
     * Get a {@link TableDescription} for the given table.
     * @param tableName The table name.
     * @param schemaName The schema name.
     * @return The {@link TableDescription} for the given table and schema names.
     */
    @Transactional
    public TableDescription getTableDescription(String tableName, String schemaName) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> TableDescription.getTableDescription(connection, tableName, schemaName));
    }

    /**
     * Get a list of table names matching <code>tableNamePattern</code>.
     * @param tableNamePattern The table name pattern to search on.  Table name pattern is in the form of a SQL LIKE
     *                         statement i.e. "%_LOG" matches all tables ending in _LOG.  May be null (searches all tables).
     * @return A list of table names that match the table name pattern.
     */
    @Transactional
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
    @Transactional
    public List<String> getTableNames(String schemaName, String tableNamePattern) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> getTableNames(connection, tableNamePattern, schemaName));
    }

    /**
     * Get a list of schema names matching the name pattern.
     * @param schemaNamePattern A pattern in the style of sql LIKE, ie '%TEST%' would get all schemas with the word
     *                          TEST in them.  Can be null (lists all schemas).
     * @return A list of schema names.
     */
    @Transactional
    public List<String> getSchemaNames(String schemaNamePattern) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> getSchemaNames(connection, schemaNamePattern));
    }

    /**
     * Get a list of all catalog names.
     * @return A list of all catalog names.
     */
    @Transactional
    public List<String> getCatalogNames() {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(DatabaseUtils::getCatalogNames);
    }

    /**
     * Select an Optional Tuple2<U,V> from a query.
     * @param query The sql query. Should yield 0 ... 1 two column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @return An Optional Tuple2<U,V>.
     */
    @Transactional
    public <T,U> Optional<Tuple2<T,U>> selectTuple(String query, List<Object> bindVars, Class<T> class1, Class<U> class2) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection ->  {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.select(resultSet, class1, class2);
        });
    }

    /**
     * Select an Optional Tuple2<U,V> from a query.
     * @param query The sql query. Should yield 0 ... 1 two column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @return An Optional Tuple2<U,V>.
     */
    @Transactional
    public <T,U> Optional<Tuple2<T,U>> selectTuple(String query, Class<T> class1, Class<U> class2, Object... bindVars) {
        return selectTuple(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2);
    }

    /**
     * Select a List of Tuple2<U,V> objects from a query.
     * @param query The sql query.  Should yield 0 ... * two column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @return A List of  Tuple2<T,U> objects.
     */
    @Transactional
    public <T,U> List<Tuple2<T,U>> selectTupleList(String query, List<Object> bindVars, Class<T> class1, Class<U> class2) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection ->  {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.selectList(resultSet, class1, class2);
        });
    }

    /**
     * Select a List of Tuple2<U,V> objects from a query.
     * @param query The sql query.  Should yield 0 ... * two column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @return A List of  Tuple2<T,U> objects.
     */
    @Transactional
    public <T,U> List<Tuple2<T,U>> selectTupleList(String query, Class<T> class1, Class<U> class2, Object... bindVars) {
        return selectTupleList(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2);
    }

    /**
     * Select an Optional Tuple3<U,V,W> from a query.
     * @param query The sql query. Should yield 0 ... 1 three column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @return An Optional Tuple3<T,U,V> object.
     */
    @Transactional
    public <T,U,V> Optional<Tuple3<T,U,V>> selectTuple(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection ->  {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.select(resultSet, class1, class2, class3);
        });
    }

    /**
     * Select an Optional Tuple3<U,V,W> from a query.
     * @param query The sql query. Should yield 0 ... 1 three column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @return An Optional Tuple3<T,U,V> object.
     */
    @Transactional
    public <T,U,V> Optional<Tuple3<T,U,V>> selectTuple(String query, Class<T> class1, Class<U> class2, Class<V> class3, Object... bindVars) {
        return selectTuple(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2, class3);
    }

    /**
     * Select a List of Tuple3<U,V,W> objects from a query.
     * @param query The sql query.  Should yield 0 ... * three column results.
     * @param bindVars List of bind variables for the query.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @return A List of  Tuple3<T,U,V> objects.
     */
    @Transactional
    public <T,U,V> List<Tuple3<T,U,V>> selectTupleList(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection ->  {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.selectList(resultSet, class1, class2, class3);
        });
    }

    /**
     * Select a List of Tuple3<U,V,W> objects from a query.
     * @param query The sql query.  Should yield 0 ... * three column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param bindVars List of bind variables for the query.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @return A List of  Tuple3<T,U,V> objects.
     */
    @Transactional
    public <T,U,V> List<Tuple3<T,U,V>> selectTupleList(String query, Class<T> class1, Class<U> class2, Class<V> class3, Object... bindVars) {
        return selectTupleList(query, bindVars == null ? Collections.emptyList() : Arrays.asList(bindVars), class1, class2, class3);
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
    @Transactional
    public <T,U,V,W> Optional<Tuple4<T,U,V,W>> selectTuple(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection ->  {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.select(resultSet, class1, class2, class3, class4);
        });
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
    @Transactional
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
    @Transactional
    public <T,U,V,W> List<Tuple4<T,U,V,W>> selectTupleList(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection ->  {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.selectList(resultSet, class1, class2, class3, class4);
        });
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
    @Transactional
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
    @Transactional
    public <T,U,V,W,X> Optional<Tuple5<T,U,V,W,X>> selectTuple(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection ->  {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.select(resultSet, class1, class2, class3, class4, class5);
        });
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
    @Transactional
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
    @Transactional
    public <T,U,V,W,X> List<Tuple5<T,U,V,W,X>> selectTupleList(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection ->  {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.selectList(resultSet, class1, class2, class3, class4, class5);
        });
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
    @Transactional
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
    @Transactional
    public <T,U,V,W,X,Y> Optional<Tuple6<T,U,V,W,X,Y>> selectTuple(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.select(resultSet, class1, class2, class3, class4, class5, class6);
        });
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
    @Transactional
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
    @Transactional
    public <T,U,V,W,X,Y> List<Tuple6<T,U,V,W,X,Y>> selectTupleList(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection ->  {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.selectList(resultSet, class1, class2, class3, class4, class5, class6);
        });
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
    @Transactional
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
    @Transactional
    public <T,U,V,W,X,Y,Z> Optional<Tuple7<T,U,V,W,X,Y,Z>> selectTuple(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Class<Z> class7) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.select(resultSet, class1, class2, class3, class4, class5, class6, class7);
        });
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
    @Transactional
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
    @Transactional
    public <T,U,V,W,X,Y,Z> List<Tuple7<T,U,V,W,X,Y,Z>> selectTupleList(String query, List<Object> bindVars, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Class<Z> class7) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            bindVariables(bindVars, preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            return TupleQuery.selectList(resultSet, class1, class2, class3, class4, class5, class6, class7);
        });
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
    @Transactional
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
