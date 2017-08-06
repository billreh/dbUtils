package net.tralfamadore.dbUtils;

import net.tralfamadore.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Class: TupleQuery
 * Created by billreh on 8/6/17.
 * @author wreh
 */
public class TupleQuery {
    /**
     * Select an Optional Tuple2<T,U> from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query. Should yield 0 ... 1 two column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @return An Optional Tuple2<T,U>.
     */
    @SuppressWarnings("unchecked")
    public static <T,U> Optional<Tuple2<T,U>> select(Connection connection, String query, Class<T> class1, Class<U> class2) {
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 2)
                throw new RuntimeException("column count != 2 for Tuple2");
            Tuple2<T,U> tuple = new Tuple2<>();
            if(resultSet.next()) {
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
            } else {
                return Optional.empty();
            }
            if(resultSet.next())
                throw new RuntimeException("returned more than one result in select query");
            return Optional.of(tuple);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    /**
     * Select a List of Tuple2<T,U> objects from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query.  Should yield 0 ... * two column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @return A List of  Tuple2<T,U> objects.
     */
    @SuppressWarnings("unchecked")
    public static <T,U> List<Tuple2<T,U>> selectList(Connection connection, String query, Class<T> class1, Class<U> class2) {
        List<Tuple2<T,U>> tuples = new ArrayList<>();
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 2)
                throw new RuntimeException("column count != 2 for Tuple2");
            while(resultSet.next()) {
                Tuple2<T,U> tuple = new Tuple2<>();
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuples.add(tuple);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tuples;
    }

    /**
     * Select an Optional Tuple3<T,U,V> from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query. Should yield 0 ... 1 three column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @return An Optional Tuple3<T,U,V> object.
     */
    @SuppressWarnings("unchecked")
    public static <T,U,V> Optional<Tuple3<T,U,V>> select(Connection connection, String query, Class<T> class1, Class<U> class2, Class<V> class3) {
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 3)
                throw new RuntimeException("column count != 3 for Tuple3");
            Tuple3<T,U,V> tuple = new Tuple3<>();
            if(resultSet.next()) {
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuple.setValue3((V)resultSet.getObject(3));
            } else {
                return Optional.empty();
            }
            if(resultSet.next())
                throw new RuntimeException("returned more than one result in select query");
            return Optional.of(tuple);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    /**
     * Select a List of Tuple3<T,U,V> objects from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query.  Should yield 0 ... * three column results.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @return A List of  Tuple3<T,U,V> objects.
     */
    @SuppressWarnings("unchecked")
    public static <T,U,V> List<Tuple3<T,U,V>> selectList(Connection connection, String query, Class<T> class1, Class<U> class2, Class<V> class3) {
        List<Tuple3<T,U,V>> tuples = new ArrayList<>();
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 3)
                throw new RuntimeException("column count != 3 for Tuple3");
            while(resultSet.next()) {
                Tuple3<T,U,V> tuple = new Tuple3<>();
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuple.setValue3((V)resultSet.getObject(3));
                tuples.add(tuple);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tuples;
    }

    /**
     * Select an Optional Tuple4<T,U,V,W> from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query. Should yield 0 ... 1 four column results.
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
    @SuppressWarnings("unchecked")
    public static <T,U,V,W> Optional<Tuple4<T,U,V,W>> select(Connection connection, String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4) {
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 4)
                throw new RuntimeException("column count != 4 for Tuple4");
            Tuple4<T,U,V,W> tuple = new Tuple4<>();
            if(resultSet.next()) {
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuple.setValue3((V)resultSet.getObject(3));
                tuple.setValue4((W)resultSet.getObject(4));
            } else {
                return Optional.empty();
            }
            if(resultSet.next())
                throw new RuntimeException("returned more than one result in select query");
            return Optional.of(tuple);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    /**
     * Select a List of Tuple4<T,U,V,W> objects from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query.  Should yield 0 ... * four column results.
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
    @SuppressWarnings("unchecked")
    public static <T,U,V,W> List<Tuple4<T,U,V,W>> selectList(Connection connection, String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4) {
        List<Tuple4<T,U,V,W>> tuples = new ArrayList<>();
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 4)
                throw new RuntimeException("column count != 4 for Tuple4");
            while(resultSet.next()) {
                Tuple4<T,U,V,W> tuple = new Tuple4<>();
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuple.setValue3((V)resultSet.getObject(3));
                tuple.setValue4((W)resultSet.getObject(4));
                tuples.add(tuple);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tuples;
    }

    /**
     * Select an Optional Tuple5<T,U,V,W,X> from a query.
     * Get a Tuple from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query. Should yield 0 ... 1 five column results.
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
    @SuppressWarnings("unchecked")
    public static <T,U,V,W,X> Optional<Tuple5<T,U,V,W,X>> select(Connection connection, String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5) {
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 5)
                throw new RuntimeException("column count != 5 for Tuple5");
            Tuple5<T,U,V,W,X> tuple = new Tuple5<>();
            if(resultSet.next()) {
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuple.setValue3((V)resultSet.getObject(3));
                tuple.setValue4((W)resultSet.getObject(4));
                tuple.setValue5((X)resultSet.getObject(5));
            } else {
                return Optional.empty();
            }
            if(resultSet.next())
                throw new RuntimeException("returned more than one result in select query");
            return Optional.of(tuple);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    /**
     * Select a List of Tuple5<T,U,V,W,X> objects from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query. Should yield 0 ... * five column results.
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
     * @return A List of  Tuple5<T,U,V,W,X> objects.
     */
    @SuppressWarnings("unchecked")
    public static <T,U,V,W,X> List<Tuple5<T,U,V,W,X>> selectList(Connection connection, String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5) {
        List<Tuple5<T,U,V,W,X>> tuples = new ArrayList<>();
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 5)
                throw new RuntimeException("column count != 5 for Tuple5");
            while(resultSet.next()) {
                Tuple5<T,U,V,W,X> tuple = new Tuple5<>();
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuple.setValue3((V)resultSet.getObject(3));
                tuple.setValue4((W)resultSet.getObject(4));
                tuple.setValue5((X)resultSet.getObject(5));
                tuples.add(tuple);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tuples;
    }

    /**
     * Select an Optional Tuple6<T,U,V,W,X,Y> from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query. Should yield 0 ... 1 six column results.
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
    @SuppressWarnings("unchecked")
    public static <T,U,V,W,X,Y> Optional<Tuple6<T,U,V,W,X,Y>> select(Connection connection, String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6) {
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 6)
                throw new RuntimeException("column count != 6 for Tuple6");
            Tuple6<T,U,V,W,X,Y> tuple = new Tuple6<>();
            if(resultSet.next()) {
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuple.setValue3((V)resultSet.getObject(3));
                tuple.setValue4((W)resultSet.getObject(4));
                tuple.setValue5((X)resultSet.getObject(5));
                tuple.setValue6((Y)resultSet.getObject(6));
            } else {
                return Optional.empty();
            }
            if(resultSet.next())
                throw new RuntimeException("returned more than one result in select query");
            return Optional.of(tuple);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    /**
     * Select a List of Tuple6<T,U,V,W,X,Y> objects from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query. Should yield 0 ... * five column results.
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
    @SuppressWarnings("unchecked")
    public static <T,U,V,W,X,Y> List<Tuple6<T,U,V,W,X,Y>> selectList(Connection connection, String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6) {
        List<Tuple6<T,U,V,W,X,Y>> tuples = new ArrayList<>();
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 6)
                throw new RuntimeException("column count != 6 for Tuple6");
            while(resultSet.next()) {
                Tuple6<T,U,V,W,X,Y> tuple = new Tuple6<>();
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuple.setValue3((V)resultSet.getObject(3));
                tuple.setValue4((W)resultSet.getObject(4));
                tuple.setValue5((X)resultSet.getObject(5));
                tuple.setValue6((Y)resultSet.getObject(6));
                tuples.add(tuple);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tuples;
    }

    /**
     * Select an Optional Tuple7<T,U,V,W,X,Y,Z> from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query. Should yield 0 ... 1 seven column results.
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
    @SuppressWarnings("unchecked")
    public static <T,U,V,W,X,Y,Z> Optional<Tuple7<T,U,V,W,X,Y,Z>> select(Connection connection, String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Class<Z> class7) {
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 7)
                throw new RuntimeException("column count != 7 for Tuple7");
            Tuple7<T,U,V,W,X,Y,Z> tuple = new Tuple7<>();
            if(resultSet.next()) {
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuple.setValue3((V)resultSet.getObject(3));
                tuple.setValue4((W)resultSet.getObject(4));
                tuple.setValue5((X)resultSet.getObject(5));
                tuple.setValue6((Y)resultSet.getObject(6));
                tuple.setValue7((Z)resultSet.getObject(7));
            } else {
                return Optional.empty();
            }
            if(resultSet.next())
                throw new RuntimeException("returned more than one result in select query");
            return Optional.of(tuple);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    /**
     * Select a List of Tuple6<T,U,V,W,X,Y,Z> objects from a query.
     * @param connection The {@link Connection}.
     * @param query The sql query. Should yield 0 ... * five column results.
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
    @SuppressWarnings("unchecked")
    public static <T,U,V,W,X,Y,Z> List<Tuple7<T,U,V,W,X,Y,Z>> selectList(Connection connection, String query, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Class<Z> class7) {
        List<Tuple7<T,U,V,W,X,Y,Z>> tuples = new ArrayList<>();
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(query);
            if(resultSet.getMetaData().getColumnCount() != 7)
                throw new RuntimeException("column count != 7 for Tuple7");
            while(resultSet.next()) {
                Tuple7<T,U,V,W,X,Y,Z> tuple = new Tuple7<>();
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuple.setValue3((V)resultSet.getObject(3));
                tuple.setValue4((W)resultSet.getObject(4));
                tuple.setValue5((X)resultSet.getObject(5));
                tuple.setValue6((Y)resultSet.getObject(6));
                tuple.setValue7((Z)resultSet.getObject(7));
                tuples.add(tuple);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tuples;
    }
}
