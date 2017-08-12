package net.tralfamadore.dbUtils;

import net.tralfamadore.*;

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
     * @param resultSet The result set to process.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @return An Optional Tuple2<T,U>.
     */
    @SuppressWarnings("unchecked")
    public static <T,U> Optional<Tuple2<T,U>> select(ResultSet resultSet, Class<T> class1, Class<U> class2) {
        try {
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
            throw new RuntimeException(e);
        }
    }

    /**
     * Select a List of Tuple2<T,U> objects from a query.
     * @param resultSet The result set to process.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @return A List of  Tuple2<T,U> objects.
     */
    @SuppressWarnings("unchecked")
    public static <T,U> List<Tuple2<T,U>> selectList(ResultSet resultSet, Class<T> class1, Class<U> class2) {
        try {
            List<Tuple2<T,U>> tuples = new ArrayList<>();
            if(resultSet.getMetaData().getColumnCount() != 2)
                throw new RuntimeException("column count != 2 for Tuple2");
            while(resultSet.next()) {
                Tuple2<T,U> tuple = new Tuple2<>();
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuples.add(tuple);
            }
            return tuples;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Select an Optional Tuple3<T,U,V> from a query.
     * @param resultSet The result set to process.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @return An Optional Tuple3<T,U,V> object.
     */
    @SuppressWarnings("unchecked")
    public static <T,U,V> Optional<Tuple3<T,U,V>> select(ResultSet resultSet, Class<T> class1, Class<U> class2, Class<V> class3) {
        try {
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
            throw new RuntimeException(e);
        }
    }

    /**
     * Select a List of Tuple3<T,U,V> objects from a query.
     * @param resultSet The result set to process.
     * @param class1 The first class to bind to.
     * @param class2 The second class to bind to.
     * @param class3 The third class to bind to.
     * @param <T> The first type.
     * @param <U> The second type.
     * @param <V> The third type.
     * @return A List of  Tuple3<T,U,V> objects.
     */
    @SuppressWarnings("unchecked")
    public static <T,U,V> List<Tuple3<T,U,V>> selectList(ResultSet resultSet, Class<T> class1, Class<U> class2, Class<V> class3) {
        try {
            List<Tuple3<T,U,V>> tuples = new ArrayList<>();
            if(resultSet.getMetaData().getColumnCount() != 3)
                throw new RuntimeException("column count != 3 for Tuple3");
            while(resultSet.next()) {
                Tuple3<T,U,V> tuple = new Tuple3<>();
                tuple.setValue1((T)resultSet.getObject(1));
                tuple.setValue2((U)resultSet.getObject(2));
                tuple.setValue3((V)resultSet.getObject(3));
                tuples.add(tuple);
            }
            return tuples;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Select an Optional Tuple4<T,U,V,W> from a query.
     * @param resultSet The result set to process.
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
    public static <T,U,V,W> Optional<Tuple4<T,U,V,W>> select(ResultSet resultSet, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4) {
        try {
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
            throw new RuntimeException(e);
        }
    }

    /**
     * Select a List of Tuple4<T,U,V,W> objects from a query.
     * @param resultSet The result set to process.
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
    public static <T,U,V,W> List<Tuple4<T,U,V,W>> selectList(ResultSet resultSet, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4) {
        try {
            List<Tuple4<T,U,V,W>> tuples = new ArrayList<>();
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
            return tuples;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Select an Optional Tuple5<T,U,V,W,X> from a query.
     * Get a Tuple from a query.
     * @param resultSet The result set to process.
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
    public static <T,U,V,W,X> Optional<Tuple5<T,U,V,W,X>> select(ResultSet resultSet, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5) {
        try {
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
            throw new RuntimeException(e);
        }
    }

    /**
     * Select a List of Tuple5<T,U,V,W,X> objects from a query.
     * @param resultSet The result set to process.
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
    public static <T,U,V,W,X> List<Tuple5<T,U,V,W,X>> selectList(ResultSet resultSet, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5) {
        try {
            List<Tuple5<T,U,V,W,X>> tuples = new ArrayList<>();
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
            return tuples;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Select an Optional Tuple6<T,U,V,W,X,Y> from a query.
     * @param resultSet The result set to process.
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
    public static <T,U,V,W,X,Y> Optional<Tuple6<T,U,V,W,X,Y>> select(ResultSet resultSet, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6) {
        try {
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
            throw new RuntimeException(e);
        }
    }

    /**
     * Select a List of Tuple6<T,U,V,W,X,Y> objects from a query.
     * @param resultSet The result set to process.
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
    public static <T,U,V,W,X,Y> List<Tuple6<T,U,V,W,X,Y>> selectList(ResultSet resultSet, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6) {
        try {
            List<Tuple6<T,U,V,W,X,Y>> tuples = new ArrayList<>();
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
            return tuples;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Select an Optional Tuple7<T,U,V,W,X,Y,Z> from a query.
     * @param resultSet The result set to process.
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
    public static <T,U,V,W,X,Y,Z> Optional<Tuple7<T,U,V,W,X,Y,Z>> select(ResultSet resultSet, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Class<Z> class7) {
        try {
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
            throw new RuntimeException(e);
        }
    }

    /**
     * Select a List of Tuple6<T,U,V,W,X,Y,Z> objects from a query.
     * @param resultSet The result set to process.
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
    public static <T,U,V,W,X,Y,Z> List<Tuple7<T,U,V,W,X,Y,Z>> selectList(ResultSet resultSet, Class<T> class1, Class<U> class2, Class<V> class3, Class<W> class4, Class<X> class5, Class<Y> class6, Class<Z> class7) {
        List<Tuple7<T,U,V,W,X,Y,Z>> tuples = new ArrayList<>();
        try {
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
            throw new RuntimeException(e);
        }
        return tuples;
    }
}
