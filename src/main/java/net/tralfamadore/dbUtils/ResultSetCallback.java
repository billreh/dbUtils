package net.tralfamadore.dbUtils;

import java.sql.ResultSet;

/**
 * Class: ResultSetCallback
 * Created by billreh on 9/3/17.
 */
@FunctionalInterface
public interface ResultSetCallback<T> {
    T apply(ResultSet resultSet);
}
