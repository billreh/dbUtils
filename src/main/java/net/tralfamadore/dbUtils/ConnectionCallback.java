package net.tralfamadore.dbUtils;

import java.sql.Connection;

/**
 * Class: ConnectionCallback
 * Created by billreh on 9/3/17.
 */
@FunctionalInterface
public interface ConnectionCallback<T> {
    T apply(Connection connection);
}
