package net.tralfamadore.dbUtils;

/**
 * Class: TransactionCallback
 * Created by billreh on 9/3/17.
 */
@FunctionalInterface
public interface TransactionCallback<T> {
    T apply(DatabaseUtils databaseUtils);
}
