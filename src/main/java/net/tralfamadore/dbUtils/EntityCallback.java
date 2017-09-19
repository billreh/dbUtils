package net.tralfamadore.dbUtils;

/**
 * Class: EntityCallback
 * Created by billreh on 9/16/17.
 */
@FunctionalInterface
public interface EntityCallback<T> {
    boolean apply(T entity);
}
