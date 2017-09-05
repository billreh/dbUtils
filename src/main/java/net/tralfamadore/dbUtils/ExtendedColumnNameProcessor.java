package net.tralfamadore.dbUtils;

/**
 * Class: ExtendedColumnNameProcessor
 * Created by billreh on 8/19/17.
 */
public interface ExtendedColumnNameProcessor extends ColumnNameProcessor {
    boolean matches(String columnName);
    boolean skipOthers();
}
