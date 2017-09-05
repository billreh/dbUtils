package net.tralfamadore.dbUtils;

import java.util.function.Function;

/**
 * Class: ColumnNameProcessor
 * Created by billreh on 8/19/17.
 * @author wreh
 */
@FunctionalInterface
public interface ColumnNameProcessor extends Function<String,String>{
}
