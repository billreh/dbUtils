package net.tralfamadore.dbUtils;

import java.util.List;

/**
 * Class: IDbUtils
 * Created by billreh on 5/6/17.
 */
public interface IDbUtils {
    List<ColumnDescription> describeTable(String tableName);

    boolean tableExists(String tableName);

    String createTable(Class entityClass);

    String createTable(Class entityClass, boolean generate);

    String createTables(String packageName);

    String createTables(String packageName, boolean generate);

    String createTables(Class... entities);

    String createTables(boolean generate, Class... entities);

    String createTables(List<Class> entityClasses);

    String createTables(List<Class> entityClasses, boolean generate);

    String dropTables(String packageName);

    String dropTables(String packageName, boolean generate);

    String dropTables(List<Class> entityClasses);

    String dropTables(List<Class> entityClasses, boolean generate);

    String dropTables(Class... entityClasses);

    String dropTables(boolean generate, Class... entityClasses);

    String dropTable(Class entityClass);

    String dropTable(Class entityClass, boolean generate);

    List<String> getTableNames();

    class ColumnDescription {
        private String name;
        private String type;
        private boolean nullable;
        private boolean pk;
        private String defaultValue;
        private String additionalArguments;

        public ColumnDescription(String name, String type, boolean nullable, boolean pk, String defaultValue,
                                 String additionalArguments)
        {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
            this.pk = pk;
            this.defaultValue = defaultValue;
            this.additionalArguments = additionalArguments;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public boolean isNullable() {
            return nullable;
        }

        public boolean isPk() {
            return pk;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public String getAdditionalArguments() {
            return additionalArguments;
        }

        void setIsPk(boolean isPk) {
            this.pk = isPk;
        }

        @Override
        public String toString() {
            return "ColumnDescription{" +
                    "name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    ", nullable=" + nullable +
                    ", pk=" + pk +
                    ", defaultValue='" + defaultValue + '\'' +
                    ", additionalArguments='" + additionalArguments + '\'' +
                    '}';
        }
    }
}
