package net.tralfamadore.dbUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A description of a database table.
 *
 * See {@link TableDescription#getTableDescription(Connection, String, String)} for how to create a
 * {@link TableDescription}.
 *
 * @author wreh
 */
public class TableDescription {
    /** The schema name */
    private String schemaName;
    /** The table name */
    private String tableName;
    /** The table comments */
    private String comments;
    /** List of {@link net.tralfamadore.dbUtils.ColumnDescription} */
    private List<ColumnDescription> columnDescriptions = new ArrayList<>();

    /**
     * Create a new TableDescription.  See {@link TableDescription#getTableDescription(Connection, String, String)} for
     * the public api for creating a new TableDescription object.
     * @param columnDescriptions A list of {@link ColumnDescription}s for each column in the table.
     * @param tableName The table name.
     * @param schemaName The schema name.
     * @param comments The table comments.
     */
    private TableDescription(List<ColumnDescription> columnDescriptions, String tableName, String schemaName, String comments) {
        this.columnDescriptions = columnDescriptions;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.comments = comments;
    }

    /**
     * Get the table name.
     * @return The table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get the schema name.
     * @return The schema name.  Can be null.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Get the table comments.
     * @return The comments on the table in the database.
     */
    public String getComments() {
        return comments;
    }

    /**
     * Get a list of all of the column names for the table.
     * @return A list of all of the column names for the table.
     */
    public List<String> getColumnNames() {
        return columnDescriptions.stream().map(ColumnDescription::getColumnName).collect(Collectors.toList());
    }

    /**
     * Get a n {@link Optional} {@link ColumnDescription} for the given column name.
     * @param columnName The name of the column to get the {@link ColumnDescription} for.
     * @return A {@link ColumnDescription} for the given column name.  Will be {@link Optional#EMPTY} if there is
     * no column found with that name.
     */
    public Optional<ColumnDescription> getColumnDescription(String columnName) {
        return columnDescriptions.stream().filter(cd -> cd.getColumnName().equals(columnName)).findFirst();
    }

    /**
     * Get a list of {@link ColumnDescription}s for all of the columns in the table.
     * @return A list of {@link ColumnDescription}s for all of the columns in the table.
     */
    public List<ColumnDescription> getColumnDescriptions() {
        return columnDescriptions;
    }

    /**
     * Get a list of {@link ColumnDescription}s for each column this is part of the primary key for the table.
     * @return A list of {@link ColumnDescription}s for each column this is part of the primary key for the table.
     */
    public List<ColumnDescription> getPrimaryKeys() {
        return columnDescriptions.stream().filter(ColumnDescription::isPrimaryKey).collect(Collectors.toList());
    }

    /**
     * Get a list of {@link ColumnDescription}s for each column this is foreign key in the table.
     * @return A list of {@link ColumnDescription}s for each column this is foreign key in the table.
     */
    public List<ColumnDescription> getForeignKeys() {
        return columnDescriptions.stream().filter(ColumnDescription::isForeignKey).collect(Collectors.toList());
    }

    /**
     * See {@link Object#toString()}
     * @return String representation of a table description.
     */
    @Override
    public String toString() {
        return "TableDescription{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", comments='" + comments + '\'' +
                ", columnDescriptions=" + columnDescriptions +
                '}';
    }

    /**
     * Create a new {@link TableDescription} object.  Uses jdbc meta data from the {@link Connection}.
     *
     * @param connection The database {@link Connection}
     * @param tableName The table name.
     * @param schemaName The schema name.  Can be null.
     * @return A {@link TableDescription} object.
     */
    public static TableDescription getTableDescription(Connection connection, String tableName, String schemaName) {
        try {
            ResultSet resultSet = connection.getMetaData().getTables(null, schemaName, tableName, null);
            String realSchemaName = null;
            String comments = null;
            if(resultSet.next()) {
                realSchemaName = resultSet.getString("TABLE_SCHEM");
                comments = resultSet.getString("REMARKS");
            }
            return new TableDescription(getColumnDescriptions(connection, tableName, schemaName), tableName, realSchemaName, comments);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a list of {@link ColumnDescription}s for all of the columns of a table.
     * @param connection The database {@link Connection}
     * @param tableName The table name.
     * @param schemaName The schema name.
     * @return A list of {@link ColumnDescription}s for all of the columns of a table.
     */
    private static List<ColumnDescription> getColumnDescriptions(Connection connection, String tableName, String schemaName) {
        List<ColumnDescription> columnDescriptions = new ArrayList<>();
        try {
            ResultSet resultSet = connection.getMetaData().getColumns(null, schemaName, tableName, "%");
            while (resultSet.next()) {
                String name = resultSet.getString("COLUMN_NAME");
                String type = resultSet.getString("TYPE_NAME");
                boolean nullable = resultSet.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                String defaultValue = resultSet.getString("COLUMN_DEF");
                int columnSize = resultSet.getInt("COLUMN_SIZE");
                String comments = resultSet.getString("REMARKS");

                ColumnDescription columnDescription
                        = new ColumnDescription(name, type, nullable, defaultValue, columnSize, comments);
                columnDescriptions.add(columnDescription);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        addPrimaryKeys(connection, tableName, schemaName, columnDescriptions);
        addForeignKeys(connection, tableName, schemaName, columnDescriptions);
        return columnDescriptions;
    }

    /**
     * Go through a list of {@link ColumnDescription}s and add foreign key data.
     * @param connection The database {@link Connection}
     * @param tableName The table name.
     * @param schemaName The schema name.
     * @param columnDescriptions A list of {@link ColumnDescription}s.
     */
    private static void addForeignKeys(Connection connection, String tableName, String schemaName, List<ColumnDescription> columnDescriptions) {
        try {
            ResultSet resultSet = connection.getMetaData().getImportedKeys(null, schemaName, tableName);
            while(resultSet.next()) {
                String pkColumnName = resultSet.getString("PKCOLUMN_NAME");
                String pkTableName = resultSet.getString("PKTABLE_NAME");
                String fkColumnName = resultSet.getString("FKCOLUMN_NAME");
                for(ColumnDescription columnDescription : columnDescriptions) {
                    if(columnDescription.getColumnName().equals(fkColumnName)) {
                        columnDescription.setForeignKey(true);
                        columnDescription.setReferencedColumn(pkColumnName);
                        columnDescription.setReferencedTable(pkTableName);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Go through a list of {@link ColumnDescription}s and add primary key data.
     * @param connection The database {@link Connection}
     * @param tableName The table name.
     * @param schemaName The schema name.
     * @param columnDescriptions A list of {@link ColumnDescription}s.
     */
    private static void addPrimaryKeys(Connection connection, String tableName, String schemaName, List<ColumnDescription> columnDescriptions) {
        try {
            ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, schemaName, tableName);
            while(resultSet.next()) {
                String pkName = resultSet.getString("COLUMN_NAME");
                for(ColumnDescription columnDescription : columnDescriptions) {
                    if(columnDescription.getColumnName().equals(pkName)) {
                        columnDescription.setPrimaryKey(true);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
