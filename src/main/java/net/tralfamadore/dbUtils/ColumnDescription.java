package net.tralfamadore.dbUtils;

/**
 * A description of a column in a database table.
 * @author wreh
 */
public class ColumnDescription {
    /** The columns name */
    private String columnName;
    /** The column type (VARCHAR, etc) */
    private String columnType;
    /** True if the column is nullable */
    private boolean nullable;
    /** True if the column is a primary key */
    private boolean pk;
    /** The default value for the column if there is one */
    private String defaultValue;
    /** True if the column is a foreign key */
    private boolean fk;
    /** If the column is a foreign key, the name of the table it references */
    private String referencedTable;
    /** If the column is a foreign key, the name of the column it references */
    private String referencedColumn;
    /** The column size (if type is VARCHAR this will be the length) */
    private int columnSize;
    /** Any comments on the column in the database */
    private final String comments;

    /**
     * Create a new Column description from the given information.
     * @param columnName The column name.
     * @param columnType The column type (VARCHAR, etc).
     * @param nullable Whether or not the column is nullable.
     * @param defaultValue The defult value for the column if there is one.
     * @param columnSize The column size (if type is VARCHAR this will be the length).
     * @param comments Any comments on the column.
     */
    ColumnDescription(String columnName, String columnType, boolean nullable, String defaultValue, int columnSize, String comments) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.columnSize = columnSize;
        this.comments = comments;
    }

    /**
     * Get the column name.
     * @return The column name.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Get the column type (VARCHAR, etc).
     * @return The column type (VARCHAR, etc).
     */
    public String getColumnType() {
        return columnType;
    }

    /**
     * Get the column size (for VARCHAR this will be the length).
     * @return The column type (VARCHAR, etc).
     */
    public int getColumnSize() {
        return columnSize;
    }

    /**
     * Get the comments on the column in the database.
     * @return THe comments on the column in the database.
     */
    public String getComments() {
        return comments;
    }

    /**
     * Get whether or not the column is nullable.
     * @return True if the column is nullable.
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Get the default value for the column.
     * @return The default value for the column.  This may return null.
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Get whether or not this column is a primary key.
     * @return True if this column is a primary key.
     */
    public boolean isPrimaryKey() {
        return pk;
    }

    /**
     * Set whether or not this column is a primary key.
     * @param pk True if this column is a primary key.
     */
    void setPrimaryKey(boolean pk) {
        this.pk = pk;
    }

    /**
     * Get whether or not this column is a foreign key.
     * @return True if this column is a foreign key.
     */
    public boolean isForeignKey() {
        return fk;
    }

    /**
     * Set whether or not this column is a foreign key.
     * @param fk True if this column is a foreign key.
     */
    void setForeignKey(boolean fk) {
        this.fk = fk;
    }

    /**
     * If this is a foreign key, get the name of the table it references.
     * @return The name of the table this foreign key references, or null if this is not a foreign key.
     */
    public String getReferencedTable() {
        return referencedTable;
    }

    /**
     * Set the name of the table this foreign key references.
     * @param referencedTable  The name of the table this foreign key references.
     */
    void setReferencedTable(String referencedTable) {
        this.referencedTable = referencedTable;
    }

    /**
     * If this is a foreign key, get the name of the column it references.
     * @return The name of the column this foreign key references, or null if this is not a foreign key.
     */
    public String getReferencedColumn() {
        return referencedColumn;
    }

    /**
     * Set the name of the column this foreign key references.
     * @param referencedColumn  The name of the table this foreign key references.
     */
    void setReferencedColumn(String referencedColumn) {
        this.referencedColumn = referencedColumn;
    }

    /**
     * See {@link Object#toString()}
     * @return A string representation of a column description.
     */
    @Override
    public String toString() {
        return "ColumnDescription{" +
                "columnName='" + columnName + '\'' +
                ", columnType='" + columnType + '\'' +
                ", nullable=" + nullable +
                ", pk=" + pk +
                ", defaultValue='" + defaultValue + '\'' +
                ", fk=" + fk +
                ", referencedTable='" + referencedTable + '\'' +
                ", referencedColumn='" + referencedColumn + '\'' +
                ", columnSize=" + columnSize +
                ", comments='" + comments + '\'' +
                '}';
    }
}
