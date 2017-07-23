package net.tralfamadore.dbUtils;

/**
 * Class: ColumnDescription
 * Created by billreh on 7/22/17.
 */
public class ColumnDescription {
    private String columnName;
    private String columnType;
    private boolean nullable;
    private boolean pk;
    private String defaultValue;
    private boolean fk;
    private String referencedTable;
    private String referencedColumn;
    private int columnSize;
    private final String comments;

    public ColumnDescription(String columnName, String columnType, boolean nullable, String defaultValue, int columnSize, String comments) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.columnSize = columnSize;
        this.comments = comments;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public String getComments() {
        return comments;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public boolean isPk() {
        return pk;
    }

    void setPk(boolean pk) {
        this.pk = pk;
    }

    public boolean isFk() {
        return fk;
    }

    void setFk(boolean fk) {
        this.fk = fk;
    }

    public String getReferencedTable() {
        return referencedTable;
    }

    void setReferencedTable(String referencedTable) {
        this.referencedTable = referencedTable;
    }

    public String getReferencedColumn() {
        return referencedColumn;
    }

    void setReferencedColumn(String referencedColumn) {
        this.referencedColumn = referencedColumn;
    }

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
