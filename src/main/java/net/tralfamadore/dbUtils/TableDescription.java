package net.tralfamadore.dbUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Class: TableDescription
 * Created by billreh on 7/22/17.
 */
public class TableDescription {
    private String schemaName;
    private String tableName;
    private String comments;
    private List<ColumnDescription> columnDescriptions = new ArrayList<>();

    public TableDescription(List<ColumnDescription> columnDescriptions, String tableName, String schemaName, String comments) {
        this.columnDescriptions = columnDescriptions;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.comments = comments;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getComments() {
        return comments;
    }

    public List<String> getColumnNames() {
        return columnDescriptions.stream().map(ColumnDescription::getColumnName).collect(Collectors.toList());
    }

    public Optional<ColumnDescription> getColumnDescription(String columnName) {
        return columnDescriptions.stream().filter(cd -> cd.getColumnName().equals(columnName)).findFirst();
    }

    public List<ColumnDescription> getColumnDescriptions() {
        return columnDescriptions;
    }

    public List<ColumnDescription> getPrimaryKeys() {
        return columnDescriptions.stream().filter(ColumnDescription::isPk).collect(Collectors.toList());
    }

    public List<ColumnDescription> getForeignKeys() {
        return columnDescriptions.stream().filter(ColumnDescription::isFk).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "TableDescription{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", comments='" + comments + '\'' +
                ", columnDescriptions=" + columnDescriptions +
                '}';
    }
}
