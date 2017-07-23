package net.tralfamadore.dbUtils;

import org.hibernate.Session;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class: DbUtils
 * Created by billreh on 7/22/17.
 */
@Repository
public class DbUtils {
    @PersistenceContext
    private EntityManager em;

    public EntityManager getEm() {
        return em;
    }

    public void setEm(EntityManager em) {
        this.em = em;
    }

    @Transactional
    public TableDescription getTableDescription(String tableName) {
        return getTableDescription(tableName, null);
    }

    @Transactional
    private TableDescription getTableDescription(String tableName, String schemaName) {
        return new TableDescription(getColumnDescriptions(tableName), tableName, schemaName);
    }

    @Transactional
    private List<ColumnDescription> getColumnDescriptions(String tableName) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> getColumnDescriptions(connection, tableName));
    }

    private List<ColumnDescription> getColumnDescriptions(Connection connection, String tableName) {
        List<ColumnDescription> columnDescriptions = new ArrayList<>();
        try {
            ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, "%");
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
        addPrimaryKeys(connection, tableName, columnDescriptions);
        addForeignKeys(connection, tableName, columnDescriptions);
        return columnDescriptions;
    }

    private void addForeignKeys(Connection connection, String tableName, List<ColumnDescription> columnDescriptions) {
        try {
            ResultSet resultSet = connection.getMetaData().getImportedKeys(null, null, tableName);
            while(resultSet.next()) {
                String pkColumnName = resultSet.getString("PKCOLUMN_NAME");
                String pkTableName = resultSet.getString("PKTABLE_NAME");
                String fkColumnName = resultSet.getString("FKCOLUMN_NAME");
                String fkTableName = resultSet.getString("FKTABLE_NAME");
                for(ColumnDescription columnDescription : columnDescriptions) {
                    if(columnDescription.getColumnName().equals(fkColumnName)) {
                        columnDescription.setFk(true);
                        columnDescription.setReferencedColumn(pkColumnName);
                        columnDescription.setReferencedTable(pkTableName);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void addPrimaryKeys(Connection connection, String tableName, List<ColumnDescription> columnDescriptions) {
        try {
            ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, tableName);
            while(resultSet.next()) {
                String pkName = resultSet.getString("COLUMN_NAME");
                for(ColumnDescription columnDescription : columnDescriptions) {
                    if(columnDescription.getColumnName().equals(pkName)) {
                        columnDescription.setPk(true);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
