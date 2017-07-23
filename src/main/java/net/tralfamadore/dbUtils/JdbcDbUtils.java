package net.tralfamadore.dbUtils;

import org.hibernate.Session;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class: JdbcDbUtils
 * Created by billreh on 7/22/17.
 */
@Repository
public class JdbcDbUtils {
    @PersistenceContext
    private EntityManager em;

    public EntityManager getEm() {
        return em;
    }

    public void setEm(EntityManager em) {
        this.em = em;
    }

    @Transactional
    public List<String> getColumnNames(String tableName) {
        return getColumnNames(tableName, "%");
    }

    @Transactional
    public List<String> getColumnNames(String tableName, String columnNamePattern) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> {
            List<String> columnNames = new ArrayList<>();
            try {
                ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, columnNamePattern);
                while (resultSet.next()) {
                    columnNames.add(resultSet.getString("COLUMN_NAME"));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return columnNames;
        });
    }

    @Transactional
    public List<IDbUtils.ColumnDescription> describeTable(String tableName) {
        List<IDbUtils.ColumnDescription> columnDescriptions = new ArrayList<>();
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> {
            try {
                ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, "%");
                while (resultSet.next()) {
                    String name = resultSet.getString("COLUMN_NAME");
                    String type = resultSet.getString("TYPE_NAME");
                    boolean nullable = resultSet.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                    String defaultValue = resultSet.getString("COLUMN_DEF");
                    String additionalArguments = "";

                    IDbUtils.ColumnDescription columnDescription
                            = new IDbUtils.ColumnDescription(name, type, nullable, false, defaultValue, additionalArguments);
                    columnDescriptions.add(columnDescription);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            findPks(tableName, columnDescriptions);
            return columnDescriptions;
        });

    }

    private void findPks(String tableName, List<IDbUtils.ColumnDescription> columnDescriptions) {
        Session hibernateSession = em.unwrap(Session.class);

        hibernateSession.doWork(connection -> {
            String pkName;
            ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, tableName);
            if(resultSet.next()) {
                pkName = resultSet.getString("COLUMN_NAME");
                for(IDbUtils.ColumnDescription columnDescription : columnDescriptions) {
                    if(columnDescription.getName().equals(pkName))
                        columnDescription.setIsPk(true);
                }
            }
        });
    }

    public boolean tableExists(String tableName) {
        return !getTableNames(tableName).isEmpty();
    }

    @Transactional
    public List<String> getTableNames() {
        return getTableNames("%");
    }

    @Transactional
    public List<String> getTableNames(String tableNamePattern) {
        Session hibernateSession = em.unwrap(Session.class);

            return hibernateSession.doReturningWork(connection -> {
            List<String> tableNames = new ArrayList<>();
            try {
                ResultSet resultSet = connection.getMetaData().getTables(null, null, tableNamePattern, null);
                while (resultSet.next()) {
                    tableNames.add(resultSet.getString("TABLE_NAME"));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return tableNames;
        });
    }

    @Transactional
    public List<String> getForeignKeys(String tableName) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> {
            List<String> tableNames = new ArrayList<>();
            try {
                ResultSet resultSet = connection.getMetaData().getImportedKeys(null, null, tableName);
                while (resultSet.next()) {
                    System.out.print(resultSet.getString("PKTABLE_NAME") + ".");
                    System.out.print(resultSet.getString("PKCOLUMN_NAME") + " -> ");
                    System.out.print(resultSet.getString("FKTABLE_NAME") + ".");
                    System.out.println(resultSet.getString("FKCOLUMN_NAME"));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return tableNames;
        });

    }
}
