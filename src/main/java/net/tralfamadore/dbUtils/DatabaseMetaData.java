package net.tralfamadore.dbUtils;

import org.hibernate.Session;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class: DbUtils
 * Created by billreh on 7/22/17.
 * @author wreh
 */
@Repository
public class DatabaseMetaData {
    /** The entity manager */
    @PersistenceContext
    private EntityManager em;

    /**
     * Get the entity manager.
     * @return The entity manager.
     */
    public EntityManager getEm() {
        return em;
    }

    /**
     * Set the entity manager.
     * @param em The entity manager.
     */
    public void setEm(EntityManager em) {
        this.em = em;
    }

    /**
     * Get a {@link TableDescription} for the given table.
     * @param tableName The table name.
     * @return The {@link TableDescription} for the given table name.
     */
    @Transactional
    public TableDescription getTableDescription(String tableName) {
        return getTableDescription(tableName, null);
    }

    /**
     * Get a {@link TableDescription} for the given table.
     * @param tableName The table name.
     * @param schemaName The schema name.
     * @return The {@link TableDescription} for the given table and schema names.
     */
    @Transactional
    public TableDescription getTableDescription(String tableName, String schemaName) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> TableDescription.getTableDescription(connection, tableName, schemaName));
    }

    /**
     * Get a list of table names matching <code>tableNamePattern</code>.
     * @param tableNamePattern The table name pattern to search on.  Table name pattern is in the form of a SQL LIKE
     *                         statement i.e. "%_LOG" matches all tables ending in _LOG.  May be null (searches all tables).
     * @return A list of table names that match the table name pattern.
     */
    @Transactional
    public List<String> getTableNames(String tableNamePattern) {
        return getTableNames(null, tableNamePattern);
    }

    /**
     * Get a list of table names matching <code>tableNamePattern</code>.
     * @param schemaName The schema name to search in.  May be null (searches all schemas).
     * @param tableNamePattern The table name pattern to search on.  Table name pattern is in the form of a SQL LIKE
     *                         statement i.e. "%_LOG" matches all tables ending in _LOG.  May be null (searches all tables).
     * @return A list of table names that match the table name pattern.
     */
    @Transactional
    public List<String> getTableNames(String schemaName, String tableNamePattern) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> getTableNames(connection, tableNamePattern, schemaName));
    }

    /**
     * Get a list of schema names matching the name pattern.
     * @param schemaNamePattern A pattern in the style of sql LIKE, ie '%TEST%' would get all schemas with the word
     *                          TEST in them.  Can be null (lists all schemas).
     * @return A list of schema names.
     */
    @Transactional
    public List<String> getSchemaNames(String schemaNamePattern) {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(connection -> getSchemaNames(connection, schemaNamePattern));
    }

    /**
     * Get a list of all catalog names.
     * @return A list of all catalog names.
     */
    @Transactional
    public List<String> getCatalogNames() {
        Session hibernateSession = em.unwrap(Session.class);

        return hibernateSession.doReturningWork(DatabaseMetaData::getCatalogNames);
    }

    /**
     * Get a list of table names matching <code>tableNamePattern</code>.
     * @param connection The {@link Connection}.
     * @param tableNamePattern The table name pattern, in the style of a sql LIKE pattern.  May be null (all tables).
     * @param schemaName The schema name.  May be null (all schemas).
     * @return A list of table names matching the pattern.
     */
    private static List<String> getTableNames(Connection connection, String tableNamePattern, String schemaName) {
        try {
            ResultSet resultSet = connection.getMetaData().getTables(null, schemaName, tableNamePattern, null);
            List<String> tableNames = new ArrayList<>();
            while(resultSet.next()) {
                tableNames.add(resultSet.getString("TABLE_NAME"));
            }
            return tableNames;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a list of schema names matching the name pattern.
     * @param connection The {@link Connection}.
     * @param schemaNamePattern A pattern in the style of sql LIKE, ie '%TEST%' would get all schemas with the word
     *                          TEST in them.  Can be null (lists all schemas).
     * @return A list of schema names.
     */
    private static List<String> getSchemaNames(Connection connection, String schemaNamePattern) {
        try {
            ResultSet resultSet = connection.getMetaData().getSchemas(null, schemaNamePattern);
            List<String> schemsNames = new ArrayList<>();
            while(resultSet.next()) {
                schemsNames.add(resultSet.getString("TABLE_SCHEM"));
            }
            return schemsNames;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a list of all catalog names.
     * @param connection The {@link Connection}.
     * @return A list of all catalog names.
     */
    private static List<String> getCatalogNames(Connection connection) {
        try {
            ResultSet resultSet = connection.getMetaData().getCatalogs();
            List<String> catalogNames = new ArrayList<>();
            while(resultSet.next()) {
                catalogNames.add(resultSet.getString("TABLE_CAT"));
            }
            return catalogNames;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
