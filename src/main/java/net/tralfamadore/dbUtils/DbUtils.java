package net.tralfamadore.dbUtils;

import org.hibernate.Session;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

/**
 * Class: DbUtils
 * Created by billreh on 7/22/17.
 * @author wreh
 */
@Repository
public class DbUtils {
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
}
