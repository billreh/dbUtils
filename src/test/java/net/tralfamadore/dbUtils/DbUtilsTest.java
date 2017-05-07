package net.tralfamadore.dbUtils;

import net.tralfamadore.config.AppConfig;
import net.tralfamadore.config.DatabaseConfig;
import net.tralfamadore.domain.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Class: net.tralfamadore.dbUtils.DbUtilsTest
 * Created by billreh on 5/6/17.
 */
public class DbUtilsTest {
    private static AnnotationConfigApplicationContext context;
    private static DbUtils dbUtils;

    @BeforeClass
    public static void setUp() {
        context = new AnnotationConfigApplicationContext();
        context.register(AppConfig.class);
        context.register(DatabaseConfig.class);
        context.refresh();
        dbUtils = context.getBean(DbUtils.class);
    }

    @AfterClass
    public static void tearDown() {
        context.close();
    }

    @Test
    public void testGetTableNames() throws Exception {
        List<String> tableNames = dbUtils.getTableNames();
        tableNames.forEach(System.out::println);
    }

    @Test
    public void testDescribeTable() throws Exception {
        List<MysqlDbUtils.ColumnDescription> description = dbUtils.describeTable("the_bean");
        description.forEach(System.out::println);

        description = dbUtils.describeTable("not_there");
        assertTrue(description.isEmpty());
    }

    @Test
    public void testTableExists() throws Exception {
        assertFalse(dbUtils.tableExists("not_there"));
        try {
            dbUtils.createTables("net.tralfamadore.domain");
        } catch(Exception ignored) { }
        assertTrue(dbUtils.tableExists("the_bean"));
        dbUtils.dropTables(ChildrenTable.class, ParentTable.class, ChildTable.class, TheAddress.class, TheBean.class);
    }

    @Test
    public void testCreateAndDropTables() throws Exception {
        try {
            dbUtils.dropTables(ChildrenTable.class, ParentTable.class, ChildTable.class, TheAddress.class, TheBean.class);
        } catch(Exception ignored) { }
        assertFalse(dbUtils.tableExists("the_address"));

        System.out.println(dbUtils.createTables("net.tralfamadore.domain"));
        assertTrue(dbUtils.tableExists("the_address"));
        assertTrue(dbUtils.tableExists("the_bean"));

        dbUtils.dropTables(ChildrenTable.class, ParentTable.class, ChildTable.class, TheAddress.class, TheBean.class);
        assertFalse(dbUtils.tableExists("the_address"));
        assertFalse(dbUtils.tableExists("the_bean"));
    }
}
