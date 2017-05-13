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

    @SuppressWarnings("Duplicates")
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
        List<MysqlDbUtils.ColumnDescription> description = dbUtils.describeTable("children_table");
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
            System.out.println(dbUtils.dropTables(ChildrenTable.class, ParentTable.class, ChildTable.class, TheAddress.class, TheBean.class));
        } catch(Exception ignored) { }
        assertFalse(dbUtils.tableExists("the_address"));
        assertFalse(dbUtils.tableExists("the_bean"));
        assertFalse(dbUtils.tableExists("parent_table"));
        assertFalse(dbUtils.tableExists("child_table"));
        assertFalse(dbUtils.tableExists("children_table"));

        System.out.println(dbUtils.createTables(TheBean.class, TheAddress.class, ChildTable.class, ChildrenTable.class, ParentTable.class));
        assertTrue(dbUtils.tableExists("the_address"));
        assertTrue(dbUtils.tableExists("the_bean"));
        assertTrue(dbUtils.tableExists("parent_table"));
        assertTrue(dbUtils.tableExists("child_table"));
        assertTrue(dbUtils.tableExists("children_table"));

        System.out.println(dbUtils.dropTables(ChildrenTable.class, ParentTable.class, ChildTable.class, TheAddress.class, TheBean.class));
        assertFalse(dbUtils.tableExists("the_address"));
        assertFalse(dbUtils.tableExists("the_bean"));
        assertFalse(dbUtils.tableExists("parent_table"));
        assertFalse(dbUtils.tableExists("child_table"));
        assertFalse(dbUtils.tableExists("children_table"));
    }

    @Test
    public void testEnum() throws Exception {
        System.out.println(dbUtils.createTable(TheBean.class, false));
    }
}
