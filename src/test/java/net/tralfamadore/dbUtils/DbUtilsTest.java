package net.tralfamadore.dbUtils;

import com.google.common.base.Stopwatch;
import net.tralfamadore.*;
import net.tralfamadore.config.AppConfig;
import net.tralfamadore.dbUtils.entity.Address;
import net.tralfamadore.dbUtils.entity.Listing;
import net.tralfamadore.dbUtils.entity.Testme;
import net.tralfamadore.domain.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Class: net.tralfamadore.dbUtils.DbUtilsTest
 * Created by billreh on 5/6/17.
 */
public class DbUtilsTest {
    private static AnnotationConfigApplicationContext context;
    private static IDbUtils dbUtils;
    private static JdbcDbUtils jdbcDbUtils;
    private static DatabaseUtils theDatabaseUtils;

    @BeforeClass
    public static void setUp() {
        context = new AnnotationConfigApplicationContext();
        context.register(AppConfig.class);
        context.refresh();
        dbUtils = context.getBean(IDbUtils.class);
        jdbcDbUtils = context.getBean(JdbcDbUtils.class);
        theDatabaseUtils = new DatabaseUtils();
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
            dbUtils.createTable(TheBean.class);
        } catch (Exception ignored) {
        }
        assertTrue(dbUtils.tableExists("the_bean"));
        dbUtils.dropTable(TheBean.class);
    }

    @Test
    public void testCreateAndDropTables() throws Exception {
        try {
            System.out.println(dbUtils.dropTables(ChildrenTable.class, ParentTable.class, ChildTable.class, TheAddress.class, TheBean.class));
        } catch (Exception ignored) {
        }
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
    public void testGetColumnNames() throws Exception {
        System.out.println(jdbcDbUtils.getColumnNames("address"));
    }

    @Test
    public void testJdbcDescribeTable() throws Exception {
        System.out.println(jdbcDbUtils.describeTable("listing"));
    }

    @Test
    public void testGetTableDescription() throws Exception {
        TableDescription tableDescription = theDatabaseUtils.getTableDescription("listing");
        tableDescription.getForeignKeys().forEach(System.out::println);
        System.out.println();
        tableDescription.getPrimaryKeys().forEach(System.out::println);
        System.out.println();
        tableDescription.getColumnDescriptions().forEach(System.out::println);
        System.out.println();
        tableDescription.getColumnNames().forEach(System.out::println);
        System.out.println();
        System.out.println(tableDescription.getSchemaName());
        System.out.println();
        System.out.println(tableDescription.getTableName());
        System.out.println();
        System.out.println(tableDescription.getColumnDescription("address_id"));

        for(ColumnDescription columnDescription : tableDescription.getForeignKeys()) {
            System.out.println(theDatabaseUtils.getTableDescription(columnDescription.getReferencedTable()));
        }
    }

    @Test
    public void testGetTheTableNames() {
        theDatabaseUtils.getTableNames(null, null).forEach(System.out::println);
    }

    @Test
    public void testgetSchemaNames() throws Exception {
        theDatabaseUtils.getSchemaNames(null).forEach(System.out::println);
    }

    @Test
    public void testgetCatalogNames() throws Exception {
        theDatabaseUtils.getCatalogNames().forEach(System.out::println);
    }

    @Test
    public void testTuple2Select() throws Exception {
        Optional<Tuple2<Long,String>> tuple2 = theDatabaseUtils.sql("select id, street from address limit 1").selectTuple(Long.class, String.class);
        if(tuple2.isPresent()) {
            System.out.println(tuple2.get().getValue1());
            System.out.println(tuple2.get().getValue2());
        } else {
            System.out.println("empty");
        }

        List<Tuple2<Long,String>> tuple2s = new DatabaseUtils().sql("select id, street from address limit 10").selectTupleList(Long.class, String.class);
        for(Tuple2<Long,String> tuple : tuple2s) {
            System.out.println(tuple.getValue1());
            System.out.println(tuple.getValue2());
        }

        Optional<Tuple3<Long,String,Double>> tuple3 = new DatabaseUtils().sql(
                "select id, stringVal, doubleVal from testme limit 1").selectTuple(Long.class, String.class, Double.class);
        if(tuple3.isPresent()) {
            System.out.println(tuple3.get().getValue1());
            System.out.println(tuple3.get().getValue2());
            System.out.println(tuple3.get().getValue3());
        } else {
            System.out.println("empty");
        }

        List<Tuple3<Long,String,Double>> tuple3s = new DatabaseUtils().sql(
                "select id, stringVal, doubleVal from testme limit 10").selectTupleList(Long.class, String.class, Double.class);
        for(Tuple3<Long,String,Double> tuple : tuple3s) {
            System.out.println(tuple.getValue1());
            System.out.println(tuple.getValue2());
            System.out.println(tuple.getValue3());
        }

        Optional<Tuple4<Long,String,Double,LocalDate>> tuple4 = theDatabaseUtils.selectTuple(
                "select id, stringVal, doubleVal, dateVal from testme limit 1", Long.class,
                String.class, Double.class, LocalDate.class);
        if(tuple4.isPresent()) {
            System.out.println(tuple4.get().getValue1());
            System.out.println(tuple4.get().getValue2());
            System.out.println(tuple4.get().getValue3());
            System.out.println(tuple4.get().getValue4());
        } else {
            System.out.println("empty");
        }

        List<Tuple4<Long,String,Double,LocalDate>> tuple4s = theDatabaseUtils.selectTupleList(
                "select id, stringVal, doubleVal, dateVal from testme limit 10", Long.class, String.class, Double.class, LocalDate.class);
        tuple4s.forEach(System.out::println);

        Optional<Tuple5<Long,String,Double,LocalDate,LocalDateTime>> tuple5 = theDatabaseUtils.selectTuple(
                "select id, stringVal, doubleVal, dateVal, timestameVal from testme limit 1", Long.class,
                String.class, Double.class, LocalDate.class, LocalDateTime.class);
        if(tuple5.isPresent()) {
            System.out.println(tuple5.get().getValue1());
            System.out.println(tuple5.get().getValue2());
            System.out.println(tuple5.get().getValue3());
            System.out.println(tuple5.get().getValue4());
            System.out.println(tuple5.get().getValue5());
        } else {
            System.out.println("empty");
        }

        List<Tuple5<Long,String,Double,LocalDate,LocalDateTime>> tuple5s = theDatabaseUtils.selectTupleList(
                "select id, stringVal, doubleVal, dateVal, timestameVal from testme limit 10", Long.class, String.class,
                Double.class, LocalDate.class, LocalDateTime.class);
        tuple5s.forEach(System.out::println);


        Optional<Tuple6<Long,String,Double,LocalDate,LocalDateTime,Integer>> tuple6 = theDatabaseUtils.selectTuple(
                "select id, stringVal, doubleVal, dateVal, timestameVal, 1 from testme limit 1", Long.class,
                String.class, Double.class, LocalDate.class, LocalDateTime.class, Integer.class);
        if(tuple6.isPresent()) {
            System.out.println(tuple6.get().getValue1());
            System.out.println(tuple6.get().getValue2());
            System.out.println(tuple6.get().getValue3());
            System.out.println(tuple6.get().getValue4());
            System.out.println(tuple6.get().getValue5());
            System.out.println(tuple6.get().getValue6());
        } else {
            System.out.println("empty");
        }

        List<Tuple6<Long,String,Double,LocalDate,LocalDateTime,Integer>> tuple6s = theDatabaseUtils.selectTupleList(
                "select id, stringVal, doubleVal, dateVal, timestameVal, 1 from testme limit 10", Long.class, String.class,
                Double.class, LocalDate.class, LocalDateTime.class, Integer.class);
        tuple6s.forEach(System.out::println);

        Optional<Tuple7<Long,String,Double,LocalDate,LocalDateTime,Integer,String>> tuple7 = theDatabaseUtils.selectTuple(
                "select id, stringVal, doubleVal, dateVal, timestameVal, 1, 'hello' from testme limit 1", Long.class,
                String.class, Double.class, LocalDate.class, LocalDateTime.class, Integer.class, String.class);
        if(tuple7.isPresent()) {
            System.out.println(tuple7.get().getValue1());
            System.out.println(tuple7.get().getValue2());
            System.out.println(tuple7.get().getValue3());
            System.out.println(tuple7.get().getValue4());
            System.out.println(tuple7.get().getValue5());
            System.out.println(tuple7.get().getValue6());
            System.out.println(tuple7.get().getValue7());
        } else {
            System.out.println("empty");
        }

        List<Tuple7<Long,String,Double,LocalDate,LocalDateTime,Integer,String>> tuple7s = theDatabaseUtils.selectTupleList(
                "select id, stringVal, doubleVal, dateVal, timestameVal, 1, 'hello' from testme limit 10", new ArrayList<>(),
                Long.class, String.class, Double.class, LocalDate.class, LocalDateTime.class, Integer.class, String.class);
        tuple7s.forEach(System.out::println);

        List<Object> bindVars = new ArrayList<>();
        bindVars.add(0);
        bindVars.add("blah");

        tuple7s = theDatabaseUtils.selectTupleList(
                "select id, stringVal, doubleVal, dateVal, timestameVal, 1, 'hello' from testme where id > ? and stringVal != ?", bindVars,
                Long.class, String.class, Double.class, LocalDate.class, LocalDateTime.class, Integer.class, String.class);
        tuple7s.forEach(System.out::println);

        tuple7s = theDatabaseUtils.selectTupleList(
                "select id, stringVal, doubleVal, dateVal, timestameVal, 1, 'hello' from testme where id > ? and stringVal != ?",
                Long.class, String.class, Double.class, LocalDate.class, LocalDateTime.class, Integer.class, String.class, 0, "blah");
        tuple7s.forEach(System.out::println);

        System.out.println(new DatabaseUtils().sql("select id, stringVal from testme limit 1").selectTuple(Long.class, String.class));
    }

    @Test
    public void testSelectMap() {
        Optional<Map<String,Object>> row = theDatabaseUtils.sql("select * from testme limit 1").selectMap();
        if(row.isPresent()) {
            row.get().keySet().forEach(k -> {
                System.out.println(k + " -> " + row.get().get(k));
            });
        }
    }

    @Test
    public void cacheTest() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Optional<Object[]> row = new DatabaseUtils().cache(2, TimeUnit.SECONDS).sql("select * from address limit 1").select();
        System.out.println(stopwatch.toString());
        stopwatch.reset();
        stopwatch.start();
        row = new DatabaseUtils().cache(2, TimeUnit.SECONDS).sql("select * from address limit 1").select();
        System.out.println(stopwatch.toString());
        Thread.sleep(1000);
        stopwatch.reset();
        stopwatch.start();
        row = new DatabaseUtils().cache(2, TimeUnit.SECONDS).sql("select * from address limit 1").select();
        System.out.println(stopwatch.toString());
        Thread.sleep(2000);
        stopwatch.reset();
        stopwatch.start();
        row = new DatabaseUtils().cache(2, TimeUnit.SECONDS).sql("select * from address limit 1").select();
        System.out.println(stopwatch.toString());
    }

    @Test(expected = ClassCastException.class)
    public void testBreakCache() throws Exception {
        Optional<Date> now = new DatabaseUtils().cache(1, TimeUnit.MINUTES).sql("select dateVal from testme limit 1").select(Date.class);
        System.out.println(now.get().getTime());
        now = new DatabaseUtils().sql("select dateVal from testme limit 1").select(Date.class);
        System.out.println(now.get().getTime());
        Optional<LocalDate> alsoNow = new DatabaseUtils().sql("select dateVal from testme limit 1").select(LocalDate.class);
        System.out.println(alsoNow.get().atStartOfDay());
    }

    @Test
    public void testTransaction() throws Exception {
        long countBefore = new DatabaseUtils().sql("select count(*) from testme").select(long.class).orElse(0L);
        new DatabaseUtils().transactionCallback(databaseUtils -> {
            databaseUtils.sql("insert into testme (stringVal, doubleVal, dateVal, timestameVal) VALUES(?, ?, ?, ?)")
                    .bindVars("hello", 2.0, new Date(), new Date()).execute();
            return null;
        });
        long countAfter = new DatabaseUtils().sql("select count(*) from testme").select(long.class).orElse(0L);
        assertEquals(countBefore + 1, countAfter);

        countBefore = new DatabaseUtils().sql("select count(*) from testme").select(long.class).orElse(0L);
        new DatabaseUtils().transactionCallback(databaseUtils -> {
            Testme testme = new Testme();
            testme.setDateval(LocalDate.now());
            testme.setDoubleval(3.3);
            testme.setStringval("moo");
            testme.setTimestameval(LocalDateTime.now());
            databaseUtils.store(testme);
            return null;
        });
        countAfter = new DatabaseUtils().sql("select count(*) from testme").select(long.class).orElse(0L);
        assertEquals(countBefore + 1, countAfter);
    }

    @Test
    public void testTransactionRollback() throws Exception {
        long countBefore = new DatabaseUtils().sql("select count(*) from testme").select(long.class).orElse(0L);
        new DatabaseUtils().transactionCallback(databaseUtils -> {
            databaseUtils.sql("insert into testme (stringVal, doubleVal, dateVal, timestameVal) VALUES(?, ?, ?, ?)")
                    .bindVars("hello", 2.0, new Date(), new Date()).execute();
            databaseUtils.rollback();
            return null;
        });
        long countAfter = new DatabaseUtils().sql("select count(*) from testme").select(long.class).orElse(0L);
        assertEquals(countBefore, countAfter);

        countBefore = new DatabaseUtils().sql("select count(*) from testme").select(long.class).orElse(0L);
        new DatabaseUtils().transactionCallback(databaseUtils -> {
            Testme testme = new Testme();
            testme.setDateval(LocalDate.now());
            testme.setDoubleval(3.3);
            testme.setStringval("moo");
            testme.setTimestameval(LocalDateTime.now());
            databaseUtils.store(testme);
            databaseUtils.rollback();
            return null;
        });
        countAfter = new DatabaseUtils().sql("select count(*) from testme").select(long.class).orElse(0L);
        assertEquals(countBefore, countAfter);
    }

    @Test
    public void testResultSet() throws Exception {
        new DatabaseUtils().sql("select * from testme").resultSetCallback(resultSet -> {
            try {
                while(resultSet.next()) {
                    System.out.println(resultSet.getObject(1));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    @Test
    public void testEntity() throws Exception {
        List<Listing> listings = new DatabaseUtils().sql("select * from listing").selectList(Listing.class);
        listings.forEach(listing -> {
            System.out.print(listing.getId() + ", ");
            System.out.print(listing.getBathrooms() + ", ");
            System.out.print(listing.getAddress().getCity() + ", ");
            System.out.print(listing.getListingDetails().get(0).getParking() + ", ");
            listing.getPhotos().forEach(photo -> System.out.print(photo.getName() + ", "));
            listing.getListingDetails().get(0).getExteriorFeatures().forEach(photo -> System.out.print(photo.getName() + ", "));
            listing.getListingDetails().get(0).getOtherRooms().forEach(photo -> System.out.print(photo.getName() + ", "));
            System.out.println();
        });
        Address.getAllAddressses().forEach(address -> System.out.print(address.getStreet() + ", "));
        System.out.println();
    }

    @Test
    public void testMysqlConfig() throws Exception {
        Optional<Testme> testme = new DatabaseUtils("mysql").sql("select * from testme limit 1").select(Testme.class);
        testme.ifPresent(testme1 -> System.out.println(testme1.getDateval()));
    }

    @Test
    public void testEntityCallback() throws Exception {
        new DatabaseUtils().sql("select * from address").entityCallback(Address.class, addr -> {
            if(!addr.getStreet().toLowerCase().contains("queen")) {
                System.out.println(addr.getStreet());
                return false;
            }
            return true;
        });
    }
}
