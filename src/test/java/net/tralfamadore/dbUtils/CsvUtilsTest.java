package net.tralfamadore.dbUtils;

import net.tralfamadore.StringUtils;
import net.tralfamadore.config.AppConfig;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class: CsvUtilsTest
 * Created by billreh on 8/18/17.
 */
public class CsvUtilsTest {
    private static AnnotationConfigApplicationContext context;
    private static CsvUtils csvUtils;
    private static ColumnNameProcessor toLower = String::toLowerCase;
    private static ColumnNameProcessor capitalize = StringUtils::capitalize;
    private static ColumnNameProcessor underscoreToSpace = n -> n.replaceAll("_", " ");
    private static ColumnNameProcessor spaceToUnderscore = n -> n.replaceAll(" ", "_");
    private static ExtendedColumnNameProcessor zipCodeProcessor = new ExtendedColumnNameProcessor() {
        @Override
        public boolean matches(String columnName) {
            return columnName.toLowerCase().equals("zip_code");
        }

        @Override
        public boolean skipOthers() {
            return true;
        }

        @Override
        public String apply(String s) {
            return "The Zip Code";
        }
    };
    private static ExtendedColumnNameProcessor zipCodeUnprocessor = new ExtendedColumnNameProcessor() {
        @Override
        public boolean matches(String columnName) {
            return columnName.equals("The Zip Code");
        }

        @Override
        public boolean skipOthers() {
            return true;
        }

        @Override
        public String apply(String s) {
            return "zip_code";
        }
    };

    @BeforeClass
    public static void setUp() {
        context = new AnnotationConfigApplicationContext();
        context.register(AppConfig.class);
        context.refresh();
        csvUtils = context.getBean(CsvUtils.class);
    }

    @Test
    public void testCsvFromQuery() {
        List<ColumnNameProcessor> processors = new ArrayList<>();
        processors.add(zipCodeProcessor);
        processors.add(underscoreToSpace);
        processors.add(toLower);
        processors.add(capitalize);
        File file = csvUtils.csvFromQuery("select * from address where street like ?",
                Arrays.asList(new Object[]{"%ueen%"}),"/Users/billreh/Desktop/address.csv", processors);
    }

    @Ignore
    @Test
    public void testInsertFromCsv() {
        List<ColumnNameProcessor> processors = new ArrayList<>();
        processors.add(zipCodeUnprocessor);
        processors.add(String::toLowerCase);
        processors.add(spaceToUnderscore);
        File file = new File("/Users/billreh/Desktop/addressIn.csv");
        csvUtils.insertFromCsv("address", file, processors);
    }
}
