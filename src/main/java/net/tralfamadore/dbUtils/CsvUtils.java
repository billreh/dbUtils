package net.tralfamadore.dbUtils;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.hibernate.Session;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class: CsvUtils
 * Created by billreh on 8/18/17.
 *
 * @author wreh
 */
@Repository
public class CsvUtils {
    /**
     * The entity manager
     */
    @PersistenceContext
    private EntityManager em;

    /**
     * Get the entity manager.
     *
     * @return The entity manager.
     */
    public EntityManager getEm() {
        return em;
    }

    /**
     * Set the entity manager.
     *
     * @param em The entity manager.
     */
    public void setEm(EntityManager em) {
        this.em = em;
    }


    /**
     * Create a csv file from a result set.
     *
     * @param query    The query to execute.
     * @param bindVars The bind variables for the query.
     * @param fileName The file to write to.
     * @return The csv file.
     */
    @Transactional
    public File csvFromQuery(String query, List<Object> bindVars, String fileName, List<ColumnNameProcessor> columnNameProcessors) {
        Session session = em.unwrap(Session.class);

        return session.doReturningWork(connection -> {
            PreparedStatement statement = connection.prepareStatement(query);
            int i = 1;
            for (Object var : bindVars)
                statement.setObject(i++, var);
            ResultSet resultSet = statement.executeQuery();
            return csvFromResultSet(resultSet, fileName, columnNameProcessors);
        });
    }

    @Transactional
    public void insertFromCsv(String tableName, File csvFile, List<ColumnNameProcessor> columnNameProcessors) {
        Session session = em.unwrap(Session.class);

        session.doWork(connection -> {
            ObjectRowProcessor rowProcessor = new ObjectRowProcessor() {
                @Override
                public void rowProcessed(Object[] row, ParsingContext context) {
                    String[] headers = context.headers();
                    List<String> translatedHeaders = new ArrayList<>();
                    for(String header : headers) {
                        header = processColumnName(header, columnNameProcessors);
                        translatedHeaders.add(header);
                    }
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append("insert into ").append(tableName).append("(");
                    stringBuilder.append(String.join(", ", translatedHeaders));
                    stringBuilder.append(") values (");
                    for (int i = 0; i < headers.length - 1; i++)
                        stringBuilder.append("?,");
                    stringBuilder.append("?)");
                    try {
                        PreparedStatement statement = connection.prepareStatement(stringBuilder.toString());
                        int i = 1;
                        for (Object column : row) {
                            statement.setObject(i++, column);
                        }
                        statement.executeUpdate();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            CsvParserSettings csvParserSettings = new CsvParserSettings();
            csvParserSettings.setProcessor(rowProcessor);
            csvParserSettings.setHeaderExtractionEnabled(true);
            CsvParser csvParser = new CsvParser(csvParserSettings);
            csvParser.parse(csvFile);
        });
    }

    private String processColumnName(String header, List<ColumnNameProcessor> columnNameProcessors) {
        for(ColumnNameProcessor columnNameProcessor : columnNameProcessors) {
            if(columnNameProcessor instanceof ExtendedColumnNameProcessor) {
                ExtendedColumnNameProcessor processor = (ExtendedColumnNameProcessor) columnNameProcessor;
                if(processor.matches(header)) {
                    header = processor.apply(header);
                    if (processor.skipOthers())
                        break;
                }
            } else {
                header = columnNameProcessor.apply(header);
            }
        }
        return header;
    }

    /**
     * Create a csv file from a result set.
     *
     * @param resultSet The result set.
     * @param fileName  The file to write to.
     * @return The csv file.
     */
    public File csvFromResultSet(ResultSet resultSet, String fileName, List<ColumnNameProcessor> columnNameProcessors) {
        File file = new File(fileName);
        FileWriter fileWriter = null;
        CsvWriter csvWriter = null;
        try {
            fileWriter = new FileWriter(file);
            csvWriter = new CsvWriter(fileWriter, new CsvWriterSettings());
            List<String> headers = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String header = metaData.getColumnName(i);
                header = processColumnName(header, columnNameProcessors);
                headers.add(header);
            }
            csvWriter.writeHeaders(headers);
            while (resultSet.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    row.add(resultSet.getObject(i));
                }
                csvWriter.writeRow(row);
            }
            return file;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            csvWriter.close();
            try {
                fileWriter.close();
            } catch (IOException e) {
                //noinspection ThrowFromFinallyBlock
                throw new RuntimeException(e);
            }
        }
    }
}
