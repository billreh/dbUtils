package net.tralfamadore.dbUtils;

import com.google.common.reflect.ClassPath;
import net.tralfamadore.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Class: DbUtils
 * Created by billreh on 5/5/17.
 */
@Repository
public class MysqlDbUtils implements DbUtils {
    @PersistenceContext
    private EntityManager em;

    public EntityManager getEm() {
        return em;
    }

    public void setEm(EntityManager em) {
        this.em = em;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> getTableNames() {
        return (List<String>)em.createNativeQuery("show tables").getResultList();
    }

    @Override
    public List<ColumnDescription> describeTable(String tableName) {
        List<ColumnDescription> columnDescriptions = new ArrayList<>();
        List rawColumnDescriptions;

        try {
            rawColumnDescriptions = em.createNativeQuery("desc " + tableName).getResultList();
        } catch(Exception e) {
            return columnDescriptions;
        }

        //noinspection unchecked
        rawColumnDescriptions.forEach(o -> {
            Object[] cols = (Object[]) o;

            String name = cols[0].toString();
            String type = cols[1].toString();
            boolean nullable = cols[2].toString().equalsIgnoreCase("yes");
            boolean pk = cols[3].toString().equalsIgnoreCase("pri");
            String defaultValue = cols[4] == null || cols[4].toString().equalsIgnoreCase("null") ?  null :
                    cols[3].toString();
            String additionalArguments = cols[5].toString();

            ColumnDescription columnDescription = new ColumnDescription(name, type, nullable, pk, defaultValue,
                    additionalArguments);

            columnDescriptions.add(columnDescription);
        });

        return columnDescriptions;
    }

    @Override
    public boolean tableExists(String tableName) {
        return !describeTable(tableName).isEmpty();
    }



    @Override
    @Transactional
    public String createTable(Class entityClass) {
        return createTable(entityClass, true);
    }

    @Override
    @Transactional
    public String createTable(Class entityClass, boolean generate) {
        if(entityClass.getAnnotation(Entity.class) == null)
            throw new RuntimeException("Class " + entityClass.getName() + " is not annotated with @Entity");

        StringBuilder sql = new StringBuilder();

        String tableName = getTableName(entityClass);
        List<String> columns = getColumnDeclarations(entityClass);
        columns.addAll(getInlineForeignKeys(entityClass));


        sql.append("CREATE TABLE ").append(tableName).append(" (\n");
        sql.append(String.join(",\n", columns));
        sql.append("\n);\n");

        if(generate)
            em.createNativeQuery(sql.toString()).executeUpdate();

        List<String> alterStatements = getAlterForeignKeys(entityClass);

        if(generate)
            alterStatements.forEach(s -> em.createNativeQuery(s).executeUpdate());

        alterStatements.forEach(s -> sql.append(s).append(";\n"));

        return sql.toString();
    }

    @Override
    @Transactional
    public String createTables(String packageName) {
        return createTables(packageName, true);
    }

    @Override
    @Transactional
    public String createTables(String packageName, boolean generate) {
        List<Class> entityClasses;
        try {
            entityClasses = getEntityClasses(packageName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return createTables(entityClasses, generate);
    }

    @Override
    @Transactional
    public String createTables(Class... entities) {
        return createTables(true, entities);
    }

    @Override
    @Transactional
    public String createTables(boolean generate, Class... entities) {
        if(entities == null || entities.length == 0)
            return "";

        return createTables(Arrays.asList(entities), generate);
    }

    @Override
    @Transactional
    public String createTables(List<Class> entityClasses) {
        return createTables(entityClasses, true);
    }

    @Override
    @Transactional
    public String createTables(List<Class> entityClasses, boolean generate) {
        StringBuilder sql = new StringBuilder();

        for(Class entityClass : entityClasses) {
            sql.append(createTable(entityClass, generate)).append("\n");
        }

        return sql.toString();
    }

    @Override
    @Transactional
    public String dropTables(String packageName) {
        return dropTables(packageName, true);
    }

    @Override
    @Transactional
    public String dropTables(String packageName, boolean generate) {
        List<Class> entityClasses;
        try {
            entityClasses = getEntityClasses(packageName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return dropTables(entityClasses, generate);
    }

    @Override
    @Transactional
    public String dropTables(List<Class> entityClasses) {
        return dropTables(entityClasses, true);
    }

    @Override
    @Transactional
    public String dropTables(List<Class> entityClasses, boolean generate) {
        StringBuilder sql = new StringBuilder();

        for(Class entityClass : entityClasses)
            sql.append(dropTable(entityClass, generate)).append("\n");

        return sql.toString();
    }

    @Override
    @Transactional
    public String dropTables(Class... entityClasses) {
        return dropTables(true, entityClasses);
    }

    @Override
    @Transactional
    public String dropTables(boolean generate, Class... entityClasses) {
        if(entityClasses == null || entityClasses.length == 0)
            return "";

        return dropTables(Arrays.asList(entityClasses), generate);
    }

    @Override
    @Transactional
    public String dropTable(Class entityClass) {
        return dropTable(entityClass, true);
    }

    @Override
    @Transactional
    public String dropTable(Class entityClass, boolean generate) {
        if(entityClass.getAnnotation(Entity.class) == null)
            throw new RuntimeException("Class " + entityClass.getName() + " is not annotated with @Entity");

        String sql = "DROP TABLE " + getTableName(entityClass);

        if(generate)
            em.createNativeQuery(sql).executeUpdate();

        return sql + ";";
    }

    private List<Class> getEntityClasses(String packageName) throws IOException {
        ClassPath classPath = ClassPath.from(ClassLoader.getSystemClassLoader());
        return classPath.getTopLevelClasses(packageName).stream().map(ClassPath.ClassInfo::load).
                collect(Collectors.toList());
    }

    private String getTableName(Class entityClass) {
        Table tableAnnotation = (Table) entityClass.getAnnotation(Table.class);

        if(tableAnnotation != null && !StringUtils.isEmpty(tableAnnotation.name()))
            return tableAnnotation.name().toUpperCase();

        return StringUtils.toDbCase(entityClass.getSimpleName());
    }

    private List<String> getColumnDeclarations(Class entityClass) {
        List<String> columns = new ArrayList<>();

        if(entityClass.getDeclaredFields() == null || entityClass.getDeclaredFields().length == 0)
            return columns;

        for(Field field : entityClass.getDeclaredFields()) {
            if(field.isAnnotationPresent(Transient.class))
                continue;
            if(field.isAnnotationPresent(OneToMany.class))
                continue;
            if(field.isAnnotationPresent(OneToOne.class)) {
                columns.add(getOneToOneDeclaration(field));
            } else {
                columns.add(getColumnDeclaration(field));
            }
        }

        return columns;
    }

    private String getColumnDeclaration(Field field) {
        String columnName = getColumnName(field);
        String columnType = getColumnType(field);
        String nullable = getNullable(field);
        String primaryKey = getPrimaryKey(field);
         return "\t" + columnName + " " + columnType + nullable + primaryKey;
    }

    private String getOneToOneDeclaration(Field field) {
        Field idColumn = getIdColumn(field.getType());
        if(idColumn == null)
            throw new RuntimeException("Can't find Id on child table of type " + field.getType());
        if(!field.isAnnotationPresent(JoinColumn.class))
            throw new RuntimeException("Can't find JoinColumn on child table of type " + field.getType());
        JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
        String columnName = joinColumn.name();
        String columnType = getColumnType(idColumn);
        String nullable = getNullable(field);
        return "\t" + columnName + " " + columnType + nullable;
    }

    private List<String> getInlineForeignKeys(Class entityClass) {
        List<String> columns = new ArrayList<>();

        if(entityClass.getDeclaredFields() == null || entityClass.getDeclaredFields().length == 0)
            return columns;

        for(Field field : entityClass.getDeclaredFields()) {
            if(field.isAnnotationPresent(OneToOne.class)) {
                Field idColumn = getIdColumn(field.getType());
                if(idColumn == null)
                    throw new RuntimeException("Can't find id column for class " + field.getType().getSimpleName());
                JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
                String columnName = joinColumn.name();
                String foreignKey = "\tFOREIGN KEY(" + columnName + ") REFERENCES " + getTableName(field.getType()) +
                        "(" + idColumn.getName() + ")";
                columns.add(foreignKey);
            }
        }

        return columns;
    }

    private List<String> getAlterForeignKeys(Class entityClass) {
        List<String> alterStatements = new ArrayList<>();

        for(Field field : entityClass.getDeclaredFields()) {
            if(field.isAnnotationPresent(OneToMany.class)) {
                Class type = field.getAnnotation(OneToMany.class).targetEntity();
                if(type == null)
                    throw new RuntimeException("Error: targetEntity attribute not set on OneToMany annotation - can't" +
                            "determine foreign key type");
                String foreignKey = field.getAnnotation(JoinColumn.class) == null ? null :
                        field.getAnnotation(JoinColumn.class).name();
                if(foreignKey == null)
                    throw new RuntimeException("Error: name attribute not set on JoinColumn annotation - can't" +
                            "determine foreign");
                String sql = "ALTER TABLE " + getTableName(type) + " ADD FOREIGN KEY(" +
                        foreignKey + ") REFERENCES " + getTableName(entityClass) +
                        "(" + field.getAnnotation(JoinColumn.class).referencedColumnName() + ")";
                alterStatements.add(sql);
            }
        }

        return alterStatements;
    }

    private Field getIdColumn(Class entityClass) {
        if(entityClass.getDeclaredFields() == null || entityClass.getDeclaredFields().length == 0)
            return null;

        for(Field field : entityClass.getDeclaredFields()) {
            if(field.isAnnotationPresent(Id.class))
                return field;
        }

        return null;
    }

    private String getColumnName(Field field) {
        if(field.isAnnotationPresent(Column.class) && !StringUtils.isEmpty(field.getAnnotation(Column.class).name()))
            return field.getAnnotation(Column.class).name();

        return StringUtils.toDbCase(field.getName());
    }

    private String getPrimaryKey(Field field) {
        if(!field.isAnnotationPresent(Id.class))
            return "";

        if(field.isAnnotationPresent(GeneratedValue.class) &&
                field.getAnnotation(GeneratedValue.class).strategy() != GenerationType.AUTO)
            return " PRIMARY KEY";

        return " AUTO_INCREMENT PRIMARY KEY";
    }

    private String getNullable(Field field) {
        if(field.isAnnotationPresent(Id.class) || field.isAnnotationPresent(NotNull.class))
            return " NOT NULL";
        return "";
    }

    private String getColumnType(Field field) {
        if(field.isAnnotationPresent(Enumerated.class)) {
            Enumerated enumerated = field.getAnnotation(Enumerated.class);
            switch (enumerated.value()) {
                case ORDINAL:
                    return "INT";
                case STRING:
                    int maxLen = 0;
                    Enum<?>[] constants = (Enum<?>[]) field.getType().getEnumConstants();
                    for(Enum constant : constants)
                        maxLen = constant.name().length() > maxLen ? constant.name().length() : maxLen;
                    return "VARCHAR(" + maxLen + ")";
            }
        }

        if(field.getType() == Long.class || field.getType() == long.class)
            return "BIGINT";
        else if(field.getType() == Integer.class || field.getType() == int.class)
            return "INT";
        else if(field.getType() == Float.class || field.getType() == float.class)
            return "FLOAT";
        else if(field.getType() == Double.class || field.getType() == double.class)
            return "DOUBLE";
        else if(field.getType() == LocalDate.class)
            return "DATE";
        else if(field.getType() == LocalDateTime.class)
            if(field.getName().toLowerCase().contains("timestamp"))
                return "TIMESTAMP";
            else
                return "DATETIME";
        else if(field.getType() == String.class) {
            Size size = field.getAnnotation(Size.class);
            if(size == null || size.max() == 0)
                throw new RuntimeException("Strings must have the @Size annotation with the max attribute set in " +
                        "order to generate a column definition.");
            return "VARCHAR(" + size.max() + ")";
        }

        throw new RuntimeException("Can't determine column type for field: " + field.getName() + ":" + field.getType() +
                ":" + field);
    }
}
