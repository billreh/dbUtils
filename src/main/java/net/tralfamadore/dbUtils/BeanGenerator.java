package net.tralfamadore.dbUtils;

import net.tralfamadore.Tuple3;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class: BeanGenerator
 * Created by billreh on 9/4/17.
 */
public class BeanGenerator {
    private StringBuffer imports = new StringBuffer();
    private File srcRoot;
    private File generatedSrcRoot;
    private Map<String,Tuple3<String,String,String>> oneToManys = new HashMap<>();
    private Map<String,Tuple3<String,String,String>> oneToOnes = new HashMap<>();
    private List<String> foreignKeys = new ArrayList<>();
    private String schemaName;
    private String packageName;
    private String className;

    public BeanGenerator foreignKey(String fieldName) {
        foreignKeys.add(fieldName);
        return this;
    }

    public BeanGenerator schemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public BeanGenerator className(String className) {
        this.className = className;
        return this;
    }

    public BeanGenerator packageName(String packageName) {
        this.packageName = packageName;
        return this;
    }

    public void createBean(String tableName) {
        StringBuffer stringBuffer = new StringBuffer();
        srcRoot = new File("src/main/java");
        if(packageName != null) {
            packageName = packageName.replaceAll("\\.", File.separator);
            srcRoot = new File(srcRoot, packageName);
        }
        generatedSrcRoot = new File(srcRoot, "generated");
        generateDirectoryHeirarcyIfNeeded(generatedSrcRoot);
        if(className == null) {
            className = dbToJava(tableName, true);
        }
        generateTopLevelEntity(packageName, tableName, className);
        imports.append("package ").append(packageName.replaceAll(File.separator, ".")).append(".generated;\n\n");
        imports.append("import javax.persistence.*;\n");
        stringBuffer.append("@MappedSuperclass\n");
        stringBuffer.append("public class ").append(className).append("Base {\n");
        TableDescription tableDescription = new DatabaseUtils().getTableDescription(tableName, schemaName);
        tableDescription.getColumnDescriptions().forEach(columnDescription -> {
            generateField(stringBuffer, columnDescription);
        });
        oneToOnes.keySet().forEach(key -> generateOneToOneField(stringBuffer, key));
        oneToManys.keySet().forEach(key -> generateOneToManyField(stringBuffer, key));
        tableDescription.getColumnDescriptions().forEach(columnDescription -> {
            generateGetterSetter(stringBuffer, columnDescription);
        });
        oneToOnes.keySet().forEach(key -> generateOneToOneGetterSetter(stringBuffer, key));
        oneToManys.keySet().forEach(key -> generateOneToManyGetterSetter(stringBuffer, key));
        imports.append("\n");
        stringBuffer.append("}");

        File out = new File(generatedSrcRoot, className + "Base.java");
        try(FileWriter fileWriter = new FileWriter(out)) {
            fileWriter.append(imports);
            fileWriter.append(stringBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DatabaseUtils.shutdown();
    }

    public BeanGenerator oneToMany(String targetEntity, String columnName, String referencedColumnName, String fieldName) {
        String targetEntityClass;
        if(targetEntity.contains("."))
            targetEntityClass = targetEntity;
        else
            targetEntityClass = packageName.replaceAll(File.separator, ".") + "." + targetEntity;
        oneToManys.put(fieldName, new Tuple3<>(targetEntityClass, columnName, referencedColumnName));
        return this;
    }

    private void generateOneToManyField(StringBuffer stringBuffer, String fieldName) {
        Tuple3<String,String,String> info = oneToManys.get(fieldName);
        String targetEntity = info.getValue1();
        String columnName = info.getValue2();
        String referencedColumnName = info.getValue3();
        addImport("java.util.List");
        addImport("java.util.ArrayList");
        stringBuffer.append("\t@OneToMany(cascade = CascadeType.ALL, orphanRemoval=true, fetch = FetchType.EAGER, targetEntity = ")
                .append(targetEntity).append(".class)\n\t@org.hibernate.annotations.Fetch(value = org.hibernate.annotations.FetchMode.SUBSELECT)\n")
                .append("\t@JoinColumn(name = \"").append(columnName).append("\", referencedColumnName = \"")
                .append(referencedColumnName).append("\", nullable=false)\n").append("\tprivate List<").append(targetEntity).append("> ")
                .append(fieldName).append(" =  new ArrayList<>();\n\n");
    }

    public BeanGenerator oneToOne(String targetEntity, String columnName, String referencedColumnName, String fieldName) {
        String targetEntityClass;
        if(targetEntity.contains("."))
            targetEntityClass = targetEntity;
        else
            targetEntityClass = packageName.replaceAll(File.separator, ".") + "." + targetEntity;
        oneToOnes.put(fieldName, new Tuple3<>(targetEntityClass, columnName, referencedColumnName));
        return this;
    }

    private void generateOneToOneField(StringBuffer stringBuffer, String fieldName) {
        Tuple3<String,String,String> info = oneToOnes.get(fieldName);
        String targetEntity = info.getValue1();
        String columnName = info.getValue2();
        String referencedColumnName = info.getValue3();
        addImport("java.util.List");
        addImport("java.util.ArrayList");
        stringBuffer.append("\t@OneToOne(cascade = CascadeType.ALL, orphanRemoval=true, fetch = FetchType.EAGER, targetEntity = ")
                .append(targetEntity).append(".class)\n").append("\t@JoinColumn(name = \"")
                .append(columnName).append("\", referencedColumnName = \"").append(referencedColumnName).append("\")\n")
                .append("\tprivate ").append(targetEntity).append(" ").append(fieldName).append(";\n\n");
    }

    private void generateOneToOneGetterSetter(StringBuffer stringBuffer, String fieldName) {
        Tuple3<String,String,String> info = oneToOnes.get(fieldName);
        String targetEntity = info.getValue1();
        String columnName = info.getValue2();
        String referencedColumnName = info.getValue3();
        String ucFieldName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);

        // Getter
        stringBuffer.append("\tpublic ").append(targetEntity).append(" get").append(ucFieldName).append("() {\n");
        stringBuffer.append("\t\treturn ").append(fieldName).append(";\n\t}\n\n");

        // Setter
        stringBuffer.append("\tpublic void set").append(ucFieldName).append("(").append(targetEntity).append(" ").append(fieldName).append(") {\n");
        stringBuffer.append("\t\tthis.").append(fieldName).append(" = ").append(fieldName).append(";\n\t}\n\n");
    }

    private void generateOneToManyGetterSetter(StringBuffer stringBuffer, String fieldName) {
        Tuple3<String,String,String> info = oneToManys.get(fieldName);
        String targetEntity = info.getValue1();
        String columnName = info.getValue2();
        String referencedColumnName = info.getValue3();
        String ucFieldName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);

        // Getter
        stringBuffer.append("\tpublic List<").append(targetEntity).append("> get").append(ucFieldName).append("() {\n");
        stringBuffer.append("\t\treturn ").append(fieldName).append(";\n\t}\n\n");

        // Setter
        stringBuffer.append("\tpublic void set").append(ucFieldName).append("(List<").append(targetEntity).append("> ").append(fieldName).append(") {\n");
        stringBuffer.append("\t\tthis.").append(fieldName).append(".clear();\n");
        stringBuffer.append("\t\tthis.").append(fieldName).append(".addAll(").append(fieldName).append(");\n\t}\n\n");
    }

    private void generateTopLevelEntity(String packageName, String tableName, String className) {
        StringBuffer sb = new StringBuffer();
        sb.append("package ").append(packageName.replaceAll(File.separator, ".")).append(";\n\n");
        sb.append("import javax.persistence.*;\n");
        sb.append("import ").append(packageName.replaceAll(File.separator, ".")).append(".generated.");
        sb.append(className).append("Base;\n\n");
        sb.append("@Entity(name = \"").append(tableName).append("\")\n");
        sb.append("public class ").append(className).append(" extends ").append(className).append("Base {\n}\n");
        File out = new File(srcRoot, className + ".java");
        if(!out.exists()) {
            try (FileWriter fileWriter = new FileWriter(out)) {
                fileWriter.append(sb);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void generateDirectoryHeirarcyIfNeeded(File path) {
        if(!path.exists()) {
            if(!path.getParentFile().exists())
                    generateDirectoryHeirarcyIfNeeded(path.getParentFile());
            if(!path.mkdir())
                throw new RuntimeException("Could not create directory: " + path);
        }
    }

    private void generateField(StringBuffer stringBuffer, ColumnDescription columnDescription) {
        String type = getType(columnDescription.getColumnType());
        String fieldName = dbToJava(columnDescription.getColumnName(), false);
        if(oneToOnes.values().stream().anyMatch(tuple -> tuple.getValue2().equals(columnDescription.getColumnName())))
            return;
        if(columnDescription.isPrimaryKey()) {
            stringBuffer.append("\t@Id\n").append("\t@GeneratedValue(strategy = GenerationType.AUTO)\n");
        } else {
            if(foreignKeys.contains(fieldName)) {
                stringBuffer.append("\t@Column(name = \"").append(columnDescription.getColumnName());
                stringBuffer.append("\", insertable = false, updatable = false)\n");
            } else {
                stringBuffer.append("\t@Column(name = \"").append(columnDescription.getColumnName()).append("\")\n");
            }
        }
        stringBuffer.append("\t").append("private ").append(type).append(" ");
        stringBuffer.append(fieldName).append(";\n\n");
    }


    private void generateGetterSetter(StringBuffer stringBuffer, ColumnDescription columnDescription) {
        String type = getType(columnDescription.getColumnType());
        String ucName = dbToJava(columnDescription.getColumnName(), true);
        String lcName = dbToJava(columnDescription.getColumnName(), false);
        if(oneToOnes.values().stream().anyMatch(tuple -> tuple.getValue2().equals(columnDescription.getColumnName())))
            return;

        // Getter
        stringBuffer.append("\tpublic ").append(type).append(" get").append(ucName).append("() {\n");
        stringBuffer.append("\t\treturn ").append(lcName).append(";\n\t}\n\n");

        // Setter
        stringBuffer.append("\tpublic void set").append(ucName).append("(").append(type);
        stringBuffer.append(" ").append(lcName).append(") {\n");
        stringBuffer.append("\t\tthis.").append(lcName).append(" = ").append(lcName).append(";\n\t}\n\n");

    }

    private String getType(String sqlType) {
        if(sqlType.toLowerCase().equals("bigint"))
            return "Long";
        if(sqlType.toLowerCase().equals("varchar"))
            return "String";
        if(sqlType.toLowerCase().equals("char"))
            return "String";
        if(sqlType.toLowerCase().equals("decimal"))
            return "Double";
        if(sqlType.toLowerCase().equals("double"))
            return "Double";
        if(sqlType.toLowerCase().equals("date")) {
            addImport("java.time.LocalDate");
            return "LocalDate";
        }
        if(sqlType.toLowerCase().equals("timestamp")) {
            addImport("java.time.LocalDateTime");
            return "LocalDateTime";
        }
        if(sqlType.toLowerCase().equals("int"))
            return "Integer";
        if(sqlType.toLowerCase().equals("integer"))
            return "Integer";
        if(sqlType.toLowerCase().equals("bit"))
            return "Integer";
        throw new RuntimeException("Unsupported type: " + sqlType);
    }

    private void addImport(String importName) {
        if(!imports.toString().contains(importName))
            imports.append("import ").append(importName).append(";\n");
    }

    private String dbToJava(String tableName, boolean capitalize) {
        StringBuffer sb = new StringBuffer();
        String[] parts = tableName.toLowerCase().replaceAll("_", " ").split(" ");
        for(String part : parts) {
            sb.append(part.toUpperCase().substring(0,1)).append(part.substring(1));
        }
        if(capitalize)
            return sb.toString();
        return sb.substring(0, 1).toLowerCase() + sb.substring(1);
    }

    public static void main(String[] args) {
        new BeanGenerator()
                .packageName("net.tralfamadore.dbUtils.entity")
                .createBean("address");

        new BeanGenerator()
                .packageName("net.tralfamadore.dbUtils.entity")
                .foreignKey("listingDetailId")
                .createBean("exterior_feature");

        new BeanGenerator()
                .packageName("net.tralfamadore.dbUtils.entity")
                .foreignKey("listingDetailId")
                .createBean("other_room");

        new BeanGenerator()
                .packageName("net.tralfamadore.dbUtils.entity")
                .foreignKey("listingId")
                .oneToMany("ExteriorFeature", "listing_detail_id", "id", "exteriorFeatures")
                .oneToMany("OtherRoom", "listing_detail_id", "id", "otherRooms")
                .createBean("listing_detail");

        new BeanGenerator()
                .packageName("net.tralfamadore.dbUtils.entity")
                .createBean("agent");

        new BeanGenerator()
                .packageName("net.tralfamadore.dbUtils.entity")
                .foreignKey("listingId")
                .createBean("photo");

        new BeanGenerator()
                .packageName("net.tralfamadore.dbUtils.entity")
                .oneToOne("Address", "address_id", "id", "address")
                .oneToOne("Agent", "agent_id", "id", "agent")
                .oneToMany("ListingDetail", "listing_id", "id", "listingDetails")
                .oneToMany("Photo", "listing_id", "id", "photos")
                .createBean("listing");
    }
}
