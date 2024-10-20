package com.discc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.reflections.Reflections;

import static org.reflections.scanners.Scanners.*;

import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

@Slf4j
public class Pojo2AvscGenerator {

    public static final String AVSC = ".avsc";

    @SneakyThrows
    public static void main(String[] args) {
        Set<Class<?>> allClasses = getAllClassesInPkg(args[0]);
        Path destinationDir = assertDestinationDir(args);
        allClasses.forEach(clazz -> {
            generateAvsc(clazz, destinationDir);
        });
    }

    private static Path assertDestinationDir(String[] args) {
        var destination =  args[1];
        Path destinationDir = Paths.get(destination);
        if (!destinationDir.toFile().exists()) {
            if (!destinationDir.toFile().mkdirs()) {
                throw new RuntimeException("Unable to create directory " + destinationDir);
            }
        } else {
            if (destinationDir.toFile().isFile()) {
                log.error("destination {} already exists as a file", destination);
                throw new RuntimeException("destination " + destination + " already exists as a file");
            }
        }
        return destinationDir;
    }

    private static Set<Class<?>> getAllClassesInPkg(String pkgName) {
        Reflections reflections = new Reflections(new ConfigurationBuilder().forPackages(pkgName).addScanners(SubTypes.filterResultsBy(c -> true)).filterInputsBy(new FilterBuilder().includePackage(pkgName)));
        Set<Class<?>> allClasses = reflections.get(SubTypes.of(Object.class).asClass());
        return allClasses;
    }

    private static void generateAvsc(Class<?> aClass, Path destinationDir) {

        try (Writer bw = Files.newBufferedWriter(destinationDir.resolve(aClass.getSimpleName() + AVSC))) {
            generateSchema(aClass, bw);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static <T> void generateSchema(Class<T> clazz, Writer writer) {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator generator = new AvroSchemaGenerator();
        generator.enableLogicalTypes();
        try {
            mapper.acceptJsonFormatVisitor(clazz, generator);
            AvroSchema generatedSchema = generator.getGeneratedSchema();
            Schema avroSchema = generatedSchema.getAvroSchema();
            writer.write(avroSchema.toString(true));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
