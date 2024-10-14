package com.discc;

import com.discc.avro.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import lombok.SneakyThrows;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Library {
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

    @SneakyThrows
    public static void main(String[] args) {
        String fname = User.class.getSimpleName().toLowerCase();
        try (Writer bw = Files.newBufferedWriter(Paths.get(args[0], fname + ".avsc"))) {
            generateSchema(User.class, bw);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
