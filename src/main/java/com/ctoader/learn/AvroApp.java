package com.ctoader.learn;


import com.ctoader.model.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroApp {

    public static void main(String[] args) {
        User user1 = User.newBuilder()
                .setName("Cristian Toader")
                .setFavoriteColor("blue")
                .setFavoriteNumber(1)
                .build();

        User user2 = User.newBuilder()
                .setName("John Doe")
                .setFavoriteColor("red")
                .setFavoriteNumber(2)
                .build();

        User user3 = User.newBuilder()
                .setName("Jim Morning")
                .setFavoriteColor("green")
                .setFavoriteNumber(3)
                .build();


        File exportFile = new File("users-export.avro");

        DatumWriter<User> userWriter = new SpecificDatumWriter<>(User.class);
        try (DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userWriter)) {
            dataFileWriter.create(user1.getSchema(), exportFile);
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);

        } catch (IOException e) {
            e.printStackTrace();
        }

        // Deserialize Users from disk
        DatumReader<User> userReader = new SpecificDatumReader<User>(User.class);
        try (DataFileReader<User> dataFileReader = new DataFileReader<User>(exportFile, userReader)) {
            User user = null;
            while (dataFileReader.hasNext()) {
                // reusing user object for less gc (based on doc)
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
