package com.subzero;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Util {
    public static String getResourceFileContent(String fileName) {
        try {
            return new String(
                Files.readAllBytes(Paths.get(Util.class.getClassLoader().getResource(fileName).toURI())),
                StandardCharsets.UTF_8
            );
        } catch (IOException | java.net.URISyntaxException e) {
            e.printStackTrace();
            return null;
        }
    }
}
