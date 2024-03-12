package com.subzero;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TestApplication {
    static {
        System.loadLibrary("subzerojni"); // "libsubzerojni.dylib" or "libsubzerojni.so""+
    }
    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }
}
