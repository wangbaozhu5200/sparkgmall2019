package com.atguigu.sparkgmallpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.sparkgmallpublisher.mapper")
public class SparkgmallPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkgmallPublisherApplication.class, args);
    }

}
