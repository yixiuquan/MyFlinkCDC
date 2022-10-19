package com.yxq.flinkcdc;

import com.yxq.flinkcdc.mysql.MySqlCDC;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MyFlinkCdcApplication {

    public static void main(String[] args) {
        SpringApplication.run(MyFlinkCdcApplication.class, args);
    }

}
