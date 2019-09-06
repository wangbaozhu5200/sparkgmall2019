package com.atguigu.sparkgmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static com.atguigu.constant.GmallConstants.GMALL_EVENTUP;
import static com.atguigu.constant.GmallConstants.GMALL_STARTUP;


@RestController
@Slf4j
public class LogController {

//    @GetMapping("test")
//    public String testDemo(){
//        return "hello demo";
//    }

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    public String doLog(@RequestParam("logString") String str){
        System.out.println(str);
        //1.添加时间
        JSONObject jsonObj = JSON.parseObject(str);
        jsonObj.put("ts",System.currentTimeMillis());
        String jsonStr = jsonObj.toString();

        //2。写入本地文件
        log.info(jsonStr);

        //3.将数据写入kafka(判断时哪种日志)
        if ("startup".equals(jsonObj.getString("type"))) {
            kafkaTemplate.send(GMALL_STARTUP,jsonStr);
        }else {
            kafkaTemplate.send(GMALL_EVENTUP,jsonStr);
        }

        return "success";
    }
}
