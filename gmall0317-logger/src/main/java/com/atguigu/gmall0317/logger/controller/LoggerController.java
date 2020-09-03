package com.atguigu.gmall0317.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Slf4j

public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/hello")
    public String sayHello(@RequestParam("name") String name){
        System.out.println("你好");

        return "你好" + name;
    }

    @RequestMapping("/applog")
    public String applog(@RequestBody String logString){
        //System.out.println(logString);

        log.error(logString);

        JSONObject jsonObject = JSON.parseObject(logString);

        if (jsonObject.getString("start")!=null &&
                jsonObject.getString("start").length()>0) {
            kafkaTemplate.send("GMALL0317_STARTUP",logString);
        }else {
            kafkaTemplate.send("GMALL0317_EVENT",logString);
        }
        return "success 0317";
    }
}
