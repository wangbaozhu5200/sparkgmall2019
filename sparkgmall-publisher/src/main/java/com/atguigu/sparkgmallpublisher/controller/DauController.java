package com.atguigu.sparkgmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.sparkgmallpublisher.service.DauService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
public class DauController {

    @Autowired
    private DauService dauService;


    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {

        //获取日活数据
        Long totalDao = dauService.getTotalDao(date);
        //获取交易额数据
        Double orderAmount = dauService.getOrderAmount(date);

        ArrayList<Map> result = new ArrayList<>();
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", totalDao);
        result.add(dauMap);

        HashMap<String, Object> newMid = new HashMap<>();
        newMid.put("id", "new_mid");
        newMid.put("name", "新增设备");
        newMid.put("value", 274);
        result.add(newMid);

        HashMap<String, Object> totalAmountMap = new HashMap<>();
        totalAmountMap.put("id", "order_amount");
        totalAmountMap.put("name", "新增交易额");
        totalAmountMap.put("value", orderAmount);
        result.add(totalAmountMap);

        return JSON.toJSONString(result);
    }

    @GetMapping("realtime-hours")
    public String getHourDau(@RequestParam("id") String id, @RequestParam("date") String today) {
        if ("dau".equals(id)) {
            JSONObject jsonObject = new JSONObject();
            //查询日活的分时统计
            Map todayHourDau = dauService.getHourDau(today);
            jsonObject.put("today", todayHourDau);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            try {
                Date yesterdayDate = DateUtils.addDays(sdf.parse(today), -1);
                String yesterdaystr = sdf.format(yesterdayDate);
                Map yesterdayDauMap = dauService.getHourDau(yesterdaystr);
                jsonObject.put("yesterday", yesterdayDauMap);

            } catch (ParseException e) {
                e.printStackTrace();
            }
            return jsonObject.toJSONString();
        } else if ("order_amount".equals(id)) {

            JSONObject jsonObject = new JSONObject();
            //查询日活的分时统计
            Map todayHourAmount = dauService.getOrderAmountHour(today);
            jsonObject.put("today", todayHourAmount);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            try {
                Date yesterdayDate = DateUtils.addDays(sdf.parse(today), -1);
                String yesterdaystr = sdf.format(yesterdayDate);
                Map yesterdayAmountMap = dauService.getOrderAmountHour(yesterdaystr);
                jsonObject.put("yesterday", yesterdayAmountMap);

            } catch (ParseException e) {
                e.printStackTrace();
            }
            return jsonObject.toJSONString();

        }
        return null;
    }
}
