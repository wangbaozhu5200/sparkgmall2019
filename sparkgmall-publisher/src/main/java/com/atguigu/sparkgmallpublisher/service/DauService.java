package com.atguigu.sparkgmallpublisher.service;

import java.util.Map;

public interface DauService {

    public Long getTotalDao(String Date);

    public Map getHourDau(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);

}
