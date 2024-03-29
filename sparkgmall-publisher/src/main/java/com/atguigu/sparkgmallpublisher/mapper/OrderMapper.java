package com.atguigu.sparkgmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    public Double selectOrderAmountTotal(String date);

    //2 查询当日交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);

}
