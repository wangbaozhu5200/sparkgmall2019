package com.atguigu.sparkgmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    public Long GetTotalDao(String Date);

    public List<Map> selectDauTotalHourMap(String date);
}
