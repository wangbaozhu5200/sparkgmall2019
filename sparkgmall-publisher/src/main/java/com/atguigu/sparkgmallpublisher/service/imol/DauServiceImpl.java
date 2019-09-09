package com.atguigu.sparkgmallpublisher.service.imol;


import com.atguigu.sparkgmallpublisher.mapper.DauMapper;
import com.atguigu.sparkgmallpublisher.mapper.OrderMapper;
import com.atguigu.sparkgmallpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DauServiceImpl implements DauService {

    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Long getTotalDao(String date) {
        return dauMapper.GetTotalDao(date);
    }

    @Override
    public Map getHourDau(String date) {
        HashMap<String, Long> hourDauMap = new HashMap<>();
        List<Map> list = dauMapper.selectDauTotalHourMap(date);
        for (Map map : list) {
            hourDauMap.put((String) map.get("LOGHOUR"), (long) map.get("CT"));
        }
        return hourDauMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        HashMap<String, Double> hourOrderMap = new HashMap<>();
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        for (Map map : list) {
            String lh = (String) map.get("CREATE_HOUR");
            Double ct = (Double) map.get("SUM_AMOUNT");
            hourOrderMap.put(lh, ct);
        }
        return hourOrderMap;
    }
}
