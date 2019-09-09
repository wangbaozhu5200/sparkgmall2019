package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constant.GmallConstants;
import com.atguigu.utils.KafkaSender;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) {
        //1.获取Canal
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop101", 11111), "example", "", "");

        while (true) {
            //抓取数据
            canalConnector.connect();
            //订阅表
            canalConnector.subscribe("gmall.*");

            Message message = canalConnector.get(100);

            //判断是否有数据
            if (message.getEntries().size() == 0) {
                System.out.println("没有数据，请稍等一下。");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //有数据存在
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //过滤掉写操作中，不是操作动作的数据
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        CanalEntry.RowChange rowChang = null;
                        try {
                            //反序列化
                            rowChang = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                            CanalEntry.EventType eventType = rowChang.getEventType();
                            String tableName = entry.getHeader().getTableName();
                            List<CanalEntry.RowData> rowDatasList = rowChang.getRowDatasList();

                            //做数据解析
                            Handler(tableName, eventType, rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    //解析数据
    private static void Handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        if ("order_info".equals(tableName)) {
            //过滤下单数据
            if (eventType.equals(CanalEntry.EventType.INSERT)) {
                for (CanalEntry.RowData rowData : rowDatasList) {
                    List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                    //new一个json对象
                    JSONObject jsonObject = new JSONObject();
                    for (CanalEntry.Column column : afterColumnsList) {
                        jsonObject.put(column.getName(), column.getValue());
                    }
                    KafkaSender.sendCanalData(GmallConstants.GMALL_ORDER_INFO, jsonObject.toJSONString());
                    System.out.println(jsonObject.toJSONString());
                }
            }
        }

    }
}
