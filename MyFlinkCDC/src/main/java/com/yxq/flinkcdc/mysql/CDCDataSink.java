package com.yxq.flinkcdc.mysql;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Date;
import java.util.Map;

/**
 * @author yxq
 * @date 2022-10-18
 */
public class CDCDataSink extends RichSinkFunction {
    private final String yyyyMMddHHmmss = "yyyy-MM-dd HH:mm:ss";
    private final String yyyyMMddTHHmmss = "yyyy-MM-dd'T'HH:mm:ss";
    private final String yyyyMMddTHHmmssZ = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    // 初始化方法
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }


    @Override
    public void invoke(Object value, Context context) throws Exception {
        String v = value.toString();
        System.out.println(">>>>>>>" + v);
        JSONObject obj = JSONObject.parseObject(v);
        JSONObject afterObj = obj.getJSONObject("after");
        String tableName = obj.getString("tableName");
        String columns = "";
        String vals = "";
        String updates = "";

        for (Map.Entry<String, Object> entry : afterObj.entrySet()) {
            String key = entry.getKey();
            Object valObj = entry.getValue();
            if ("create_time".equals(key)) {
                if (valObj instanceof String) {
                    String val = valObj.toString();
                    if (val.contains("T") && val.endsWith("Z")) {
                        valObj = val.replace("T", " ").replace("Z", "");
                    }
                } else {
                    valObj = DateFormatUtils.format(new Date((Long) valObj), yyyyMMddHHmmss);
                }
            }
            columns += "`" + key + "`,";
            if (valObj instanceof String) {
                vals += "'" + valObj + "',";
                updates += "`" + key + "`='" + valObj + "',";
            } else {
                vals += valObj + ",";
                updates += "`" + key + "`=" + valObj + ",";
            }
        }
        if (columns.endsWith(",")) {
            columns = columns.substring(0, columns.length() - 1);
        }
        if (vals.endsWith(",")) {
            vals = vals.substring(0, vals.length() - 1);
        }
        if (updates.endsWith(",")) {
            updates = updates.substring(0, updates.length() - 1);
        }
        String sql = " INSERT INTO `testdb`.`" + tableName + "` (" + columns + ") VALUES " + "(" + vals + ")" +
                " ON DUPLICATE KEY UPDATE " + updates;
        System.out.println(">>>>>>>" + sql);
        //  保存数据库
        MySqlDBUtils.executeSql(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}