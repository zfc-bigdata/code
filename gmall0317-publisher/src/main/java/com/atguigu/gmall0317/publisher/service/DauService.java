package com.atguigu.gmall0317.publisher.service;

import java.io.IOException;
import java.util.Map;

public interface DauService {

    //查询日活总数
    public Long getDauTotal(String date) throws IOException;
    // 查询日活分时数据
    public Map<String,Long> getDauHour(String date);

}
