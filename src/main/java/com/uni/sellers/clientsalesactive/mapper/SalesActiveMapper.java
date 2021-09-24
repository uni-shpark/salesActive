package com.uni.sellers.clientsalesactive.mapper;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Mapper;
import org.json.simple.JSONObject;

@Mapper
public interface SalesActiveMapper {

	public List<Map<String, Object>> selectBizFileStore(JSONObject map);
}
