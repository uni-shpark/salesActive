package com.uni.sellers.clientsalesactive.mongo.sql;

import java.util.Date;
import java.util.Map;

import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.uni.sellers.util.CommonDateUtils;
import com.uni.sellers.util.CommonUtils;

@Service
public class SQLCommon {

	protected String current_user;
	protected String current_date;

	protected Date startDate;
	protected Date endDate;

	protected String dateCategory;
	protected Date interval_startDate;
	protected Date interval_endDate;
	protected Date year;
	
	protected MongoTemplate mongoTemplate;
	
	public void setTemporaryArgs(Map<String, Object> commandMap, MongoTemplate mongoTemplate) {

		if(commandMap == null) {
			this.mongoTemplate = mongoTemplate;
			return;
		}
		this.mongoTemplate = mongoTemplate;
		this.current_user = (String) commandMap.get("global_member_id");
		this.current_date = (String) commandMap.get("current_date");
		String endDate = (String) commandMap.get("endDate");
		endDate += " 23:59:59";
		this.startDate = CommonDateUtils.stringToDate((String) commandMap.get("startDate"), "yyyy-MM-dd");
		this.endDate = CommonDateUtils.stringToDate(endDate, "yyyy-MM-dd HH:mm:ss");

		this.year = CommonDateUtils.stringToDate((String)commandMap.get("startDate"), "yyyy");
		
		this.dateCategory = (String) commandMap.get("dateCategory");

		if ("y".equals(this.dateCategory)) {
			this.interval_startDate = CommonDateUtils.stringToDate(
					CommonDateUtils.getDate(3, ((String) commandMap.get("startDate")).replaceAll("-", ""), -1),
					"yyyy-MM-dd");
			this.interval_endDate = CommonDateUtils.stringToDate(
					CommonDateUtils.getDate(3, ((String) commandMap.get("endDate")).replaceAll("-", ""), -1),
					"yyyy-MM-dd");
		} else if ("q".equals(this.dateCategory)) {
			this.interval_startDate = CommonDateUtils.stringToDate(
					CommonDateUtils.getDate(2, ((String) commandMap.get("startDate")).replaceAll("-", ""), -3),
					"yyyy-MM-dd");
			this.interval_endDate = CommonDateUtils.stringToDate(
					CommonDateUtils.getDate(2, ((String) commandMap.get("endDate")).replaceAll("-", ""), -3),
					"yyyy-MM-dd");
		} else if ("m".equals(this.dateCategory)) {
			this.interval_startDate = CommonDateUtils.stringToDate(
					CommonDateUtils.getDate(2, ((String) commandMap.get("startDate")).replaceAll("-", ""), -1),
					"yyyy-MM-dd");
			this.interval_endDate = CommonDateUtils.stringToDate(
					CommonDateUtils.getDate(2, ((String) commandMap.get("startDate")).replaceAll("-", ""), -1),
					"yyyy-MM-dd");
		} else {
			this.interval_startDate = CommonDateUtils.stringToDate(
					CommonDateUtils.getDate(2, ((String) commandMap.get("startDate")).replaceAll("-", ""), -3),
					"yyyy-MM-dd");
			this.interval_endDate = CommonDateUtils.stringToDate(
					CommonDateUtils.getDate(2, ((String) commandMap.get("endDate")).replaceAll("-", ""), -3),
					"yyyy-MM-dd");
		}
		
		System.out.println(startDate.toString());
		System.out.println(this.endDate.toString());
		System.out.println(interval_startDate.toString());
		System.out.println(interval_endDate.toString());
		System.out.println(year.toString());
	}
}
