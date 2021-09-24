package com.uni.sellers.clientsalesactive.mongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.uni.sellers.util.CommonDateUtils;
import com.uni.sellers.util.CommonUtils;

@Service
public class MongoDBService {

	@Resource
	private MongoTemplate mongoTemplate;
	
	@Resource 
	private MongoDBDao mongDBDao;
	
	@Resource 
	private TemporaryCollection temporarySQLs;
	
	public JSONArray getOpportunityLog(Map<String, Object> commandMap) {

		JSONArray objlist = new JSONArray();
		
		try {

			commandMap.put("current_date", CommonDateUtils.getTimeStamp(7));
			temporarySQLs.setTemporaryArgs(commandMap, mongoTemplate);
			if(!temporarySQLs.isSuccess()) {
				throw new Exception("temporarySQLs Failed!");
			}
			mongDBDao.setTemporaryArgs(commandMap, mongoTemplate);
			objlist = mongDBDao.selectOpportunityDashBoardDivision();
			
			temporarySQLs.removeTemporary();
		} catch (Exception e) {
			temporarySQLs.removeTemporary();
			e.printStackTrace();
		} finally {
			
		}
		
		return objlist;

	}
	
	public boolean insertOpportunity(JSONObject commandMap) {

		JSONObject obj = null;
		try {

			mongDBDao.setTemporaryArgs(null, mongoTemplate);
			obj = mongDBDao.insertOppotunity(commandMap);
			
			Object tmp = commandMap.get("salesPlanData");
			
			JSONArray salesPlanList = CommonUtils.stringTojsonArray(""+commandMap.get("salesPlanData"));
			
			if(salesPlanList.size() == 0) {
				commandMap.put("OPPORTUNITY_ID", obj.get("OPPORTUNITY_ID"));
				obj = mongDBDao.insertOpportunitySalesPlan(commandMap);
			}else {
				for(Object getMap : salesPlanList){
					
					JSONObject value = (JSONObject) getMap;
					
					value.put("OPPORTUNITY_ID",obj.get("OPPORTUNITY_ID"));
					value.put("hiddenModalCreatorId",commandMap.get("hiddenModalCreatorId"));
					value.put("hiddenModalOwnerId",commandMap.get("hiddenModalOwnerId"));
					obj = mongDBDao.insertOpportunitySalesPlan(value);
				}
			}
			
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
}
