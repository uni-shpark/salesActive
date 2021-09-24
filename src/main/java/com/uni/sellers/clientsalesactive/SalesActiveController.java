package com.uni.sellers.clientsalesactive;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import com.uni.sellers.clientsalesactive.mongo.DBDataMig;
import com.uni.sellers.clientsalesactive.mongo.MongoDBService;
import com.uni.sellers.clientsalesactive.rabbit.MessageProducer;

@RestController
public class SalesActiveController {

	@Autowired
    MessageProducer msgProducer;
	
	@Autowired
	private MongoTemplate mongoTemplate;
	
	@Resource 
	MongoDBService mongoDBService;
	

	@RequestMapping(value = "/dataMig.do", method = RequestMethod.GET)
	public void dataMig() {
		
//		String [] tables = {"opportunity_action_plan", "opportunity_amount", "opportunity_amount_split", "opportunity_amount_update_history", "opportunity_checklist", "opportunity_checklist_owner", "opportunity_file_store", "opportunity_hidden_action", "opportunity_hidden_log", "opportunity_hidden_log_coaching_talk", "opportunity_log", "opportunity_log_coaching_talk", "opportunity_log_history_backup", "opportunity_log_revgp_history", "opportunity_log_tcv_history", "opportunity_log_update_history", "opportunity_milestone", "opportunity_purchase_product_list", "opportunity_sales_product_list", "opportunity_salescycle_check", "opportunity_win_plan"};
		String [] tables = {"opportunity_action_plan", "opportunity_amount", "opportunity_amount_split", "opportunity_amount_update_history", "opportunity_checklist", "opportunity_checklist_owner", "opportunity_file_store", "opportunity_hidden_action", "opportunity_hidden_log", "opportunity_hidden_log_coaching_talk", "opportunity_log", "opportunity_log_coaching_talk", "opportunity_log_history_backup", "opportunity_milestone", "opportunity_purchase_product_list", "opportunity_sales_product_list", "opportunity_salescycle_check", "opportunity_win_plan","coaching_talk", "our_members_info", "our_division_info", "our_team_info", "our_product_info", "client_individual_info", "client_company_info", "client_event_action", "client_event_file_store", "client_issue_log", "client_event_log", "com_client_list", "code_industry_segment", "erp_sales_actual", "erp_sales_plan"};
//		String [] tables = {"coaching_talk", "our_members_info", "our_division_info", "our_team_info", "our_product_info", "client_individual_info", "client_company_info", "client_event_action", "client_event_file_store", "client_issue_log", "client_event_log", "com_client_list", "code_industry_segment", "erp_sales_actual", "erp_sales_plan"};
//		String [] tables = {"opportunity_log", "opportunity_amount"};
		
		long start = System.currentTimeMillis();
		for (String collection : tables) {
			System.out.println(" table name : " + collection);
			List<Map<String,Object>> resultlist = DBDataMig.getData("select * from " + collection);
			int id = 1;
			for (Map<String,Object> result : resultlist) {
				result.put("_id", new HashMap<String, Object>().put("sequenceNextValue","_id"));
				Object obj = mongoTemplate.insert(result, collection);
				id ++;
				if((""+id).endsWith("1000")) {
					System.out.println(collection + " completed : " + id);
				}
			}
		}
		System.out.println(System.currentTimeMillis() - start);
	}

	@RequestMapping(value = "/selectOppotunity.do", method = RequestMethod.POST)
	@ResponseStatus(value = HttpStatus.OK)
	public @ResponseBody JSONArray getOpportunityLog(@RequestParam Map<String, Object> commandMap, HttpServletResponse response) {

		System.out.println("STEP 1 : " + commandMap.toString());
		response.setContentType("application/json; charset=utf-8");

		try {
			JSONArray result = mongoDBService.getOpportunityLog(commandMap);
			
//			Thread.sleep(50000);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@RequestMapping(value = "/autoScaling.do", method = RequestMethod.GET)
	@ResponseStatus(value = HttpStatus.OK)
	public @ResponseBody JSONArray doRun() {

		System.out.println("STEP 1 : autoScaling.do");
		
		String s = "11111111111111111111111111111111111111111111111111111111111111111111111111111111";
		for (int i = 0; i < 100; i++) {
			s = s+s;
		}
		return null;
	}
	
	@RequestMapping(value = "/insertOpportunity.do", method = RequestMethod.POST)
	@ResponseStatus(value = HttpStatus.OK)
	public @ResponseBody int insertOpportunity(@RequestParam Map<String, Object> commandMap, HttpServletResponse response) {

		System.out.println("STEP 1 : " + commandMap.toString());
//		response.setContentType("application/json; charset=utf-8");

		try {
			msgProducer.run("insertOpportunity", commandMap);
//			System.out.println(result.size() + " / " + result.toJSONString());
			
			return 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		throw new RuntimeException("insert failed !!!");
	}
}
