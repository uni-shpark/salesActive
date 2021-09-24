package com.uni.sellers.clientsalesactive.mongo;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.stereotype.Service;

import com.mongodb.client.AggregateIterable;
import com.uni.sellers.clientsalesactive.mongo.sql.SQLCommon;
import com.uni.sellers.util.CommonDateUtils;
import com.uni.sellers.util.CommonUtils;

@Service
public class MongoDBDao extends SQLCommon {
	@SuppressWarnings("unchecked")
	public JSONObject insertOpportunitySalesPlan(JSONObject commandMap) throws Exception {
		JSONObject obj = new JSONObject();
		Date currentTime = CommonDateUtils.getCurrentTime();
		obj.put("_id", new HashMap<String, Object>().put("sequenceNextValue","_id"));
		obj.put("OPPORTUNITY_ID", commandMap.get("OPPORTUNITY_ID"));
		obj.put("CREATOR_ID", commandMap.get("hiddenModalCreatorId"));
		obj.put("BASIS_MONTH", CommonDateUtils.emptyToNull(commandMap.get("basisMonth_s")));
		obj.put("BASIS_MONTH_C", CommonDateUtils.emptyToNull(commandMap.get("basisMonth_c")));
		obj.put("BASIS_PLAN_REVENUE_AMOUNT", CommonUtils.emptyToZero(commandMap.get("amount_r"),3));
		obj.put("BASIS_PLAN_GP_AMOUNT", CommonUtils.emptyToZero(commandMap.get("amount_g"),3));
		obj.put("MEMBER_ID_NUM", CommonUtils.emptyToNull(commandMap.get("hiddenModalOwnerId")));
		obj.put("SYS_REGISTER_DATE", currentTime);
		obj.put("SYS_UPDATE_DATE", currentTime);
		
		return mongoTemplate.insert(obj,"opportunity_amount");
		
	}
	@SuppressWarnings("unchecked")
	public JSONObject insertOppotunity(JSONObject commandMap) throws Exception {

		JSONObject obj = new JSONObject();

		obj.put("_id", new HashMap<String, Object>().put("sequenceNextValue","_id"));
		obj.put("OPPORTUNITY_ID", getLastOpportunityID());
		obj.put("CREATOR_ID", CommonUtils.emptyToNull(""+commandMap.get("hiddenModalCreatorId")));
		obj.put("EXEC_ID", CommonUtils.emptyToNull(commandMap.get("hiddenModalExecId")));
		obj.put("COMPANY_ID", CommonUtils.emptyToNull(commandMap.get("hiddenModalCompanyId")));
		obj.put("CUSTOMER_ID", CommonUtils.emptyToZero(commandMap.get("hiddenModalCustomerId"), 2));
		obj.put("SUBJECT", CommonUtils.emptyToNull(commandMap.get("textModalSubject")));
		obj.put("CONTRACT_AMOUNT", CommonUtils.emptyToZero(commandMap.get("hiddenModalContractAmount"), 3));
		obj.put("PARTNER_ROLE", CommonUtils.emptyToNull(commandMap.get("textModalPartnerRole")));
		obj.put("CONTRACT_DATE", CommonUtils.emptyToNull(commandMap.get("textModalContractDate")));
		obj.put("OWNER_ID", CommonUtils.emptyToNull(commandMap.get("hiddenModalOwnerId")));
		obj.put("FORECAST_YN", CommonUtils.emptyToNull(commandMap.get("checkModalForecastYN")));
		obj.put("ROUTE", CommonUtils.emptyToNull(commandMap.get("textModalRoute")));
		obj.put("SALES_PARTNER", CommonUtils.emptyToNull(commandMap.get("hiddenModalPartnerId")));
		obj.put("DETAIL_CONENTS", CommonUtils.emptyToNull(commandMap.get("textareaModalDetailConents")));
		obj.put("CONTRACT_ST_DATE", CommonDateUtils.emptyToNull(commandMap.get("textModalContractStDate")));
		obj.put("CONTRACT_ED_DATE", CommonDateUtils.emptyToNull(commandMap.get("textModalContractEdDate")));
		obj.put("SALES_CYCLE", CommonUtils.emptyToNull(commandMap.get("hiddenModalSalesCycle")));

		if ("4".equals("" + commandMap.get("hiddenModalSalesCycle"))) {
			obj.put("CLOSE_CATEGORY", CommonUtils.emptyToNull(commandMap.get("selectSalesCloseCategory")));
			obj.put("CLOSE_DETAIL", CommonUtils.emptyToNull(commandMap.get("textareaSalesCloseDetail")));
		} else {
			obj.put("CLOSE_CATEGORY", null);
			obj.put("CLOSE_DETAIL", null);
		}
		obj.put("IDENTIFIER_ID", CommonUtils.emptyToNull(commandMap.get("hiddenModalIdentifierId")));
		obj.put("DISCRIMINATE_VALUE", CommonUtils.emptyToNull(commandMap.get("textareaModalDiscriminateValue")));
		obj.put("CATEGORY_CD", CommonUtils.emptyToNull(commandMap.get("selectModalCategoryCd")));
		obj.put("CLOSE_DATE", CommonDateUtils.emptyToNull(""+commandMap.get("DETAIL_CONENTS")));
		obj.put("TYPE_CD", CommonUtils.emptyToNull(commandMap.get("selectModalTypeCd")));
		obj.put("PROJECT_FORM_CD", CommonUtils.emptyToNull(commandMap.get("selectModalProjectForm")));
		obj.put("PROBABILITY_CD", 1);
		obj.put("X_LOG_CD", "N");
		obj.put("OMM_CD", "N");
		obj.put("ERP_CLIENT_CD", CommonUtils.emptyToNull(commandMap.get("hiddenModalClientMaster")));
		obj.put("ERP_CLIENT_DECISION_CD", CommonUtils.emptyToNull(commandMap.get("hiddenModalClientDecision")));
		obj.put("ERP_CLIENT_CATEGORY_CD", CommonUtils.emptyToNull(commandMap.get("selectModalClientCategoryCd")));
		obj.put("BUY_CD", CommonUtils.emptyToNull(commandMap.get("selectModalBuyCd")));
		obj.put("TEMP_FLAG", CommonUtils.emptyToNull(commandMap.get("hiddenModalTempFlag")));
		obj.put("ERP_OPP_CD", CommonUtils.emptyToZero(""+commandMap.get("hiddenModalBudgetAmount"),2));
		obj.put("CLIENT_INFO_1", CommonUtils.emptyToNull(commandMap.get("textClientInfo1")));
		obj.put("CLIENT_INFO_2", CommonUtils.emptyToNull(commandMap.get("textClientInfo2")));
		obj.put("CLIENT_INFO_3", CommonUtils.emptyToNull(commandMap.get("textClientInfo3")));
		obj.put("CLIENT_INFO_4", CommonUtils.emptyToNull(commandMap.get("textClientInfo4")));
		obj.put("OPPORTUNITY_HIDDEN_ID", CommonUtils.emptyToZero(commandMap.get("hiddenModalOpportunityhiddenId"),2));

		return mongoTemplate.insert(obj,"opportunity_log");
		
	}

	public JSONArray selectOpportunityDashBoardDivision() throws Exception {

		JSONArray results = new JSONArray();

		JSONObject esp = selectOpportunityDashBoardDivision_ESP();
		JSONObject oa_close = selectOpportunityDashBoardDivision_OA_CLOSE();
		JSONObject oa_ing = selectOpportunityDashBoardDivision_OA_ING();
		JSONObject oas_close = selectOpportunityDashBoardDivision_OAS_CLOSE();
		JSONObject oas_ing = selectOpportunityDashBoardDivision_OAS_ING();
		JSONObject oa_this = selectOpportunityDashBoardDivision_OA_THIS();
		JSONObject oas_this = selectOpportunityDashBoardDivision_OAS_THIS();
		JSONObject oa_last = selectOpportunityDashBoardDivision_OA_LAST();
		JSONObject oas_last = selectOpportunityDashBoardDivision_OAS_LAST();

		Set<String> set = esp.keySet();
		Iterator<String> it = set.iterator();

		while (it.hasNext()) {
			JSONObject opdata = new JSONObject();
			String key = it.next();
			opdata = setOpportunityDashBoardDivision(opdata, (JSONObject) esp.get(key));
			opdata = setOpportunityDashBoardDivision(opdata, (JSONObject) oa_close.get(key));
			opdata = setOpportunityDashBoardDivision(opdata, (JSONObject) oa_ing.get(key));
			opdata = setOpportunityDashBoardDivision(opdata, (JSONObject) oas_close.get(key));
			opdata = setOpportunityDashBoardDivision(opdata, (JSONObject) oas_ing.get(key));
			opdata = setOpportunityDashBoardDivision(opdata, (JSONObject) oa_this.get(key));
			opdata = setOpportunityDashBoardDivision(opdata, (JSONObject) oas_this.get(key));
			opdata = setOpportunityDashBoardDivision(opdata, (JSONObject) oa_last.get(key));
			opdata = setOpportunityDashBoardDivision(opdata, (JSONObject) oas_last.get(key));

//			System.out.println(opdata.toJSONString());
			results.add(opdata);

		}
		return results;

	}

	public long getLastOpportunityID() throws Exception{
		
		AggregateIterable<Document> results = null;
		
        List<? extends Bson> pipeline = Arrays.asList(
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document())
                                .append("MAX(OPPORTUNITY_ID)", new Document()
                                        .append("$max", "$OPPORTUNITY_ID")
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("OPPORTUNITY_ID", new Document()
                                        .append("$toString", "$MAX(OPPORTUNITY_ID)")
                                )
                                .append("_id", 0)
                        )
        );
        
		results = mongoTemplate.getCollection("opportunity_log").aggregate(pipeline).allowDiskUse(true);

		JSONObject obj = new JSONObject();
		for (Document dbObject : results) {

			JSONObject jobj = CommonUtils.stringTojson(dbObject.toJson());
			return ((long)CommonUtils.emptyToZero(""+jobj.get("OPPORTUNITY_ID"),2 )) + 1;
		}
		
		return 0L;
		
        
	}
	public JSONObject setOpportunityDashBoardDivision(JSONObject... objs) {

		JSONObject opdata = objs[0];
		JSONObject target = objs[1];

		Set<String> set = target.keySet();
		Iterator<String> it = set.iterator();

		if (opdata.size() == 0) {

			opdata.put("DIVISION_NAME", target.get("DIVISION_NAME"));
			opdata.put("MEMBER_DIVISION", target.get("MEMBER_DIVISION"));
		}
		while (it.hasNext()) {

			String key = it.next();

			if ("DIVISION_NAME".equals(key) || "MEMBER_DIVISION".equals(key))
				continue;

			if (target.get(key) == null || "null".equals(target.get(key)))
				opdata.put(key, 0);
			else
				opdata.put(key, target.get(key));
		}

//		System.out.println(target.toJSONString());
		return opdata;
	}
	
	private JSONObject selectOpportunityDashBoardDivision_ESP() throws Exception {

		AggregateIterable<Document> results = null;
		JSONArray array = null;

		List<? extends Bson> pipeline = Arrays.asList(
				new Document().append("$project", new Document().append("_id", 0).append("OMI", "$$ROOT")),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_DIVISION").append("from", "our_division_info")
								.append("foreignField", "DIVISION_NO").append("as", "ODI")),
				new Document().append("$unwind",
						new Document().append("path", "$ODI").append("preserveNullAndEmptyArrays", false)),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_ID_NUM").append("from", "ESP")
								.append("foreignField", "MEMBER_ID_NUM").append("as", "ESP")),
				new Document().append("$unwind",
						new Document().append("path", "$ESP").append("preserveNullAndEmptyArrays", true)),
				new Document().append("$match",
						new Document().append("$and",
								Arrays.asList(new Document().append("ODI.DIVISION_TYPE", "S"),
										new Document().append("$or",
												Arrays.asList(new Document().append("ESP.CURRENT_USER", new BsonNull()),
														new Document().append("ESP.CURRENT_USER", this.current_user))),
										new Document().append("$or",
												Arrays.asList(new Document().append("ESP.CURRENT_TIME", new BsonNull()),
														new Document().append("ESP.CURRENT_TIME",
																this.current_date)))))),
				new Document().append("$group",
						new Document()
								.append("_id",
										new Document().append("ODI\u1390DIVISION_NAME", "$ODI.DIVISION_NAME")
												.append("OMI\u1390MEMBER_DIVISION", "$OMI.MEMBER_DIVISION"))
								.append("SUM(ESP\u1390TARGET_REV)", new Document().append("$sum", "$ESP.TARGET_REV"))
								.append("SUM(ESP\u1390TARGET_GP)", new Document().append("$sum", "$ESP.TARGET_GP"))),
				new Document().append("$project",
						new Document().append("MEMBER_DIVISION", "$_id.OMI\u1390MEMBER_DIVISION")
								.append("DIVISION_NAME", "$_id.ODI\u1390DIVISION_NAME")
								.append("TARGET_REV",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(ESP\u1390TARGET_REV)")))
								.append("TARGET_GP",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(ESP\u1390TARGET_GP)")))
								.append("_id", 0)));

		results = mongoTemplate.getCollection("OMI").aggregate(pipeline).allowDiskUse(true);

		JSONObject obj = new JSONObject();
		for (Document dbObject : results) {

			if (dbObject.size() < 1)
				return obj;

			JSONObject jobj = CommonUtils.stringTojson(dbObject.toJson());
			String member_division = "" + jobj.get("MEMBER_DIVISION");
			obj.put(member_division, jobj);
		}

		return obj;
	}

	private JSONObject selectOpportunityDashBoardDivision_OA_CLOSE() throws Exception {

		AggregateIterable<Document> results = null;
		JSONArray array = null;

		List<? extends Bson> pipeline = Arrays.asList(
				new Document().append("$project", new Document().append("_id", 0).append("OMI", "$$ROOT")),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_DIVISION").append("from", "our_division_info")
								.append("foreignField", "DIVISION_NO").append("as", "ODI")),
				new Document().append("$unwind",
						new Document().append("path", "$ODI").append("preserveNullAndEmptyArrays", false)),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_ID_NUM").append("from", "OA_CLOSE")
								.append("foreignField", "MEMBER_ID_NUM").append("as", "OA_CLOSE")),
				new Document().append("$unwind",
						new Document().append("path", "$OA_CLOSE").append("preserveNullAndEmptyArrays", true)),
				new Document().append("$match",
						new Document().append("$and", Arrays.asList(new Document().append("ODI.DIVISION_TYPE", "S"),
								new Document().append("$or",
										Arrays.asList(new Document().append("OA_CLOSE.CURRENT_USER", new BsonNull()),
												new Document().append("OA_CLOSE.CURRENT_USER", this.current_user))),
								new Document().append("$or",
										Arrays.asList(new Document().append("OA_CLOSE.CURRENT_TIME", new BsonNull()),
												new Document().append("OA_CLOSE.CURRENT_TIME", this.current_date)))))),
				new Document().append("$group",
						new Document()
								.append("_id",
										new Document().append("ODI\u1390DIVISION_NAME", "$ODI.DIVISION_NAME")
												.append("OMI\u1390MEMBER_DIVISION", "$OMI.MEMBER_DIVISION"))
								.append("SUM(OA_CLOSE\u1390REV_CLOSE_FC_IN)",
										new Document().append("$sum", "$OA_CLOSE.REV_CLOSE_FC_IN"))
								.append("SUM(OA_CLOSE\u1390REV_CLOSE_FC_OUT)",
										new Document().append("$sum", "$OA_CLOSE.REV_CLOSE_FC_OUT"))
								.append("SUM(OA_CLOSE\u1390GP_CLOSE_FC_IN)",
										new Document().append("$sum", "$OA_CLOSE.GP_CLOSE_FC_IN"))
								.append("SUM(OA_CLOSE\u1390GP_CLOSE_FC_OUT)",
										new Document().append("$sum", "$OA_CLOSE.GP_CLOSE_FC_OUT"))),
				new Document().append("$project", new Document()
						.append("MEMBER_DIVISION", "$_id.OMI\u1390MEMBER_DIVISION")
						.append("DIVISION_NAME", "$_id.ODI\u1390DIVISION_NAME")
						.append("REV_CLOSE_FC_IN",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OA_CLOSE\u1390REV_CLOSE_FC_IN)")))
						.append("REV_CLOSE_FC_OUT",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OA_CLOSE\u1390REV_CLOSE_FC_OUT)")))
						.append("GP_CLOSE_FC_IN",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OA_CLOSE\u1390GP_CLOSE_FC_IN)")))
						.append("GP_CLOSE_FC_OUT",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OA_CLOSE\u1390GP_CLOSE_FC_OUT)")))
						.append("_id", 0)));

		results = mongoTemplate.getCollection("OMI").aggregate(pipeline).allowDiskUse(true);

		JSONObject obj = new JSONObject();
		for (Document dbObject : results) {

			if (dbObject.size() < 1)
				return obj;

			JSONObject jobj = CommonUtils.stringTojson(dbObject.toJson());
			String member_division = "" + jobj.get("MEMBER_DIVISION");
			obj.put(member_division, jobj);
		}

		return obj;
	}

	private JSONObject selectOpportunityDashBoardDivision_OA_ING() throws Exception {

		AggregateIterable<Document> results = null;
		JSONArray array = null;

		List<? extends Bson> pipeline = Arrays.asList(
				new Document().append("$project", new Document().append("_id", 0).append("OMI", "$$ROOT")),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_DIVISION").append("from", "our_division_info")
								.append("foreignField", "DIVISION_NO").append("as", "ODI")),
				new Document().append("$unwind",
						new Document().append("path", "$ODI").append("preserveNullAndEmptyArrays", false)),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_ID_NUM").append("from", "OA_CLOSE")
								.append("foreignField", "MEMBER_ID_NUM").append("as", "OA_CLOSE")),
				new Document().append("$unwind",
						new Document().append("path", "$OA_CLOSE").append("preserveNullAndEmptyArrays", true)),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_ID_NUM").append("from", "OA_ING")
								.append("foreignField", "MEMBER_ID_NUM").append("as", "OA_ING")),
				new Document().append("$unwind",
						new Document().append("path", "$OA_ING").append("preserveNullAndEmptyArrays", true)),
				new Document().append("$match", new Document().append("$and",
						Arrays.asList(new Document().append("ODI.DIVISION_TYPE", "S"),
								new Document().append("$or",
										Arrays.asList(
												new Document().append("OA_ING.CURRENT_USER", new BsonNull()),
												new Document().append("OA_ING.CURRENT_USER", this.current_user))),
								new Document().append("$or",
										Arrays.asList(new Document().append("OA_ING.CURRENT_TIME", new BsonNull()),
												new Document().append("OA_ING.CURRENT_TIME", this.current_date)))))),
				new Document().append("$group", new Document()
						.append("_id",
								new Document().append("ODI\u1390DIVISION_NAME", "$ODI.DIVISION_NAME")
										.append("OMI\u1390MEMBER_DIVISION", "$OMI.MEMBER_DIVISION"))
						.append("SUM(OA_ING\u1390REV_ING_FC_IN)",
								new Document().append("$sum", "$OA_ING.REV_ING_FC_IN"))
						.append("SUM(OA_ING\u1390REV_ING_FC_OUT)",
								new Document().append("$sum", "$OA_ING.REV_ING_FC_OUT"))
						.append("SUM(OA_ING\u1390GP_ING_FC_IN)", new Document().append("$sum", "$OA_ING.GP_ING_FC_IN"))
						.append("SUM(OA_ING\u1390GP_ING_FC_OUT)",
								new Document().append("$sum", "$OA_ING.GP_ING_FC_OUT"))),
				new Document().append("$project",
						new Document().append("MEMBER_DIVISION", "$_id.OMI\u1390MEMBER_DIVISION")
								.append("DIVISION_NAME", "$_id.ODI\u1390DIVISION_NAME")
								.append("REV_ING_FC_IN",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_ING\u1390REV_ING_FC_IN)")))
								.append("REV_ING_FC_OUT",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_ING\u1390REV_ING_FC_OUT)")))
								.append("GP_ING_FC_IN",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_ING\u1390GP_ING_FC_IN)")))
								.append("GP_ING_FC_OUT",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_ING\u1390GP_ING_FC_OUT)")))
								.append("_id", 0)));

		results = mongoTemplate.getCollection("OMI").aggregate(pipeline).allowDiskUse(true);

		JSONObject obj = new JSONObject();
		for (Document dbObject : results) {

			if (dbObject.size() < 1)
				return obj;

			JSONObject jobj = CommonUtils.stringTojson(dbObject.toJson());
			String member_division = "" + jobj.get("MEMBER_DIVISION");
			obj.put(member_division, jobj);
		}

		return obj;
	}

	private JSONObject selectOpportunityDashBoardDivision_OAS_CLOSE() throws Exception {

		AggregateIterable<Document> results = null;
		JSONArray array = null;

		List<? extends Bson> pipeline = Arrays.asList(
				new Document().append("$project", new Document().append("_id", 0).append("OMI", "$$ROOT")),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_DIVISION").append("from", "our_division_info")
								.append("foreignField", "DIVISION_NO").append("as", "ODI")),
				new Document().append("$unwind",
						new Document().append("path", "$ODI").append("preserveNullAndEmptyArrays", false)),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_ID_NUM").append("from", "OAS_CLOSE")
								.append("foreignField", "MEMBER_ID_NUM").append("as", "OAS_CLOSE")),
				new Document().append("$unwind",
						new Document().append("path", "$OAS_CLOSE").append("preserveNullAndEmptyArrays", true)),
				new Document().append("$match",
						new Document().append("$and", Arrays.asList(new Document().append("ODI.DIVISION_TYPE", "S"),
								new Document().append("$or",
										Arrays.asList(new Document().append("OAS_CLOSE.CURRENT_USER", new BsonNull()),
												new Document().append("OAS_CLOSE.CURRENT_USER", this.current_user))),
								new Document().append("$or",
										Arrays.asList(new Document().append("OAS_CLOSE.CURRENT_TIME", new BsonNull()),
												new Document().append("OAS_CLOSE.CURRENT_TIME", this.current_date)))))),
				new Document().append("$group",
						new Document()
								.append("_id",
										new Document().append("ODI\u1390DIVISION_NAME", "$ODI.DIVISION_NAME")
												.append("OMI\u1390MEMBER_DIVISION", "$OMI.MEMBER_DIVISION"))
								.append("SUM(OAS_CLOSE\u1390REV_CLOSE_SPLIT_FC_IN)",
										new Document().append("$sum", "$OAS_CLOSE.REV_CLOSE_SPLIT_FC_IN"))
								.append("SUM(OAS_CLOSE\u1390REV_CLOSE_SPLIT_FC_OUT)",
										new Document().append("$sum", "$OAS_CLOSE.REV_CLOSE_SPLIT_FC_OUT"))
								.append("SUM(OAS_CLOSE\u1390GP_CLOSE_SPLIT_FC_IN)",
										new Document().append("$sum", "$OAS_CLOSE.GP_CLOSE_SPLIT_FC_IN"))
								.append("SUM(OAS_CLOSE\u1390GP_CLOSE_SPLIT_FC_OUT)",
										new Document().append("$sum", "$OAS_CLOSE.GP_CLOSE_SPLIT_FC_OUT"))),
				new Document().append("$project", new Document()
						.append("MEMBER_DIVISION", "$_id.OMI\u1390MEMBER_DIVISION")
						.append("DIVISION_NAME", "$_id.ODI\u1390DIVISION_NAME")
						.append("REV_CLOSE_SPLIT_FC_IN",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_CLOSE\u1390REV_CLOSE_SPLIT_FC_IN)")))
						.append("REV_CLOSE_SPLIT_FC_OUT",
								new Document().append("$toString",
										new Document().append("$toLong",
												"$SUM(OAS_CLOSE\u1390REV_CLOSE_SPLIT_FC_OUT)")))
						.append("GP_CLOSE_SPLIT_FC_IN",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_CLOSE\u1390GP_CLOSE_SPLIT_FC_IN)")))
						.append("GP_CLOSE_SPLIT_FC_OUT",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_CLOSE\u1390GP_CLOSE_SPLIT_FC_OUT")))
						.append("_id", 0)));

		results = mongoTemplate.getCollection("OMI").aggregate(pipeline).allowDiskUse(true);

		JSONObject obj = new JSONObject();
		for (Document dbObject : results) {

			if (dbObject.size() < 1)
				return obj;

			JSONObject jobj = CommonUtils.stringTojson(dbObject.toJson());
			String member_division = "" + jobj.get("MEMBER_DIVISION");
			obj.put(member_division, jobj);
		}

		return obj;
	}

	private JSONObject selectOpportunityDashBoardDivision_OAS_ING() throws Exception {

		AggregateIterable<Document> results = null;
		JSONArray array = null;

		List<? extends Bson> pipeline = Arrays.asList(
				new Document().append("$project", new Document().append("_id", 0).append("OMI", "$$ROOT")),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_DIVISION").append("from", "our_division_info")
								.append("foreignField", "DIVISION_NO").append("as", "ODI")),
				new Document().append("$unwind",
						new Document().append("path", "$ODI").append("preserveNullAndEmptyArrays", false)),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_ID_NUM").append("from", "OAS_ING")
								.append("foreignField", "MEMBER_ID_NUM").append("as", "OAS_ING")),
				new Document().append("$unwind",
						new Document().append("path", "$OAS_ING").append("preserveNullAndEmptyArrays", true)),
				new Document().append("$match",
						new Document().append("$and", Arrays.asList(new Document().append("ODI.DIVISION_TYPE", "S"),
								new Document().append("$or",
										Arrays.asList(new Document().append("OAS_ING.CURRENT_USER", new BsonNull()),
												new Document().append("OAS_ING.CURRENT_USER", this.current_user))),
								new Document().append("$or",
										Arrays.asList(new Document().append("OAS_ING.CURRENT_TIME", new BsonNull()),
												new Document().append("OAS_ING.CURRENT_TIME", this.current_date)))))),
				new Document().append("$group",
						new Document()
								.append("_id",
										new Document().append("ODI\u1390DIVISION_NAME", "$ODI.DIVISION_NAME")
												.append("OMI\u1390MEMBER_DIVISION", "$OMI.MEMBER_DIVISION"))
								.append("SUM(OAS_ING\u1390REV_ING_SPLIT_FC_IN)",
										new Document().append("$sum", "$OAS_ING.REV_ING_SPLIT_FC_IN"))
								.append("SUM(OAS_ING\u1390REV_ING_SPLIT_FC_OUT)",
										new Document().append("$sum", "$OAS_ING.REV_ING_SPLIT_FC_OUT"))
								.append("SUM(OAS_ING\u1390GP_ING_SPLIT_FC_IN)",
										new Document().append("$sum", "$OAS_ING.GP_ING_SPLIT_FC_IN"))
								.append("SUM(OAS_ING\u1390GP_ING_SPLIT_FC_OUT)",
										new Document().append("$sum", "$OAS_ING.GP_ING_SPLIT_FC_OUT"))),
				new Document().append("$project", new Document()
						.append("MEMBER_DIVISION", "$_id.OMI\u1390MEMBER_DIVISION")
						.append("DIVISION_NAME", "$_id.ODI\u1390DIVISION_NAME")
						.append("REV_ING_SPLIT_FC_IN",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_ING\u1390REV_ING_SPLIT_FC_IN)")))
						.append("REV_ING_SPLIT_FC_OUT",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_ING\u1390REV_ING_SPLIT_FC_OUT)")))
						.append("GP_ING_SPLIT_FC_IN",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_ING\u1390GP_ING_SPLIT_FC_IN)")))
						.append("GP_ING_SPLIT_FC_OUT",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_ING\u1390GP_ING_SPLIT_FC_OUT)")))
						.append("_id", 0)));
		results = mongoTemplate.getCollection("OMI").aggregate(pipeline).allowDiskUse(true);

		JSONObject obj = new JSONObject();
		for (Document dbObject : results) {

			if (dbObject.size() < 1)
				return obj;

			JSONObject jobj = CommonUtils.stringTojson(dbObject.toJson());
			String member_division = "" + jobj.get("MEMBER_DIVISION");
			obj.put(member_division, jobj);
		}

		return obj;
	}

	private JSONObject selectOpportunityDashBoardDivision_OA_THIS() throws Exception {

		AggregateIterable<Document> results = null;
		JSONArray array = null;

		List<? extends Bson> pipeline = Arrays.asList(
				new Document().append("$project", new Document().append("_id", 0).append("OMI", "$$ROOT")),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_DIVISION").append("from", "our_division_info")
								.append("foreignField", "DIVISION_NO").append("as", "ODI")),
				new Document().append("$unwind",
						new Document().append("path", "$ODI").append("preserveNullAndEmptyArrays", false)),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_ID_NUM").append("from", "OA_THIS")
								.append("foreignField", "MEMBER_ID_NUM").append("as", "OA_THIS")),
				new Document().append("$unwind",
						new Document().append("path", "$OA_THIS").append("preserveNullAndEmptyArrays", true)),
				new Document().append("$match",
						new Document().append("$and", Arrays.asList(new Document().append("ODI.DIVISION_TYPE", "S"),
								new Document().append("$or",
										Arrays.asList(new Document().append("OA_THIS.CURRENT_USER", new BsonNull()),
												new Document().append("OA_THIS.CURRENT_USER", this.current_user))),
								new Document().append("$or",
										Arrays.asList(new Document().append("OA_THIS.CURRENT_TIME", new BsonNull()),
												new Document().append("OA_THIS.CURRENT_TIME", this.current_date)))))),
				new Document().append("$group",
						new Document()
								.append("_id",
										new Document().append("ODI\u1390DIVISION_NAME", "$ODI.DIVISION_NAME")
												.append("OMI\u1390MEMBER_DIVISION", "$OMI.MEMBER_DIVISION"))
								.append("SUM(OA_THIS\u1390REV_THIS_FC_IN)",
										new Document().append("$sum", "$OA_THIS.REV_THIS_FC_IN"))
								.append("SUM(OA_THIS\u1390REV_THIS_FC_OUT)",
										new Document().append("$sum", "$OA_THIS.REV_THIS_FC_OUT"))
								.append("SUM(OA_THIS\u1390GP_THIS_FC_IN)",
										new Document().append("$sum", "$OA_THIS.GP_THIS_FC_IN"))
								.append("SUM(OA_THIS\u1390GP_THIS_FC_OUT)",
										new Document().append("$sum", "$OA_THIS.GP_THIS_FC_OUT"))),
				new Document().append("$project",
						new Document().append("MEMBER_DIVISION", "$_id.OMI\u1390MEMBER_DIVISION")
								.append("DIVISION_NAME", "$_id.ODI\u1390DIVISION_NAME")
								.append("REV_THIS_FC_IN",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_THIS\u1390_THIS_FC_IN)")))
								.append("REV_THIS_FC_OUT",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_THIS\u1390REV_THIS_FC_OUT)")))
								.append("GP_THIS_FC_IN",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_THIS\u1390GP_THIS_FC_IN)")))
								.append("GP_THIS_FC_OUT",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_THIS\u1390GP_THIS_FC_OUT)")))
								.append("_id", 0)));
		results = mongoTemplate.getCollection("OMI").aggregate(pipeline).allowDiskUse(true);

		JSONObject obj = new JSONObject();
		for (Document dbObject : results) {

			if (dbObject.size() < 1)
				return obj;

			JSONObject jobj = CommonUtils.stringTojson(dbObject.toJson());
			String member_division = "" + jobj.get("MEMBER_DIVISION");
			obj.put(member_division, jobj);
		}

		return obj;
	}

	private JSONObject selectOpportunityDashBoardDivision_OAS_THIS() throws Exception {

		AggregateIterable<Document> results = null;
		JSONArray array = null;

		List<? extends Bson> pipeline = Arrays.asList(
				new Document().append("$project", new Document().append("_id", 0).append("OMI", "$$ROOT")),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_DIVISION").append("from", "our_division_info")
								.append("foreignField", "DIVISION_NO").append("as", "ODI")),
				new Document().append("$unwind",
						new Document().append("path", "$ODI").append("preserveNullAndEmptyArrays", false)),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_ID_NUM").append("from", "OAS_THIS")
								.append("foreignField", "MEMBER_ID_NUM").append("as", "OAS_THIS")),
				new Document().append("$unwind",
						new Document().append("path", "$OAS_THIS").append("preserveNullAndEmptyArrays", true)),
				new Document().append("$match",
						new Document().append("$and", Arrays.asList(new Document().append("ODI.DIVISION_TYPE", "S"),
								new Document().append("$or",
										Arrays.asList(new Document().append("OAS_THIS.CURRENT_USER", new BsonNull()),
												new Document().append("OAS_THIS.CURRENT_USER", this.current_user))),
								new Document().append("$or",
										Arrays.asList(new Document().append("OAS_THIS.CURRENT_TIME", new BsonNull()),
												new Document().append("OAS_THIS.CURRENT_TIME", this.current_date)))))),
				new Document().append("$group",
						new Document()
								.append("_id",
										new Document().append("ODI\u1390DIVISION_NAME", "$ODI.DIVISION_NAME")
												.append("OMI\u1390MEMBER_DIVISION", "$OMI.MEMBER_DIVISION"))
								.append("SUM(OAS_THIS\u1390REV_THIS_SPLIT_FC_IN)",
										new Document().append("$sum", "$OAS_THIS.REV_THIS_SPLIT_FC_IN"))
								.append("SUM(OAS_THIS\u1390REV_THIS_SPLIT_FC_OUT)",
										new Document().append("$sum", "$OAS_THIS.REV_THIS_SPLIT_FC_OUT"))
								.append("SUM(OAS_THIS\u1390GP_THIS_SPLIT_FC_IN)",
										new Document().append("$sum", "$OAS_THIS.GP_THIS_SPLIT_FC_IN"))
								.append("SUM(OAS_THIS\u1390GP_THIS_SPLIT_FC_OUT)",
										new Document().append("$sum", "$OAS_THIS.GP_THIS_SPLIT_FC_OUT"))),
				new Document().append("$project", new Document()
						.append("MEMBER_DIVISION", "$_id.OMI\u1390MEMBER_DIVISION")
						.append("DIVISION_NAME", "$_id.ODI\u1390DIVISION_NAME")
						.append("REV_THIS_SPLIT_FC_IN",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_THIS\u1390REV_THIS_SPLIT_FC_IN)")))
						.append("REV_THIS_SPLIT_FC_OUT",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_THIS\u1390REV_THIS_SPLIT_FC_OUT)")))
						.append("GP_THIS_SPLIT_FC_IN",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_THIS\u1390GP_THIS_SPLIT_FC_IN)")))
						.append("GP_THIS_SPLIT_FC_OUT",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_THIS\u1390GP_THIS_SPLIT_FC_OUT)")))
						.append("_id", 0)));
		results = mongoTemplate.getCollection("OMI").aggregate(pipeline).allowDiskUse(true);

		JSONObject obj = new JSONObject();
		for (Document dbObject : results) {

			if (dbObject.size() < 1)
				return obj;

			JSONObject jobj = CommonUtils.stringTojson(dbObject.toJson());
			String member_division = "" + jobj.get("MEMBER_DIVISION");
			obj.put(member_division, jobj);
		}

		return obj;
	}

	private JSONObject selectOpportunityDashBoardDivision_OA_LAST() throws Exception {

		AggregateIterable<Document> results = null;
		JSONArray array = null;

		List<? extends Bson> pipeline = Arrays.asList(
				new Document().append("$project", new Document().append("_id", 0).append("OMI", "$$ROOT")),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_DIVISION").append("from", "our_division_info")
								.append("foreignField", "DIVISION_NO").append("as", "ODI")),
				new Document().append("$unwind",
						new Document().append("path", "$ODI").append("preserveNullAndEmptyArrays", false)),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_ID_NUM").append("from", "OA_LAST")
								.append("foreignField", "MEMBER_ID_NUM").append("as", "OA_LAST")),
				new Document().append("$unwind",
						new Document().append("path", "$OA_LAST").append("preserveNullAndEmptyArrays", true)),
				new Document().append("$match",
						new Document().append("$and", Arrays.asList(new Document().append("ODI.DIVISION_TYPE", "S"),
								new Document().append("$or",
										Arrays.asList(new Document().append("OA_LAST.CURRENT_USER", new BsonNull()),
												new Document().append("OA_LAST.CURRENT_USER", this.current_user))),
								new Document().append("$or",
										Arrays.asList(new Document().append("OA_LAST.CURRENT_TIME", new BsonNull()),
												new Document().append("OA_LAST.CURRENT_TIME", this.current_date)))))),
				new Document().append("$group",
						new Document()
								.append("_id",
										new Document().append("ODI\u1390DIVISION_NAME", "$ODI.DIVISION_NAME")
												.append("OMI\u1390MEMBER_DIVISION", "$OMI.MEMBER_DIVISION"))
								.append("SUM(OA_LAST\u1390REV_LAST_FC_IN)",
										new Document().append("$sum", "$OA_LAST.REV_LAST_FC_IN"))
								.append("SUM(OA_LAST\u1390REV_LAST_FC_OUT)",
										new Document().append("$sum", "$OA_LAST.REV_LAST_FC_OUT"))
								.append("SUM(OA_LAST\u1390GP_LAST_FC_IN)",
										new Document().append("$sum", "$OA_LAST.GP_LAST_FC_IN"))
								.append("SUM(OA_LAST\u1390GP_LAST_FC_OUT)",
										new Document().append("$sum", "$OA_LAST.GP_LAST_FC_OUT"))),
				new Document().append("$project",
						new Document().append("MEMBER_DIVISION", "$_id.OMI\u1390MEMBER_DIVISION")
								.append("DIVISION_NAME", "$_id.ODI\u1390DIVISION_NAME")
								.append("REV_LAST_FC_IN",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_LAST\u1390REV_LAST_FC_IN)")))
								.append("REV_LAST_FC_OUT",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_LAST\u1390REV_LAST_FC_OUT)")))
								.append("GP_LAST_FC_IN",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_LAST\u1390GP_LAST_FC_IN)")))
								.append("GP_LAST_FC_OUT",
										new Document().append("$toString",
												new Document().append("$toLong", "$SUM(OA_LAST\u1390GP_LAST_FC_OUT)")))
								.append("_id", 0)));
		results = mongoTemplate.getCollection("OMI").aggregate(pipeline).allowDiskUse(true);

		JSONObject obj = new JSONObject();
		for (Document dbObject : results) {

			if (dbObject.size() < 1)
				return obj;

			JSONObject jobj = CommonUtils.stringTojson(dbObject.toJson());
			String member_division = "" + jobj.get("MEMBER_DIVISION");
			obj.put(member_division, jobj);
		}

		return obj;
	}

	private JSONObject selectOpportunityDashBoardDivision_OAS_LAST() throws Exception {

		AggregateIterable<Document> results = null;
		JSONArray array = null;

		List<? extends Bson> pipeline = Arrays.asList(
				new Document().append("$project", new Document().append("_id", 0).append("OMI", "$$ROOT")),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_DIVISION").append("from", "our_division_info")
								.append("foreignField", "DIVISION_NO").append("as", "ODI")),
				new Document().append("$unwind",
						new Document().append("path", "$ODI").append("preserveNullAndEmptyArrays", false)),
				new Document().append("$lookup",
						new Document().append("localField", "OMI.MEMBER_ID_NUM").append("from", "OAS_LAST")
								.append("foreignField", "MEMBER_ID_NUM").append("as", "OAS_LAST")),
				new Document().append("$unwind",
						new Document().append("path", "$OAS_LAST").append("preserveNullAndEmptyArrays", true)),
				new Document().append("$match",
						new Document().append("$and", Arrays.asList(new Document().append("ODI.DIVISION_TYPE", "S"),
								new Document().append("$or",
										Arrays.asList(new Document().append("OAS_LAST.CURRENT_USER", new BsonNull()),
												new Document().append("OAS_LAST.CURRENT_USER", this.current_user))),
								new Document().append("$or",
										Arrays.asList(new Document().append("OAS_LAST.CURRENT_TIME", new BsonNull()),
												new Document().append("OAS_LAST.CURRENT_TIME", this.current_date)))))),
				new Document().append("$group",
						new Document()
								.append("_id",
										new Document().append("ODI\u1390DIVISION_NAME", "$ODI.DIVISION_NAME")
												.append("OMI\u1390MEMBER_DIVISION", "$OMI.MEMBER_DIVISION"))
								.append("SUM(OAS_LAST\u1390REV_LAST_SPLIT_FC_IN)",
										new Document().append("$sum", "$OAS_LAST.REV_LAST_SPLIT_FC_IN"))
								.append("SUM(OAS_LAST\u1390REV_LAST_SPLIT_FC_OUT)",
										new Document().append("$sum", "$OAS_LAST.REV_LAST_SPLIT_FC_OUT"))
								.append("SUM(OAS_LAST\u1390GP_LAST_SPLIT_FC_IN)",
										new Document().append("$sum", "$OAS_LAST.GP_LAST_SPLIT_FC_IN"))
								.append("SUM(OAS_LAST\u1390GP_LAST_SPLIT_FC_OUT)",
										new Document().append("$sum", "$OAS_LAST.GP_LAST_SPLIT_FC_OUT"))),
				new Document().append("$project", new Document()
						.append("MEMBER_DIVISION", "$_id.OMI\u1390MEMBER_DIVISION")
						.append("DIVISION_NAME", "$_id.ODI\u1390DIVISION_NAME")
						.append("REV_LAST_SPLIT_FC_IN",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_LAST\u1390REV_LAST_SPLIT_FC_IN)")))
						.append("REV_LAST_SPLIT_FC_OUT",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_LAST\u1390REV_LAST_SPLIT_FC_OUT)")))
						.append("GP_LAST_SPLIT_FC_IN",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_LAST\u1390GP_LAST_SPLIT_FC_IN)")))
						.append("GP_LAST_SPLIT_FC_OUT",
								new Document().append("$toString",
										new Document().append("$toLong", "$SUM(OAS_LAST\u1390GP_LAST_SPLIT_FC_OUT)")))
						.append("_id", 0)));
		results = mongoTemplate.getCollection("OMI").aggregate(pipeline).allowDiskUse(true);

		JSONObject obj = new JSONObject();
		for (Document dbObject : results) {

			if (dbObject.size() < 1)
				return obj;

			JSONObject jobj = CommonUtils.stringTojson(dbObject.toJson());
			String member_division = "" + jobj.get("MEMBER_DIVISION");
			obj.put(member_division, jobj);
		}

		return obj;
	}

}
