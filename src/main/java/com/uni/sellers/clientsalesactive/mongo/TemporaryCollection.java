package com.uni.sellers.clientsalesactive.mongo;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.stereotype.Service;

import com.mongodb.client.AggregateIterable;
import com.uni.sellers.clientsalesactive.mongo.sql.SQLCommon;

@Service
public class TemporaryCollection extends SQLCommon {

	public boolean isSuccess() {

		boolean result = false;
		AggregateIterable<Document> results = null;

		try {
			results = mongoTemplate.getCollection("our_members_info").aggregate(createOpportunityLog_OMI())
					.allowDiskUse(true);

			for (Document dbObject : results) {
				if (dbObject != null) {
//					//					System.out.println("temporarySQLs OMI complete");
					break;
				}
			}
			
			results = mongoTemplate.getCollection("erp_sales_plan").aggregate(createOpportunityLog_ESP())
					.allowDiskUse(true);

			for (Document dbObject : results) {
				if (dbObject != null) {
					//					System.out.println("temporarySQLs ESP complete");
					break;
				}
			}
			
			results = mongoTemplate.getCollection("opportunity_log").aggregate(createOpportunityLog_OA_CLOSE())
					.allowDiskUse(true);

			for (Document dbObject : results) {
				if (dbObject != null) {
					//					System.out.println("temporarySQLs OA_CLOSE complete");
					break;
				}
			}
			
			results = mongoTemplate.getCollection("opportunity_log").aggregate(createOpportunityLog_OA_ING())
					.allowDiskUse(true);

			for (Document dbObject : results) {
				if (dbObject != null) {
					//					System.out.println("temporarySQLs OA_ING complete");
					break;
				}
			}
			
			results = mongoTemplate.getCollection("opportunity_log").aggregate(createOpportunityLog_OAS_CLOSE())
					.allowDiskUse(true);

			for (Document dbObject : results) {
				if (dbObject != null) {
					//					System.out.println("temporarySQLs OAS_CLOSE complete");
					break;
				}
			}
			
			results = mongoTemplate.getCollection("opportunity_log").aggregate(createOpportunityLog_OAS_ING())
					.allowDiskUse(true);

			for (Document dbObject : results) {
				if (dbObject != null) {
					//					System.out.println("temporarySQLs OAS_ING complete");
					break;
				}
			}
			
			results = mongoTemplate.getCollection("opportunity_log").aggregate(createOpportunityLog_OA_THIS())
					.allowDiskUse(true);

			for (Document dbObject : results) {
				if (dbObject != null) {
					//					System.out.println("temporarySQLs OA_THIS complete");
					break;
				}
			}
			
			results = mongoTemplate.getCollection("opportunity_log").aggregate(createOpportunityLog_OAS_THIS())
					.allowDiskUse(true);

			for (Document dbObject : results) {
				if (dbObject != null) {
					//					System.out.println("temporarySQLs OAS_THIS complete");
					break;
				}
			}
			
			results = mongoTemplate.getCollection("opportunity_log").aggregate(createOpportunityLog_OA_LAST())
					.allowDiskUse(true);

			for (Document dbObject : results) {
				if (dbObject != null) {
					//					System.out.println("temporarySQLs OA_LAST complete");
					break;
				}
			}
			
			results = mongoTemplate.getCollection("opportunity_log").aggregate(createOpportunityLog_OAS_LAST())
					.allowDiskUse(true);

			for (Document dbObject : results) {
				if (dbObject != null) {
					//					System.out.println("temporarySQLs OAS_LAST complete");
					break;
				}
			}
			return true;
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;

	}

	private List<? extends Bson> createOpportunityLog_OMI() throws ParseException {
		List<? extends Bson> pipeline = Arrays
				.asList(new Document().append("$limit", 1.0),
						new Document().append("$project", new Document().append("_id", "$$REMOVE")),
						new Document().append("$lookup",
								new Document().append("from", "our_members_info").append("pipeline",
										Arrays.asList(new Document().append("$match", new Document().append("$or",
												Arrays.asList(new Document().append("STOP_DATE", new BsonNull()),
														new Document().append("STOP_DATE",
																new Document().append("$gte", this.startDate)))))))
										.append("as", "omi1")),
						new Document().append("$lookup",
								new Document().append("from", "our_members_info").append("pipeline", Arrays.asList(
										new Document().append("$project",
												new Document().append("_id", 0).append("IOMI", "$$ROOT")),
										new Document().append("$lookup",
												new Document().append("localField", "IOMI.MEMBER_ID_NUM")
														.append("from", "opportunity_amount")
														.append("foreignField", "MEMBER_ID_NUM").append("as", "IOA")),
										new Document().append("$unwind",
												new Document().append("path", "$IOA")
														.append("preserveNullAndEmptyArrays", false)),
										new Document().append("$match",
												new Document().append("IOMI.STOP_DATE", new BsonNull()).append(
														"IOA.BASIS_MONTH",
														new Document().append("$gte", this.startDate).append("$lte",
																this.endDate))),
										new Document().append("$group", new Document().append(
												"_id",
												new Document().append("IOMI\u1390MEMBER_ID_NUM", "$IOMI.MEMBER_ID_NUM")
														.append("IOMI\u1390MEMBER_DIVISION", "$IOMI.MEMBER_DIVISION")
														.append("IOMI\u1390HAN_NAME", "$IOMI.HAN_NAME")
														.append("IOMI\u1390MEMBER_TEAM", "$IOMI.MEMBER_TEAM"))),
										new Document().append("$project", new Document()
												.append("IOMI.MEMBER_DIVISION", "$_id.IOMI\u1390MEMBER_DIVISION")
												.append("IOMI.MEMBER_TEAM", "$_id.IOMI\u1390MEMBER_TEAM")
												.append("IOMI.MEMBER_ID_NUM", "$_id.IOMI\u1390MEMBER_ID_NUM")
												.append("IOMI.HAN_NAME", "$_id.IOMI\u1390HAN_NAME").append("_id", 0))))
										.append("as", "omi2")),
						new Document().append("$project",
								new Document().append("union",
										new Document().append("$concatArrays", Arrays.asList("$omi1", "$omi2")))),
						new Document().append("$unwind",
								new Document().append("path", "$union").append("preserveNullAndEmptyArrays", true)),
						new Document().append("$replaceRoot", new Document().append("newRoot", "$union")),
						new Document().append("$group",
								new Document().append("_id", new Document().append("MEMBER_ID_NUM", "$MEMBER_ID_NUM")
										.append("MEMBER_DIVISION", "$MEMBER_DIVISION").append("HAN_NAME", "$HAN_NAME")
										.append("MEMBER_TEAM", "$MEMBER_TEAM"))),
						new Document().append("$unwind",
								new Document().append("path", "$_id").append("preserveNullAndEmptyArrays", true)),
						new Document().append("$replaceRoot", new Document().append("newRoot", "$_id")),
						new Document().append("$project", new Document().append("MEMBER_ID_NUM", "$MEMBER_ID_NUM")
								.append("MEMBER_DIVISION", new Document().append("$toInt", "$MEMBER_DIVISION"))
								.append("HAN_NAME", "$HAN_NAME").append("MEMBER_TEAM", "$MEMBER_TEAM")
								.append("CURRENT_USER", this.current_user).append("CURRENT_TIME", this.current_date)),
						new Document().append("$merge", new Document("into", "OMI").append("whenMatched", "replace")));

		return pipeline;
	}

	private List<? extends Bson> createOpportunityLog_ESP() throws ParseException {
		
        List<? extends Bson> pipeline = Arrays.asList(
                new Document()
                        .append("$match", new Document()
                                .append("BASIS_MONTH", new Document()
                                        .append("$gte", this.startDate)
                                        .append("$lte", this.endDate)
                                )
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("MEMBER_ID_NUM", "$MEMBER_ID_NUM")
                                        .append("CURRENT_USER", "$CURRENT_USER")
                                        .append("CURRENT_TIME", "$CURRENT_TIME")
                                )
                                .append("SUM(TARGET_REVENUE_AMOUNT)", new Document()
                                        .append("$sum", "$TARGET_REVENUE_AMOUNT")
                                )
                                .append("SUM(TARGET_GP_AMOUNT)", new Document()
                                        .append("$sum", "$TARGET_GP_AMOUNT")
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("MEMBER_ID_NUM", "$_id.MEMBER_ID_NUM")
                                .append("TARGET_REV", "$SUM(TARGET_REVENUE_AMOUNT)")
                                .append("TARGET_GP", "$SUM(TARGET_GP_AMOUNT)")
                                .append("CURRENT_USER", this.current_user)
                                .append("CURRENT_TIME", this.current_date)
                                .append("_id", 0)
                        ),
                        new Document().append("$merge", new Document("into", "ESP").append("whenMatched", "replace")));
//        new Document().append("$out", "ESP"));

//        System.out.println(pipeline.toString());
		return pipeline;
	}

	private List<? extends Bson> createOpportunityLog_OA_CLOSE() throws ParseException {

        List<? extends Bson> pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("OL", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "OL.OPPORTUNITY_ID")
                                .append("from", "opportunity_amount")
                                .append("foreignField", "OPPORTUNITY_ID")
                                .append("as", "OA")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$OA")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$match", new Document()
                                .append("$and", Arrays.asList(
                                        new Document()
                                                .append("OL.TEMP_FLAG", "N"),
                                        new Document()
                                                .append("OL.SALES_CYCLE", "4"),
                                        new Document()
                                                .append("OL.CLOSE_CATEGORY", "1"),
                                        new Document()
                                                .append("OA.BASIS_MONTH", new Document()
                                                        .append("$gte", this.year)
                                                ),
                                        new Document()
                                                .append("OA.BASIS_MONTH", new Document()
                                                        .append("$lte", this.endDate)
                                                )
                                    )
                                )
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("OL\u1390FORECAST_YN", "$OL.FORECAST_YN")
                                        .append("OA\u1390MEMBER_ID_NUM", "$OA.MEMBER_ID_NUM")
                                        .append("CURRENT_USER", "$CURRENT_USER")
                                        .append("CURRENT_TIME", "$CURRENT_TIME")
                                )
                                .append("SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)", new Document()
                                        .append("$sum", "$OA.BASIS_PLAN_REVENUE_AMOUNT")
                                )
                                .append("SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)", new Document()
                                        .append("$sum", "$OA.BASIS_PLAN_GP_AMOUNT")
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("MEMBER_ID_NUM", "$_id.OA\u1390MEMBER_ID_NUM")
                                .append("REV_CLOSE_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("REV_CLOSE_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_CLOSE_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_CLOSE_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("CURRENT_USER", this.current_user)
                                .append("CURRENT_TIME", this.current_date)
                                .append("_id", 0)
                        ),new Document().append("$merge", new Document("into", "OA_CLOSE").append("whenMatched", "replace")));

		return pipeline;
	}

	private List<? extends Bson> createOpportunityLog_OA_ING() throws ParseException {

        List<? extends Bson> pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("OL", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "OL.OPPORTUNITY_ID")
                                .append("from", "opportunity_amount")
                                .append("foreignField", "OPPORTUNITY_ID")
                                .append("as", "OA")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$OA")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$match", new Document()
                                .append("OL.TEMP_FLAG", "N")
                                .append("OL.SALES_CYCLE", new Document()
                                        .append("$ne", "4")
                                )
                                .append("OA.BASIS_MONTH", new Document()
                                        .append("$gte", this.startDate)
                                        .append("$lte", this.endDate)
                                )
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("OL\u1390FORECAST_YN", "$OL.FORECAST_YN")
                                        .append("OA\u1390MEMBER_ID_NUM", "$OA.MEMBER_ID_NUM")
                                        .append("CURRENT_USER", "$CURRENT_USER")
                                        .append("CURRENT_TIME", "$CURRENT_TIME")
                                )
                                .append("SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)", new Document()
                                        .append("$sum", "$OA.BASIS_PLAN_REVENUE_AMOUNT")
                                )
                                .append("SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)", new Document()
                                        .append("$sum", "$OA.BASIS_PLAN_GP_AMOUNT")
                                )
                                .append("SUM(OA\u1390ERP_REV)", new Document()
                                        .append("$sum", "$OA.ERP_REV")
                                )
                                .append("SUM(OA\u1390ERP_GP)", new Document()
                                        .append("$sum", "$OA.ERP_GP")
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("MEMBER_ID_NUM", "$_id.OA\u1390MEMBER_ID_NUM")
                                .append("REV_ING_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                new Document()
                                                        .append("$subtract", Arrays.asList(
                                                                "$SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)",
                                                                "$SUM(OA\u1390ERP_REV)"
                                                            )
                                                        ),
                                                0.0
                                            )
                                        )
                                )
                                .append("REV_ING_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                new Document()
                                                        .append("$subtract", Arrays.asList(
                                                                "$SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)",
                                                                "$SUM(OA\u1390ERP_REV)"
                                                            )
                                                        ),
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_ING_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                new Document()
                                                        .append("$subtract", Arrays.asList(
                                                                "$SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)",
                                                                "$SUM(OA\u1390ERP_GP)"
                                                            )
                                                        ),
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_ING_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                new Document()
                                                        .append("$subtract", Arrays.asList(
                                                                "$SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)",
                                                                "$SUM(OA\u1390ERP_GP)"
                                                            )
                                                        ),
                                                0.0
                                            )
                                        )
                                )
                                .append("CURRENT_USER", this.current_user)
                                .append("CURRENT_TIME", this.current_date)
                                .append("_id", 0)
                        ),
				new Document().append("$merge", new Document("into", "OA_ING").append("whenMatched", "replace")));

		return pipeline;
	}

	private List<? extends Bson> createOpportunityLog_OAS_CLOSE() throws ParseException {

        List<? extends Bson> pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("OL", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "OL.OPPORTUNITY_ID")
                                .append("from", "opportunity_amount_slpit")
                                .append("foreignField", "OPPORTUNITY_ID")
                                .append("as", "OAS")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$OAS")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$match", new Document()
                                .append("OL.TEMP_FLAG", "N")
                                .append("OL.SALES_CYCLE", "4")
                                .append("OL.CLOSE_CATEGORY", "1")
                                .append("OAS.SPLIT_DATE", new Document()
                                        .append("$gte", this.year)
                                        .append("$lte", this.endDate)
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("MEMBER_ID_NUM", "$_id.OAS\u1390MEMBER_ID_NUM")
                                .append("REV_CLOSE_SPLIT_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_REV)",
                                                0.0
                                            )
                                        )
                                )
                                .append("REV_CLOSE_SPLIT_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_REV)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_CLOSE_SPLIT_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_GP)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_CLOSE_SPLIT_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_GP)",
                                                0.0
                                            )
                                        )
                                )
                                .append("CURRENT_USER", this.current_user)
                                .append("CURRENT_TIME", this.current_date)
                                .append("_id", 0)
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("OL\u1390FORECAST_YN", "$OL.FORECAST_YN")
                                        .append("OAS\u1390MEMBER_ID_NUM", "$OAS.MEMBER_ID_NUM")
                                        .append("CURRENT_USER", "$CURRENT_USER")
                                        .append("CURRENT_TIME", "$CURRENT_TIME")
                                )
                                .append("SUM(OAS\u1390SPLIT_REV)", new Document()
                                        .append("$sum", "$OAS.SPLIT_REV")
                                )
                                .append("SUM($OAS\u1390SPLIT_GP)", new Document()
                                        .append("$sum", "$OAS.SPLIT_GP")
                                )
                        ),new Document().append("$merge", new Document("into", "OAS_CLOSE").append("whenMatched", "replace")));
		return pipeline;
	}

	private List<? extends Bson> createOpportunityLog_OAS_ING() throws ParseException {

        List<? extends Bson> pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("OL", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "OL.OPPORTUNITY_ID")
                                .append("from", "opportunity_amount_split")
                                .append("foreignField", "OPPORTUNITY_ID")
                                .append("as", "OAS")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$OAS")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$match", new Document()
                                .append("OL.TEMP_FLAG", "N")
                                .append("OL.SALES_CYCLE", new Document()
                                        .append("$ne", "4")
                                )
                                .append("OAS.SPLIT_DATE", new Document()
                                        .append("$gte", this.startDate)
                                        .append("$lte", this.endDate)
                                )
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("OL\u1390FORECAST_YN", "$OL.FORECAST_YN")
                                        .append("OAS\u1390MEMBER_ID_NUM", "$OAS.MEMBER_ID_NUM")
                                        .append("CURRENT_USER", "$CURRENT_USER")
                                        .append("CURRENT_TIME", "$CURRENT_TIME")
                                )
                                .append("SUM(OAS\u1390SPLIT_REV)", new Document()
                                        .append("$sum", "$OAS.SPLIT_REV")
                                )
                                .append("SUM(OAS\u1390SPLIT_GP)", new Document()
                                        .append("$sum", "$OAS.SPLIT_GP")
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("MEMBER_ID_NUM", "$OAS.MEMBER_ID_NUM")
                                .append("REV_ING_SPLIT_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_REV)",
                                                0.0
                                            )
                                        )
                                )
                                .append("REV_ING_SPLIT_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_REV)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_ING_SPLIT_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_GP)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_ING_SPLIT_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_GP)",
                                                0.0
                                            )
                                        )
                                )
                                .append("CURRENT_USER", this.current_user)
                                .append("CURRENT_TIME", this.current_date)
                                .append("_id", 0)
                        ),new Document().append("$merge", new Document("into", "OAS_ING").append("whenMatched", "replace")));

		return pipeline;

	}

	private List<? extends Bson> createOpportunityLog_OA_THIS() throws ParseException {

        List<? extends Bson> pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("OL", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "OL.OPPORTUNITY_ID")
                                .append("from", "opportunity_amount")
                                .append("foreignField", "OPPORTUNITY_ID")
                                .append("as", "OA")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$OA")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$match", new Document()
                                .append("$or", Arrays.asList(
                                        new Document()
                                                .append("OL.CLOSE_CATEGORY", new BsonNull()),
                                        new Document()
                                                .append("OL.CLOSE_CATEGORY", "1")
                                    )
                                )
                                .append("OL.TEMP_FLAG", "N")
                                .append("OA.BASIS_MONTH", new Document()
                                        .append("$gte", this.startDate)
                                        .append("$lte", this.endDate)
                                )
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("OL\u1390FORECAST_YN", "$OL.FORECAST_YN")
                                        .append("OA\u1390MEMBER_ID_NUM", "$OA.MEMBER_ID_NUM")
                                        .append("CURRENT_USER", "$CURRENT_USER")
                                        .append("CURRENT_TIME", "$CURRENT_TIME")
                                )
                                .append("SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)", new Document()
                                        .append("$sum", "$OA.BASIS_PLAN_REVENUE_AMOUNT")
                                )
                                .append("SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)", new Document()
                                        .append("$sum", "$OA.BASIS_PLAN_GP_AMOUNT")
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("MEMBER_ID_NUM", "$_id.OA\u1390MEMBER_ID_NUM")
                                .append("REV_THIS_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("REV_THIS_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_THIS_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_THIS_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("CURRENT_USER", this.current_user)
                                .append("CURRENT_TIME", this.current_date)
                                .append("_id", 0)
                        ),
				new Document().append("$merge", new Document("into", "OA_THIS").append("whenMatched", "replace")));

		return pipeline;
	}

	private List<? extends Bson> createOpportunityLog_OAS_THIS() throws ParseException {

        List<? extends Bson> pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("OL", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "OL.OPPORTUNITY_ID")
                                .append("from", "opportunity_amount_split")
                                .append("foreignField", "OPPORTUNITY_ID")
                                .append("as", "OAS")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$OAS")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$match", new Document()
                                .append("$or", Arrays.asList(
                                        new Document()
                                                .append("OL.CLOSE_CATEGORY", new BsonNull()),
                                        new Document()
                                                .append("OL.CLOSE_CATEGORY", "1")
                                    )
                                )
                                .append("OL.TEMP_FLAG", "N")
                                .append("OAS.SPLIT_DATE", new Document()
                                        .append("$gte", this.startDate)
                                        .append("$lte", this.endDate)
                                )
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("OL\u1390FORECAST_YN", "$OL.FORECAST_YN")
                                        .append("OAS\u1390MEMBER_ID_NUM", "$OAS.MEMBER_ID_NUM")
                                        .append("CURRENT_USER", "$CURRENT_USER")
                                        .append("CURRENT_TIME", "$CURRENT_TIME")
                                )
                                .append("SUM(OAS\u1390SPLIT_REV)", new Document()
                                        .append("$sum", "$OAS.SPLIT_REV")
                                )
                                .append("SUM(OAS\u1390SPLIT_GP)", new Document()
                                        .append("$sum", "$OAS.SPLIT_GP")
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("MEMBER_ID_NUM", "$OAS.MEMBER_ID_NUM")
                                .append("REV_THIS_SPLIT_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_REV)",
                                                0.0
                                            )
                                        )
                                )
                                .append("REV_THIS_SPLIT_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_REV)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_THIS_SPLIT_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_GP)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_THIS_SPLIT_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_GP)",
                                                0.0
                                            )
                                        )
                                )
                                .append("CURRENT_USER", this.current_user)
                                .append("CURRENT_TIME", this.current_date)
                                .append("_id", 0)
                        ),
				new Document().append("$merge", new Document("into", "OAS_THIS").append("whenMatched", "replace")));

		return pipeline;
	}

	private List<? extends Bson> createOpportunityLog_OA_LAST() throws ParseException {

        List<? extends Bson> pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("OL", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "OL.OPPORTUNITY_ID")
                                .append("from", "opportunity_amount")
                                .append("foreignField", "OPPORTUNITY_ID")
                                .append("as", "OA")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$OA")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$match", new Document()
                                .append("$or", Arrays.asList(
                                        new Document()
                                                .append("OL.CLOSE_CATEGORY", new BsonNull()),
                                        new Document()
                                                .append("OL.CLOSE_CATEGORY", "1")
                                    )
                                )
                                .append("OL.TEMP_FLAG", "N")
                                .append("OA.BASIS_MONTH", new Document()
                                        .append("$gte", this.interval_startDate)
                                        .append("$lte", this.interval_endDate)
                                )
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("OL\u1390FORECAST_YN", "$OL.FORECAST_YN")
                                        .append("OA\u1390MEMBER_ID_NUM", "$OA.MEMBER_ID_NUM")
                                        .append("CURRENT_USER", "$CURRENT_USER")
                                        .append("CURRENT_TIME", "$CURRENT_TIME")
                                )
                                .append("SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)", new Document()
                                        .append("$sum", "$OA.BASIS_PLAN_REVENUE_AMOUNT")
                                )
                                .append("SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)", new Document()
                                        .append("$sum", "$OA.BASIS_PLAN_GP_AMOUNT")
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("MEMBER_ID_NUM", "$_id.OA\u1390MEMBER_ID_NUM")
                                .append("REV_LAST_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("REV_LAST_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_REVENUE_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_LAST_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_LAST_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OA\u1390BASIS_PLAN_GP_AMOUNT)",
                                                0.0
                                            )
                                        )
                                )
                                .append("CURRENT_USER", this.current_user)
                                .append("CURRENT_TIME", this.current_date)
                                .append("_id", 0)
                        ),
				new Document().append("$merge", new Document("into", "OA_LAST").append("whenMatched", "replace")));

		return pipeline;

	}

	private List<? extends Bson> createOpportunityLog_OAS_LAST() throws ParseException {

        List<? extends Bson> pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("OL", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "OL.OPPORTUNITY_ID")
                                .append("from", "opportunity_amount_split")
                                .append("foreignField", "OPPORTUNITY_ID")
                                .append("as", "OAS")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$OAS")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$match", new Document()
                                .append("$or", Arrays.asList(
                                        new Document()
                                                .append("OL.CLOSE_CATEGORY", new BsonNull()),
                                        new Document()
                                                .append("OL.CLOSE_CATEGORY", "1")
                                    )
                                )
                                .append("OL.TEMP_FLAG", "N")
                                .append("OAS.SPLIT_DATE", new Document()
                                        .append("$gte", this.interval_startDate)
                                        .append("$lte", this.interval_endDate)
                                )
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("OL\u1390FORECAST_YN", "$OL.FORECAST_YN")
                                        .append("OAS\u1390MEMBER_ID_NUM", "$OAS.MEMBER_ID_NUM")
                                        .append("CURRENT_USER", "$CURRENT_USER")
                                        .append("CURRENT_TIME", "$CURRENT_TIME")
                                )
                                .append("SUM(OAS\u1390SPLIT_REV)", new Document()
                                        .append("$sum", "$OAS.SPLIT_REV")
                                )
                                .append("SUM(OAS\u1390SPLIT_GP)", new Document()
                                        .append("$sum", "$OAS.SPLIT_GP")
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("MEMBER_ID_NUM", "$OAS.MEMBER_ID_NUM")
                                .append("REV_LAST_SPLIT_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_REV)",
                                                0.0
                                            )
                                        )
                                )
                                .append("REV_LAST_SPLIT_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_REV)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_LAST_SPLIT_FC_IN", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "In"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_GP)",
                                                0.0
                                            )
                                        )
                                )
                                .append("GP_LAST_SPLIT_FC_OUT", new Document()
                                        .append("$cond", Arrays.asList(
                                                new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$_id.OL\u1390FORECAST_YN",
                                                                "Out"
                                                            )
                                                        ),
                                                "$SUM(OAS\u1390SPLIT_GP)",
                                                0.0
                                            )
                                        )
                                )
                                .append("CURRENT_USER", this.current_user)
                                .append("CURRENT_TIME", this.current_date)
                                .append("_id", 0)
                        ),
						new Document().append("$merge",
								new Document("into", "OAS_LAST").append("whenMatched", "replace")));

		return pipeline;
	}

	public void removeTemporary() {
		Document doc = new Document().append("CURRENT_USER", this.current_user).append("CURRENT_TIME", this.current_date);
//		System.out.println(doc.toJson());
		mongoTemplate.getCollection("OMI").deleteMany(doc).getDeletedCount();
		mongoTemplate.getCollection("ESP").deleteMany(doc).getDeletedCount();
		mongoTemplate.getCollection("OA_CLOSE").deleteMany(doc).getDeletedCount();
		mongoTemplate.getCollection("OA_ING").deleteMany(doc).getDeletedCount();
		mongoTemplate.getCollection("OAS_CLOSE").deleteMany(doc).getDeletedCount();
		mongoTemplate.getCollection("OAS_ING").deleteMany(doc).getDeletedCount();
		mongoTemplate.getCollection("OA_THIS").deleteMany(doc).getDeletedCount();
		mongoTemplate.getCollection("OAS_THIS").deleteMany(doc).getDeletedCount();
		mongoTemplate.getCollection("OA_LAST").deleteMany(doc).getDeletedCount();
		mongoTemplate.getCollection("OAS_LAST").deleteMany(doc).getDeletedCount();
		
	}
}
