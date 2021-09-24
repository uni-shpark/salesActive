package com.uni.sellers.clientsalesactive.rabbit;

import java.lang.reflect.Method;

import javax.annotation.Resource;

import org.json.simple.JSONObject;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.uni.sellers.clientsalesactive.mongo.MongoDBService;
import com.uni.sellers.util.CommonUtils;

@Service
public class MessageListener {

	@Resource
	MongoDBService service;

	@SuppressWarnings("unchecked")
	@RabbitListener(queues = "sellers")
	public void receiveMessage(Message message) {

		JSONObject commandMap = null;
		String command = new String(message.getBody());
		
		try {
			
			commandMap = CommonUtils.stringTojson(command);
			String function = "" + commandMap.get("function");
			
			System.out.println("command : " + commandMap.toString());
			System.out.println("function : " + function);

			Method method = service.getClass().getMethod(function, JSONObject.class);
		
			//JSONObject commandMap = (JSONObject)CommonUtils.stringTojson(data);
			boolean result = (boolean) method.invoke(service, commandMap);
			
			System.out.println("result : " + result);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}