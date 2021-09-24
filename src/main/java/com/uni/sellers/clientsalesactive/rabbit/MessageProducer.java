package com.uni.sellers.clientsalesactive.rabbit;

import java.io.IOException;
import java.util.Map;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageBuilderSupport;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.uni.sellers.util.CommonUtils;

@Component
public class MessageProducer implements CommandLineRunner {

	private static final String QueueExchange = "sellers-exchanges";
	private static final String QueueBind = "sellers-queue";
	private final RabbitTemplate rabbitTemplate;

	public MessageProducer(RabbitTemplate rabbitTemplate) {
		this.rabbitTemplate = rabbitTemplate;
	}

	@Override
	public void run(String... args) {
		System.out.println("Sending message..." + args);

		if (args.length < 2) {
			return;
		}
		String commanmap = args[0];
		String function = args[1];

		//String data = commanmap + "   " + function;
		String data = function + "   " + commanmap; 
		
		rabbitTemplate.convertAndSend(QueueExchange, QueueBind, data);
	}
	
	
	public void run(Object... args) {
		System.out.println("Sending message..." + args);

		if (args.length < 2) {
			return;
		}
		Map<String, Object> commanmap = (Map<String, Object>)args[1];
		String function = ""+args[0];

		System.out.println(commanmap.toString());
		commanmap.put("function", function);
		
		try {
			rabbitTemplate.convertAndSend(QueueExchange, QueueBind, CommonUtils.mapTojson(commanmap));
		} catch (AmqpException | IOException e) {
			e.printStackTrace();
		}
	}

}