package com.uni.sellers.datasource;
 
import java.util.Enumeration;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.core.MethodParameter;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import com.uni.sellers.util.CommonUtils;
 
public class CustomMapArgumentResolver implements HandlerMethodArgumentResolver{
	
    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return CommandMap.class.isAssignableFrom(parameter.getParameterType());
    }
 
    @Override
    public Object resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer, NativeWebRequest webRequest, WebDataBinderFactory binderFactory) throws Exception {
        CommandMap commandMap = new CommandMap();
         
        HttpServletRequest request = (HttpServletRequest) webRequest.getNativeRequest();
        Enumeration<?> enumeration = request.getParameterNames();
        HttpSession session = ((HttpServletRequest)request).getSession();
        
        String key = null;
        String[] values = null;
        while(enumeration.hasMoreElements()){
            key = (String) enumeration.nextElement();
            values = request.getParameterValues(key);
            
			if ("jsp".equals(key)) {
				String value = "";
				for (String arg : values) {
					value += arg; 
				}
				logging_jennifer(key, value);
			} 
			
            if(values != null){
                commandMap.put(key, (values.length > 1) ? values:values[0] );
            }
        }
        if(!CommonUtils.isEmpty(session.getAttribute("auth"))){
        	commandMap.put("global_role_code",session.getAttribute("auth").toString());
        }
        if(!CommonUtils.isEmpty(session.getAttribute("userInfo"))){
        	Map<String, Object> userInfo = (Map<String, Object>) session.getAttribute("userInfo");
        	commandMap.put("global_member_division",userInfo.get("MEMBER_DIVISION"));
        	commandMap.put("global_member_team",userInfo.get("MEMBER_TEAM"));
        	commandMap.put("global_member_id",userInfo.get("MEMBER_ID_NUM"));
        	commandMap.put("global_company_cd",userInfo.get("COMPANY_CD"));
        	if(userInfo.get("DEVICE_KEY") != null){
        		commandMap.put("global_device_token",userInfo.get("DEVICE_KEY"));
        	}
        }
        return commandMap;
    }
    
    public void logging_jennifer(String key, String values) {
    	
    	System.out.println("#####################" + key + " = " + values.toString());
    }
}