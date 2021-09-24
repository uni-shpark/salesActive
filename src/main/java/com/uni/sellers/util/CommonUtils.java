package com.uni.sellers.util;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CommonUtils {

	// 랜덤 문자열(숫자포함) 생성
	public static String getRandomString() {
		return UUID.randomUUID().toString().replaceAll("-", "");
	}

	public static Object emptyToNull(Object value) {

		if (CommonUtils.isEmpty(value)) {
			return null;
		} else {
			return ""+value;
		}

	}
	
	public static Object emptyToZero(Object value, int type) {

		if (CommonUtils.isEmpty(value)) {
			return 0;
		} else {
			switch (type) {
			
			case 1 : // int
				return Integer.parseInt(""+value);
			case 2 : // long
				return Long.parseLong(""+value);
			case 3 : // double
				return Double.parseDouble(""+value);
			}
			return value;
		}

	}
	
	/**
	 * @author : 욱이
	 * @Date : 2016. 09. 29.
	 * @explain : pattern 형식 리턴 ex) yyyyMMdd, yyyy-MM-dd, yyyy-MM-dd HH:mm:ss.SSS
	 */
	public static String currentDate(String pattern) {
		Date date = new Date();
		SimpleDateFormat thismonth;
		thismonth = new SimpleDateFormat(pattern);
		return thismonth.format(date);
	}

	/**
	 * @author : 욱이
	 * @Date : 2016. 7. 26.
	 * @explain : Object null or "" 체크
	 */
	public static boolean isEmpty(Object s) {
		if (s == null) {
			return true;
		}
		if ((s instanceof String) && (((String) s).trim().length() == 0)) {
			return true;
		}
		if (s instanceof Map) {
			return ((Map<?, ?>) s).isEmpty();
		}
		if (s instanceof List) {
			return ((List<?>) s).isEmpty();
		}
		if (s instanceof Object[]) {
			return (((Object[]) s).length == 0);
		}
		return false;
	}

	/**
	 * @author : 욱이
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 * @Date : 2016. 09. 25.
	 * @explain : jsonList
	 */
	public static ArrayList<HashMap<String, Object>> jsonList(String data)
			throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		ArrayList<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		String jsonData = data;
		list = mapper.readValue(jsonData, new TypeReference<ArrayList<HashMap<String, Object>>>() {
		});
		return list;
	}

	@SuppressWarnings("unchecked")
	public static JSONObject getJsonStringFromMap( Map<String, Object> map )
    {
        JSONObject jsonObject = new JSONObject();
        for( Map.Entry<String, Object> entry : map.entrySet() ) {
            String key = entry.getKey();
            Object value = entry.getValue();
            jsonObject.put(key, value);
        }
        
        return jsonObject;
    }
	
	@SuppressWarnings("unchecked")
	public static JSONArray getJsonArrayFromList(List<Map<String, Object>> list) {
		JSONArray jsonArray = new JSONArray();
		for (Map<String, Object> map : list) {
			jsonArray.add(getJsonStringFromMap(map));
		}

		return jsonArray;
	}

	public static Object mapTojson(Map<String, Object> data)
			throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		JSONParser parser = new JSONParser();
		try {
			return parser.parse(objectMapper.writeValueAsString(data));
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return null;
	}

	public static JSONObject stringTojson(String data) throws JsonParseException, JsonMappingException, IOException {
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(data);
			
			return obj == null ? null:(JSONObject)obj ;
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return null;
	}

	public static JSONArray stringTojsonArray(String data) throws JsonParseException, JsonMappingException, IOException {
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(data);
			
			return obj == null ? null:(JSONArray)obj ;
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return null;
	}
	/**
	 * @author : 욱이
	 * @date : 2017. 3. 2.
	 * @explain : 리스트 페이징 계산 처리 paging 정보와 list count를 받아서 계산
	 */
	public static Map<String, Object> pagingSum(Map<String, Object> map, int listCount) {
		int currentPage = Integer.parseInt((String) map.get("currentPage")); // 현재 페이지
		int pagingSize = Integer.parseInt((String) map.get("pagingSize")); // 페이징 보여줄 갯수
		int rowCount = Integer.parseInt((String) map.get("rowCount")); // 리스트 가져올 갯수

		int startPage = ((currentPage - 1) / pagingSize * pagingSize) + 1; // 시작블럭숫자 (1~5페이지일경우 1, 6~10일경우 6)
		int endPage = ((currentPage - 1) / pagingSize * pagingSize) + pagingSize; // 끝 블럭 숫자 (1~5일 경우 5, 6~10일경우 10)
		int toEndPage = listCount == 0 ? 1 : ((listCount - 1) / rowCount) + 1;

		if (endPage > toEndPage) {
			endPage = toEndPage;
		}
		if (startPage < 1) {
			startPage = 1;
		}

		map.put("pagingSize", pagingSize); // 페이징 보여줄 갯수만큼 foreach 돌리고
		map.put("currentPage", currentPage); // 현재 선택한 페이지가 뭔지 active 해주고
		map.put("pageStart", (currentPage * rowCount) - rowCount); // 쿼리에서 사용할 변수
		map.put("toEndPage", toEndPage); // 계속 < > 버튼 누르지 못하게
		map.put("endPage", endPage);
		map.put("startPage", startPage);
		map.put("numberPagingUseYn", "Y");
		return map;
	}

	/**
	 * @author : 욱이
	 * @date : 2017. 4. 27.
	 * @explain : ip 정보 가져오기 (WebServer, WAS, L4, proxy)
	 */
	public static String getIp(HttpServletRequest request) throws Exception {
		String ip = request.getHeader("X-Forwarded-For");
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("WL-Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("HTTP_CLIENT_IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("HTTP_X_FORWARDED_FOR");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getRemoteAddr();
		}
		return ip;
	}

	/**
	 * @author : 욱이
	 * @date : 2017. 4. 27.
	 * @explain : 브라우저 정보 가져오기
	 */
	public static String getBrowser() {
		ServletRequestAttributes sra = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
		HttpServletRequest request = sra.getRequest();
		// 에이전트
		String agent = request.getHeader("User-Agent");
		// 브라우져 구분
		String browser = null;
		if (agent != null) {
			if (agent.indexOf("Trident") > -1 || agent.indexOf("MSIE") > -1) {
				browser = "MSIE";
			} else if (agent.indexOf("Safari") > -1) {
				if (agent.indexOf("Chrome") > -1) {
					browser = "Chrome";
				} else {
					browser = "Safari";
				}
			} else if (agent.indexOf("Opera") > -1) {
				browser = "Opera";
			} else if (agent.indexOf("iPhone") > -1 && agent.indexOf("Mobile") > -1) {
				browser = "iPhone";
			} else if (agent.indexOf("Android") > -1 && agent.indexOf("Mobile") > -1) {
				browser = "Android";
			} else {
				browser = "Etc";
			}
		}
		return browser;
	}

}