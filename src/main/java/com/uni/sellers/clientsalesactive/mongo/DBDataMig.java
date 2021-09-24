package com.uni.sellers.clientsalesactive.mongo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.uni.sellers.util.CommonUtils;

public class DBDataMig {

	public static Connection getConnection() {

//		String url = "jdbc:mysql://127.0.0.1:3306/sellers_test";
		String url = "jdbc:mysql://mysql-service:3306/sellers_test";
		String user = "sellers";
		String password = "Sellers123!@#";

		Connection conn = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, user, password);
			// processing here
		} catch (Exception e) {
			e.printStackTrace();
		}

		return conn;
	}

	public static List<Map<String, Object>> getData(String sql) {

		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
		Map<String, Object> row = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;

		try {

			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			rsmd = rs.getMetaData();

			int colCnt = rsmd.getColumnCount();

			while (rs.next()) {

				row = new HashMap<String, Object>();
				for (int i = 1; i <= colCnt; i++) {
					String colName = rsmd.getColumnName(i).toUpperCase();
					Object value = validation(rs.getObject(colName));

					if (value == null) {
						row.put(colName, value);
						continue;
					}

					int type = rsmd.getColumnType(i);
					switch (type) {
						
					case -5:
						row.put(colName, Long.parseLong("" + value));
						continue;
					case 3:
						row.put(colName, Double.parseDouble("" + value));
						continue;
					case 4:
					case 5:
						row.put(colName, Integer.parseInt("" + value));
						continue;
					case 12: //varchar
					case 1:  //char
					case -1:
						row.put(colName, "" + value);
						continue;
					case 91: // Date : 91, Time : 92, Timestamp : 93 == 다 데이트
					case 92:
					case 93:
						row.put(colName, value);
						continue;
					default:
						row.put(colName, value);
						System.out.println("==========" + colName + " # " + type + " # " + sql.split("from")[1]);
					}

				}
				results.add(row);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}

	public static Object validation(Object value) {

		if (CommonUtils.isEmpty(value)) {
			return null;
		} else
			return value;

	}
}
