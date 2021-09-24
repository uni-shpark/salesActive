package com.uni.sellers.datasource;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import com.uni.sellers.util.CommonUtils;

public class AbstractDAO {
	protected Log log = LogFactory.getLog(AbstractDAO.class);
	
	@Autowired
	@Resource(name="sqlSessionTemplate")
	private SqlSessionTemplate sqlSession;
	
	/*@Autowired
	@Resource(name="sqlSessionTemplateBatch")
	private SqlSessionTemplate sqlSessionBatch;
	
	@Autowired
	@Resource(name="sqlSessionTemplate2")
	private SqlSessionTemplate sqlSession2;*/
	
	protected void printQueryId(String queryId) {
		if(log.isDebugEnabled()){
			log.debug("\t QueryId  \t:  " + queryId);
		}
	}
	
	public Object insert(String queryId, Object params){
		printQueryId(queryId);
		// Elastic CreateData 메서드 호출211111
		return sqlSession.insert(queryId, params);
	}
	
	public Object update(String queryId, Object params){
		printQueryId(queryId);
		return sqlSession.update(queryId, params);
	}
	
	/*public Object insertBatch(String queryId, Object params){
		printQueryId(queryId);
		return sqlSessionBatch.insert(queryId, params);
	}
	
	public Object updateBatch(String queryId, Object params){
		printQueryId(queryId);
		return sqlSessionBatch.update(queryId, params);
	}*/
	
	public Object delete(String queryId, Object params){
		printQueryId(queryId);
		return sqlSession.delete(queryId, params);
	}
	
	public Object selectOne(String queryId){
		printQueryId(queryId);
		return sqlSession.selectOne(queryId);
	}
	
	public Object selectOne(String queryId, Object params){
		printQueryId(queryId);
		return sqlSession.selectOne(queryId, params);
	}
	
	@SuppressWarnings("rawtypes")
	public List selectList(String queryId){
		printQueryId(queryId);
		return sqlSession.selectList(queryId);
	}
	
	@SuppressWarnings("rawtypes")
	public List selectList(String queryId, Object params){
		printQueryId(queryId);
		return sqlSession.selectList(queryId,params);
	}
	
	@SuppressWarnings("unchecked")
	public Object selectPagingList(String queryId, Object params){
	    printQueryId(queryId);
	    Map<String,Object> map = (Map<String,Object>)params;
	     
	    String strPageIndex = (String)map.get("PAGE_INDEX");
	    String strPageRow = (String)map.get("PAGE_ROW");
	   
	    int nPageIndex = 0;
	    int nPageRow = 5;
	     
	    if(CommonUtils.isEmpty(strPageIndex) == false){
	        nPageIndex = Integer.parseInt(strPageIndex)-1;
	    }
	    if(CommonUtils.isEmpty(strPageRow) == false){
	        nPageRow = Integer.parseInt(strPageRow);
	    }
	    map.put("START", (nPageIndex * nPageRow) + 1);
	    map.put("END", (nPageIndex * nPageRow) + nPageRow);
	     
	    return sqlSession.selectList(queryId, map);
	}
	
	
	/*public Object selectOne_mssql(String queryId, Object params){
		printQueryId(queryId);
		return sqlSession2.selectOne(queryId, params);
	}
	
	@SuppressWarnings("rawtypes")
	public List selectList_mssql(String queryId, Object params){
		printQueryId(queryId);
		return sqlSession2.selectList(queryId,params);
	}*/
	
}
