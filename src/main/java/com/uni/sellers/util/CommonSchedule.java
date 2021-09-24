package com.uni.sellers.util;

import org.springframework.stereotype.Component;

@Component
public class CommonSchedule {
	/*
	@Resource(name="commonService")
    private CommonService commonService;
	
    @Resource(name="commonDAO")
	private CommonDAO commonDAO;
    
    @Resource(name="trackingService")
    private TrackingService trackingService;
    
// @Resource(name="duzonService")
//	private DuzonService duzonService;
    

	초 0-59 , - * / 
	분 0-59 , - * / 
	시 0-23 , - * / 
	일 1-31 , - * ? / L W
	월 1-12 or JAN-DEC , - * / 
	요일 1-7 or SUN-SAT , - * ? / L # 
	년(옵션) 1970-2099 , - * /
	* : 모든 값
	? : 특정 값 없음
	- : 범위 지정에 사용
	, : 여러 값 지정 구분에 사용
	/ : 초기값과 증가치 설정에 사용
	L : 지정할 수 있는 범위의 마지막 값
	W : 월~금요일 또는 가장 가까운 월/금요일
	# : 몇 번째 무슨 요일 2#1 => 첫 번째 월요일
	 
	예제) Expression Meaning 
	초 분 시 일 월 주(년)
	 "0 0 12 * * ?" : 아무 요일, 매월, 매일 12:00:00
	 "0 15 10 ? * *" : 모든 요일, 매월, 아무 날이나 10:15:00 
	 "0 15 10 * * ?" : 아무 요일, 매월, 매일 10:15:00 
	 "0 15 10 * * ? *" : 모든 연도, 아무 요일, 매월, 매일 10:15 
	 "0 15 10 * * ? : 2005" 2005년 아무 요일이나 매월, 매일 10:15 
	 "0 * 14 * * ?" : 아무 요일, 매월, 매일, 14시 매분 0초 
	 "0 0/5 14 * * ?" : 아무 요일, 매월, 매일, 14시 매 5분마다 0초 
	 "0 0/5 14,18 * * ?" : 아무 요일, 매월, 매일, 14시, 18시 매 5분마다 0초 
	 "0 0-5 14 * * ?" : 아무 요일, 매월, 매일, 14:00 부터 매 14:05까지 매 분 0초 
	 "0 10,44 14 ? 3 WED" : 3월의 매 주 수요일, 아무 날짜나 14:10:00, 14:44:00 
	 "0 15 10 ? * MON-FRI" : 월~금, 매월, 아무 날이나 10:15:00 
	 "0 15 10 15 * ?" : 아무 요일, 매월 15일 10:15:00 
	 "0 15 10 L * ?" : 아무 요일, 매월 마지막 날 10:15:00 
	 "0 15 10 ? * 6L" : 매월 마지막 금요일 아무 날이나 10:15:00 
	 "0 15 10 ? * 6L 2002-2005" : 2002년부터 2005년까지 매월 마지막 금요일 아무 날이나 10:15:00 
	 "0 15 10 ? * 6#3" : 매월 3번째 금요일 아무 날이나 10:15:00

	
	//ex> @Scheduled(cron="0 0 02 * * ?") = 매일 새벽2시에 실행
	//ex> @Scheduled(cron="0 0 02 2,20 * ?") = 매월 2일,20일 새벽2시에 실행
    
    *//**
     * @author 	: 욱이
     * @Date		: 2016. 7. 14.
     * @explain	: 영업관리 -> 영업기회 히스토리 프로시저 호출 weekly 
     * @order		: 1-1
     * @time   	: 매일 23시 58분
     *//*
    //@Scheduled(cron="0 58 23 * * ?")
    public void callOpportunity() throws Exception{
    	commonService.callOpportunity();
    }
    
    *//**
	 * @author 	: 욱이
	 * @Date		: 2016. 7. 14.
	 * @explain	: 영업관리 -> 생산성 분석 프로시저 호출 daily
	 * @order		: 1-2
     * @time   	: 매일 23시 59분
	 *//*
    //@Scheduled(cron="0 59 23 * * ?")
	public void callFaceTime() throws Exception{
		commonService.callFaceTime(CommonUtils.currentDate("yyyyMMdd"));
	}
    
    *//**
	 * @author 	: 욱이
	 * @Date		: 2016. 7. 14.
	 * @explain	: 생산성 -> 반복일정 계산
	 * @order		: 1-3
     * @time   	: 매일 0시 0분
	 *//*
    //@Scheduled(cron="0 0 00 * * ?")
	public void callRRuleEventProductivity() throws Exception{
		commonService.callRRuleEventProductivity(CommonUtils.currentDate("yyyyMMdd"));
	}
    
    
    *//**
     * @author 	: 욱이
     * @Date		: 2018. 04. 13.
     * @eplain	: 더존 주간보고서 연동
     * @order		: 2-1
     * @time   	: 매일 06시 00분
     *//*
    @Scheduled(cron="0 0 06 * * ?")
    public void selectDashBoardCheckList() throws Exception{
    	duzonService.selectDashBoardCheckList(null);
    }
    
    *//**
     * @author 	: 욱이
     * @Date		: 2018. 04. 13.
     * @explain	: 더존 개인별달성현황 연동
     * @order		: 2-2
     * @time   	: 매일 06시 10분
     *//*
    @Scheduled(cron="0 10 06 * * ?")
    public void selectErpSalesActual() throws Exception{
    	duzonService.selectErpSalesActual(null);
    }
    
    *//**
     * @author 	: 욱이
     * @Date		: 2016. 7. 14.
     * @explain	: 영업관리 -> 영업기회 salescycle update
     * @order		: 2-4
     * @time   	: 매일 06시 30분
     *//*
    @Scheduled(cron="0 30 06 * * ?")
    public void callUpdateOpportunitySalesCycle() throws Exception{
    	commonDAO.callUpdateOpportunitySalesCycle();
    }
    
    *//**
     * @author 	: 욱이
     * @Date		: 2016. 7. 14.
     * @explain	: 영업관리 -> 영업기회 ERP 프로젝트 코드 연동
     * @order		: 2-3
     * @time   	: 매일 06시 20분
     *//*
    @Scheduled(cron="0 20 06 * * ?")
    public void callErpDashboardCheckList() throws Exception{
    	commonDAO.callErpDashboardCheckList();
    }
    
    *//**
     * @author 	: 욱이
     * @Date		: 2018. 03. 14.
     * @explain	: 더존 사원명부, 부서, 팀 연동 
     * @order		:  3-1
     * @time   	: 매일 새벽 1시
     *//*
    @Scheduled(cron="0 0 01 * * ?")
    public void selectOurDivisionInfo() throws Exception{
    	duzonService.selectOurDivisionInfo(null);
    	List<Map<String, Object>> list = duzonService.selectOurDivisionInfo(null);
		duzonService.selectOurDivisionInfo2(null, list);
		list.clear();
    	list = duzonService.selectOurTeamInfo(null);
		duzonService.selectOurTeamInfo2(null, list);
		list.clear();
		duzonService.selectOurMemberInfo(null);
		duzonService.selectOurMemberInfo2(null);
    }
    
    *//**
     * @author 	: 욱이
     * @Date		: 2018. 04. 19.
     * @explain	: 영업기회 목록 연동
     * @order		:  3-2
     * @time   	: 매일 새벽 1시 05분
     *//*
    @Scheduled(cron="0 05 01 * * 2")
    public void selectErpSfaSoptyH() throws Exception{
    	duzonService.selectErpSfaSoptyH(null);
    }
    
    *//**
     * @author 	: 욱이
     * @Date		: 2018. 04. 03.
     * @explain	: 더존 사전손익 연동
     * @order		: 3-3
     * @time   	: 매일 새벽 1시 10분 
     *//*
    @Scheduled(cron="0 10 01 * * ?")
    public void selectSfaPfls() throws Exception{
    	duzonService.selectSfaPfls(null);
    }
    
    *//**
     * @author 	: 욱이
     * @Date		: 2018. 04. 13.
     * @explain	: 사전손익 -> 영업기회 업데이트
     * @order		: 3-4
     * @time   	: 매일 새벽 1시 15분 
     *//*
    @Scheduled(cron="0 15 01 * * ?")
    public void updateOppToInst() throws Exception{
    	duzonService.updateOppToInst(null);
    }
    
    
    *//**
     * @author 	: 봉준
     * @Date		: 2018. 03. 15.
     * @explain	: 더존 신용평가현황 연동
     * @order		: 
     * @time   	: 매월1일 새벽 5시
     *//*
    @Scheduled(cron="0 0 05 1 * ?")
    public void selectCreditRatingStatusInfo() throws Exception{
    	duzonService.selectCreditRatingStatusInfo(null);
    }
    
    *//**
     * @author 	: 봉준
     * @Date		: 2018. 03. 16.
     * @explain	: 더존 매출목표 연동
     * @order		: 
     * @time   	: 매월1일 새벽 5시 05분
     *//*
    @Scheduled(cron="0 05 05 1 * ?")
    public void selectSalesGoalInfo() throws Exception{
    	duzonService.selectSalesGoalInfo(null);
    }
    
    *//**
     * @author 	: 봉준
     * @Date		: 2018. 03. 21.
     * @explain	: 더존 고객사 테이블 연동
     * @order		: 
     * @time   	: 매일 새벽 5시 10분 
     *//*
    @Scheduled(cron="0 10 05 * * ?")
    public void selectClientCompanyInfo() throws Exception{
    	duzonService.selectClientCompanyInfo(null);
    	duzonService.updateCloseClientIndividualinfo(null);
    }
    
    *//**
     * @author 	: 봉준
     * @Date		: 2018. 03. 22.
     * @explain	: 더존 거래처대표 테이블 연동
     * @order		: 
     * @time   	: 매일 새벽 5시 15분
     *//*
    @Scheduled(cron="0 15 05 * * ?")
    public void selectClientSalesmanInfo() throws Exception{
    	duzonService.selectClientSalesmanInfo(null);
    }
    
    *//**
     * @author 	: 봉준
     * @Date		: 2018. 03. 22.
     * @explain	: 더존 품목정보 테이블 연동
     * @order		: 
     * @time   	: 매일 새벽 5시 20분
     *//*
    @Scheduled(cron="0 20 05 * * ?")
    public void selectOurProductInfo() throws Exception{
    	duzonService.selectOurProductInfo(null);
    }
    
    *//**
     * @author 	: 봉준
     * @Date		: 2018. 03. 22.
     * @explain	: 더존 공통코드 테이블 연동(프로젝트형태, 영업구분)
     * @order		: 
     * @time   	: 매주 월요일 새벽 5시 25분
     *//*
    @Scheduled(cron="0 25 05 * * 2")
    public void selectComCode() throws Exception{
    	duzonService.selectComCode(null);
    }
    
    *//**
     * @author 	: 장성훈남
     * @Date		: 2017. 4. 05.
     * @explain	: Tracking 서비스 실행
     * @order		: 
     * @time   	: 매일 08시 30분
     *//*
    //@Scheduled(cron="0 30 08 ? * ?")
    public void trackingProcess() throws Exception{
    	trackingService.checkTrackingList();
    }
    
    *//**
     * @author 	: 욱이
     * @Date		: 2017. 07. 04.
     * @explain	: MS Azure가 쓰레기라서 20분마다 DB 연결 끊기지 않도록 SELECT 1 조회
     * @order		: 
     * @time   	: 매일 20분마다
     *//*
   // @Scheduled(cron="0 0/20 * * * ?")
    public void select1() throws Exception{
    	commonDAO.select1();
    }
    
*/    
}

