package constants;

public class AllConstants {
	public static final String SEPARATOR = ",";
	
	public static float RATIO = (float)1/(float)1;
	
	//******************************** MyPage dataset configuration *****************************************************//
	public static final int MYPAGE_ID_FROM = 1;
	public static 		int MYPAGE_ID_TO = (int)((float)100000*RATIO);
			
	public static final int MYPAGE_NAME_LEN_FROM = 10;
	public static final int MYPAGE_NAME_LEN_TO = 20;
	
	public static final int MYPAGE_NATIONALITY_LEN_FROM = 10;
	public static final int MYPAGE_NATIONALITY_LEN_TO = 20;
	
	public static final int MYPAGE_COUNTRYCODE_FROM = 1;
	public static final int MYPAGE_COUNTRYCODE_TO = 10;
	
	public static final int MYPAGE_HOBBY_LEN_FROM = 10;
	public static final int MYPAGE_HOBBY_LEN_TO = 20;
	//*******************************************************************************************************************//
	//******************************** Friends dataset configuration ****************************************************//
	public static final int FRIENDS_REL_FROM = 1;
	public static 		int FRIENDS_REL_TO = (int)((float)20000000 * RATIO);
	
	public static final int FRIENDS_DATE_FROM = 1;
	public static 		int FRIENDS_DATE_TO = (int)((float)1000000*RATIO);
	
	public static final String[] FRIENDS_DESC = {
			"This person is College Friend",
			"This person is Family Friend",
			"This person is Childhood Friend",
			"This is Unknown relationship",
			"This person is colleague"
	};
	//*******************************************************************************************************************//
	
	//******************************** AccessLog dataset configuration **************************************************//
	public static final int ACCESSLOG_ID_FROM 			= 1;
	public static 		int ACCESSLOG_ID_TO 			= (int)((float)10000000 * RATIO);
	public static final int ACCESSLOG_ACCESSTIME_FROM 	= 1;
	public static 		int ACCESSLOG_ACCESSTIME_TO 	= (int)((float)1000000*RATIO);
	
	public static final String[] ACCESSLOG_DESC = {
			"Just viewed the your Profile",
			"Added as New Friend to your profile", 
			"Left you a message in your message box",
			"Suggested you some Facebook Friends",
			"Referred you some groups of your interest"
			
	};
	//*******************************************************************************************************************//
	public static void recalculate()
	{
		MYPAGE_ID_TO = (int)((float)100000*RATIO);
		FRIENDS_REL_TO = (int)((float)20000000 * RATIO);
		FRIENDS_DATE_TO = (int)((float)1000000*RATIO);
		ACCESSLOG_ID_TO 			= (int)((float)10000000 * RATIO);
		ACCESSLOG_ACCESSTIME_TO 	= (int)((float)1000000*RATIO);
	}
}
