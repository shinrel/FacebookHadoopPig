package project1.obj;

import constants.AllConstants;
import utils.Generator;

public class AccessLog {
	private int accessId;
	private int byWho;
	private int whatPage;
	private String typeOfAccess;
	private int accessTime;
	
	public AccessLog() {
		
	}
	
	public AccessLog(int accessId, int byWho, int whatPage, String typeOfAccess, int accessTime) {
		super();
		this.accessId = accessId;
		this.byWho = byWho;
		this.whatPage = whatPage;
		this.typeOfAccess = typeOfAccess;
		this.accessTime = accessTime;
	}
	
	public static AccessLog[] generateAccessLog()
	{
		AccessLog[] accessLogs = new AccessLog[AllConstants.ACCESSLOG_ID_TO - AllConstants.ACCESSLOG_ID_FROM + 1];
		for (int i = 0; i <= (AllConstants.ACCESSLOG_ID_TO - AllConstants.ACCESSLOG_ID_FROM); i++) {
			int accessId = Generator.genAccessId();
			int byWho = Generator.genRanInt(AllConstants.MYPAGE_ID_FROM, AllConstants.MYPAGE_ID_TO);
			int whatPage = Generator.genRanInt(AllConstants.MYPAGE_ID_FROM, AllConstants.MYPAGE_ID_TO);
			String typeOfAccess = Generator.genRandomTypeOfAccessLog();
			int accessTime = Generator.genRanInt(AllConstants.ACCESSLOG_ACCESSTIME_FROM, AllConstants.ACCESSLOG_ACCESSTIME_TO);
			accessLogs[i] = new AccessLog(accessId, byWho, whatPage, typeOfAccess, accessTime);
			if (i% 1000000 == 0) {
				System.out.println(i + " AccessLogs generated");
			}
		}
		return accessLogs;
		
	}
	
	@Override
	public String toString()
	{
		return this.accessId + AllConstants.SEPARATOR +
				this.byWho + AllConstants.SEPARATOR +
				this.whatPage + AllConstants.SEPARATOR +
				this.typeOfAccess + AllConstants.SEPARATOR +
				this.accessTime;
	}
	public int getAccessId() {
		return accessId;
	}
	public void setAccessId(int accessId) {
		this.accessId = accessId;
	}
	public int getByWho() {
		return byWho;
	}
	public void setByWho(int byWho) {
		this.byWho = byWho;
	}
	public int getWhatPage() {
		return whatPage;
	}
	public void setWhatPage(int whatPage) {
		this.whatPage = whatPage;
	}
	public String getTypeOfAccess() {
		return typeOfAccess;
	}
	public void setTypeOfAccess(String typeOfAccess) {
		this.typeOfAccess = typeOfAccess;
	}
	public int getAccessTime() {
		return accessTime;
	}
	public void setAccessTime(int accessTime) {
		this.accessTime = accessTime;
	}
	
	
	
}
