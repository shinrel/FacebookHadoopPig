package project1.obj;

import constants.AllConstants;

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
