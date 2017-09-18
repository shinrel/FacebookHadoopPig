package project1.obj;

import constants.AllConstants;
import utils.Generator;

public class MyPage {
	private int ID;
	private String name;
	private String nationality;
	private int countryCode;
	private String hobby;
	
	
	public MyPage()
	{
		
	}
	
	
	public MyPage(int iD, String name, String nationality, int countryCode, String hobby) {
		super();
		ID = iD;
		this.name = name;
		this.nationality = nationality;
		this.countryCode = countryCode;
		this.hobby = hobby;
	}


	public static MyPage newRanInst()
	{
		Generator.generateNationality();
		int ID = Generator.genMyPageId();
		String name = Generator.genRandomString(AllConstants.MYPAGE_NAME_LEN_FROM, 
												AllConstants.MYPAGE_NAME_LEN_TO);
//		String nationality = Generator.genRandomString(AllConstants.MYPAGE_NATIONALITY_LEN_FROM, 
//														AllConstants.MYPAGE_NATIONALITY_LEN_TO);
		int countryCode = Generator.genRanInt(AllConstants.MYPAGE_COUNTRYCODE_FROM,
											  AllConstants.MYPAGE_COUNTRYCODE_TO);
		String nationality = Generator.getNationality(countryCode);
		String hobby = Generator.genRandomString(AllConstants.MYPAGE_HOBBY_LEN_FROM, 
												 AllConstants.MYPAGE_HOBBY_LEN_TO);
		MyPage page = new MyPage(ID, name, nationality, countryCode, hobby);
	
		return page;
	}
	
	public static MyPage[] generateMyPages()
	{
		MyPage[] pages = new MyPage[AllConstants.MYPAGE_ID_TO - AllConstants.MYPAGE_ID_FROM + 1];
		for (int i = 0; i < (AllConstants.MYPAGE_ID_TO - AllConstants.MYPAGE_ID_FROM + 1); i++) {
			pages[i] = MyPage.newRanInst();
		}
		return pages;
	}
	
	@Override
	public String toString() {
		return ID + AllConstants.SEPARATOR +
				name + AllConstants.SEPARATOR +
				nationality + AllConstants.SEPARATOR +
				countryCode + AllConstants.SEPARATOR +
				hobby;
	}

	public int getID() {
		return ID;
	}

	public void setID(int iD) {
		ID = iD;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getNationality() {
		return nationality;
	}

	public void setNationality(String nationality) {
		this.nationality = nationality;
	}

	public int getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(int countryCode) {
		this.countryCode = countryCode;
	}

	public String getHobby() {
		return hobby;
	}

	public void setHobby(String hobby) {
		this.hobby = hobby;
	}
	
	
}
