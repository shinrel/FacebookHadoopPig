package utils;

import java.util.HashSet;
import java.util.Random;

import constants.AllConstants;

public class Generator {
	public static final String UPPER_SEQUENCE = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	public static final String LOWER_SEQUENCE = UPPER_SEQUENCE.toLowerCase();
	private static int MYPAGE_ID_INDENTIFIER = 1;
	private static int FRIENDREL_ID_INDENTIFIER = 1;
	private static int ACCESS_ID_INDENTIFIER = 1;
	private static Random random = new Random();
	private static String[] nationalities = new String[AllConstants.MYPAGE_COUNTRYCODE_TO - AllConstants.MYPAGE_COUNTRYCODE_FROM + 1];
	public static int genMyPageId()
	{
		int Id =  MYPAGE_ID_INDENTIFIER;
		MYPAGE_ID_INDENTIFIER++;
		return Id;
	}
	
	public static int genFriendRelId()
	{
		int Id =  FRIENDREL_ID_INDENTIFIER;
		FRIENDREL_ID_INDENTIFIER++;
		return Id;
	}
	public static void generateNationality()
	{
		if (nationalities[0] != null) return;
		int valid = 0;
		HashSet<String> nationalitySet = new HashSet<String>();
		while(true) {
			String nationality = genRandomString(AllConstants.MYPAGE_NATIONALITY_LEN_FROM, AllConstants.MYPAGE_NATIONALITY_LEN_TO);
			if (!nationalitySet.contains(nationality)) {
				nationalities[valid] = nationality;
				nationalitySet.add(nationality);
				valid ++;
			}
			if (valid >= 10) break;
		}
	}
	public static String getNationality(int countryCode)
	{
		return (nationalities[countryCode-1]);
	}
	public static int genAccessId()
	{
		int Id =  ACCESS_ID_INDENTIFIER;
		ACCESS_ID_INDENTIFIER++;
		return Id;
	}
	
	public static String genRandomFriendsDesc()
	{
		int idx = genRanInt(0, AllConstants.FRIENDS_DESC.length - 1);
		return AllConstants.FRIENDS_DESC[idx];
	}
	
	public static String genRandomTypeOfAccessLog()
	{
		int idx = genRanInt(0, AllConstants.ACCESSLOG_DESC.length - 1);
		return AllConstants.ACCESSLOG_DESC[idx];
	}
	
	public static String genRandomString(int lenFrom, int lenTo)
	{
		StringBuilder ranSeqBuilder = new StringBuilder(); 
		
		
		 //select random len from lenFrom to lenTo
		int ranLen = random.nextInt(lenTo - lenFrom + 1) + lenFrom;
		int i = 0;
		while (i < ranLen) {
			int ranPos = random.nextInt(LOWER_SEQUENCE.length());
			char ranChar = LOWER_SEQUENCE.charAt(ranPos);
			ranSeqBuilder.append(ranChar);
			i += 1;
		}
		
		return ranSeqBuilder.toString();
	}
	public static int genRanInt(int from, int to)
	{
		return (random.nextInt(to - from + 1) + from);
	}
	public static void main(String[] args) {
		//Test here
		String s = genRandomString(10,20);
		System.out.println(s + 
				"  --> " + s.length());
		while (true) {
			int ranNum = genRanInt(10, 20);
			if (ranNum == 10) {
				System.out.println(ranNum);
				break;
			}
			if (ranNum == 20) {
				System.out.println(ranNum);
				break;
			}
		}
	}
}
