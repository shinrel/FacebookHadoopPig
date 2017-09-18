package project1.obj;

import java.util.HashSet;

import constants.AllConstants;
import utils.Generator;

public class Friends {
	private int friendRel;
	private int personId;
	private int myFriend;
	private int dateOfFriendship;
	private String desc;
	
	public Friends() {
		
	}
	
	
	public Friends(int friendRel, int personId, int myFriend, int dateOfFriendship, String desc) {
		super();
		this.friendRel = friendRel;
		this.personId = personId;
		this.myFriend = myFriend;
		this.dateOfFriendship = dateOfFriendship;
		this.desc = desc;
	}

	public static Friends newRanInst()
	{
		int friendRel = Generator.genFriendRelId();
		int personId = Generator.genRanInt(AllConstants.MYPAGE_ID_FROM, AllConstants.MYPAGE_ID_TO);
		int myFriend = Generator.genRanInt(AllConstants.MYPAGE_ID_FROM, AllConstants.MYPAGE_ID_TO);
		int dateOfFriendship = Generator.genRanInt(AllConstants.FRIENDS_DATE_FROM, AllConstants.FRIENDS_DATE_TO);
		String desc = Generator.genRandomFriendsDesc();
		Friends friendRelObj = new Friends(friendRel, personId, myFriend, dateOfFriendship, desc); 
		return friendRelObj;
	}
	
	public static Friends[] generateFriends()
	{
		HashSet<String> existedRels = new HashSet<String>();
		Friends[] friends = new Friends[AllConstants.FRIENDS_REL_TO - AllConstants.FRIENDS_REL_FROM + 1];
		for (int i = 0; i <= (AllConstants.FRIENDS_REL_TO - AllConstants.FRIENDS_REL_FROM); i++) {
			int friendRel = Generator.genFriendRelId();
			int personId = Generator.genRanInt(AllConstants.MYPAGE_ID_FROM, AllConstants.MYPAGE_ID_TO);
			//one can't make friend request to a same person twicce.
			int myFriend = -1;
			while (true) {
				myFriend = Generator.genRanInt(AllConstants.MYPAGE_ID_FROM, AllConstants.MYPAGE_ID_TO);
				String key = personId + ":" + myFriend;
				if (!existedRels.contains(key)) {
					existedRels.add(key);
					break;
				}
			}
			int dateOfFriendship = Generator.genRanInt(AllConstants.FRIENDS_DATE_FROM, AllConstants.FRIENDS_DATE_TO);
			String desc = Generator.genRandomFriendsDesc();
			friends[i] = new Friends(friendRel, personId, myFriend, dateOfFriendship, desc);
			if ((i+1)% 1000000 == 0) {
				System.out.println((i + 1) + " Friends generated");
			}
		}
		return friends;
	}
	
	@Override
	public String toString()
	{
		return this.friendRel + AllConstants.SEPARATOR +
				this.personId + AllConstants.SEPARATOR +
				this.myFriend + AllConstants.SEPARATOR +
				this.dateOfFriendship + AllConstants.SEPARATOR +
				this.desc;
	}
	public int getFriendRel() {
		return friendRel;
	}
	public void setFriendRel(int friendRel) {
		this.friendRel = friendRel;
	}
	public int getPersonId() {
		return personId;
	}
	public void setPersonId(int personId) {
		this.personId = personId;
	}
	public int getMyFriend() {
		return myFriend;
	}
	public void setMyFriend(int myFriend) {
		this.myFriend = myFriend;
	}
	public int getDateOfFriendship() {
		return dateOfFriendship;
	}
	public void setDateOfFriendship(int dateOfFriendship) {
		this.dateOfFriendship = dateOfFriendship;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	
	
}
