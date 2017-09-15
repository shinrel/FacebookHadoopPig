package project1.obj;

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
		Friends friend = new Friends();
		return friend;
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
