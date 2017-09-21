

import constants.AllConstants;
import project1.obj.AccessLog;
import project1.obj.Friends;
import project1.obj.MyPage;
import utils.FileUtils;

public class Main {
	public static void writeObj(Object obj, String fileout) {
		FileUtils.openFile(fileout);
		FileUtils.writeToFile(obj.toString());
		FileUtils.flushToFile();
		FileUtils.closeFile();
	}
	public static void writeObjs(Object[] objs, String fileout) {
		FileUtils.openFile(fileout);
		int i = 0;
		for (Object obj : objs) {
			FileUtils.writeToFile(obj.toString());
			if (i ++ % 1000 == 0) {
				FileUtils.flushToFile();
			}
		}
		FileUtils.flushToFile();
		FileUtils.closeFile();
	}
	public static void main(String[] args)
	{
//		MyPage[] pages = new MyPage[AllConstants.MYPAGE_ID_TO - AllConstants.MYPAGE_ID_FROM + 1];
//		for (int i = AllConstants.MYPAGE_ID_FROM - 1; i < AllConstants.MYPAGE_ID_TO; i++) {
////			System.out.println(MyPage.newRanInst().toString());
//			pages[i] = MyPage.newRanInst();
//			System.out.println(pages[i].toString());
//		}
		boolean TEST = true;
		String mypageFileOut = "datasets/proj1/mypage.csv";
		String friendsFileOut = "datasets/proj1/friends.csv";
		String accessLogFileOut = "datasets/proj1/access-log.csv";
		
		if (TEST) {
			AllConstants.RATIO = (float)1/(float)100;
			AllConstants.recalculate();
			mypageFileOut = "datasets/proj1/mypage_test.csv";
			friendsFileOut = "datasets/proj1/friends_test.csv";
			accessLogFileOut = "datasets/proj1/access-log_test.csv";
		} 
		System.out.println("Generating MyPage dataset");
		MyPage[] pages = MyPage.generateMyPages();
		writeObjs(pages, mypageFileOut);
		
		System.out.println("Generating Friends dataset");
		Friends[] friends = Friends.generateFriends();
		writeObjs(friends, friendsFileOut);
		
		System.out.println("Generating AccessLog dataset");
		AccessLog[] accessLogs = AccessLog.generateAccessLog();
		writeObjs(accessLogs, accessLogFileOut);
	}
}
