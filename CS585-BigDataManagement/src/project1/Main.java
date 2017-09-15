package project1;

import project1.obj.MyPage;

public class Main {
	public static void main(String[] args)
	{
		for (int i = 1; i <= 100; i++) {
			System.out.println(MyPage.newRanInst().toString());
		}
	}
}
