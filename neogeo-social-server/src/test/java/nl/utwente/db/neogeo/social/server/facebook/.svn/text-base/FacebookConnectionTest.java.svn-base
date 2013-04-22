package nl.utwente.db.neogeo.social.server.facebook;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class FacebookConnectionTest {

	public final static String ACCESS_TOKEN = "AAAAAAITEghMBAGYTYOJulGYcs08FCZBe6IbQFe69KfDXqdoYk7SguS5wxKHn1GstWPzU9dtSmGFagOuEYaCfQKhNoFGUtgbstTRfgcAZDZD";
	public String mockURL = "http://localhost:8080/neogeo-social/facebook-mock/";
	
	private FacebookConnection facebookConnection = new FacebookConnection(ACCESS_TOKEN);
//	private FacebookConnection facebookConnection = new FacebookConnectionMock(mockURL);

	@Test
	public void getCopyOfData() {
		System.out.print("FacebookConnectionTest.getCopyOfData: ");

		long starttime = System.currentTimeMillis();
		facebookConnection.getCopyOfData();
		long endtime = System.currentTimeMillis();
		
		Assert.assertTrue(endtime > starttime);
		
		System.out.println((endtime - starttime) + "ms");
	}
	
//	@Test
//	public void getCurrentUser() {
//		System.out.print("FacebookConnectionTest.getCurrentUser: ");
//
//		long starttime = System.currentTimeMillis();
//		assertNotNull(facebookConnection.getCurrentUser());
//		long endtime = System.currentTimeMillis();
//
//		System.out.println((endtime - starttime) + "ms");
//	}
//
//	@Test
//	public void getCurrentUsersFriends() {
//		System.out.print("FacebookConnectionTest.getCurrentUsersFriends: ");
//
//		long starttime = System.currentTimeMillis();
//		assertNotNull(facebookConnection.getCurrentUsersFriends());
//		long endtime = System.currentTimeMillis();
//
//		System.out.println((endtime - starttime) + "ms");
//	}
//
//	@Test
//	public void getCurrentUsersFriendsDetailed() {
//		System.out.print("FacebookConnectionTest.getCurrentUsersFriendsDetailed: ");
//
//		long starttime = System.currentTimeMillis();
//		assertNotNull(facebookConnection.getCurrentUsersFriends(false));
//		long endtime = System.currentTimeMillis();
//
//		System.out.println((endtime - starttime) + "ms");
//	}
//
//	@Test
//	public void getCurrentUsersInterests() {
//		System.out.print("FacebookConnectionTest.getCurrentUsersInterests: ");
//
//		long starttime = System.currentTimeMillis();
//		assertNotNull(facebookConnection.getCurrentUsersInterests());
//		long endtime = System.currentTimeMillis();
//
//		System.out.println((endtime - starttime) + "ms");
//	}
//
//	@Test
//	public void getCurrentUsersInterestsDetailed() {
//		System.out.print("FacebookConnectionTest.getCurrentUsersInterestsDetailed: ");
//
//		long starttime = System.currentTimeMillis();
//		assertNotNull(facebookConnection.getCurrentUsersInterests(false));
//		long endtime = System.currentTimeMillis();
//
//		System.out.println((endtime - starttime) + "ms");
//	}
//
//	// TODO: memory leaks?
//	@Test
//	public void getCurrentUsersFriendsInterests() {
//		System.out.print("FacebookConnectionTest.getCurrentUsersFriendsInterests: ");
//		boolean foundAnInterest = false;
//
//		long starttime = System.currentTimeMillis();
//
//		for (User friend : facebookConnection.getCurrentUsersFriends()) {
//			foundAnInterest = !facebookConnection.getInterests(friend).isEmpty() || foundAnInterest;
//		}
//
//		long endtime = System.currentTimeMillis();
//
//		assertTrue(foundAnInterest);
//
//		System.out.println((endtime - starttime) + "ms");
//	}
//
//	@Test
//	public void getCurrentUsersFriendsInterestsDetailed() {
//		System.out.print("FacebookConnectionTest.getCurrentUsersFriendsInterestsDetailed: ");
//		boolean foundAnInterest = false;
//
//		long starttime = System.currentTimeMillis();
//
//		for (User friend : facebookConnection.getCurrentUsersFriends()) {
//			foundAnInterest = !facebookConnection.getInterests(friend, false).isEmpty() || foundAnInterest;
//		}
//
//		long endtime = System.currentTimeMillis();
//
//		assertTrue(foundAnInterest);
//
//		System.out.println((endtime - starttime) + "ms");
//	}
}
