package nl.utwente.db.neogeo.twitter.harvest.type;

public class SampleTweet {
    public String m_idstr;
	public String m_tweet;
	public String m_time;
	public String m_place;
	public String m_json;
	public SampleTweet(
			String idstr,
			String tweet,
			String time,
			String place,
			String json)
	{
		m_idstr = idstr;
		m_tweet = tweet;
		m_time = time;
		m_place = place;
		m_json = json;
	}
}
