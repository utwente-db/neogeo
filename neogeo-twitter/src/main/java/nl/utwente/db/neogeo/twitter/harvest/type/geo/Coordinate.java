package nl.utwente.db.neogeo.twitter.harvest.type.geo;

public class Coordinate {
	public double m_latitude;
	public double m_longitude;
	public Coordinate(double latitude, double longitude) {
		m_latitude = latitude;
		m_longitude = longitude;
	}
	public Coordinate(Coordinate co) {
		m_latitude = co.m_latitude;
		m_longitude = co.m_longitude;
	}
}
