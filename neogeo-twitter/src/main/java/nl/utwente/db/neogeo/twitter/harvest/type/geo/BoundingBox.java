package nl.utwente.db.neogeo.twitter.harvest.type.geo;

public class BoundingBox {

	Coordinate m_upLeft;
	Coordinate m_upRight;
	Coordinate m_downLeft;
	Coordinate m_downRight;

	public BoundingBox (
			Coordinate upLeft,
			Coordinate upRight,
			Coordinate downLeft,
			Coordinate downRight) {
		m_upLeft = new Coordinate(upLeft);
		m_upRight = new Coordinate(upRight);
		m_downLeft = new Coordinate(downLeft);
		m_downRight = new Coordinate(downRight);
		
	}
	
}
