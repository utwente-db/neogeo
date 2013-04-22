package nl.utwente.db.neogeo.core.model;

import java.util.List;

public class PointOfInterestCategory extends BaseModelObject {
	private List<PointOfInterest> pointsOfInterest;

	public List<PointOfInterest> getPointsOfInterest() {
		return pointsOfInterest;
	}

	public void setPointsOfInterest(List<PointOfInterest> pointsOfInterest) {
		this.pointsOfInterest = pointsOfInterest;
	}

	@Override
	public String toString() {
		return "PointOfInterestCategory [id=" + id + ", timestamp=" + timestamp + ", name=" + name
				+ "]";
	}
}
