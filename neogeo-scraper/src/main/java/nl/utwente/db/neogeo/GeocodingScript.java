package nl.utwente.db.neogeo;

import java.util.Arrays;
import java.util.List;

import nl.utwente.db.neogeo.core.model.Address;
import nl.utwente.db.neogeo.core.model.PointOfInterest;
import nl.utwente.db.neogeo.core.model.Town;
import nl.utwente.db.neogeo.db.daos.AddressDAO;
import nl.utwente.db.neogeo.db.daos.PointOfInterestDAO;
import nl.utwente.db.neogeo.db.utils.HibernateUtils;
import nl.utwente.db.neogeo.db.utils.NeoGeoDBUtils;
import nl.utwente.db.neogeo.utils.GeoUtils;


public class GeocodingScript {
	
	private static final boolean verbose = true;
	private static final boolean onlyEnschede = true;
	
	public static void main(String[] args) {
		// clearGeocoding(false /* only -1.0 values */);
		// runGeocoder();
//		checkGeocoding();
		geocodeStreets(Arrays.asList(new String[]{"Capitool"}), NeoGeoDBUtils.getTownByName("Enschede"));
	}
	
	public static void clearGeocoding() {
		PointOfInterestDAO poiDAO = new PointOfInterestDAO();

		if (verbose)
			System.out.println("#!Clearing all geocoding in POI.");
		for (PointOfInterest poi : poiDAO.findAll()) {
			HibernateUtils.getSession().refresh(poi);
			if (poi.getX() != 0.0) {
				poi.setX(0.0);
				poi.setY(0.0);
				poiDAO.makePersistent(poi);
			}
		}
		HibernateUtils.commit(true);
		if (verbose)
			System.out.println("#!Cleared all geocoding in POI.");
	}

	public static void checkGeocoding() {
		int total=0, bad_addr=0, accepted=0, rejected=0;
		
		if ( verbose )
			System.out.println("#!Checking all geocoding in POI.");
		PointOfInterestDAO poiDAO = new PointOfInterestDAO();
		for (PointOfInterest poi : poiDAO.findAll()) {
			char show = 0;
			if ( onlyEnschede && !"Enschede".equals(poi.getTown().getName()))
				continue;
			total++;
			if (poi.getTown().getName() == null || poi.getStreetName() == null || poi.getStreetName().equals("Postbus")
					|| poi.getHouseNumber() == null) {
				bad_addr++;
				show = '-';
			} else if (poi.getX() == 0.0) {
				show = '+';
				rejected++;
			} else
				accepted++;
			if ( show != 0 )
				System.out.println(show +" " + poi.getTown().getName() + "+" + poi.getStreetName() + "+"
					+ poi.getHouseNumber() + "+" + poi.getPostalCode() + " URL=" + poi.getSourceUrl());
		}
		if ( verbose )
			System.out.println("#!Checked geocodings, total="+total+", bad_addr="+bad_addr+", accepted="+ accepted +", rejected="+rejected+", %="+((double)rejected/(double)(rejected+accepted))+".");
	}

	public static void runGeocoder() {
		int accept=0, reject=0, count=0;

		if ( verbose )
			System.out.println("#!Starting geocode run.");
		PointOfInterestDAO poiDAO = new PointOfInterestDAO();
		for (PointOfInterest poi : poiDAO.findAll()) {
			HibernateUtils.getSession().refresh(poi); // may be inefficient but it works
			if ( poi.getX() == 0)  {
				if (onlyEnschede && !poi.getTown().getName().equals("Enschede") )
					continue;
				if (poi.getStreetName() == null || "Postbus".equals(poi.getStreetName()))
					continue;
				double[] coordinates = GeoUtils.geocode("nl", poi);

				if (coordinates != null) {
						accept++;
						poi.setX(coordinates[0]);
						poi.setY(coordinates[1]);
						poiDAO.makePersistent(poi);
				} else
						reject++;
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (++count > 200) {
					HibernateUtils.commit(true);
					if (verbose)
						System.out.println(">>Pauze: accept="+accept+", reject="+reject);
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					count = 0;
					// return;
				}
				
			}
		}
		HibernateUtils.commit(true);
		if (verbose)
			System.out.println("#! Finished geocode run: accepted="+accept+", rejected="+reject);
	}

	public static void geocodeStreets(List<String> streetNames, Town town) {
		AddressDAO addressDAO = new AddressDAO();
		
		int houseNumber = 1;
		String houseNumberExtension = "";

		for (String streetName : streetNames) {
			Address address = new Address();
			
			address.setName(streetName + " " + houseNumber + houseNumberExtension + " " + town.getName());
			address.setStreetName(streetName);
			address.setHouseNumber(houseNumber + houseNumberExtension);
			address.setTown(town);
			
			double[] geocodingResults = GeoUtils.geocode("nl", address);
			
			// TODO when not?
			if (true) {
				address.setX(geocodingResults[0]);
				address.setY(geocodingResults[1]);
			
				addressDAO.makePersistent(address);
				HibernateUtils.commit();
			}
			
			// TODO set next housenumber and/or next housenumberextension based on results
			
			// TODO when are we done with this street?
			if (true) {
				break;
			}
		}
	}
}
