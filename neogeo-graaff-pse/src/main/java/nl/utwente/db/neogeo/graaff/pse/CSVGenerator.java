package nl.utwente.db.neogeo.graaff.pse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.core.model.PointOfInterest;
import nl.utwente.db.neogeo.core.model.PointOfInterestCategory;
import nl.utwente.db.neogeo.db.daos.PointOfInterestDAO;
import nl.utwente.db.neogeo.db.utils.NeoGeoDBUtils;

import org.apache.log4j.Logger;

public class CSVGenerator {
	private Logger logger = Logger.getLogger(CSVGenerator.class);
	
	public static void main(String[] args) {
		CSVGenerator generator = new CSVGenerator();
		
		Map<String, String> categoryNames = new HashMap<String, String>();
		
		categoryNames.put("Bloemen En Planten", "Flowerists");
		categoryNames.put("Campings", "Campsites");
		categoryNames.put("Garages", "Auto repair shops");
		categoryNames.put("Kappers", "Barbers");
		categoryNames.put("Maneges", "Horseback riding");
		categoryNames.put("Restaurants", "Restaurants");
		categoryNames.put("Supermarkten", "Supermarkets");
		categoryNames.put("Ziekenhuizen", "Hospitals");
		categoryNames.put("Zwembaden En -scholen", "Swimming pools");
		
		generator.generate(categoryNames, new File("/home/victor/ut/poi-size-estimation/data/pois.csv"));
	}

	public void generate(Map<String, String> categoryAndDisplayNames, File outputFile) {
		try {
			FileWriter fileWriter = new FileWriter(outputFile);
			
			for (Entry<String, String> categoryAndDisplayName : categoryAndDisplayNames.entrySet()) {
				fileWriter.write(categoryAndDisplayName.getValue());
				System.out.println("Starting CSV generation for " + categoryAndDisplayName.getKey());
				
				PointOfInterestCategory category = NeoGeoDBUtils.getCategoryByName(categoryAndDisplayName.getKey());
				PointOfInterest poiTemplate = new PointOfInterest();
				
				poiTemplate.setCategory(category);
				PointOfInterestDAO poiDAO = new PointOfInterestDAO();
				
				List<PointOfInterest> pois = poiDAO.findByExample(poiTemplate);
				System.out.println(pois.size());

				int i = 0;
				
				StringBuffer sb = new StringBuffer();
				
				for (PointOfInterest poi : pois) {
					PointOfInterest nearestNeighbour = poiDAO.getNearestNeighbour(poi, 500);
					
					if (nearestNeighbour == null) {
						continue;
					}
					
					double distance = poiDAO.getDistance(poi, nearestNeighbour);
					sb.append(";" + distance);
					
					if ((++i % 100) == 0) {
						logger.debug("Generated CSV for " + i + " " + categoryAndDisplayName.getKey() + " so far");
						System.out.println("Generated CSV for " + i + " " + categoryAndDisplayName.getKey() + " so far");
					}
				}
				
				fileWriter.write(" (" + i + ")" + sb.toString() + "\n");
			}
			
			fileWriter.close();
		} catch (IOException e) {
			throw new NeoGeoException("Unable to generate POI Size Estimation CSV", e);
		}
	}
}
