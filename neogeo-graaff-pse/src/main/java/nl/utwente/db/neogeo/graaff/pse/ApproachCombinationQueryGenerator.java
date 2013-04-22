package nl.utwente.db.neogeo.graaff.pse;

import java.util.Arrays;
import java.util.List;

public class ApproachCombinationQueryGenerator {
	public final static String[] APPROACHES_ARRAY = new String[]{
		"ca",
		"ca_osm",
		"gc_voronoi_10",
		"gc_voronoi_25",
		"gc_voronoi_50",
		"voronoi_10",
		"voronoi_25",
		"voronoi_50",
		"osm_voronoi",
		"gc_osm_voronoi"
	};
	
	public final static List<String> APPROACHES = Arrays.asList(APPROACHES_ARRAY);
	
	public static void main(String[] args) {
		for (String approach1 : APPROACHES) {
			for (String approach2 : APPROACHES) {
				if (approach1.compareTo(approach2) < 0 && !isSimilarApproach(approach1, approach2)) {
					String combinedApproachName = approach1 + "_union_" + approach2;
					
//					String query = "SELECT AddGeometryColumn('neogeo', 'pois', '" + combinedApproachName  + "', 28992, 'GEOMETRY', 2);\n";
//
//					query += "UPDATE neogeo.pois \n";
//					query += "SET " + combinedApproachName + " = ST_Union(" + approach1 + ", " + approach2 + ");\n";
//
//					query += "CREATE INDEX pois_" + combinedApproachName + " ON neogeo.pois USING gist(" + combinedApproachName + ");\n";
					String query = "'" + combinedApproachName + "',";
					System.out.println(query);
				}
			}
		}
	}

	private static boolean isSimilarApproach(String approach1, String approach2) {
		boolean result = false;
		
		// This simply results in the smaller or larger of the two approaches
		result |= (approach1.startsWith("voronoi") || approach1.startsWith("gc_voronoi")) &&
				(approach2.startsWith("voronoi") || approach2.startsWith("gc_voronoi"));
		
		// This simply results in ca (union) or ca_osm (isect)
		result |= approach1.equals("ca") && approach2.equals("ca_osm");
		
		result |= approach1.equals("osm_voronoi") && approach2.equals("gc_osm_voronoi");
		
		return result;
	}
}