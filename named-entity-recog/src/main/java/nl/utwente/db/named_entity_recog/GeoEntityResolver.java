package nl.utwente.db.named_entity_recog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;


/**
 *
 * @author badiehm & flokstra
 */
public class GeoEntityResolver
{
	public static final boolean verbose = false;

	Connection c;
	ZipcodeStreetDB zipdb = null;
	NationaalRegisterStreetDB nrgdb = null;
	
	public GeoEntityResolver(Connection c) throws SQLException {
		this.c = c;
		zipdb = new ZipcodeStreetDB(c);
		nrgdb = new NationaalRegisterStreetDB(c);
	}
	
	public void resolve(NamedEntity entity) throws SQLException {
		resolveCity(entity);
		resolveStreet(null,entity);
		
	}
	
	protected void resolveCity(NamedEntity entity) throws SQLException {
		StringBuilder sql_bld = new StringBuilder("select name,latitude,longitude,country,alternatenames,population,elevation,fclass from geoname where (lower(name) = ?)");
		if ( has_country_filter() )
			sql_bld.append(" AND "+get_country_filter());
		if ( has_fclass_filter() )
			sql_bld.append(" AND "+get_fclass_filter());
		PreparedStatement ps = c.prepareStatement(sql_bld+";");
        String candidate = entity.getMention().toLowerCase();
        ps.setString(1, candidate);
        if ( verbose )
        	System.out.println("#!SQL: "+ps + ";");
        ResultSet rs = ps.executeQuery();
        
        while (rs.next())
        {
            GeoNameEntity ge = new GeoNameEntity(entity, rs.getDouble("latitude"),
                    rs.getDouble("longitude"), rs.getString("country"),
                    rs.getString("alternatenames"),
                    rs.getInt("population"),
                    rs.getInt("elevation"),
                    rs.getString("fclass"));
            entity.addResolved(ge);
            if (verbose) {
                System.out.println("RESOLVED CITY[" + ge + "]");
            }
        }

	}
	
	protected void resolveStreet(String possibleCities[], NamedEntity streetEntity)
			throws SQLException {
		Connection c = GeoNamesDB.geoNameDBConnection();

		ZipcodeStreetDB zipStreetDB = new ZipcodeStreetDB(c);
		NationaalRegisterStreetDB nrgStreetDB = new NationaalRegisterStreetDB(c);

		if (zipStreetDB.existsStreet(streetEntity.getName())) {
			Vector<StreetEntity> v = nrgStreetDB.selectStreetEntity(streetEntity);
			for (int i = 0; i < v.size(); i++) {
				streetEntity.addResolved(v.elementAt(i));
				if ( verbose )
					System.out.println("+" + v.elementAt(i));
			}
		} else {
			if ( verbose )
				System.out.println("- Street: " + streetEntity.getName() + " does not exist");
		}

	}
	
	//
	//
	//
	
	private String flt_country[] = null;
	public static final String filter_nl[] = { "NL" };
	public static final String filter_nl_be_de[] = { "NL", "BE", "DE" };
	
	public void set_country_filter(String f[]) {
		flt_country = f;
	}
	
	public boolean has_country_filter() {
		return flt_country != null;
	}
	
	public String get_country_filter() {
		if ( has_country_filter() ) {
			StringBuilder res = new StringBuilder();
			
			for(int i=0; i<flt_country.length; i++ ) {
				if ( i > 0 )
					res.append(" OR ");
				res.append("country=\'"+flt_country[i]+"\'");
			}
			return "("+res.toString()+")";
		} else
			return "@TRUE";
	}
	
	private String flt_fclass[] = null;
	public static final String filter_p[] = { "P" };
	
	public void set_fclass_filter(String f[]) {
		flt_fclass = f;
	}
	
	public boolean has_fclass_filter() {
		return flt_fclass != null;
	}
	
	public String get_fclass_filter() {
		if ( has_fclass_filter() ) {
			StringBuilder res = new StringBuilder();
			
			for(int i=0; i<flt_fclass.length; i++ ) {
				if ( i > 0 )
					res.append(" OR ");
				res.append("fclass=\'"+flt_fclass[i]+"\'");
			}
			return "("+res.toString()+")";
		} else
			return "@TRUE";
	}
	
}
