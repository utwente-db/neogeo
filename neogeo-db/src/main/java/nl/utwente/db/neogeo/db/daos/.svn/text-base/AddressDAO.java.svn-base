package nl.utwente.db.neogeo.db.daos;

import java.util.ArrayList;
import java.util.List;

import nl.utwente.db.neogeo.core.model.Address;
import nl.utwente.db.neogeo.core.model.Town;
import nl.utwente.db.neogeo.db.hibernate.BaseModelObjectDAO;
import nl.utwente.db.neogeo.db.utils.HibernateUtils;

public class AddressDAO extends BaseModelObjectDAO<Address> {
	@Override
	public List<Address> findByExample(Address exampleInstance) {
		List<String> excludeProperties = new ArrayList<String>();

		if (exampleInstance.getLatitude() == 0) {
			excludeProperties.add("latitude");
		}

		if (exampleInstance.getLongitude() == 0) {
			excludeProperties.add("longitude");
		}
		
		if (exampleInstance.getX() == 0) {
			excludeProperties.add("x");
		}

		if (exampleInstance.getY() == 0) {
			excludeProperties.add("y");
		}
		
		return findByExample(exampleInstance, excludeProperties);
	}
	
	@Override
	public Address makePersistent(Address address) {
		// TODO this must be possible otherwise, but Hibernate keeps throwing TransientObjectExceptions
		// Maybe this has been solved already by setting the FlushMode to MANUAL on the session
		address.setTown((Town)HibernateUtils.getSession().load(Town.class, address.getTown().getId()));
		
		return super.makePersistent(address);
	}
}