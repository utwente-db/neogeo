package nl.utwente.db.neogeo.scraper.workflow.tasks;

import java.sql.BatchUpdateException;
import java.util.ArrayList;
import java.util.List;

import nl.utwente.db.neogeo.core.model.ModelObject;
import nl.utwente.db.neogeo.db.hibernate.BaseModelObjectDAO;
import nl.utwente.db.neogeo.db.utils.HibernateUtils;
import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.scraper.ScraperMessage;

import org.hibernate.HibernateException;

public class PersistenceTask extends AbstractScraperTask<ModelObject, ModelObject> {

	private int nrPOIsInSession = 0;
	private int maxNrPOIsInSession = 25;
	
	private List<ModelObject> queue = new ArrayList<ModelObject>();
	
	public ScraperMessage<ModelObject> next() {
		ModelObject modelObject = getNextInputMessage().getBody();
		
		@SuppressWarnings({ "rawtypes", "unchecked" })
		BaseModelObjectDAO<ModelObject> dao = new BaseModelObjectDAO(modelObject.getClass());
		dao.makePersistent(modelObject);
		
		queue.add(modelObject);
		
		try {
			if (++nrPOIsInSession >= maxNrPOIsInSession) {
				HibernateUtils.commit();
				nrPOIsInSession = 0;
				queue.clear();
			}
		} catch (HibernateException e) {
			String message = "";
			Throwable cause = e.getCause();

			if (cause instanceof BatchUpdateException) {
				message = " " + ((BatchUpdateException)cause).getNextException().getMessage();
			}

			throw new ScraperException("Could not store one of these modelObjects: " + queue + "\n" + message, e);
		}

		return createScraperMessage(modelObject);
	}

	public int getMaxNrPOIsInSession() {
		return maxNrPOIsInSession;
	}

	public void setMaxNrPOIsInSession(int maxNrPOIsInSession) {
		this.maxNrPOIsInSession = maxNrPOIsInSession;
	}

	@Override
	public boolean hasNext() {
		boolean result = super.hasNext();
		
		if (result == false) {
			HibernateUtils.closeSession();
		}
		
		return result;
	}
}
