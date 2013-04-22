import java.util.List;

import nl.utwente.db.neogeo.core.model.PointOfInterestCategory;
import nl.utwente.db.neogeo.db.daos.PointOfInterestCategoryDAO;
import nl.utwente.db.neogeo.db.utils.HibernateUtils;
import nl.utwente.db.neogeo.db.utils.translators.DatabaseTranslator;
import nl.utwente.db.neogeo.utils.translators.MicrosoftTranslator;

import org.junit.Ignore;
import org.junit.Test;

@Ignore("This is a one-time task, more than a test")
public class TranslationTest {
	@Test
	public void test() {
		HibernateUtils.getSession().beginTransaction();
		
		PointOfInterestCategoryDAO poiCategoryDao = new PointOfInterestCategoryDAO();
		List<PointOfInterestCategory> poiCategories = poiCategoryDao.findAll();
		
		MicrosoftTranslator microsoftTranslator = new MicrosoftTranslator();
		DatabaseTranslator databaseTranslator = new DatabaseTranslator();
		
		for (PointOfInterestCategory poiCategory : poiCategories) {
			String fromLanguageCode = "nl";
			String toLanguageCode = "en";
			String textToTranslate = poiCategory.getName();
			String translation = microsoftTranslator.translate(fromLanguageCode, toLanguageCode, textToTranslate);
			
			System.out.println(translation);
			
			// Keep English on the 'lefthandside'
			databaseTranslator.addTranslation(toLanguageCode, translation, fromLanguageCode, textToTranslate);
		}
		
		HibernateUtils.getSession().getTransaction().commit();
	}
}
