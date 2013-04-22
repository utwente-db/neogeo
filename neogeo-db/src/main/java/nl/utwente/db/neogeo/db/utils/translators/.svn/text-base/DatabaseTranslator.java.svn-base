package nl.utwente.db.neogeo.db.utils.translators;

import java.util.List;

import nl.utwente.db.neogeo.db.model.Translation;
import nl.utwente.db.neogeo.db.model.dao.TranslationDAO;
import nl.utwente.db.neogeo.utils.translators.Translator;

public class DatabaseTranslator implements Translator {
	public String translate(String fromLanguageCode, String toLanguageCode, String textToTranslate) {
		Translation translationTemplate = new Translation();
		TranslationDAO dao = new TranslationDAO();
		
		translationTemplate.setFromLanguageCode(fromLanguageCode);
		translationTemplate.setFromLanguageText(textToTranslate);
		translationTemplate.setToLanguageCode(toLanguageCode);
		
		List<Translation> results = dao.findByExample(translationTemplate);
		
		if (!results.isEmpty()) {
			return results.get(0).getToLanguageText();
		}

		// No results found yet, turn search around
		translationTemplate = new Translation();
		
		translationTemplate.setToLanguageCode(fromLanguageCode);
		translationTemplate.setToLanguageText(textToTranslate);
		translationTemplate.setFromLanguageCode(toLanguageCode);
		
		results = dao.findByExample(translationTemplate);
		
		if (results.isEmpty()) {
			return null;
		}
		
		return results.get(0).getFromLanguageText();
	}
	
	public void addTranslation(String fromLanguageCode, String fromLanguageText, String toLanguageCode, String toLanguageText) {
		Translation translation = new Translation();
		TranslationDAO dao = new TranslationDAO();
		
		translation.setFromLanguageCode(fromLanguageCode);
		translation.setFromLanguageText(fromLanguageText);
		translation.setToLanguageCode(toLanguageCode);
		translation.setToLanguageText(toLanguageText);
		
		dao.makePersistent(translation);
	}
}