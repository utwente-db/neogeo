package nl.utwente.db.neogeo.utils.translators;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.utils.NeoGeoProperties;

import com.memetix.mst.language.Language;
import com.memetix.mst.translate.Translate;

public class MicrosoftTranslator implements Translator {
	private final String clientId;
	private final String clientSecret;

	public MicrosoftTranslator() {
		this.clientId = NeoGeoProperties.getInstance().getProperty("translate.microsoft.clientid");
		this.clientSecret = NeoGeoProperties.getInstance().getProperty("translate.microsoft.clientsecret");
	}
	
	public MicrosoftTranslator(String clientId, String clientSecret) {
		this.clientId = clientId;
		this.clientSecret = clientSecret;
	}
	
	public String translate(String fromLanguageCode, String toLanguageCode, String textToTranslate) {
        try{
            Translate.setClientId(clientId);
            Translate.setClientSecret(clientSecret);
            
            return Translate.execute(textToTranslate, Language.fromString(fromLanguageCode), Language.fromString(toLanguageCode));
        } catch(Exception e) {
            throw new NeoGeoException("Unable to translate text " + textToTranslate + " from " + fromLanguageCode + " to " + toLanguageCode, e);
        }
	}
}