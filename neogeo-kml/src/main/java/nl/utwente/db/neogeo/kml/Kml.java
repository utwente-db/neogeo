package nl.utwente.db.neogeo.kml;

public class Kml {
	private Document Document;
	private Folder Folder;
	private String xmlns = "http://www.opengis.net/kml/2.2";
	private String hint;

	public Document getDocument() {
		return Document;
	}

	public void setDocument(Document document) {
		Document = document;
	}

	public Folder getFolder() {
		return Folder;
	}

	public void setFolder(Folder folder) {
		Folder = folder;
	}

	public String getXmlns() {
		return xmlns;
	}

	public void setXmlns(String xmlns) {
		this.xmlns = xmlns;
	}

	public String getHint() {
		return hint;
	}

	public void setHint(String hint) {
		this.hint = hint;
	}
}
