package nl.utwente.db.neogeo.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import nl.utwente.db.neogeo.core.NeoGeoException;

import org.springframework.core.io.ClassPathResource;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class FileUtils {
	public static String getFileAsString(File file) {
		FileReader fileReader;
		StringBuffer result = new StringBuffer();

		try {
			fileReader = new FileReader(file);
		} catch (FileNotFoundException e) {
			throw new NeoGeoException("File not found." , e);
		}

		BufferedReader in = new BufferedReader(fileReader);
		String line = null;

		try {
			while ((line = in.readLine()) != null) {
				result.append(line);
				result.append('\n');
			}
		} catch (IOException e) {
			throw new NeoGeoException("IOException", e);
		}

		return result.toString();
	}

	public static String getFileAsString(String relativePath) {
		return getFileAsString(getFileFromClassPath(relativePath));
	}

	public static String getAbsolutePath(String relativePath) {
		return getFileFromClassPath(relativePath).getAbsolutePath();
	}

	public static void writeFile(String fileName, String contents) {
		writeFile(new File(fileName), contents);
	}
	
	public static void writeFile(File outputFile, String contents) {
		try {
			FileWriter fileWriter = new FileWriter(outputFile);

			fileWriter.write(contents);

			fileWriter.close();
		} catch (IOException e) {
			throw new NeoGeoException("Could not write file: " + outputFile.getAbsolutePath(), e);
		}
	}

	public static Properties getProperties(String propertiesFileName) {
		Properties properties = new Properties();
		ClassPathResource resource = new ClassPathResource(propertiesFileName);

		try {
			File propertiesFile = resource.getFile();
			FileInputStream stream = new FileInputStream(propertiesFile);

			properties.load(stream);
		} catch (IOException e) {
			throw new NeoGeoException("Unable to read properties file '" + propertiesFileName + "'");
		}

		return properties;
	}

	public static Document getXMLDocument(String filePath) {
		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder;
		Document document;

		try {
			documentBuilder = documentBuilderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new NeoGeoException("Could not create DocumentBuilder", e);
		}

		try {
			document = documentBuilder.parse(filePath);
		} catch (IOException e) {
			throw new NeoGeoException("IOException for file " + filePath, e);
		} catch (SAXException e) {
			throw new NeoGeoException("SAXException for file " + filePath, e);
		}

		return document;
	}

	public static File getFileFromClassPath(String relativePath) {
		ClassPathResource resource = new ClassPathResource(relativePath);

		try {
			return resource.getFile();
		} catch (IOException e) {
			throw new NeoGeoException("Unable to load classpath resource " + relativePath, e);
		}
	}
}
