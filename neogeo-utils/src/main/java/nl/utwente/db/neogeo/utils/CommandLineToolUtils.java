package nl.utwente.db.neogeo.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.utwente.db.neogeo.core.NeoGeoException;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

public class CommandLineToolUtils {
	public static String getOption(String[] args, String optionName) {
		return (getOption(getOptionMap(args), optionName));
	}
	
	public static Map<String, String> getOptionMap(String[] args) {
		Map<String, String> result = new HashMap<String, String>();
		
		for (int i = 0; i < args.length; i++) {
			String argument = args[i];
			String key, value;
			
			while (argument.startsWith("-")) {
				argument = argument.substring(1);
			}
			
			if (argument.contains("=")) {
				key = argument.substring(0, argument.indexOf("="));
				value = argument.substring(argument.indexOf("=") + 1);
			} else {
				key = argument;
				value = args[i+1];
				
				i++;
			}
			
			result.put(key, value);
		}
		
		return result;
	}
	
	public static String getOption(Map<String, String> optionMap, String option) {
		if (optionMap.containsKey(option)) {
			return optionMap.get(option);
		}
		
		return optionMap.get(option.substring(0, 1));
	}
		
	public static void parseOptions(String[] args, Object object) {
		setOptions(getOptionMap(args), object);
	}
	
	public static void setOptions(Map<String, String> optionMap, Object object) {
		List<String> variableNames = SpringUtils.getWritableVariableNames(object);
		
		for (String variableName : variableNames) {
			SpringUtils.setProperty(object, variableName, getOption(optionMap, variableName));
		}
	}
	
	public static String generateUsageDescription(Object object) {
		return generateUsageDescription(object, "", new ArrayList<String>());
	}

	public static String generateUsageDescription(Object object, Collection<String> usedPrefixes) {
		return generateUsageDescription(object, "", usedPrefixes);
	}
	
	public static String generateUsageDescription(Object object, String linePrefix, Collection<String> usedPrefixes) {
		if (object == null) {
			throw new NeoGeoException("Unable to generate usage description for null object.");
		}
		
		String result = linePrefix + object.getClass().getSimpleName() + " Usage:\n";
		
		result += linePrefix + "  [OPTION_NAME=OPTION_VALUE]*\n\n";
		result += linePrefix + "  Possible options:\n";
		
		BeanWrapper beanWrapper = new BeanWrapperImpl(object);
		List<String> variableNames = SpringUtils.getWritableVariableNamesForBeanWrapper(beanWrapper);
		
		for (String variableName : variableNames) {
			result += linePrefix + "  ";
			
			String firstChar = variableName.substring(0, 1);
			
			if (StringUtils.getNrPrefixOccurences(variableNames, firstChar) == 1 && !usedPrefixes.contains(firstChar)) {
				String variableNamePrefix = variableName.substring(0, 1);
				
				// "-v, "
				result += "-" + variableNamePrefix + ", ";
				usedPrefixes.add(variableNamePrefix);
			} else {
				// Placeholder for alignment
				result += "    ";
			}
			
			// --variableName
			result += "--" + variableName + "\n";
		}
		
		return result;
	}
}
