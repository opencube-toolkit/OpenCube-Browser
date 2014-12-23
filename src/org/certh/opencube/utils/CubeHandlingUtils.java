package org.certh.opencube.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.certh.opencube.SPARQL.CubeSPARQL;

public class CubeHandlingUtils {

	/*
	 * Input: a List of dimensions Output: A HashMap with the URI - List of
	 * values for these dimensions
	 */
	public static HashMap<LDResource, List<LDResource>> getDimsValues(
			List<LDResource> dimensions, String cubeURI, String cubeGraph,
			String cubeDSDGraph, boolean useCodeLists, String lang,String defaultLang,
			boolean ignoreLang,String SPARQLservice) {

		// Create an executor to hold all threads
		ExecutorService executor = Executors.newFixedThreadPool(dimensions
				.size());
		List<Future<HashMap<LDResource, List<LDResource>>>> list = new ArrayList<Future<HashMap<LDResource, List<LDResource>>>>();

		// Create one thread for each dimension
		for (final LDResource vRes : dimensions) {
			Callable<HashMap<LDResource, List<LDResource>>> worker = new DimensionValuesThread(
					vRes, cubeURI, cubeGraph, cubeDSDGraph, useCodeLists,lang,defaultLang,ignoreLang,
					SPARQLservice);
			Future<HashMap<LDResource, List<LDResource>>> submit = executor
					.submit(worker);
			list.add(submit);

		}

		// Retrieve the results from all threads
		HashMap<LDResource, List<LDResource>> dimensionsAndValues = new HashMap<LDResource, List<LDResource>>();

		for (Future<HashMap<LDResource, List<LDResource>>> future : list) {
			try {
				HashMap<LDResource, List<LDResource>> dimVaulesHashMap = future
						.get();
				dimensionsAndValues.putAll(dimVaulesHashMap);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		executor.shutdown();

		return dimensionsAndValues;

	}

	/*
	 * Input: a List of dimensions Output: A HashMap with the URI - List of
	 * values for these dimensions
	 */
	public static HashMap<LDResource, List<LDResource>> getDimsValuesFromSlice(
			List<LDResource> dimensions, String cubeURI, String cubeGraph,
			String cubeDSDGraph, String sliceGraph, boolean useCodeLists, 
			String lang,String defaultLang, boolean ignoreLang,String SPARQLservice) {

		// Create an executor to hold all threads
		ExecutorService executor = Executors.newFixedThreadPool(dimensions
				.size());
		List<Future<HashMap<LDResource, List<LDResource>>>> list = new ArrayList<Future<HashMap<LDResource, List<LDResource>>>>();

		// Create one thread for each dimension
		for (final LDResource vRes : dimensions) {
			Callable<HashMap<LDResource, List<LDResource>>> worker = new DimensionValuesThread(
					vRes, cubeURI, cubeGraph, cubeDSDGraph, sliceGraph,
					useCodeLists, lang,defaultLang, ignoreLang,SPARQLservice);
			Future<HashMap<LDResource, List<LDResource>>> submit = executor
					.submit(worker);
			list.add(submit);

		}

		// Retrieve the results from all threads
		HashMap<LDResource, List<LDResource>> dimensionsAndValues = new HashMap<LDResource, List<LDResource>>();

		for (Future<HashMap<LDResource, List<LDResource>>> future : list) {
			try {
				HashMap<LDResource, List<LDResource>> dimVaulesHashMap = future
						.get();
				dimensionsAndValues.putAll(dimVaulesHashMap);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		executor.shutdown();

		return dimensionsAndValues;

	}

	/*
	 * Input: a List with all the dimensions URIs Output: Select the first 2
	 * dimensions for visualization and return them as List
	 */
	public static List<LDResource> getRandomDims4Visualisation(
			List<LDResource> allDimensions,
			HashMap<LDResource, List<LDResource>> allDimensionsValues) {

		List<LDResource> tmpallDimensions = new ArrayList<LDResource>();
		tmpallDimensions.addAll(allDimensions);
		// If there exist at least 2 dimensions
		List<LDResource> visualDimensions = null;
		if (tmpallDimensions.size() > 1) {
			visualDimensions = new ArrayList<LDResource>(2);

			// find dimension with most values
			LDResource dim1 = tmpallDimensions.get(0);
			for (LDResource ldr : tmpallDimensions) {
				int dim1NumberOfValues = allDimensionsValues.get(dim1).size();
				int lrdNumberOfValues = allDimensionsValues.get(ldr).size();
				if (lrdNumberOfValues > dim1NumberOfValues) {
					dim1 = ldr;
				}
			}

			tmpallDimensions.remove(dim1);
			// find 2nd dimension with most values
			LDResource dim2 = tmpallDimensions.get(0);
			for (LDResource ldr : tmpallDimensions) {
				int dim2NumberOfValues = allDimensionsValues.get(dim2).size();
				int lrdNumberOfValues = allDimensionsValues.get(ldr).size();
				if (lrdNumberOfValues > dim2NumberOfValues) {
					dim2 = ldr;
				}
			}

			visualDimensions.add(dim2);
			visualDimensions.add(dim1);

		} else { // If there is only one dimension
			visualDimensions = allDimensions;
		}

		return visualDimensions;

	}

	/*
	 * Input: a List with all the dimensions URIs, Visual dimensions Output:
	 * Select the rest dimensions (other that the visualized) and return them as
	 * List
	 */
	public static List<LDResource> getFixedDimensions(
			List<LDResource> allDimensions, List<LDResource> visualDimensions) {

		List<LDResource> fixedDimensions = new ArrayList<LDResource>(
				allDimensions);

		fixedDimensions.removeAll(visualDimensions);
		return fixedDimensions;

	}

	/*
	 * Input: a HashMap with all the dimension values and a list with fixed
	 * dimensions Output: a random selected value for each fixed dimensions
	 */
	public static HashMap<LDResource, LDResource> getFixedDimensionsRandomSelectedValues(
			HashMap<LDResource, List<LDResource>> allDimensionValues,
			List<LDResource> fixedDimensions,
			HashMap<LDResource, LDResource> previousfixedDimensionsSelectedValues) {

		HashMap<LDResource, LDResource> fixedDimSelectedValues = new HashMap<LDResource, LDResource>();
		for (LDResource fixedDim : fixedDimensions) {
			//If there is a selected value keep it
			if(previousfixedDimensionsSelectedValues.containsKey(fixedDim)){
				fixedDimSelectedValues.put(fixedDim,previousfixedDimensionsSelectedValues.get(fixedDim) );
			//else get a new one
			}else{
				if(allDimensionValues.get(fixedDim).size()>0){
					fixedDimSelectedValues.put(fixedDim,allDimensionValues.get(fixedDim).get(0));
				}
			}
			
		}

		return fixedDimSelectedValues;

	}
	
	//Get the selected language
	public static String getSelectedLanguage(List<String> availableLanguages,
			String selectedLanguage){

		//No language tag exists
		if(availableLanguages.size()==0){
			return "";
		//If there is already a selected language keep it
		}else if(selectedLanguage!=null && !selectedLanguage.equals("")){ 
			return selectedLanguage;
		//Else use by default the en
		}else if(availableLanguages.contains("en")){
			return "en";
		//If en is not available return the first available language
		}else{
			return availableLanguages.get(0);
		}
	}
	
	//Get the selected language
	public static List<LDResource> getSelectedMeasure(List<LDResource> availableMeasures,
			List<LDResource> selectedMeasures){

		//No measures exist
		if(availableMeasures.size()==0){
			return null;
		//If there is already a selected measure keep it
		}else if(selectedMeasures.size()>0){ 
			return selectedMeasures;
		//If no measure is selected return the first available measure
		}else{
			List<LDResource> tmpMeasures=new ArrayList<LDResource>();
			tmpMeasures.add(availableMeasures.get(0));
			return tmpMeasures;
		}
	}

	
	// Areti
	public static List<LDResource> getMultiValDimensions(
			List<LDResource> dimensions, String serviceURI) {

		List<LDResource> visualDimensions = new ArrayList<LDResource>();
		for (LDResource vRes : dimensions) {

			// if(IWBquery_mapview.checkMultiValDimension(vRes
			// .getURI()))
			visualDimensions.add(vRes);
		}

		return visualDimensions;
	}

	public static HashMap<LDResource, List<LDResource>> getAttrValues(
			// Areti
			List<LDResource> attributes, String cubeURI, String cubeGraph,
			String cubeDSDGraph, String lang, String defaultLang, boolean ignoreLang,
			String serviceURI) {

		// Create an executor to hold all threads
		ExecutorService executor = Executors.newFixedThreadPool(attributes
				.size());
		List<Future<HashMap<LDResource, List<LDResource>>>> list = new ArrayList<Future<HashMap<LDResource, List<LDResource>>>>();

		// Create one thread for each attribute
		for (final LDResource vRes : attributes) {
			Callable<HashMap<LDResource, List<LDResource>>> worker = new AttributeValuesThread(
					vRes, cubeURI, cubeGraph, cubeDSDGraph, lang,defaultLang, ignoreLang,serviceURI);
			Future<HashMap<LDResource, List<LDResource>>> submit = executor
					.submit(worker);
			list.add(submit);
		}

		// Retrieve the results from all threads
		HashMap<LDResource, List<LDResource>> attributesAndValues = new HashMap<LDResource, List<LDResource>>();

		for (Future<HashMap<LDResource, List<LDResource>>> future : list) {
			try {
				HashMap<LDResource, List<LDResource>> attrVaulesHashMap = future
						.get();
				attributesAndValues.putAll(attrVaulesHashMap);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		executor.shutdown();

		return attributesAndValues;

	}
	
	//Get the Levels of all the Cube dimensions.
	//If no level exists an empty List is returned
	public static HashMap<LDResource, List<LDResource>> getDimensionsLevels(String cubeURI,
			List<LDResource> cubeDimensions, String cubeDSDGraph, String cubeGraph,
			String lang, String defaultLang, boolean ignoreLang,String SPARQLservice){
			
		
		// Create an executor to hold all threads
		ExecutorService executor = Executors.newFixedThreadPool(cubeDimensions.size());
		List<Future<HashMap<LDResource, List<LDResource>>>> list = new ArrayList<Future<HashMap<LDResource, List<LDResource>>>>();

		// Create one thread for each dimension
		for (final LDResource dim : cubeDimensions) {
			Callable<HashMap<LDResource, List<LDResource>>> worker = new DimensionLevelsThread(
					dim, cubeURI, cubeGraph, cubeDSDGraph, lang,defaultLang,ignoreLang,SPARQLservice);
			Future<HashMap<LDResource, List<LDResource>>> submit = executor.submit(worker);
			list.add(submit);

		}

		// Retrieve the results from all threads
		HashMap<LDResource, List<LDResource>> dimensionsAndLevels = new HashMap<LDResource, List<LDResource>>();

		for (Future<HashMap<LDResource, List<LDResource>>> future : list) {
			try {
				HashMap<LDResource, List<LDResource>> dimLevelsHashMap = future.get();
				dimensionsAndLevels.putAll(dimLevelsHashMap);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		executor.shutdown();

		return dimensionsAndLevels;
	}
	
	//Get the skos:ConceptSchemes of all the Cube dimensions.
	//If no skos:ConceptScheme exists an empty List is returned
	public static HashMap<LDResource, List<LDResource>> getDimensionsConceptSchemes(
			List<LDResource> cubeDimensions, String cubeDSDGraph, String lang, 
			String defaultlang, boolean ignoreLang,String SPARQLservice){
		
		// Create an executor to hold all threads
		ExecutorService executor = Executors.newFixedThreadPool(cubeDimensions.size());
		List<Future<HashMap<LDResource, List<LDResource>>>> list = new ArrayList<Future<HashMap<LDResource, List<LDResource>>>>();

		// Create one thread for each dimension
		for (final LDResource dim : cubeDimensions) {
			Callable<HashMap<LDResource, List<LDResource>>> worker = new DimensionConceptSchemesThread(
					dim, cubeDSDGraph, lang, defaultlang, ignoreLang, SPARQLservice); 
			Future<HashMap<LDResource, List<LDResource>>> submit = executor.submit(worker);
			list.add(submit);

		}

		// Retrieve the results from all threads
		HashMap<LDResource, List<LDResource>> dimensionsConceptSchemes =new HashMap<LDResource, List<LDResource>>();

		for (Future<HashMap<LDResource, List<LDResource>>> future : list) {
			try {
				HashMap<LDResource, List<LDResource>> dimConceptSchemesHashMap = future.get();
				dimensionsConceptSchemes.putAll(dimConceptSchemesHashMap);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		executor.shutdown();

		return dimensionsConceptSchemes;	
	}


}
