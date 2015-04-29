package org.certh.opencube.SPARQL;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.certh.opencube.utils.CubeHandlingUtils;
import org.certh.opencube.utils.LDResource;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

public class SelectionSPARQL {

	// private static LDResource indic = new LDResource(
	// "http://eurostat.linked-statistics.org/property#indic_en");

	public static List<LDResource> getAllAvailableCubesAndSlices(String SPARQLservice) {

		String getAllAvailableCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "SELECT DISTINCT ?dataset ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAllAvailableCubes_query += "SERVICE " + SPARQLservice + " { ";
		}

		getAllAvailableCubes_query += "GRAPH ?cubeGraph{{?dataset rdf:type qb:DataSet}" +
				"UNION {?dataset rdf:type qb:Slice} "
				+ "OPTIONAL{?dataset rdfs:label ?label.}} }";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAllAvailableCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getAllAvailableCubes_query);

		ArrayList<LDResource> allCubes = new ArrayList<LDResource>();

		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset")
						.stringValue());
				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}
				allCubes.add(ldr);
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return allCubes;
	}

	public static Map<LDResource, Integer> getAllAvailableCubesAndDimCount(
			String SPARQLservice) {

		String getAllAvailableCubesAndDimCount_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "select distinct ?dataset (COUNT(DISTINCT ?dim) AS ?dimcount) ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAllAvailableCubesAndDimCount_query += "SERVICE " + SPARQLservice
					+ " { ";
		}

		getAllAvailableCubesAndDimCount_query += "GRAPH ?cubeGraph{"
				+ "?dataset rdf:type qb:DataSet.?dataset qb:structure ?dsd."
				+ "OPTIONAL{?dataset rdfs:label ?label.}}"
				+ "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp.?comp qb:dimension ?dim.}}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAllAvailableCubesAndDimCount_query += "}";
		}

		getAllAvailableCubesAndDimCount_query += "GROUP by ?dataset ?label";

		TupleQueryResult res = QueryExecutor
				.executeSelect(getAllAvailableCubesAndDimCount_query);

		Map<LDResource, Integer> allCubesAndDimCount = new HashMap<LDResource, Integer>();

		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset")
						.stringValue());
				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}
				allCubesAndDimCount.put(ldr, Integer.parseInt(bindingSet
						.getValue("dimcount").stringValue()));
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return allCubesAndDimCount;
	}

	// Get all compatible cubes
	// Compatible: - have the dimensions of the original cube
	// - the dimensions have the same skos:ConceptScheme
	// - the dimension instances are at the same xkos:ClassificationLevel
	// - All the dimension (except the expansion) have 100% overlap
	// - The measure has complement size > 0
	public static HashMap<LDResource, List<LDResource>> getMeasureCompatibleCubes(
			LDResource selectedCube, String selectedCubeGraph,
			String selectedCubeDSDgraph,
			Map<LDResource, Integer> allCubesAndDimCount,
			List<LDResource> cubeDimensions,
			HashMap<LDResource, List<LDResource>> dimensionsLevels,
			HashMap<LDResource, List<LDResource>> dimensionsConceptSchemes,
			List<LDResource> cubeMeasures, double dimensionOverlapThreshold,
			String selectedLanguage, String defaultLang, boolean ignoreLang,
			String SPARQLservice) {

		String getCompatibleCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "select distinct ?dataset ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice + " { ";
		}

		getCompatibleCubes_query += "GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet."
				+ "?dataset qb:structure ?dsd "
				+ "OPTIONAL{?dataset rdfs:label ?label}}";

		int i = 1;
		for (LDResource dim : cubeDimensions) {
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"
					+ i + ".";

			// If there is a code list check the code list
			if (dimensionsConceptSchemes.get(dim) != null
					&& dimensionsConceptSchemes.get(dim).size() > 0) {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"
						+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes
						.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"
							+ conceptScheme.getURI() + ">.";
				}
				// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"
						+ dim.getURI() + ">.";
			}

			getCompatibleCubes_query += "}";

			i++;
		}

		getCompatibleCubes_query += "}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getCompatibleCubes_query);

		List<LDResource> compatibleCubes = new ArrayList<LDResource>();

		HashMap<LDResource, List<LDResource>> compatibleCubesAndMeasures = new HashMap<LDResource, List<LDResource>>();
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset")
						.stringValue());

				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}

				// A cube to be measure compatible must have the same number of
				// dimensions
				// The query returns cubes that have at least the same dims as
				// the original cube
				// but may have more
				if (allCubesAndDimCount.get(ldr) == cubeDimensions.size()) {
					compatibleCubes.add(ldr);
				}

			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();

		// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
		LDResource obsvalue = new LDResource(
				"http://purl.org/linked-data/sdmx/2009/measure#obsValue");
		// EUROSTAT END //////////////////////

		for (LDResource cube : compatibleCubes) {
			// The above query returns all compatible cubes including the
			// original cube
			if (!cube.equals(selectedCube)) {
				// Get the original cube dimension values
				if (originalCubeDimensionsValues.keySet().isEmpty()) {
					originalCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(cubeDimensions,
									"<" + selectedCube.getURI() + ">",
									selectedCubeGraph, selectedCubeDSDgraph,
									false, selectedLanguage, defaultLang,
									ignoreLang, SPARQLservice);

					// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
					// cubeMeasures
					// .addAll(originalCubeDimensionsValues.get(indic));
					// cubeMeasures.remove(obsvalue);
					// EUROSTAT END //////////////////////

				}
				// Get the compatible Cube Graph
				String cubeGraph = CubeSPARQL.getCubeSliceGraph(
						"<" + cube.getURI() + ">", SPARQLservice);

				// Get the compatible Cube Structure graph
				String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"
						+ cube.getURI() + ">", cubeGraph, SPARQLservice);

				boolean levelFound = true;
				// check the potential compatible cube if it has the same levels
				// at dimensions
				for (LDResource dim : cubeDimensions) {
					for (LDResource level : dimensionsLevels.get(dim)) {
						if (!askDimensionLevelInDataCube(cube, level,
								cubeGraph, cubeDSDGraph, SPARQLservice)) {
							levelFound = false;
							break;
						}
					}
					if (!levelFound) {
						break;
					}
				}

				// If it has the same dimension levels
				if (levelFound) {

					// Get all Cube dimensions
					List<LDResource> compatibleCubeDimensions = CubeSPARQL
							.getDataCubeDimensions("<" + cube.getURI() + ">",
									cubeGraph, cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);

					// the same number of dimensions
					HashMap<LDResource, List<LDResource>> compatibleCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(compatibleCubeDimensions,
									"<" + cube.getURI() + ">", cubeGraph,
									cubeDSDGraph, false, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);

					boolean isDimensionCompatible = true;
					// check if there is an intersection (at least one) at the
					// values of each dimension

					// REMOVE THE MEASURE DIMENSION -- EUROSTAT
					List<LDResource> correctDims = new ArrayList<LDResource>(
							cubeDimensions);
					// correctDims.remove(indic);
					// END EUROSTAT /////////////////////

					for (LDResource dim : correctDims) {
						List<LDResource> dimValues = originalCubeDimensionsValues
								.get(dim);
						List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues
								.get(dim);
						List<LDResource> intersection = new ArrayList<LDResource>(
								dimValues);
						intersection.retainAll(compatibleCubeDimValues);

						double overlap = (double) intersection.size()
								/ (double) dimValues.size();

						if (overlap < dimensionOverlapThreshold) {
							isDimensionCompatible = false;
							break;
						}
					}

					// check measure
					if (isDimensionCompatible) {
						// Get the Cube measure
						List<LDResource> compMeasures = CubeSPARQL
								.getDataCubeMeasure("<" + cube.getURI() + ">",
										cubeGraph, cubeDSDGraph,
										selectedLanguage, defaultLang,
										ignoreLang, SPARQLservice);

						// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
						// compMeasures.addAll(compatibleCubeDimensionsValues
						// .get(indic));
						// compMeasures.remove(obsvalue);
						// EUROSTAT END //////////////////////

						// get the measure complement
						// need to have at least one new measure
						List<LDResource> measureComplement = new ArrayList<LDResource>(
								compMeasures);
						measureComplement.removeAll(cubeMeasures);

						if (measureComplement.size() > 0) {
							compatibleCubesAndMeasures.put(cube, compMeasures);
						}
					}
				}
			}
		}
		return compatibleCubesAndMeasures;
	}

	// Get all compatible cubes
	// Compatible: - have the dimensions of the original cube
	// - the dimensions have the same skos:ConceptScheme
	// - the dimension instances are at the same xkos:ClassificationLevel
	// - All the dimension (except the expansion) have 100% overlap
	// - The measure has complement size > 0
	public static int linkMeasureCompatibleCubes(LDResource selectedCube,
			String selectedCubeGraph, String selectedCubeDSDgraph,
			Map<LDResource, Integer> allCubesAndDimCount,
			List<LDResource> cubeDimensions,
			HashMap<LDResource, List<LDResource>> dimensionsLevels,
			HashMap<LDResource, List<LDResource>> dimensionsConceptSchemes,
			List<LDResource> cubeMeasures, double dimensionOverlapThreshold,
			String selectedLanguage, String defaultLang, boolean ignoreLang,
			String SPARQLservice) {

		String getCompatibleCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "select distinct ?dataset ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice + " { ";
		}

		getCompatibleCubes_query += "GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet."
				+ "?dataset qb:structure ?dsd "
				+ "OPTIONAL{?dataset rdfs:label ?label}}";

		int i = 1;
		for (LDResource dim : cubeDimensions) {
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"
					+ i + ".";

			// If there is a code list check the code list
			if (dimensionsConceptSchemes.get(dim) != null
					&& dimensionsConceptSchemes.get(dim).size() > 0) {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"
						+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes
						.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"
							+ conceptScheme.getURI() + ">.";
				}
				// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"
						+ dim.getURI() + ">.";
			}

			getCompatibleCubes_query += "}";

			i++;
		}

		getCompatibleCubes_query += "}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getCompatibleCubes_query);

		List<LDResource> compatibleCubes = new ArrayList<LDResource>();

		int numberOfCompatibleCubes = 0;
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset")
						.stringValue());

				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}

				// A cube to be measure compatible must have the same number of
				// dimensions
				// The query returns cubes that have at least the same dims as
				// the original cube
				// but may have more
				if (allCubesAndDimCount.get(ldr) == cubeDimensions.size()) {
					compatibleCubes.add(ldr);
				}

			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();

		// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
		// LDResource obsvalue = new LDResource(
		// "http://purl.org/linked-data/sdmx/2009/measure#obsValue");
		// EUROSTAT END //////////////////////

		for (LDResource cube : compatibleCubes) {
			// The above query returns all compatible cubes including the
			// original cube
			if (!cube.equals(selectedCube)) {
				// Get the original cube dimension values
				if (originalCubeDimensionsValues.keySet().isEmpty()) {
					originalCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(cubeDimensions,
									"<" + selectedCube.getURI() + ">",
									selectedCubeGraph, selectedCubeDSDgraph,
									false, selectedLanguage, defaultLang,
									ignoreLang, SPARQLservice);

					// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
					// cubeMeasures
					// .addAll(originalCubeDimensionsValues.get(indic));
					// cubeMeasures.remove(obsvalue);
					// EUROSTAT END //////////////////////

				}
				// Get the compatible Cube Graph
				String cubeGraph = CubeSPARQL.getCubeSliceGraph(
						"<" + cube.getURI() + ">", SPARQLservice);

				// Get the compatible Cube Structure graph
				String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"
						+ cube.getURI() + ">", cubeGraph, SPARQLservice);

				boolean levelFound = true;
				// check the potential compatible cube if it has the same levels
				// at dimensions
				for (LDResource dim : cubeDimensions) {
					for (LDResource level : dimensionsLevels.get(dim)) {
						if (!askDimensionLevelInDataCube(cube, level,
								cubeGraph, cubeDSDGraph, SPARQLservice)) {
							levelFound = false;
							break;
						}
					}
					if (!levelFound) {
						break;
					}
				}

				// If it has the same dimension levels
				if (levelFound) {

					// Get all Cube dimensions
					List<LDResource> compatibleCubeDimensions = CubeSPARQL
							.getDataCubeDimensions("<" + cube.getURI() + ">",
									cubeGraph, cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);

					// the same number of dimensions
					HashMap<LDResource, List<LDResource>> compatibleCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(compatibleCubeDimensions,
									"<" + cube.getURI() + ">", cubeGraph,
									cubeDSDGraph, false, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);

					boolean isDimensionCompatible = true;
					// check if there is an intersection (at least one) at the
					// values of each dimension

					// REMOVE THE MEASURE DIMENSION -- EUROSTAT
					List<LDResource> correctDims = new ArrayList<LDResource>(
							cubeDimensions);
					// correctDims.remove(indic);
					// END EUROSTAT /////////////////////

					for (LDResource dim : correctDims) {
						List<LDResource> dimValues = originalCubeDimensionsValues
								.get(dim);
						List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues
								.get(dim);
						List<LDResource> intersection = new ArrayList<LDResource>(
								dimValues);
						intersection.retainAll(compatibleCubeDimValues);

						double overlap = (double) intersection.size()/ (double) dimValues.size();

						if (overlap < dimensionOverlapThreshold) {
							isDimensionCompatible = false;
							break;
						}
					}

					// check measure
					if (isDimensionCompatible) {
						// Get the Cube measure
						List<LDResource> compMeasures = CubeSPARQL
								.getDataCubeMeasure("<" + cube.getURI() + ">",
										cubeGraph, cubeDSDGraph,
										selectedLanguage, defaultLang,
										ignoreLang, SPARQLservice);

						// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
						// compMeasures.addAll(compatibleCubeDimensionsValues.get(indic));
						// compMeasures.remove(obsvalue);
						// EUROSTAT END //////////////////////

						// get the measure complement
						// need to have at least one new measure
						List<LDResource> measureComplement = new ArrayList<LDResource>(compMeasures);
						measureComplement.removeAll(cubeMeasures);

						if (measureComplement.size() > 0) {
					
							// Add new link
							String create_link_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
									+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
									+ "INSERT DATA  {";

							if (SPARQLservice != null) {
								create_link_query += "SERVICE " + SPARQLservice	+ " {";
							}

							if (selectedCubeGraph != null) {
								create_link_query += "GRAPH <" + selectedCubeGraph + "> {";
							}

							// cube1 -> measureCompatible -> cube2
							create_link_query += 
									"<"+ selectedCube.getURI()+ "> qb:measureCompatible <" + cube.getURI()+ ">}";

							if (cubeGraph != null) {
								create_link_query += "}";
							}

							if (SPARQLservice != null) {
								create_link_query += "}";
							}

							QueryExecutor.executeUPDATE(create_link_query);
							numberOfCompatibleCubes++;
						}
					}
				}
			}
		}
		return numberOfCompatibleCubes;
	}

	public static HashMap<LDResource, List<LDResource>> getLinkAddValueToLevelCompatibleCubes(LDResource initialCube,
			String initialCubeGraph,String initialCubeDSDGraph,
			LDResource expansionDimension,String lang, String defaultlang,
			boolean ignoreLang, String SPARQLservice){
		
		String getCompatibleCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "select distinct ?cube ?label where {";
		
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice	+ " {";
		}

		if (initialCubeGraph != null) {
			getCompatibleCubes_query += "GRAPH <" + initialCubeGraph + "> {";
		}
		
		getCompatibleCubes_query+=
				"<"+initialCube.getURI()+"> qb:dimensionValueCompatible ?linkspec." +
					"?linkspec qb:compatibleDimension <"+expansionDimension.getURI()+">."+
					"?linkspec qb:compatibleCube ?cube." +
					"OPTIONAL{GRAPH ?x{?cube skos:prefLabel|rdfs:label ?label.}}}";	
		
		if (initialCubeGraph != null) {
			getCompatibleCubes_query += "}";
		}
		
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}
		
		TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);

		List<LDResource> compatibleCubes = new ArrayList<LDResource>();

		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("cube").stringValue());
				
				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}
				
				//add each compatible cube once - not once for every label
				if(!compatibleCubes.contains(ldr)){
					compatibleCubes.add(ldr);
				}
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		
	
		List<LDResource> originalCubeDimValues=CubeSPARQL.getDimensionValues(
				expansionDimension.getURI(), "<"+initialCube.getURI()+">", initialCubeGraph,initialCubeDSDGraph,
				lang, defaultlang, ignoreLang, SPARQLservice);
		
		HashMap<LDResource, List<LDResource>> compatibleCubesAndDimValues=
				new HashMap<LDResource, List<LDResource>>();
		
		for(LDResource compCube:compatibleCubes){
			String compCubeURI = "<" + compCube.getURI() + ">";
			
			String compCubeGraph = CubeSPARQL.getCubeSliceGraph(compCubeURI,SPARQLservice);

			String compCubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
					compCubeURI, compCubeGraph, SPARQLservice);

			List<LDResource> compCubeDimValues=CubeSPARQL.getDimensionValues(
					expansionDimension.getURI(), compCubeURI, compCubeGraph,compCubeDSDGraph,
					lang, defaultlang, ignoreLang, SPARQLservice);
			List<LDResource> newValues = new ArrayList<LDResource>(compCubeDimValues);
			newValues.removeAll(originalCubeDimValues);
			compatibleCubesAndDimValues.put(compCube, newValues);
			
		}
				
		return compatibleCubesAndDimValues;
	}
	
	
	public static HashMap<LDResource, List<LDResource>> getLinkAddMeasureCompatibleCubes(
			LDResource initialCube,	String initialCubeGraph,String initialCubeDSDGraph,
			String lang, String defaultlang,
			boolean ignoreLang, String SPARQLservice){
		
		String getCompatibleCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "select distinct ?cube ?label where {";
		
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice	+ " {";
		}

		if (initialCubeGraph != null) {
			getCompatibleCubes_query += "GRAPH <" + initialCubeGraph + "> {";
		}
		
		getCompatibleCubes_query+=
				"<"+initialCube.getURI()+"> qb:measureCompatible ?cube." +
					"OPTIONAL{GRAPH ?x{?cube skos:prefLabel|rdfs:label ?label.}}}";	
		
		if (initialCubeGraph != null) {
			getCompatibleCubes_query += "}";
		}
		
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}
		
		TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);

		List<LDResource> compatibleCubes = new ArrayList<LDResource>();

		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("cube").stringValue());
				
				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}
				
				//add each compatible cube once - not once for every label
				if(!compatibleCubes.contains(ldr)){
					compatibleCubes.add(ldr);
				}
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		
	
		List<LDResource> originalCubeMeasures=CubeSPARQL.getDataCubeMeasure(
				"<"+initialCube.getURI()+">", initialCubeGraph,initialCubeDSDGraph,
				lang, defaultlang, ignoreLang, SPARQLservice);
		
		HashMap<LDResource, List<LDResource>> compatibleCubesAndMeasures=
				new HashMap<LDResource, List<LDResource>>();
		
		for(LDResource compCube:compatibleCubes){
			String compCubeURI = "<" + compCube.getURI() + ">";
			
			String compCubeGraph = CubeSPARQL.getCubeSliceGraph(compCubeURI,SPARQLservice);

			String compCubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
					compCubeURI, compCubeGraph, SPARQLservice);

			List<LDResource> compCubeMeasures=CubeSPARQL.getDataCubeMeasure(
					compCubeURI, compCubeGraph,compCubeDSDGraph,
					lang, defaultlang, ignoreLang, SPARQLservice);
			List<LDResource> newMeasures = new ArrayList<LDResource>(compCubeMeasures);
			newMeasures.removeAll(originalCubeMeasures);
			compatibleCubesAndMeasures.put(compCube, newMeasures);
			
		}
				
		return compatibleCubesAndMeasures;
	}
	
	// Get all compatible cubes
	// Compatible: - have the dimensions of the original cube
	// - the dimensions have the same skos:ConceptScheme
	// - the dimension instances are at the same xkos:ClassificationLevel
	// - All the dimension (except the expansion) have 100% overlap
	// - The expansion dimension has complement size > 0
	// - The measures have overlap 100%
	public static HashMap<LDResource, List<LDResource>> getAddValueToLevelCompatibleCubes(
			LDResource selectedCube, String selectedCubeGraph,
			String selectedCubeDSDgraph,
			Map<LDResource, Integer> allCubesAndDimCount,
			List<LDResource> cubeDimensions,
			HashMap<LDResource, List<LDResource>> dimensionsLevels,
			List<LDResource> cubeMeasures,
			HashMap<LDResource, List<LDResource>> dimensionsConceptSchemes,
			LDResource expansionDimension, double overlapThreshold,
			String selectedLanguage, String defaultLang, boolean ignoreLang,
			String SPARQLservice) {

		String getCompatibleCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "select distinct ?dataset ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice + " { ";
		}

		getCompatibleCubes_query += "GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet."
				+ "?dataset qb:structure ?dsd "
				+ "OPTIONAL{?dataset rdfs:label ?label}}";

		int i = 1;

		for (LDResource dim : cubeDimensions) {

			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"
					+ i + ".";

			// If there is a code list check the code list
			if (dimensionsConceptSchemes.get(dim) != null
					&& dimensionsConceptSchemes.get(dim).size() > 0) {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"
						+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes
						.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"
							+ conceptScheme.getURI() + ">.";
				}

				// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"
						+ dim.getURI() + ">.";
			}

			getCompatibleCubes_query += "}";
			i++;
		}

		i = 1;

		// Measures
		for (LDResource measure : cubeMeasures) {
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{"
					+ "?dsd qb:component ?measurecomp" + i + "."
					+ "?measurecomp" + i + " qb:measure <" + measure.getURI()
					+ ">.}";
		}

		getCompatibleCubes_query += "}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getCompatibleCubes_query);

		List<LDResource> potentialCompatibleCubes = new ArrayList<LDResource>();

		// The result is cube -> all values of the expansion dimension
		HashMap<LDResource, List<LDResource>> compatibleCubes = new HashMap<LDResource, List<LDResource>>();
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset")
						.stringValue());

				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}

				// A cube to be add value to level compatible must have the same
				// number of dimensions
				// The query returns cubes that have at least the same dims as
				// the original cube but may have more
				if (allCubesAndDimCount.get(ldr) == cubeDimensions.size()) {
					potentialCompatibleCubes.add(ldr);
				}
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
		LDResource obsvalue = new LDResource(
				"http://purl.org/linked-data/sdmx/2009/measure#obsValue");
		// EUROSTAT END //////////////////////

		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();

		// REMOVE THE MEASURE DIMENSION -- EUROSTAT
		List<LDResource> correctDims = new ArrayList<LDResource>(cubeDimensions);
		// correctDims.remove(indic);
		// END EUROSTAT /////////////////////

		for (LDResource cube : potentialCompatibleCubes) {
			// The above query returns all compatible cubes including the
			// original cube
			if (!cube.equals(selectedCube)) {

				// Get the original cube dimension values
				if (originalCubeDimensionsValues.keySet().isEmpty()) {
					originalCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(cubeDimensions,
									"<" + selectedCube.getURI() + ">",
									selectedCubeGraph, selectedCubeDSDgraph,
									false, selectedLanguage, defaultLang,
									ignoreLang, SPARQLservice);

					// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
					// cubeMeasures
					// .addAll(originalCubeDimensionsValues.get(indic));
					// cubeMeasures.remove(obsvalue);
					// EUROSTAT END //////////////////////
				}

				// Get the compatible Cube Graph
				String cubeGraph = CubeSPARQL.getCubeSliceGraph(
						"<" + cube.getURI() + ">", SPARQLservice);

				// Get the compatible Cube Structure graph
				String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"
						+ cube.getURI() + ">", cubeGraph, SPARQLservice);

				boolean levelFound = true;
				// check the potential compatible cube if it has the same levels
				// at dimensions
				for (LDResource dim : cubeDimensions) {
					for (LDResource level : dimensionsLevels.get(dim)) {
						if (!askDimensionLevelInDataCube(cube, level,
								cubeGraph, cubeDSDGraph, SPARQLservice)) {
							levelFound = false;
							break;
						}
					}
					if (!levelFound) {
						break;
					}
				}

				// If it has the same dimension levels
				if (levelFound) {

					// Get all Cube dimensions
					List<LDResource> compatibleCubeDimensions = CubeSPARQL
							.getDataCubeDimensions("<" + cube.getURI() + ">",
									cubeGraph, cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);

					HashMap<LDResource, List<LDResource>> compatibleCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(compatibleCubeDimensions,
									"<" + cube.getURI() + ">", cubeGraph,
									cubeDSDGraph, false, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);

					boolean isDimensionCompatible = true;
					// check if there is an intersection (at least one) at the
					// values of each dimension
					for (LDResource dim : correctDims) {

						List<LDResource> dimValues = originalCubeDimensionsValues
								.get(dim);
						List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues
								.get(dim);

						// For the expansion dimension we need the complement
						// size to be >0
						if (dim.equals(expansionDimension)) {
							List<LDResource> dimensionComplement = new ArrayList<LDResource>(
									compatibleCubeDimValues);
							dimensionComplement.removeAll(dimValues);

							// If the expansion dimension complement is empty ->
							// cubes are not compatible
							if (dimensionComplement.size() == 0) {
								isDimensionCompatible = false;
								break;
							}

							// For all other dimensions (except expansion) wee
							// need 100% overlap
						} else {
							List<LDResource> intersection = new ArrayList<LDResource>(
									dimValues);
							intersection.retainAll(compatibleCubeDimValues);
							double overlap = (double) intersection.size()
									/ (double) dimValues.size();

							// If overlap is under the theshold -> cubes are not
							// compatible
							if (overlap < overlapThreshold) {
								isDimensionCompatible = false;
								break;
							}
						}
					}

					// check measure
					if (isDimensionCompatible) {
						// Get the Cube measure
						List<LDResource> compMeasures = CubeSPARQL
								.getDataCubeMeasure("<" + cube.getURI() + ">",
										cubeGraph, cubeDSDGraph,
										selectedLanguage, defaultLang,
										ignoreLang, SPARQLservice);

						// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
						// compMeasures.addAll(compatibleCubeDimensionsValues.get(indic));
						// compMeasures.remove(obsvalue);
						// EUROSTAT END //////////////////////

						List<LDResource> measureIntersection = new ArrayList<LDResource>(
								cubeMeasures);
						measureIntersection.retainAll(compMeasures);

						double meausureOverlap = (double) measureIntersection
								.size() / (double) cubeMeasures.size();

						if (meausureOverlap >= overlapThreshold) {

							// RETURN ONLY THE NEW VALUES FOR EACH CUBE FOR THE
							// SELECTED DIM
							List<LDResource> newValues = new ArrayList<LDResource>(
									compatibleCubeDimensionsValues
											.get(expansionDimension));
							newValues.removeAll(originalCubeDimensionsValues
									.get(expansionDimension));
							compatibleCubes.put(cube, newValues);
						}

					}
				}
			}
		}
		System.out.println("Compatible cubes: "
				+ compatibleCubes.keySet().size());
		return compatibleCubes;
	}

	// Create Links for all add value to level compatible cubes
	// Compatible:
	// - have the dimensions of the original cube
	// - the dimensions have the same skos:ConceptScheme
	// - the dimension instances are at the same xkos:ClassificationLevel
	// - All the dimension (except the expansion) have 100% overlap
	// - The expansion dimension has complement size > 0
	// - The measures have overlap 100%
	public static int linkAddValueToLevelCompatibleCubes(
			LDResource selectedCube, String selectedCubeGraph,
			String selectedCubeDSDgraph,
			Map<LDResource, Integer> allCubesAndDimCount,
			List<LDResource> cubeDimensions,
			HashMap<LDResource, List<LDResource>> dimensionsLevels,
			List<LDResource> cubeMeasures,
			HashMap<LDResource, List<LDResource>> dimensionsConceptSchemes,
			LDResource expansionDimension, double overlapThreshold,
			String selectedLanguage, String defaultLang, boolean ignoreLang,
			String SPARQLservice) {

		String getCompatibleCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "select distinct ?dataset ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice + " { ";
		}

		getCompatibleCubes_query += "GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet."
				+ "?dataset qb:structure ?dsd "
				+ "OPTIONAL{?dataset rdfs:label ?label}}";

		int i = 1;

		for (LDResource dim : cubeDimensions) {

			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"
					+ i + ".";

			// If there is a code list check the code list
			if (dimensionsConceptSchemes.get(dim) != null
					&& dimensionsConceptSchemes.get(dim).size() > 0) {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"
						+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes
						.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"
							+ conceptScheme.getURI() + ">.";
				}

				// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"
						+ dim.getURI() + ">.";
			}

			getCompatibleCubes_query += "}";
			i++;
		}

		i = 1;

		// Measures
		for (LDResource measure : cubeMeasures) {
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{"
					+ "?dsd qb:component ?measurecomp" + i + "."
					+ "?measurecomp" + i + " qb:measure <" + measure.getURI()
					+ ">.}";
		}

		getCompatibleCubes_query += "}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getCompatibleCubes_query);

		List<LDResource> potentialCompatibleCubes = new ArrayList<LDResource>();

		// The result is cube -> all values of the expansion dimension
		int numberOfCompatibleCubes = 0;
		
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset")
						.stringValue());

				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}

				// A cube to be add value to level compatible must have the same
				// number of dimensions
				// The query returns cubes that have at least the same dims as
				// the original cube but may have more
				if (allCubesAndDimCount.get(ldr) == cubeDimensions.size()) {
					potentialCompatibleCubes.add(ldr);
				}
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
		LDResource obsvalue = new LDResource(
				"http://purl.org/linked-data/sdmx/2009/measure#obsValue");
		// EUROSTAT END //////////////////////

		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();

		// REMOVE THE MEASURE DIMENSION -- EUROSTAT
		List<LDResource> correctDims = new ArrayList<LDResource>(cubeDimensions);
		// correctDims.remove(indic);
		// END EUROSTAT /////////////////////

		for (LDResource cube : potentialCompatibleCubes) {
			// The above query returns all compatible cubes including the
			// original cube
			if (!cube.equals(selectedCube)) {

				// Get the original cube dimension values
				if (originalCubeDimensionsValues.keySet().isEmpty()) {
					originalCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(cubeDimensions,
									"<" + selectedCube.getURI() + ">",
									selectedCubeGraph, selectedCubeDSDgraph,
									false, selectedLanguage, defaultLang,
									ignoreLang, SPARQLservice);

					// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
					// cubeMeasures
					// .addAll(originalCubeDimensionsValues.get(indic));
					// cubeMeasures.remove(obsvalue);
					// EUROSTAT END //////////////////////
				}

				// Get the compatible Cube Graph
				String cubeGraph = CubeSPARQL.getCubeSliceGraph(
						"<" + cube.getURI() + ">", SPARQLservice);

				// Get the compatible Cube Structure graph
				String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"
						+ cube.getURI() + ">", cubeGraph, SPARQLservice);

				boolean levelFound = true;
				// check the potential compatible cube if it has the same levels
				// at dimensions
				for (LDResource dim : cubeDimensions) {
					for (LDResource level : dimensionsLevels.get(dim)) {
						if (!askDimensionLevelInDataCube(cube, level,
								cubeGraph, cubeDSDGraph, SPARQLservice)) {
							levelFound = false;
							break;
						}
					}
					if (!levelFound) {
						break;
					}
				}

				// If it has the same dimension levels
				if (levelFound) {

					// Get all Cube dimensions
					List<LDResource> compatibleCubeDimensions = CubeSPARQL
							.getDataCubeDimensions("<" + cube.getURI() + ">",
									cubeGraph, cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);

					HashMap<LDResource, List<LDResource>> compatibleCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(compatibleCubeDimensions,
									"<" + cube.getURI() + ">", cubeGraph,
									cubeDSDGraph, false, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);

					boolean isDimensionCompatible = true;
					// check if there is an intersection (at least one) at the
					// values of each dimension
					for (LDResource dim : correctDims) {

						List<LDResource> dimValues = originalCubeDimensionsValues
								.get(dim);
						List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues
								.get(dim);

						// For the expansion dimension we need the complement
						// size to be >0
						if (dim.equals(expansionDimension)) {
							List<LDResource> dimensionComplement = new ArrayList<LDResource>(
									compatibleCubeDimValues);
							dimensionComplement.removeAll(dimValues);

							// If the expansion dimension complement is empty ->
							// cubes are not compatible
							if (dimensionComplement.size() == 0) {
								isDimensionCompatible = false;
								break;
							}

							// For all other dimensions (except expansion) wee
							// need 100% overlap
						} else {
							List<LDResource> intersection = new ArrayList<LDResource>(
									dimValues);
							intersection.retainAll(compatibleCubeDimValues);
							double overlap = (double) intersection.size()
									/ (double) dimValues.size();

							// If overlap is under the theshold -> cubes are not
							// compatible
							if (overlap < overlapThreshold) {
								isDimensionCompatible = false;
								break;
							}
						}
					}

					// check measure
					if (isDimensionCompatible) {
						// Get the Cube measure
						List<LDResource> compMeasures = CubeSPARQL
								.getDataCubeMeasure("<" + cube.getURI() + ">",
										cubeGraph, cubeDSDGraph,
										selectedLanguage, defaultLang,
										ignoreLang, SPARQLservice);

						// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
						// compMeasures.addAll(compatibleCubeDimensionsValues.get(indic));
						// compMeasures.remove(obsvalue);
						// EUROSTAT END //////////////////////

						List<LDResource> measureIntersection = new ArrayList<LDResource>(
								cubeMeasures);
						measureIntersection.retainAll(compMeasures);

						double meausureOverlap = (double) measureIntersection
								.size() / (double) cubeMeasures.size();

						if (meausureOverlap >= overlapThreshold) {

							// create random link specification
							Random rand = new Random();
							long rnd = Math.abs(rand.nextLong());

							String linkSpecification = "<http://www.fluidops.com/resource/linkSpecification_"
									+ rnd + ">";

							// Add new link
							String create_link_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
									+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
									+ "INSERT DATA  {";

							if (SPARQLservice != null) {
								create_link_query += "SERVICE " + SPARQLservice
										+ " {";
							}

							if (selectedCubeGraph != null) {
								create_link_query += "GRAPH <"
										+ selectedCubeGraph + "> {";
							}

							// cube1 -> dimensionValueCompatible ->
							// linkspecification
							// linkspecification -> compatibleCube -> cube2
							// linkspecification -> compatibleDimension ->
							// expansionDimension
							create_link_query += linkSpecification
									+ " rdf:type qb:LinkSpecification. " + "<"
									+ selectedCube.getURI()
									+ "> qb:dimensionValueCompatible "
									+ linkSpecification + "."
									+ linkSpecification
									+ " qb:compatibleCube <" + cube.getURI()
									+ ">." + linkSpecification
									+ " qb:compatibleDimension <"
									+ expansionDimension.getURI() + ">}";

							if (cubeGraph != null) {
								create_link_query += "}";
							}

							if (SPARQLservice != null) {
								create_link_query += "}";
							}

							QueryExecutor.executeUPDATE(create_link_query);
							numberOfCompatibleCubes++;
						}
					}
				}
			}
		}

		return numberOfCompatibleCubes;
	}

	// Get all compatible cubes
	// Compatible: - have the dimensions of the original cube
	// - the dimensions have the same skos:ConceptScheme
	// - the dimension instances are at the same xkos:ClassificationLevel
	public static HashMap<LDResource, HashMap<LDResource, List<LDResource>>> getStatisticsForAddValueToLevelCompatibleCubes(
			LDResource selectedCube, String selectedCubeGraph,
			String selectedCubeDSDgraph,
			Map<LDResource, Integer> allCubesAndDimCount,
			List<LDResource> cubeDimensions,
			HashMap<LDResource, List<LDResource>> dimensionsLevels,
			List<LDResource> cubeMeasures,
			HashMap<LDResource, List<LDResource>> dimensionsConceptSchemes,
			String selectedLanguage, String defaultLang, boolean ignoreLang,
			String SPARQLservice) {

		String getCompatibleCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "select distinct ?dataset ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice + " { ";
		}

		getCompatibleCubes_query += "GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet."
				+ "?dataset qb:structure ?dsd "
				+ "OPTIONAL{?dataset rdfs:label ?label}}";

		int i = 1;

		for (LDResource dim : cubeDimensions) {

			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"
					+ i + ".";

			// If there is a code list check the code list
			if (dimensionsConceptSchemes.get(dim) != null
					&& dimensionsConceptSchemes.get(dim).size() > 0) {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"
						+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes
						.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"
							+ conceptScheme.getURI() + ">.";
				}
				// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"
						+ dim.getURI() + ">.";
			}

			getCompatibleCubes_query += "}";
			i++;
		}

		i = 1;

		// Measures
		for (LDResource measure : cubeMeasures) {
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?measurecomp"
					+ i
					+ "."
					+ "?measurecomp"
					+ i
					+ " qb:measure <"
					+ measure.getURI() + ">.}";
		}

		getCompatibleCubes_query += "}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getCompatibleCubes_query);

		List<LDResource> potentialCompatibleCubes = new ArrayList<LDResource>();

		HashMap<LDResource, HashMap<LDResource, List<LDResource>>> compatibleCubes = new HashMap<LDResource, HashMap<LDResource, List<LDResource>>>();
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset")
						.stringValue());

				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}

				// A cube to be add value to level compatible must have the same
				// number of dimensions
				// The query returns cubes that have at least the same dims as
				// the original cube
				// but may have more
				if (allCubesAndDimCount.get(ldr) == cubeDimensions.size()) {
					potentialCompatibleCubes.add(ldr);
				}

			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
		LDResource obsvalue = new LDResource(
				"http://purl.org/linked-data/sdmx/2009/measure#obsValue");
		// EUROSTAT END //////////////////////

		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();

		// REMOVE THE MEASURE DIMENSION -- EUROSTAT
		List<LDResource> correctDims = new ArrayList<LDResource>(cubeDimensions);
		// correctDims.remove(indic);
		// END EUROSTAT /////////////////////

		PrintWriter writer = null;
		try {
			writer = new PrintWriter(new BufferedWriter(new FileWriter(
					"cluster_overlap_complementarity.txt", true)));
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		PrintWriter writerLabels = null;
		try {
			writerLabels = new PrintWriter(new BufferedWriter(new FileWriter(
					"cluster_overlap_complementarity_labels.txt", true)));
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		String labels = "Cube 1 URI, Cube 2 URI,";
		for (LDResource d : correctDims) {
			labels += d.getURIorLabel() + " Overlap," + d.getURIorLabel()
					+ " Complement size,";
		}
		labels += "Measure Overlap, Measure Complement size";
		writerLabels.println(labels);
		writerLabels.close();

		for (LDResource cube : potentialCompatibleCubes) {
			// The above query returns all compatible cubes including the
			// original cube
			if (!cube.equals(selectedCube)) {
				String cubeLine = selectedCube.getURI() + "," + cube.getURI();

				// Get the original cube dimension values
				if (originalCubeDimensionsValues.keySet().isEmpty()) {
					originalCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(cubeDimensions,
									"<" + selectedCube.getURI() + ">",
									selectedCubeGraph, selectedCubeDSDgraph,
									false, selectedLanguage, defaultLang,
									ignoreLang, SPARQLservice);

					// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
					// cubeMeasures
					// .addAll(originalCubeDimensionsValues.get(indic));
					// cubeMeasures.remove(obsvalue);
					// EUROSTAT END //////////////////////
				}

				// Get the compatible Cube Graph
				String cubeGraph = CubeSPARQL.getCubeSliceGraph(
						"<" + cube.getURI() + ">", SPARQLservice);

				// Get the compatible Cube Structure graph
				String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"
						+ cube.getURI() + ">", cubeGraph, SPARQLservice);

				boolean levelFound = true;
				// check the potential compatible cube if it has the same levels
				// at dimensions
				for (LDResource dim : cubeDimensions) {
					for (LDResource level : dimensionsLevels.get(dim)) {
						if (!askDimensionLevelInDataCube(cube, level,
								cubeGraph, cubeDSDGraph, SPARQLservice)) {
							levelFound = false;
							break;
						}
					}
					if (!levelFound) {
						break;
					}
				}

				// If it has the same dimension levels
				if (levelFound) {

					// Get all Cube dimensions
					List<LDResource> compatibleCubeDimensions = CubeSPARQL
							.getDataCubeDimensions("<" + cube.getURI() + ">",
									cubeGraph, cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);

					HashMap<LDResource, List<LDResource>> compatibleCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(compatibleCubeDimensions,
									"<" + cube.getURI() + ">", cubeGraph,
									cubeDSDGraph, false, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);

					// boolean isDimensionCompatible=true;
					// check if there is an intersection (at least one) at the
					// values of each dimension
					for (LDResource dim : correctDims) {
						List<LDResource> dimValues = originalCubeDimensionsValues
								.get(dim);
						List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues
								.get(dim);
						List<LDResource> intersection = new ArrayList<LDResource>(
								dimValues);
						intersection.retainAll(compatibleCubeDimValues);

						List<LDResource> dimensionComplement = new ArrayList<LDResource>(
								compatibleCubeDimValues);
						dimensionComplement.removeAll(dimValues);

						double overlap = (double) intersection.size()
								/ (double) dimValues.size();

						cubeLine += "," + overlap + ","
								+ dimensionComplement.size();
						// if(overlap<dimensionOverlapThreshold){
						// isDimensionCompatible=false;
						// break;
						// }
					}

					// check measure

					// if(isDimensionCompatible){
					// Get the Cube measure
					List<LDResource> compMeasures = CubeSPARQL
							.getDataCubeMeasure("<" + cube.getURI() + ">",
									cubeGraph, cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);

					// FOR EUROSTAT ONLY -- COMMENT OUT IF NOT NEEDED
					// compMeasures.addAll(compatibleCubeDimensionsValues
					// .get(indic));
					// compMeasures.remove(obsvalue);
					// EUROSTAT END //////////////////////

					List<LDResource> measureIntersection = new ArrayList<LDResource>(
							cubeMeasures);
					measureIntersection.retainAll(compMeasures);
					double meausureOverlap = (double) measureIntersection
							.size() / (double) cubeMeasures.size();

					List<LDResource> measureComplement = new ArrayList<LDResource>(
							compMeasures);
					measureComplement.removeAll(cubeMeasures);

					cubeLine += "," + meausureOverlap + ","
							+ measureComplement.size();
					writer.println(cubeLine);
					/*
					 * List<LDResource> measureIntersection=new
					 * ArrayList<LDResource>(cubeMeasures);
					 * measureIntersection.retainAll(compMeasures); double
					 * meausureOverlap
					 * =(double)measureIntersection.size()/(double
					 * )cubeMeasures.size();
					 * 
					 * //FOR EACH COMPARISON WRITE MEASURE OVERLAP
					 * writer.println
					 * (selectedCube.getURI()+","+cube.getURI()+",MEASURE,"
					 * +meausureOverlap);
					 * 
					 * //We need the intersection to be under the defined
					 * threshold if(meausureOverlap<=measureOverlapThreashold){
					 * compatibleCubesAndMeasures.put(cube,compMeasures); }
					 */
					// }
				}
			}
		}
		writer.close();
		System.out.println("Compatible cubes: "
				+ compatibleCubes.keySet().size());
		return compatibleCubes;
	}

	// Get all compatible cubes
	// Compatible: - have the dimensions of the original cube
	// - the dimensions have the same skos:ConceptScheme
	// - the dimension instances are at the same xkos:ClassificationLevel
	public static HashMap<LDResource, HashMap<LDResource, List<LDResource>>> getClusterCubesAndDimValues(
			LDResource selectedCube, String selectedCubeGraph,
			String selectedCubeDSDgraph,
			Map<LDResource, Integer> allCubesAndDimCount,
			List<LDResource> cubeDimensions,
			HashMap<LDResource, List<LDResource>> dimensionsLevels,
			List<LDResource> cubeMeasures,
			HashMap<LDResource, List<LDResource>> dimensionsConceptSchemes,
			String selectedLanguage, String defaultLang, boolean ignoreLang,
			String SPARQLservice) {

		String getCompatibleCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "select distinct ?dataset ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice + " { ";
		}

		getCompatibleCubes_query += "GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet."
				+ "?dataset qb:structure ?dsd "
				+ "OPTIONAL{?dataset rdfs:label ?label}}";

		int i = 1;

		for (LDResource dim : cubeDimensions) {

			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"
					+ i + ".";

			// If there is a code list check the code list
			if (dimensionsConceptSchemes.get(dim) != null
					&& dimensionsConceptSchemes.get(dim).size() > 0) {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"
						+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes
						.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"
							+ conceptScheme.getURI() + ">.";
				}
				// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"
						+ dim.getURI() + ">.";
			}

			getCompatibleCubes_query += "}";
			i++;
		}

		i = 1;

		// Measures
		for (LDResource measure : cubeMeasures) {
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?measurecomp"
					+ i
					+ "."
					+ "?measurecomp"
					+ i
					+ " qb:measure <"
					+ measure.getURI() + ">.}";
		}

		getCompatibleCubes_query += "}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getCompatibleCubes_query);

		HashMap<LDResource, HashMap<LDResource, List<LDResource>>> clusterCubesAndDims = new HashMap<LDResource, HashMap<LDResource, List<LDResource>>>();
		List<LDResource> potentialCompatibleCubes = new ArrayList<LDResource>();
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset")
						.stringValue());

				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}

				// A cube to be add value to level compatible must have the same
				// number of dimensions
				// The query returns cubes that have at least the same dims as
				// the original cube
				// but may have more
				if (allCubesAndDimCount.get(ldr) == cubeDimensions.size()) {
					potentialCompatibleCubes.add(ldr);
				}

			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		for (LDResource cube : potentialCompatibleCubes) {

			// Get Cube Graph
			String cubeGraph = CubeSPARQL.getCubeSliceGraph("<" + cube.getURI()
					+ ">", SPARQLservice);

			// Get Cube Structure graph
			String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
					"<" + cube.getURI() + ">", cubeGraph, SPARQLservice);

			// Get all Cube dimensions
			List<LDResource> compatibleCubeDimensions = CubeSPARQL
					.getDataCubeDimensions("<" + cube.getURI() + ">",
							cubeGraph, cubeDSDGraph, selectedLanguage,
							defaultLang, ignoreLang, SPARQLservice);

			// the same number of dimensions
			// if(compatibleCubeDimensions.size()==cubeDimensions.size()){
			HashMap<LDResource, List<LDResource>> compatibleCubeDimensionsValues = CubeHandlingUtils
					.getDimsValues(compatibleCubeDimensions,
							"<" + cube.getURI() + ">", cubeGraph, cubeDSDGraph,
							false, selectedLanguage, defaultLang, ignoreLang,
							SPARQLservice);

			clusterCubesAndDims.put(cube, compatibleCubeDimensionsValues);
			// }
		}

		System.out.println("Compatible cubes: "
				+ clusterCubesAndDims.keySet().size());
		return clusterCubesAndDims;

	}

	// ASK if a cube contains data at a specific dimension level
	// (check if exists an observation with this value)
	public static boolean askDimensionLevelInDataCube(LDResource cube,
			LDResource dimensionLevel, String cubeGraph, String cubeDSDgraph,
			String SPARQLservice) {
		String askDimensionLevelInDataCube_query = ""
				+ "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "ASK where{";

		if (SPARQLservice != null) {
			askDimensionLevelInDataCube_query += "SERVICE " + SPARQLservice
					+ " {";
		}

		if (cubeGraph != null) {
			askDimensionLevelInDataCube_query += "GRAPH <" + cubeGraph + "> {";
		}

		askDimensionLevelInDataCube_query += "?obs qb:dataSet <"
				+ cube.getURI() + ">." + "?obs ?dim ?value.";

		if (cubeGraph != null) {
			askDimensionLevelInDataCube_query += "}";
		}

		if (cubeDSDgraph != null) {
			askDimensionLevelInDataCube_query += "GRAPH <" + cubeDSDgraph
					+ "> {";
		}

		askDimensionLevelInDataCube_query += "<" + dimensionLevel.getURI()
				+ "> skos:member ?value.}";

		if (cubeDSDgraph != null) {
			askDimensionLevelInDataCube_query += "}";
		}

		if (SPARQLservice != null) {
			askDimensionLevelInDataCube_query += "}";
		}

		return QueryExecutor.executeASK(askDimensionLevelInDataCube_query);
	}

	public static String mergeCubesAddAttributeValue(LDResource originalCube,
			LDResource expanderCube, LDResource expansionDim,
			String selectedLanguage, String defaultLang, boolean ignoreLang,
			String SPARQLservice) {

		// ///////// ORIGINAL CUBE ///////////////////////////

		String originalCubeURI = "<" + originalCube.getURI() + ">";

		// Get Original Cube Graph
		String originalCubeGraph = CubeSPARQL.getCubeSliceGraph(
				originalCubeURI, SPARQLservice);

		// Get original Cube Structure graph
		String originalCubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
				originalCubeURI, originalCubeGraph, SPARQLservice);

		// Get original Cube Dimensions
		List<LDResource> originalCubeDimensions = CubeSPARQL
				.getDataCubeDimensions(originalCubeURI, originalCubeGraph,
						originalCubeDSDGraph, selectedLanguage, defaultLang,
						ignoreLang, SPARQLservice);

		// Get original Cube Measures
		List<LDResource> originalCubeMeasures = CubeSPARQL.getDataCubeMeasure(
				originalCubeURI, originalCubeGraph, originalCubeDSDGraph,
				selectedLanguage, defaultLang, ignoreLang, SPARQLservice);

		HashMap<LDResource, List<LDResource>> originalCubeAllDimensionsValues = CubeHandlingUtils
				.getDimsValues(originalCubeDimensions, originalCubeURI,
						originalCubeGraph, originalCubeDSDGraph, false,
						selectedLanguage, defaultLang, ignoreLang,
						SPARQLservice);

		// ///////// EXPANDER CUBE ///////////////////////////

		String expanderCubeURI = "<" + expanderCube.getURI() + ">";

		// Get Original Cube Graph
		String expanderCubeGraph = CubeSPARQL.getCubeSliceGraph(
				expanderCubeURI, SPARQLservice);

		// Get original Cube Structure graph
		String expanderCubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
				expanderCubeURI, expanderCubeGraph, SPARQLservice);

		// Get original Cube Dimensions
		List<LDResource> expanderCubeDimensions = CubeSPARQL
				.getDataCubeDimensions(expanderCubeURI, expanderCubeGraph,
						expanderCubeDSDGraph, selectedLanguage, defaultLang,
						ignoreLang, SPARQLservice);

		// Get original Cube Measures
		List<LDResource> expanderCubeMeasures = CubeSPARQL.getDataCubeMeasure(
				expanderCubeURI, expanderCubeGraph, expanderCubeDSDGraph,
				selectedLanguage, defaultLang, ignoreLang, SPARQLservice);

		HashMap<LDResource, List<LDResource>> expanderCubeAllDimensionsValues = CubeHandlingUtils
				.getDimsValues(expanderCubeDimensions, expanderCubeURI,
						expanderCubeGraph, expanderCubeDSDGraph, false,
						selectedLanguage, defaultLang, ignoreLang,
						SPARQLservice);

		// create random DSD, Cube URI and Cube Graph URI
		Random rand = new Random();
		long rnd = Math.abs(rand.nextLong());

		String newDSD_URI = "<http://www.fluidops.com/resource/dsd_" + rnd
				+ ">";
		String newCube_URI = "<http://www.fluidops.com/resource/cube_" + rnd
				+ ">";
		String newCubeGraph_URI = "<http://www.fluidops.com/resource/graph_"
				+ rnd + ">";

		// Add new DSD
		// ADD THE NEW DSD AT THE ORIGINAL CUBE DSD GRAPH
		String create_new_merged_dsd_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_merged_dsd_query += "SERVICE " + SPARQLservice + " {";
		}

		if (originalCubeDSDGraph != null) {
			create_new_merged_dsd_query += "GRAPH <" + originalCubeDSDGraph
					+ "> {";
		}

		create_new_merged_dsd_query += newDSD_URI
				+ " rdf:type qb:DataStructureDefinition.";

		// Add dimensions to DSD
		for (LDResource ldr : originalCubeDimensions) {
			rnd = Math.abs(rand.nextLong());
			String newComponentSpecification_URI = "<http://www.fluidops.com/resource/componentSpecification_"
					+ rnd + ">";
			create_new_merged_dsd_query += newDSD_URI + " qb:component "
					+ newComponentSpecification_URI + "."
					+ newComponentSpecification_URI + " qb:dimension <"
					+ ldr.getURI() + ">.";
		}

		// //Set<LDResource> mergedMeasures=new
		// HashSet<LDResource>(originalCubeMeasures);
		// mergedMeasures.addAll(expanderCubeMeasures);
		// Add measures to DSD
		for (LDResource m : originalCubeMeasures) {
			rnd = Math.abs(rand.nextLong());
			String newComponentSpecification_URI = "<http://www.fluidops.com/resource/componentSpecification_"
					+ rnd + ">";
			create_new_merged_dsd_query += newDSD_URI + " qb:component "
					+ newComponentSpecification_URI + "."
					+ newComponentSpecification_URI + " qb:measure <"
					+ m.getURI() + ">.";
		}

		create_new_merged_dsd_query += "} ";

		if (originalCubeDSDGraph != null) {
			create_new_merged_dsd_query += "}";
		}

		if (SPARQLservice != null) {
			create_new_merged_dsd_query += "}";
		}

		QueryExecutor.executeUPDATE(create_new_merged_dsd_query);

		// GET ORIGINAL CUBE OBSERVATIONS
		String getOriginalCubeObservations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "Select ";
		int i = 1;

		// Add dimension variables to query
		for (LDResource ldr : originalCubeDimensions) {
			getOriginalCubeObservations_query += "?dim" + i + " ";
			i++;
		}

		i = 1;
		// Add dimension variables to query
		for (LDResource ldr : originalCubeMeasures) {
			getOriginalCubeObservations_query += "?measure" + i + " ";
			i++;
		}

		getOriginalCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "SERVICE " + SPARQLservice
					+ " {";
		}

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "GRAPH <" + originalCubeGraph
					+ "> {";
		}

		getOriginalCubeObservations_query += "?obs qb:dataSet "
				+ originalCubeURI + ".";

		i = 1;
		for (LDResource ldr : originalCubeDimensions) {
			getOriginalCubeObservations_query += "?obs <" + ldr.getURI()
					+ "> ?dim" + i + ".";
			i++;
		}

		i = 1;
		for (LDResource m : originalCubeMeasures) {
			getOriginalCubeObservations_query += "?obs <" + m.getURI()
					+ "> ?measure" + i + ".";
			i++;
		}

		getOriginalCubeObservations_query += "}";

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getOriginalCubeObservations_query);

		// CREATE NEW MERGED CUBE
		String create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_cube_query += "SERVICE " + SPARQLservice + " {";
		}

		create_new_cube_query += "GRAPH " + newCubeGraph_URI + " {"
				+ newCube_URI + " rdf:type qb:DataSet." + newCube_URI
				+ " qb:structure " + newDSD_URI + ".";

		int count = 0;
		int obsCount = 1;
		try {

			// Store original cube observations
			List<BindingSet> bs = new ArrayList<BindingSet>();

			while (res.hasNext()) {
				bs.add(res.next());
			}

			// Create new observations for the new merged cube.
			// Insert cubes in sets of 100

			for (BindingSet bindingSet : bs) {

				// Observation URI
				String newObservation_URI = "<http://www.fluidops.com/resource/observation_"
						+ obsCount + ">";
				create_new_cube_query += newObservation_URI
						+ " rdf:type qb:Observation." + newObservation_URI
						+ " qb:dataSet " + newCube_URI + ".";

				i = 1;
				for (LDResource ldr : originalCubeDimensions) {
					String dimValue = bindingSet.getValue("dim" + i)
							.stringValue();
					// Is URI
					if (dimValue.contains("http")) {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> <" + dimValue + ">.";

						// Is literal
					} else {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> \"" + dimValue + "\".";
					}
					i++;
				}

				i = 1;
				for (LDResource ldr : originalCubeMeasures) {
					String measureValue = bindingSet.getValue("measure" + i)
							.stringValue();
					// Is URI
					if (measureValue.contains("http")) {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> <" + measureValue + ">.";

						// Is literal
					} else {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> \"" + measureValue + "\".";
					}
					i++;
				}

				// If |observations|= 100 execute insert
				if (count == 100) {
					count = 0;
					create_new_cube_query += "}}";

					if (SPARQLservice != null) {
						create_new_cube_query += "}";
					}
					QueryExecutor.executeUPDATE(create_new_cube_query);

					// Initialize query to insert more observations
					create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
							+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
							+ "INSERT DATA  {";

					if (SPARQLservice != null) {
						create_new_cube_query += "SERVICE " + SPARQLservice
								+ " {";
					}

					create_new_cube_query += "GRAPH " + newCubeGraph_URI + " {";
				} else {
					count++;
				}
				obsCount++;
			}

		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		// If there are observations not yet inserted
		if (count > 0) {
			create_new_cube_query += "}}";

			if (SPARQLservice != null) {
				create_new_cube_query += "}";
			}
			QueryExecutor.executeUPDATE(create_new_cube_query);

		}

		// //////////////////////////////////////////////////////////

		// GET EXPANDER CUBE OBSERVATIONS
		String getExpanderCubeObservations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "Select ";
		i = 1;

		// Add dimension variables to query
		for (LDResource ldr : expanderCubeDimensions) {
			getExpanderCubeObservations_query += "?dim" + i + " ";
			i++;
		}

		// GET ONLY THE NEW MEASURES
		// ArrayList<LDResource> expandedMeasures=new
		// ArrayList<LDResource>(expanderCubeMeasures);
		// expandedMeasures.removeAll(originalCubeMeasures);
		i = 1;
		// Add measure variables to query
		for (LDResource ldr : expanderCubeMeasures) {
			getExpanderCubeObservations_query += "?measure" + i + " ";
			i++;
		}

		getExpanderCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "SERVICE " + SPARQLservice
					+ " {";
		}

		if (expanderCubeGraph != null) {
			getExpanderCubeObservations_query += "GRAPH <" + expanderCubeGraph
					+ "> {";
		}

		getExpanderCubeObservations_query += "?obs qb:dataSet "
				+ expanderCubeURI + ".";

		i = 1;
		for (LDResource ldr : expanderCubeDimensions) {
			getExpanderCubeObservations_query += "?obs <" + ldr.getURI()
					+ "> ?dim" + i + ".";
			i++;
		}

		i = 1;
		for (LDResource m : expanderCubeMeasures) {
			getExpanderCubeObservations_query += "?obs <" + m.getURI()
					+ "> ?measure" + i + ".";
			i++;
		}

		getExpanderCubeObservations_query += "}";

		if (originalCubeGraph != null) {
			getExpanderCubeObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "}";
		}

		res = QueryExecutor.executeSelect(getExpanderCubeObservations_query);

		// INSERT NEW MEAURES TO MERGED CUBE
		create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_cube_query += "SERVICE " + SPARQLservice + " {";
		}

		create_new_cube_query += "GRAPH " + newCubeGraph_URI + " {";

		count = 0;
		try {

			// Store expander cube observations
			List<BindingSet> bs = new ArrayList<BindingSet>();

			while (res.hasNext()) {
				bs.add(res.next());
			}

			// Create new observations for the new merged cube.
			// Insert cubes in sets of 100

			// Add the values of the expansion dimension of the expander cube
			List<LDResource> expansionDimNewValues = new ArrayList<LDResource>(
					expanderCubeAllDimensionsValues.get(expansionDim));

			// Remove the values of the expansion dimension of the original cube
			expansionDimNewValues.removeAll(originalCubeAllDimensionsValues
					.get(expansionDim));

			int expansionDimIndex = expanderCubeDimensions
					.indexOf(expansionDim) + 1;

			for (BindingSet bindingSet : bs) {

				// Get the value of the expansion dim of the observation
				LDResource expansionValueLdr = new LDResource(bindingSet
						.getValue("dim" + expansionDimIndex).stringValue());

				// If the observation has a NEW value at the expansion dimension
				if (expansionDimNewValues.contains(expansionValueLdr)) {

					// Observation URI
					String newObservation_URI = "<http://www.fluidops.com/resource/observation_"
							+ obsCount + ">";
					create_new_cube_query += newObservation_URI
							+ " rdf:type qb:Observation." + newObservation_URI
							+ " qb:dataSet " + newCube_URI + ".";

					i = 1;
					for (LDResource ldr : expanderCubeDimensions) {
						String dimValue = bindingSet.getValue("dim" + i)
								.stringValue();
						// Is URI
						if (dimValue.contains("http")) {
							create_new_cube_query += newObservation_URI + " <"
									+ ldr.getURI() + "> <" + dimValue + ">.";

							// Is literal
						} else {
							create_new_cube_query += newObservation_URI + " <"
									+ ldr.getURI() + "> \"" + dimValue + "\".";
						}
						i++;
					}

					// NA DW PWS THA BALW TA MULTIPLE MEASURES
					i = 1;
					for (LDResource ldr : expanderCubeMeasures) {
						String measureValue = bindingSet
								.getValue("measure" + i).stringValue();
						// Is URI
						if (measureValue.contains("http")) {
							create_new_cube_query += newObservation_URI + " <"
									+ ldr.getURI() + "> <" + measureValue
									+ ">.";

							// Is literal
						} else {
							create_new_cube_query += newObservation_URI + " <"
									+ ldr.getURI() + "> \"" + measureValue
									+ "\".";
						}
						i++;
					}

					// If |observations|= 100 execute insert
					if (count == 100) {
						count = 0;
						create_new_cube_query += "}}";

						if (SPARQLservice != null) {
							create_new_cube_query += "}";
						}
						QueryExecutor.executeUPDATE(create_new_cube_query);

						// Initialize query to insert more observations
						create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
								+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
								+ "INSERT DATA  {";

						if (SPARQLservice != null) {
							create_new_cube_query += "SERVICE " + SPARQLservice
									+ " {";
						}

						create_new_cube_query += "GRAPH " + newCubeGraph_URI
								+ " {";
					} else {
						count++;
					}
					obsCount++;
				}
			}

		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		// If there are observations not yet inserted
		if (count > 0) {
			create_new_cube_query += "}}";

			if (SPARQLservice != null) {
				create_new_cube_query += "}";
			}
			QueryExecutor.executeUPDATE(create_new_cube_query);
		}

		return newCube_URI;
	}

	public static String mergeCubesAddMeasure(LDResource originalCube,
			LDResource expanderCube, String selectedLanguage,
			String defaultLang, boolean ignoreLang, String SPARQLservice) {

		// ///////// ORIGINAL CUBE ///////////////////////////

		String originalCubeURI = "<" + originalCube.getURI() + ">";

		// Get Original Cube Graph
		String originalCubeGraph = CubeSPARQL.getCubeSliceGraph(
				originalCubeURI, SPARQLservice);

		// Get original Cube Structure graph
		String originalCubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
				originalCubeURI, originalCubeGraph, SPARQLservice);

		// Get original Cube Dimensions
		List<LDResource> originalCubeDimensions = CubeSPARQL
				.getDataCubeDimensions(originalCubeURI, originalCubeGraph,
						originalCubeDSDGraph, selectedLanguage, defaultLang,
						ignoreLang, SPARQLservice);

		// Get original Cube Measures
		List<LDResource> originalCubeMeasures = CubeSPARQL.getDataCubeMeasure(
				originalCubeURI, originalCubeGraph, originalCubeDSDGraph,
				selectedLanguage, defaultLang, ignoreLang, SPARQLservice);

		// ///////// EXPANDER CUBE ///////////////////////////

		String expanderCubeURI = "<" + expanderCube.getURI() + ">";

		// Get Original Cube Graph
		String expanderCubeGraph = CubeSPARQL.getCubeSliceGraph(
				expanderCubeURI, SPARQLservice);

		// Get original Cube Structure graph
		String expanderCubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
				expanderCubeURI, expanderCubeGraph, SPARQLservice);

		// Get original Cube Dimensions
		List<LDResource> expanderCubeDimensions = CubeSPARQL
				.getDataCubeDimensions(expanderCubeURI, expanderCubeGraph,
						expanderCubeDSDGraph, selectedLanguage, defaultLang,
						ignoreLang, SPARQLservice);

		// Get original Cube Measures
		List<LDResource> expanderCubeMeasures = CubeSPARQL.getDataCubeMeasure(
				expanderCubeURI, expanderCubeGraph, expanderCubeDSDGraph,
				selectedLanguage, defaultLang, ignoreLang, SPARQLservice);

		// create random DSD, Cube URI and Cube Graph URI
		Random rand = new Random();
		long rnd = Math.abs(rand.nextLong());

		String newDSD_URI = "<http://www.fluidops.com/resource/dsd_" + rnd
				+ ">";
		String newCube_URI = "<http://www.fluidops.com/resource/cube_" + rnd
				+ ">";
		String newCubeGraph_URI = "<http://www.fluidops.com/resource/graph_"
				+ rnd + ">";

		// Add new DSD
		// ADD THE NEW DSD AT THE ORIGINAL CUBE DSD GRAPH
		String create_new_merged_dsd_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_merged_dsd_query += "SERVICE " + SPARQLservice + " {";
		}

		if (originalCubeDSDGraph != null) {
			create_new_merged_dsd_query += "GRAPH <" + originalCubeDSDGraph
					+ "> {";
		}

		create_new_merged_dsd_query += newDSD_URI
				+ " rdf:type qb:DataStructureDefinition.";

		// Add dimensions to DSD
		for (LDResource ldr : originalCubeDimensions) {
			rnd = Math.abs(rand.nextLong());
			String newComponentSpecification_URI = "<http://www.fluidops.com/resource/componentSpecification_"
					+ rnd + ">";
			create_new_merged_dsd_query += newDSD_URI + " qb:component "
					+ newComponentSpecification_URI + "."
					+ newComponentSpecification_URI + " qb:dimension <"
					+ ldr.getURI() + ">.";
		}

		Set<LDResource> mergedMeasures = new HashSet<LDResource>(
				originalCubeMeasures);
		mergedMeasures.addAll(expanderCubeMeasures);
		// Add measures to DSD
		for (LDResource m : mergedMeasures) {
			rnd = Math.abs(rand.nextLong());
			String newComponentSpecification_URI = "<http://www.fluidops.com/resource/componentSpecification_"
					+ rnd + ">";
			create_new_merged_dsd_query += newDSD_URI + " qb:component "
					+ newComponentSpecification_URI + "."
					+ newComponentSpecification_URI + " qb:measure <"
					+ m.getURI() + ">.";
		}

		create_new_merged_dsd_query += "} ";

		if (originalCubeDSDGraph != null) {
			create_new_merged_dsd_query += "}";
		}

		if (SPARQLservice != null) {
			create_new_merged_dsd_query += "}";
		}

		QueryExecutor.executeUPDATE(create_new_merged_dsd_query);

		// GET ORIGINAL CUBE OBSERVATIONS
		String getOriginalCubeObservations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "Select ";
		int i = 1;

		// Add dimension variables to query
		for (LDResource ldr : originalCubeDimensions) {
			getOriginalCubeObservations_query += "?dim" + i + " ";
			i++;
		}

		i = 1;
		// Add dimension variables to query
		for (LDResource ldr : originalCubeMeasures) {
			getOriginalCubeObservations_query += "?measure" + i + " ";
			i++;
		}

		getOriginalCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "SERVICE " + SPARQLservice
					+ " {";
		}

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "GRAPH <" + originalCubeGraph
					+ "> {";
		}

		getOriginalCubeObservations_query += "?obs qb:dataSet "
				+ originalCubeURI + ".";

		i = 1;
		for (LDResource ldr : originalCubeDimensions) {
			getOriginalCubeObservations_query += "?obs <" + ldr.getURI()
					+ "> ?dim" + i + ".";
			i++;
		}

		i = 1;
		for (LDResource m : originalCubeMeasures) {
			getOriginalCubeObservations_query += "?obs <" + m.getURI()
					+ "> ?measure" + i + ".";
			i++;
		}

		getOriginalCubeObservations_query += "}";

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getOriginalCubeObservations_query);

		// CREATE NEW MERGED CUBE
		String create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_cube_query += "SERVICE " + SPARQLservice + " {";
		}

		create_new_cube_query += "GRAPH " + newCubeGraph_URI + " {"
				+ newCube_URI + " rdf:type qb:DataSet." + newCube_URI
				+ " qb:structure " + newDSD_URI + ".";

		int count = 0;
		int obsCount = 1;
		try {

			// Store original cube observations
			List<BindingSet> bs = new ArrayList<BindingSet>();

			while (res.hasNext()) {
				bs.add(res.next());
			}

			// Create new observations for the new merged cube.
			// Insert cubes in sets of 100

			for (BindingSet bindingSet : bs) {

				// Observation URI
				String newObservation_URI = "<http://www.fluidops.com/resource/observation_"
						+ obsCount + ">";
				create_new_cube_query += newObservation_URI
						+ " rdf:type qb:Observation." + newObservation_URI
						+ " qb:dataSet " + newCube_URI + ".";

				i = 1;
				for (LDResource ldr : originalCubeDimensions) {
					String dimValue = bindingSet.getValue("dim" + i)
							.stringValue();
					// Is URI
					if (dimValue.contains("http")) {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> <" + dimValue + ">.";

						// Is literal
					} else {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> \"" + dimValue + "\".";
					}
					i++;
				}

				i = 1;
				for (LDResource ldr : originalCubeMeasures) {
					String measureValue = bindingSet.getValue("measure" + i)
							.stringValue();
					// Is URI
					if (measureValue.contains("http")) {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> <" + measureValue + ">.";

						// Is literal
					} else {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> \"" + measureValue + "\".";
					}
					i++;
				}

				// If |observations|= 100 execute insert
				if (count == 100) {
					count = 0;
					create_new_cube_query += "}}";

					if (SPARQLservice != null) {
						create_new_cube_query += "}";
					}
					QueryExecutor.executeUPDATE(create_new_cube_query);

					// Initialize query to insert more observations
					create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
							+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
							+ "INSERT DATA  {";

					if (SPARQLservice != null) {
						create_new_cube_query += "SERVICE " + SPARQLservice
								+ " {";
					}

					create_new_cube_query += "GRAPH " + newCubeGraph_URI + " {";
				} else {
					count++;
				}
				obsCount++;
			}

		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		// If there are observations not yet inserted
		if (count > 0) {
			create_new_cube_query += "}}";

			if (SPARQLservice != null) {
				create_new_cube_query += "}";
			}
			QueryExecutor.executeUPDATE(create_new_cube_query);

		}

		// //////////////////////////////////////////////////////////

		// GET EXPANDER CUBE OBSERVATIONS
		String getExpanderCubeObservations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "Select ";
		i = 1;

		// Add dimension variables to query
		for (LDResource ldr : expanderCubeDimensions) {
			getExpanderCubeObservations_query += "?dim" + i + " ";
			i++;
		}

		// GET ONLY THE NEW MEASURES
		ArrayList<LDResource> expandedMeasures = new ArrayList<LDResource>(
				expanderCubeMeasures);
		// expandedMeasures.removeAll(originalCubeMeasures);
		i = 1;
		// Add measure variables to query
		for (LDResource ldr : expandedMeasures) {
			getExpanderCubeObservations_query += "?measure" + i + " ";
			i++;
		}

		getExpanderCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "SERVICE " + SPARQLservice
					+ " {";
		}

		if (expanderCubeGraph != null) {
			getExpanderCubeObservations_query += "GRAPH <" + expanderCubeGraph
					+ "> {";
		}

		getExpanderCubeObservations_query += "?obs qb:dataSet "
				+ expanderCubeURI + ".";

		i = 1;
		for (LDResource ldr : expanderCubeDimensions) {
			getExpanderCubeObservations_query += "?obs <" + ldr.getURI()
					+ "> ?dim" + i + ".";
			i++;
		}

		i = 1;
		for (LDResource m : expandedMeasures) {
			getExpanderCubeObservations_query += "?obs <" + m.getURI()
					+ "> ?measure" + i + ".";
			i++;
		}

		getExpanderCubeObservations_query += "}";

		if (originalCubeGraph != null) {
			getExpanderCubeObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "}";
		}

		res = QueryExecutor.executeSelect(getExpanderCubeObservations_query);

		// INSERT NEW MEAURES TO MERGED CUBE
		create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_cube_query += "SERVICE " + SPARQLservice + " {";
		}

		create_new_cube_query += "GRAPH " + newCubeGraph_URI + " {";

		count = 0;
		try {

			// Store expander cube observations
			List<BindingSet> bs = new ArrayList<BindingSet>();

			while (res.hasNext()) {
				bs.add(res.next());
			}

			// Create new observations for the new merged cube.
			// Insert cubes in sets of 100

			for (BindingSet bindingSet : bs) {

				// Observation URI
				String newObservation_URI = "<http://www.fluidops.com/resource/observation_"
						+ obsCount + ">";
				create_new_cube_query += newObservation_URI
						+ " rdf:type qb:Observation." + newObservation_URI
						+ " qb:dataSet " + newCube_URI + ".";

				i = 1;
				for (LDResource ldr : expanderCubeDimensions) {
					String dimValue = bindingSet.getValue("dim" + i)
							.stringValue();
					// Is URI
					if (dimValue.contains("http")) {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> <" + dimValue + ">.";

						// Is literal
					} else {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> \"" + dimValue + "\".";
					}
					i++;
				}

				// NA DW PWS THA BALW TA MULTIPLE MEASURES
				i = 1;
				for (LDResource ldr : expandedMeasures) {
					String measureValue = bindingSet.getValue("measure" + i)
							.stringValue();
					// Is URI
					if (measureValue.contains("http")) {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> <" + measureValue + ">.";

						// Is literal
					} else {
						create_new_cube_query += newObservation_URI + " <"
								+ ldr.getURI() + "> \"" + measureValue + "\".";
					}
					i++;
				}

				// If |observations|= 100 execute insert
				if (count == 100) {
					count = 0;
					create_new_cube_query += "}}";

					if (SPARQLservice != null) {
						create_new_cube_query += "}";
					}
					QueryExecutor.executeUPDATE(create_new_cube_query);

					// Initialize query to insert more observations
					create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
							+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
							+ "INSERT DATA  {";

					if (SPARQLservice != null) {
						create_new_cube_query += "SERVICE " + SPARQLservice
								+ " {";
					}

					create_new_cube_query += "GRAPH " + newCubeGraph_URI + " {";
				} else {
					count++;
				}
				obsCount++;

			}

		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		// If there are observations not yet inserted
		if (count > 0) {
			create_new_cube_query += "}}";

			if (SPARQLservice != null) {
				create_new_cube_query += "}";
			}
			QueryExecutor.executeUPDATE(create_new_cube_query);
		}

		return newCube_URI;
	}
	
	
	public static String mergeCubesAddAttributeValueMinimal(LDResource originalCube,
			LDResource expanderCube, LDResource expansionDim,
			String selectedLanguage, String defaultLang, boolean ignoreLang,
			String SPARQLservice) {

		/////////////////////  ORIGINAL CUBE ///////////////////////////

		String originalCubeURI = "<" + originalCube.getURI() + ">";

		// Get Original Cube Graph
		String originalCubeGraph = CubeSPARQL.getCubeSliceGraph(
				originalCubeURI, SPARQLservice);

		// Get original Cube Structure graph
		String originalCubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
				originalCubeURI, originalCubeGraph, SPARQLservice);
		
		String originalCubeDSD=CubeSPARQL.getCubeDSD(originalCubeURI, originalCubeGraph);
		
		List<LDResource> originalCubeExpansionDimensionValues=
				CubeSPARQL.getDimensionValues(expansionDim.getURI(),
						originalCubeURI, originalCubeGraph, originalCubeDSDGraph,
						selectedLanguage, defaultLang, ignoreLang, SPARQLservice); 
		
		// ///////// EXPANDER CUBE ///////////////////////////

		String expanderCubeURI = "<" + expanderCube.getURI() + ">";

		// Get Original Cube Graph
		String expanderCubeGraph = CubeSPARQL.getCubeSliceGraph(
				expanderCubeURI, SPARQLservice);

		// Get original Cube Structure graph
		String expanderCubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
				expanderCubeURI, expanderCubeGraph, SPARQLservice);

		// Get original Cube Dimensions
		List<LDResource> expanderCubeDimensions = CubeSPARQL
				.getDataCubeDimensions(expanderCubeURI, expanderCubeGraph,
						expanderCubeDSDGraph, selectedLanguage, defaultLang,
						ignoreLang, SPARQLservice);

		// Get original Cube Measures
		List<LDResource> expanderCubeMeasures = CubeSPARQL.getDataCubeMeasure(
				expanderCubeURI, expanderCubeGraph, expanderCubeDSDGraph,
				selectedLanguage, defaultLang, ignoreLang, SPARQLservice);

		
		List<LDResource>expanderCubeExpansionDimensionValues=
				CubeSPARQL.getDimensionValues(expansionDim.getURI(),
						expanderCubeURI, expanderCubeGraph, expanderCubeDSDGraph,
						selectedLanguage, defaultLang, ignoreLang, SPARQLservice); 
		
		// create random DSD, Cube URI and Cube Graph URI
		Random rand = new Random();
		long rnd = Math.abs(rand.nextLong());

	
		String newCube_URI = "<http://www.fluidops.com/resource/cube_" + rnd+ ">";
	
		// GET ORIGINAL CUBE OBSERVATIONS
		String getOriginalCubeObservations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "Select ?obs ";
		int i = 1;

		getOriginalCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "SERVICE " + SPARQLservice
					+ " {";
		}

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "GRAPH <" + originalCubeGraph
					+ "> {";
		}

		getOriginalCubeObservations_query += "?obs qb:dataSet "	+ originalCubeURI + ".";

		getOriginalCubeObservations_query += "}";

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getOriginalCubeObservations_query);

		// CREATE NEW MERGED CUBE
		String create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_cube_query += "SERVICE " + SPARQLservice + " {";
		}

		//The new cube has the same structure as the original
		create_new_cube_query += "GRAPH <" +originalCubeGraph  + "> {"
				+ newCube_URI + " rdf:type qb:DataSet." + newCube_URI
				+ " qb:structure <" + originalCubeDSD + ">.";

		int count = 0;
		int obsCount = 1;
		try {

			// Store original cube observations
			List<BindingSet> bs = new ArrayList<BindingSet>();

			while (res.hasNext()) {
				bs.add(res.next());
			}

			// Create new observations for the new merged cube.
			// Insert cubes in sets of 100

			for (BindingSet bindingSet : bs) {
			
				create_new_cube_query +=  "<"+bindingSet.getValue("obs")
						+ "> qb:dataSet " + newCube_URI + ".";
			
				// If |observations|= 100 execute insert
				if (create_new_cube_query.length()>195000) {
					count = 0;
					create_new_cube_query += "}}";

					if (SPARQLservice != null) {
						create_new_cube_query += "}";
					}
					System.out.println(create_new_cube_query.length());
					QueryExecutor.executeUPDATE(create_new_cube_query);
					
					// Initialize query to insert more observations
					create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
							+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
							+ "INSERT DATA  {";

					if (SPARQLservice != null) {
						create_new_cube_query += "SERVICE " + SPARQLservice
								+ " {";
					}

					create_new_cube_query += "GRAPH <" + originalCubeGraph + "> {";
				} else {
					count++;
				}
				obsCount++;
			}

		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		// If there are observations not yet inserted
		if (count > 0) {
			create_new_cube_query += "}}";

			if (SPARQLservice != null) {
				create_new_cube_query += "}";
			}
			QueryExecutor.executeUPDATE(create_new_cube_query);

		}

		// //////////////////////////////////////////////////////////

		// GET EXPANDER CUBE OBSERVATIONS
		String getExpanderCubeObservations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "Select ";
		i = 1;

		// Add dimension variables to query
		for (LDResource ldr : expanderCubeDimensions) {
			getExpanderCubeObservations_query += "?dim" + i + " ";
			i++;
		}

		// GET ONLY THE NEW MEASURES
		i = 1;
		// Add measure variables to query
		for (LDResource ldr : expanderCubeMeasures) {
			getExpanderCubeObservations_query += "?measure" + i + " ";
			i++;
		}

		getExpanderCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "SERVICE " + SPARQLservice
					+ " {";
		}

		if (expanderCubeGraph != null) {
			getExpanderCubeObservations_query += "GRAPH <" + expanderCubeGraph
					+ "> {";
		}

		getExpanderCubeObservations_query += "?obs qb:dataSet "
				+ expanderCubeURI + ".";

		i = 1;
		for (LDResource ldr : expanderCubeDimensions) {
			getExpanderCubeObservations_query += "?obs <" + ldr.getURI()
					+ "> ?dim" + i + ".";
			i++;
		}

		i = 1;
		for (LDResource m : expanderCubeMeasures) {
			getExpanderCubeObservations_query += "?obs <" + m.getURI()
					+ "> ?measure" + i + ".";
			i++;
		}

		getExpanderCubeObservations_query += "}";

		if (expanderCubeGraph != null) {
			getExpanderCubeObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "}";
		}

		res = QueryExecutor.executeSelect(getExpanderCubeObservations_query);

		// INSERT NEW MEAURES TO MERGED CUBE
		create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_cube_query += "SERVICE " + SPARQLservice + " {";
		}

		create_new_cube_query += "GRAPH <" + originalCubeGraph + "> {";

		count = 0;
		try {

			// Store expander cube observations
			List<BindingSet> bs = new ArrayList<BindingSet>();

			while (res.hasNext()) {
				bs.add(res.next());
			}

			// Create new observations for the new merged cube.
			// Insert cubes in sets of 100

			// Add the values of the expansion dimension of the expander cube
			List<LDResource> expansionDimNewValues = new ArrayList<LDResource>(
					expanderCubeExpansionDimensionValues);

			// Remove the values of the expansion dimension of the original cube
			expansionDimNewValues.removeAll(originalCubeExpansionDimensionValues);

			int expansionDimIndex = expanderCubeDimensions
					.indexOf(expansionDim) + 1;

			for (BindingSet bindingSet : bs) {

				// Get the value of the expansion dim of the observation
				LDResource expansionValueLdr = new LDResource(bindingSet
						.getValue("dim" + expansionDimIndex).stringValue());

				// If the observation has a NEW value at the expansion dimension
				if (expansionDimNewValues.contains(expansionValueLdr)) {

					// Observation URI
					String newObservation_URI = "<http://www.fluidops.com/resource/observation_"
							+ obsCount + ">";
					create_new_cube_query += newObservation_URI
							+ " rdf:type qb:Observation." + newObservation_URI
							+ " qb:dataSet " + newCube_URI + ".";

					i = 1;
					for (LDResource ldr : expanderCubeDimensions) {
						String dimValue = bindingSet.getValue("dim" + i)
								.stringValue();
						// Is URI
						if (dimValue.contains("http")) {
							create_new_cube_query += newObservation_URI + " <"
									+ ldr.getURI() + "> <" + dimValue + ">.";

						// Is literal
						} else {
							create_new_cube_query += newObservation_URI + " <"
									+ ldr.getURI() + "> \"" + dimValue + "\".";
						}
						i++;
					}

					// NA DW PWS THA BALW TA MULTIPLE MEASURES
					i = 1;
					for (LDResource ldr : expanderCubeMeasures) {
						String measureValue = bindingSet
								.getValue("measure" + i).stringValue();
						// Is URI
						if (measureValue.contains("http")) {
							create_new_cube_query += newObservation_URI + " <"
									+ ldr.getURI() + "> <" + measureValue
									+ ">.";

							// Is literal
						} else {
							create_new_cube_query += newObservation_URI + " <"
									+ ldr.getURI() + "> \"" + measureValue
									+ "\".";
						}
						i++;
					}

					// If |observations|= 100 execute insert
					//MAX SPARQL SIZE 200000
					if (create_new_cube_query.length()>195000) {
						count = 0;
						create_new_cube_query += "}}";

						if (SPARQLservice != null) {
							create_new_cube_query += "}";
						}
						QueryExecutor.executeUPDATE(create_new_cube_query);

						System.out.println(create_new_cube_query.length());
						// Initialize query to insert more observations
						create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
								+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
								+ "INSERT DATA  {";

						if (SPARQLservice != null) {
							create_new_cube_query += "SERVICE " + SPARQLservice
									+ " {";
						}

						create_new_cube_query += "GRAPH <" + originalCubeGraph + "> {";
					} else {
						count++;
					}
					obsCount++;
				}
			}

		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		// If there are observations not yet inserted
		if (count > 0) {
			create_new_cube_query += "}}";

			if (SPARQLservice != null) {
				create_new_cube_query += "}";
			}
			QueryExecutor.executeUPDATE(create_new_cube_query);
		}

		return newCube_URI;
	}
	
	public static String mergeCubesAddAttributeValueExtraMinimal(LDResource originalCube,
			LDResource expanderCube, LDResource expansionDim,
			String selectedLanguage, String defaultLang, boolean ignoreLang,
			String SPARQLservice) {

		/////////////////////  ORIGINAL CUBE ///////////////////////////

		String originalCubeURI = "<" + originalCube.getURI() + ">";

		// Get Original Cube Graph
		String originalCubeGraph = CubeSPARQL.getCubeSliceGraph(
				originalCubeURI, SPARQLservice);

		// Get original Cube Structure graph
		String originalCubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
				originalCubeURI, originalCubeGraph, SPARQLservice);
		
		String originalCubeDSD=CubeSPARQL.getCubeDSD(originalCubeURI, originalCubeGraph);

		
		List<LDResource> originalCubeExpansionDimensionValues=
				CubeSPARQL.getDimensionValues(expansionDim.getURI(),
						originalCubeURI, originalCubeGraph, originalCubeDSDGraph,
						selectedLanguage, defaultLang, ignoreLang, SPARQLservice); 
	

		// ///////// EXPANDER CUBE ///////////////////////////

		String expanderCubeURI = "<" + expanderCube.getURI() + ">";

		// Get Original Cube Graph
		String expanderCubeGraph = CubeSPARQL.getCubeSliceGraph(
				expanderCubeURI, SPARQLservice);

		// Get original Cube Structure graph
		String expanderCubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
				expanderCubeURI, expanderCubeGraph, SPARQLservice);

		// Get original Cube Dimensions
		List<LDResource> expanderCubeDimensions = CubeSPARQL
				.getDataCubeDimensions(expanderCubeURI, expanderCubeGraph,
						expanderCubeDSDGraph, selectedLanguage, defaultLang,
						ignoreLang, SPARQLservice);

		// Get original Cube Measures
		List<LDResource> expanderCubeMeasures = CubeSPARQL.getDataCubeMeasure(
				expanderCubeURI, expanderCubeGraph, expanderCubeDSDGraph,
				selectedLanguage, defaultLang, ignoreLang, SPARQLservice);

		
		List<LDResource>expanderCubeExpansionDimensionValues=
				CubeSPARQL.getDimensionValues(expansionDim.getURI(),
						expanderCubeURI, expanderCubeGraph, expanderCubeDSDGraph,
						selectedLanguage, defaultLang, ignoreLang, SPARQLservice); 
		
		// create random DSD, Cube URI and Cube Graph URI
		Random rand = new Random();
		long rnd = Math.abs(rand.nextLong());


		String newCube_URI = "<http://www.fluidops.com/resource/cube_" + rnd+ ">";

		// GET ORIGINAL CUBE OBSERVATIONS
		String getOriginalCubeObservations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "Select ?obs ";
		int i = 1;

/*		// Add dimension variables to query
		for (LDResource ldr : originalCubeDimensions) {
			getOriginalCubeObservations_query += "?dim" + i + " ";
			i++;
		}

		i = 1;
		// Add dimension variables to query
		for (LDResource ldr : originalCubeMeasures) {
			getOriginalCubeObservations_query += "?measure" + i + " ";
			i++;
		}*/

		getOriginalCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "SERVICE " + SPARQLservice
					+ " {";
		}

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "GRAPH <" + originalCubeGraph
					+ "> {";
		}

		getOriginalCubeObservations_query += "?obs qb:dataSet "	+ originalCubeURI + ".";

	
		getOriginalCubeObservations_query += "}";

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getOriginalCubeObservations_query);

		// CREATE NEW MERGED CUBE
		String create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_cube_query += "SERVICE " + SPARQLservice + " {";
		}

		//The new cube has the same structure as the original
		create_new_cube_query += "GRAPH <" +originalCubeGraph  + "> {"
				+ newCube_URI + " rdf:type qb:DataSet." + newCube_URI
				+ " qb:structure <" + originalCubeDSD + ">.";

		int count = 0;
		int obsCount = 1;
		try {

			// Store original cube observations
			List<BindingSet> bs = new ArrayList<BindingSet>();

			while (res.hasNext()) {
				bs.add(res.next());
			}

			// Create new observations for the new merged cube.
			// Insert cubes in sets of 100

			for (BindingSet bindingSet : bs) {

				create_new_cube_query +=  "<"+bindingSet.getValue("obs")
						+ "> qb:dataSet " + newCube_URI + ".";
		
				// If |observations|= 100 execute insert
				if (create_new_cube_query.length()>195000) {
					count = 0;
					create_new_cube_query += "}}";

					if (SPARQLservice != null) {
						create_new_cube_query += "}";
					}
					System.out.println(create_new_cube_query.length());
					QueryExecutor.executeUPDATE(create_new_cube_query);
					
					// Initialize query to insert more observations
					create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
							+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
							+ "INSERT DATA  {";

					if (SPARQLservice != null) {
						create_new_cube_query += "SERVICE " + SPARQLservice
								+ " {";
					}

					create_new_cube_query += "GRAPH <" + originalCubeGraph + "> {";
				} else {
					count++;
				}
				obsCount++;
			}

		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		// If there are observations not yet inserted
		if (count > 0) {
			create_new_cube_query += "}}";

			if (SPARQLservice != null) {
				create_new_cube_query += "}";
			}
			QueryExecutor.executeUPDATE(create_new_cube_query);

		}

		// //////////////////////////////////////////////////////////

		// GET EXPANDER CUBE OBSERVATIONS
		String getExpanderCubeObservations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "Select ?obs ?expvalue ";
		
		getExpanderCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "SERVICE " + SPARQLservice
					+ " {";
		}

		if (expanderCubeGraph != null) {
			getExpanderCubeObservations_query += "GRAPH <" + expanderCubeGraph
					+ "> {";
		}

		getExpanderCubeObservations_query += "?obs qb:dataSet "	+ expanderCubeURI + ".";

		getExpanderCubeObservations_query += "?obs <" + expansionDim.getURI()	+ "> ?expvalue.";
		
		getExpanderCubeObservations_query += "}";

		if (expanderCubeGraph != null) {
			getExpanderCubeObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "}";
		}

		res = QueryExecutor.executeSelect(getExpanderCubeObservations_query);

		// INSERT NEW MEAURES TO MERGED CUBE
		create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_cube_query += "SERVICE " + SPARQLservice + " {";
		}

		create_new_cube_query += "GRAPH <" + originalCubeGraph + "> {";

		count = 0;
		try {

			// Store expander cube observations
			List<BindingSet> bs = new ArrayList<BindingSet>();

			while (res.hasNext()) {
				bs.add(res.next());
			}

			// Create new observations for the new merged cube.
			// Insert cubes in sets of 100

			// Add the values of the expansion dimension of the expander cube
			List<LDResource> expansionDimNewValues = new ArrayList<LDResource>(
					expanderCubeExpansionDimensionValues);

			// Remove the values of the expansion dimension of the original cube
			expansionDimNewValues.removeAll(originalCubeExpansionDimensionValues);

			int expansionDimIndex = expanderCubeDimensions
					.indexOf(expansionDim) + 1;

			for (BindingSet bindingSet : bs) {

				// Get the value of the expansion dim of the observation
				LDResource expansionValueLdr = new LDResource(bindingSet.getValue("expvalue").stringValue());

				// If the observation has a NEW value at the expansion dimension
				if (expansionDimNewValues.contains(expansionValueLdr)) {


					create_new_cube_query +=  "<"+bindingSet.getValue("obs")+ "> qb:dataSet " + newCube_URI + ".";
				
					// If |observations|= 100 execute insert
					//MAX SPARQL SIZE 200000
					if (create_new_cube_query.length()>195000) {
						count = 0;
						create_new_cube_query += "}}";

						if (SPARQLservice != null) {
							create_new_cube_query += "}";
						}
						QueryExecutor.executeUPDATE(create_new_cube_query);

						System.out.println(create_new_cube_query.length());
						// Initialize query to insert more observations
						create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
								+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
								+ "INSERT DATA  {";

						if (SPARQLservice != null) {
							create_new_cube_query += "SERVICE " + SPARQLservice
									+ " {";
						}

						create_new_cube_query += "GRAPH <" + originalCubeGraph + "> {";
					} else {
						count++;
					}
					obsCount++;
				}
			}

		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		// If there are observations not yet inserted
		if (count > 0) {
			create_new_cube_query += "}}";

			if (SPARQLservice != null) {
				create_new_cube_query += "}";
			}
			QueryExecutor.executeUPDATE(create_new_cube_query);
		}

		return newCube_URI;
	}


}
