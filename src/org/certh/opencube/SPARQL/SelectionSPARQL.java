package org.certh.opencube.SPARQL;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.certh.opencube.utils.CubeHandlingUtils;
import org.certh.opencube.utils.LDResource;
import org.openrdf.model.Literal;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

public class SelectionSPARQL {

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

		TupleQueryResult res = QueryExecutor.executeSelect(getAllAvailableCubes_query);

		ArrayList<LDResource> allCubes = new ArrayList<LDResource>();

		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
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
	
	
	public static List<LDResource> getMaximalCubesAndSlices(String lang,
		String defaultlang,	boolean ignoreLang,String SPARQLservice) {

		String getAllAvailableCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX opencube: <http://opencube-project.eu/>"
				+ "SELECT DISTINCT ?dataset ?aggset " +
				"?label (COUNT(DISTINCT ?dim) AS ?dimcount) where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAllAvailableCubes_query += "SERVICE " + SPARQLservice + " { ";
		}
		
		getAllAvailableCubes_query += "GRAPH ?cubeGraph{{?dataset rdf:type qb:DataSet." +
				"?dataset qb:structure ?dsd. }" +
				"UNION {?dataset rdf:type qb:Slice} "
				+ "OPTIONAL{?dataset rdfs:label ?label.}} " +
				"GRAPH ?cubeDSDgraph{?dsd qb:component ?comp.?comp qb:dimension ?dim." +
				"OPTIONAL{?dataset qb:aggregationSet|opencube:aggregationSet ?aggset.}}}";
				

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAllAvailableCubes_query += "}";
		}

		getAllAvailableCubes_query +="GROUP by ?dataset ?aggset ?label";		
		TupleQueryResult res = QueryExecutor.executeSelect(getAllAvailableCubes_query);
		ArrayList<LDResource> allCubes = new ArrayList<LDResource>();		
		HashMap<LDResource, LDResource> mapAggsetCube=new HashMap<LDResource, LDResource>();
		HashMap<LDResource, Integer> mapAggsetMaxDimCount=new HashMap<LDResource, Integer>();

		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				if(bindingSet.getValue("dataset")!=null){
					LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());					
					if (bindingSet.getValue("label") != null) {
						ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));				
					}									
					if(bindingSet.getValue("aggset") != null){
						LDResource aggset_ldr = new LDResource(bindingSet.getValue("aggset").stringValue());						
						if(mapAggsetMaxDimCount.get(aggset_ldr)!=null){
							Integer dimcount= new Integer(bindingSet.getValue("dimcount").stringValue());
							int existingDimCount=mapAggsetMaxDimCount.get(aggset_ldr);
							if(dimcount>=existingDimCount){
								LDResource existing_ldr=mapAggsetCube.get(aggset_ldr);									
								if(existing_ldr.getLanguage()!=null&&
										ldr.getLanguage()!=null&&
										existing_ldr.equals(ldr)){
									//The new ldr has the preferred language
									if(ldr.getLanguage().equals(lang)){
										mapAggsetCube.put(aggset_ldr, ldr);
										mapAggsetMaxDimCount.put(aggset_ldr, dimcount);
									//The new ldr has the default language and the existing does 
									//not have the preferred language
									}else if (ldr.getLanguage().equals(defaultlang)&&
											!existing_ldr.getLanguage().equals(lang)){
										mapAggsetCube.put(aggset_ldr, ldr);
										mapAggsetMaxDimCount.put(aggset_ldr, dimcount);
									}
								}else if(ldr.getLanguage()!=null){
									mapAggsetCube.put(aggset_ldr, ldr);
									mapAggsetMaxDimCount.put(aggset_ldr, dimcount);
								}else  if((existing_ldr.getLanguage()==null&&
										ldr.getLanguage()==null)){
									mapAggsetCube.put(aggset_ldr, ldr);
									mapAggsetMaxDimCount.put(aggset_ldr, dimcount);
									
								}							
							}
						}else{
							Integer dimcount= new Integer(bindingSet.getValue("dimcount").stringValue());
							mapAggsetCube.put(aggset_ldr, ldr);
							mapAggsetMaxDimCount.put(aggset_ldr, dimcount);
							
						}
					}else{
						boolean found=false;
						for(LDResource existing_ldr:allCubes){
							if(ldr.equals(existing_ldr)){
								found=true;
								if(existing_ldr.getLanguage()!=null&&
										ldr.getLanguage()!=null&&
										existing_ldr.equals(ldr)){
									//The new ldr has the preferred language
									if(ldr.getLanguage().equals(lang)){
										allCubes.remove(existing_ldr);
										allCubes.add(ldr);
									//The new ldr has the default language and the existing does 
									//not have the preferred language
									}else if (ldr.getLanguage().equals(defaultlang)&&
											!existing_ldr.getLanguage().equals(lang)){
										allCubes.remove(existing_ldr);
										allCubes.add(ldr);
									}
								}else if(ldr.getLanguage()!=null){
									allCubes.remove(existing_ldr);
									allCubes.add(ldr);
								}else  if((existing_ldr.getLanguage()==null&&
										ldr.getLanguage()==null)){
									allCubes.remove(existing_ldr);
									allCubes.add(ldr);									
								}					
							}
						}						
						if(!found){
							allCubes.add(ldr);
						}
					}	
				}
			}			
			for(LDResource aggsetLdr:mapAggsetCube.keySet()){
				allCubes.add(mapAggsetCube.get(aggsetLdr));
			}			
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return allCubes;
	}
	

	public static Map<LDResource, Integer> getAllAvailableCubesAndDimCountWithLinks(
			String SPARQLservice) {

		String getAllAvailableCubesAndDimCount_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX opencube: <http://opencube-project.eu/> "
				+ "select distinct ?dataset (COUNT(DISTINCT ?dim) AS ?dimcount) ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAllAvailableCubesAndDimCount_query += "SERVICE " + SPARQLservice	+ " { ";
		}

		getAllAvailableCubesAndDimCount_query += "GRAPH ?cubeGraph{"
				+ "?dataset rdf:type qb:DataSet.?dataset qb:structure ?dsd." +
				"?dataset qb:dimensionValueCompatible|qb:measureCompatible|" +
				"opencube:dimensionValueCompatible|opencube:measureCompatible ?compCube." //get only cubes that have links
				+ "OPTIONAL{?dataset rdfs:label ?label.}}"
				+ "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp.?comp qb:dimension ?dim.}}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAllAvailableCubesAndDimCount_query += "}";
		}

		getAllAvailableCubesAndDimCount_query += "GROUP by ?dataset ?label";
		TupleQueryResult res = QueryExecutor.executeSelect(getAllAvailableCubesAndDimCount_query);
		Map<LDResource, Integer> allCubesAndDimCount = new HashMap<LDResource, Integer>();
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}
				allCubesAndDimCount.put(ldr, Integer.parseInt(bindingSet.getValue("dimcount").stringValue()));
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return allCubesAndDimCount;
	}
	
	public static Map<LDResource, Integer> getAllAvailableCubesAndDimCount(String SPARQLservice) {
		String getAllAvailableCubesAndDimCount_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "select distinct ?dataset (COUNT(DISTINCT ?dim) AS ?dimcount) ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAllAvailableCubesAndDimCount_query += "SERVICE " + SPARQLservice	+ " { ";
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
		TupleQueryResult res = QueryExecutor.executeSelect(getAllAvailableCubesAndDimCount_query);
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
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp" + i + ".";
			// If there is a code list check the code list
			if (dimensionsConceptSchemes.get(dim) != null
					&& dimensionsConceptSchemes.get(dim).size() > 0) {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"	+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"
							+ conceptScheme.getURI() + ">.";
				}
			// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"	+ dim.getURI() + ">.";
			}
			getCompatibleCubes_query += "}";
			i++;
		}

		getCompatibleCubes_query += "}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);

		List<LDResource> compatibleCubes = new ArrayList<LDResource>();

		HashMap<LDResource, List<LDResource>> compatibleCubesAndMeasures = new HashMap<LDResource, List<LDResource>>();
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}
				// A cube to be measure compatible must have the same number of dimensions
				// The query returns cubes that have at least the same dims as
				// the original cube but may have more
				if (allCubesAndDimCount.get(ldr) == cubeDimensions.size()) {
					compatibleCubes.add(ldr);
				}
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();
		
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
						if (!CubeSPARQL.askDimensionLevelInDataCube(
								"<"+ cube.getURI() + ">", level,
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
					List<LDResource> correctDims = new ArrayList<LDResource>(cubeDimensions);					

					for (LDResource dim : correctDims) {
						List<LDResource> dimValues = originalCubeDimensionsValues.get(dim);
						List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues
								.get(dim);
						List<LDResource> intersection = new ArrayList<LDResource>(dimValues);
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
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"
							+ conceptScheme.getURI() + ">.";
				}
				// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"	+ dim.getURI() + ">.";
			}
			getCompatibleCubes_query += "}";
			i++;
		}

		getCompatibleCubes_query += "}";
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);
		List<LDResource> compatibleCubes = new ArrayList<LDResource>();
		int numberOfCompatibleCubes = 0;
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}

				// A cube to be measure compatible must have the same number of dimensions
				// The query returns cubes that have at least the same dims as
				// the original cube but may have more
				if (allCubesAndDimCount.get(ldr) == cubeDimensions.size()) {
					compatibleCubes.add(ldr);
				}

			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();
		for (LDResource cube : compatibleCubes) {
			// The above query returns all compatible cubes including the original cube
			if (!cube.equals(selectedCube)) {
				// Get the original cube dimension values
				if (originalCubeDimensionsValues.keySet().isEmpty()) {
					originalCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(cubeDimensions,
									"<" + selectedCube.getURI() + ">",
									selectedCubeGraph, selectedCubeDSDgraph,
									false, selectedLanguage, defaultLang,
									ignoreLang, SPARQLservice);
				}
				// Get the compatible Cube Graph
				String cubeGraph = CubeSPARQL.getCubeSliceGraph(
						"<" + cube.getURI() + ">", SPARQLservice);

				// Get the compatible Cube Structure graph
				String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"
						+ cube.getURI() + ">", cubeGraph, SPARQLservice);

				boolean levelFound = true;
				// check the potential compatible cube if it has the same levels at dimensions
				for (LDResource dim : cubeDimensions) {
					for (LDResource level : dimensionsLevels.get(dim)) {
						if (!CubeSPARQL.askDimensionLevelInDataCube(
								"<"+ cube.getURI() + ">", level,
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
					List<LDResource> correctDims = new ArrayList<LDResource>(
							cubeDimensions);					

					for (LDResource dim : correctDims) {
						List<LDResource> dimValues = originalCubeDimensionsValues.get(dim);
						List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues.get(dim);
						List<LDResource> intersection = new ArrayList<LDResource>(dimValues);
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

						// get the measure complement
						// need to have at least one new measure
						List<LDResource> measureComplement = new ArrayList<LDResource>(compMeasures);
						measureComplement.removeAll(cubeMeasures);
						if (measureComplement.size() > 0) {							
							// Add new link
							String create_link_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
									+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
									+ "PREFIX opencube: <http://opencube-project.eu/>"
									+ "INSERT DATA  {";

							if (SPARQLservice != null) {
								create_link_query += "SERVICE " + SPARQLservice	+ " {";
							}

							if (selectedCubeGraph != null) {
								create_link_query += "GRAPH <" + selectedCubeGraph + "> {";
							}

							// cube1 -> measureCompatible -> cube2
							create_link_query += 
									"<"+ selectedCube.getURI()+ "> opencube:measureCompatible <" + cube.getURI()+ ">}";

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
	
	
	public static int linkMeasureCompatibleCubesAndLevels(LDResource selectedCube,
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
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"
							+ conceptScheme.getURI() + ">.";
				}
			// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"+ dim.getURI() + ">.";
			}
			getCompatibleCubes_query += "}";
			i++;
		}
		getCompatibleCubes_query += "}";
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);
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

				// A cube to be measure compatible must have the same number of dimensions
				// The query returns cubes that have at least the same dims as
				// the original cube but may have more
				if (allCubesAndDimCount.get(ldr) == cubeDimensions.size()) {
					compatibleCubes.add(ldr);
				}
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();
		Map<LDResource, List<LDResource>>originalLevelsValues=new HashMap<LDResource, List<LDResource>>();
		
		for (LDResource cube : compatibleCubes) {
			// The above query returns all compatible cubes including the original cube
			if (!cube.equals(selectedCube)) {
				// Get the original cube dimension values
				if (originalCubeDimensionsValues.keySet().isEmpty()) {
					originalCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(cubeDimensions,
									"<" + selectedCube.getURI() + ">",
									selectedCubeGraph, selectedCubeDSDgraph,
									false, selectedLanguage, defaultLang,
									ignoreLang, SPARQLservice);
												
					//Get all the values for each level (level -> values[])
					//To get the values use the originalCubeDimensionsValues
					for(LDResource dim:originalCubeDimensionsValues.keySet()){
						for(LDResource value:originalCubeDimensionsValues.get(dim)){
							//if there is a level at the value put the value at the list
					
							if(value.getLevel()!=null){
								LDResource thisLevel=new LDResource(value.getLevel());
								List<LDResource> currentLevelValues=originalLevelsValues.get(thisLevel);
								//If there are no values for the level yet
								if(currentLevelValues==null){
									currentLevelValues=new ArrayList<LDResource>();
								}
								currentLevelValues.add(value);
								Collections.sort(currentLevelValues);
								originalLevelsValues.put(thisLevel, currentLevelValues);
							}
						}
					}	
				}
				
				// Get the compatible Cube Graph
				String cubeGraph = CubeSPARQL.getCubeSliceGraph("<" + cube.getURI() + ">", SPARQLservice);
				// Get the compatible Cube Structure graph
				String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"
						+ cube.getURI() + ">", cubeGraph, SPARQLservice);					
				boolean levelFound = true;
				Map<LDResource,List<LDResource>> commonLevelsPerDim=new HashMap<LDResource, List<LDResource>>();
				
				// check the potential compatible cube if it has the same levels at dimensions
				for (LDResource dim : cubeDimensions) {
					
					//If there are levels at the original cube
					if (dimensionsLevels.get(dim).size()>0){
						List<LDResource> commonLevels=new ArrayList<LDResource>();
						for (LDResource level : dimensionsLevels.get(dim)) {
							if (CubeSPARQL.askDimensionLevelInDataCube(
									"<"+ cube.getURI() + ">", level,
									cubeGraph, cubeDSDGraph, SPARQLservice)) {
								commonLevels.add(level);
							}
						}					
						commonLevelsPerDim.put(dim, commonLevels);						
						//No common levels found
						if(commonLevels.size()==0){
							levelFound=false;
							break;
						}						
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
					
					Map<LDResource, List<LDResource>>compatibleLevelsValues=new HashMap<LDResource, List<LDResource>>();
					
					//Get all the values for each level (level -> values[])
					//To get the values use the compatibleCubeDimensionsValues
					for(LDResource dim:compatibleCubeDimensionsValues.keySet()){
						for(LDResource value:compatibleCubeDimensionsValues.get(dim)){
							//if there is a level at the value put the value at the list					
							if(value.getLevel()!=null){
								LDResource thisLevel=new LDResource(value.getLevel());
								List<LDResource> currentLevelValues=compatibleLevelsValues.get(thisLevel);
								//If there are no values for the level yet
								if(currentLevelValues==null){
									currentLevelValues=new ArrayList<LDResource>();
								}
								currentLevelValues.add(value);
								Collections.sort(currentLevelValues);
								compatibleLevelsValues.put(thisLevel, currentLevelValues);
							}
						}
					}

					boolean isDimensionCompatible = true;
					// check if there is an intersection (at least one) at the
					// values of each dimension				
					for (LDResource dim : cubeDimensions) {						
						if(dimensionsLevels.get(dim).size()>0){
							List<LDResource> commonLevels=commonLevelsPerDim.get(dim);
							for(LDResource commonLevel:commonLevels){
								List<LDResource> levelValues = originalLevelsValues.get(commonLevel);
								List<LDResource> compatibleCubeLevelValues = compatibleLevelsValues.get(commonLevel);
								List<LDResource> intersection = new ArrayList<LDResource>(levelValues);
								intersection.retainAll(compatibleCubeLevelValues);
								double overlap = (double) intersection.size()/ (double) levelValues.size();
								if (overlap < dimensionOverlapThreshold) {
									isDimensionCompatible = false;
									break;
								}								
							}							
							if(!isDimensionCompatible){
								break;								
							}							
						}else{
							List<LDResource> dimValues = originalCubeDimensionsValues.get(dim);
							List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues.get(dim);
							List<LDResource> intersection = new ArrayList<LDResource>(dimValues);
							intersection.retainAll(compatibleCubeDimValues);
							double overlap = (double) intersection.size()/ (double) dimValues.size();
							if (overlap < dimensionOverlapThreshold) {
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
										cubeGraph, cubeDSDGraph,selectedLanguage,
										defaultLang,ignoreLang, SPARQLservice);						

						// get the measure complement need to have at least one new measure
						List<LDResource> measureComplement = new ArrayList<LDResource>(compMeasures);
						measureComplement.removeAll(cubeMeasures);
						if (measureComplement.size() > 0) {					
							// Add new link
							String create_link_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
									+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
									+ "PREFIX opencube: <http://opencube-project.eu/>"
									+ "INSERT DATA  {";

							if (SPARQLservice != null) {
								create_link_query += "SERVICE " + SPARQLservice	+ " {";
							}

							if (selectedCubeGraph != null) {
								create_link_query += "GRAPH <" + selectedCubeGraph + "> {";
							}

							// cube1 -> measureCompatible -> cube2
							create_link_query += "<"+ selectedCube.getURI()+
									"> opencube:measureCompatible <" + cube.getURI()+ ">}";

							if (cubeGraph != null) {
								create_link_query += "}";							}

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
			String initialCubeGraph,String initialCubeDSDGraph,	LDResource expansionDimension,String lang, 
			String defaultlang, boolean ignoreLang, String SPARQLservice){
		
		String getCompatibleCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX opencube: <http://opencube-project.eu/> "
				+ "select distinct ?cube ?label where {";
		
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice	+ " {";
		}

		if (initialCubeGraph != null) {
			getCompatibleCubes_query += "GRAPH <" + initialCubeGraph + "> {";
		}
		
		getCompatibleCubes_query+=
				"<"+initialCube.getURI()+"> qb:dimensionValueCompatible|opencube:dimensionValueCompatible ?linkspec." +
					"?linkspec qb:compatibleDimension|opencube:compatibleDimension <"+expansionDimension.getURI()+">."+
					"?linkspec qb:compatibleCube|opencube:compatibleCube ?cube." +
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
					ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));				
				}
				
				//Add the first instance of the dimension (regardless of the language)
				if(!compatibleCubes.contains(ldr)){
					compatibleCubes.add(ldr);
				}else{
					//If ignore language
					if(ignoreLang){
						//First en then everything else
						if(ldr.getLanguage().equals("en")){
							compatibleCubes.remove(ldr);
							compatibleCubes.add(ldr);
						}
					}else{
						for(LDResource exisitingLdr:compatibleCubes){
							//Find the existing dimension that has the same URI (different language)
							if(exisitingLdr.equals(ldr)){
								//The new ldr has the preferred language
								if(ldr.getLanguage().equals(lang)){
									compatibleCubes.remove(ldr);
									compatibleCubes.add(ldr);
								//The new ldr has the default language and the existing does 
								//not have the preferred language
								}else if (ldr.getLanguage().equals(defaultlang)&&
									!exisitingLdr.getLanguage().equals(lang)){
									compatibleCubes.remove(ldr);
									compatibleCubes.add(ldr);
								}
							}
						}
					}
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
			String lang, String defaultlang, boolean ignoreLang, String SPARQLservice){
		
		String getCompatibleCubes_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX opencube: <http://opencube-project.eu/> "
				+ "select distinct ?cube ?label where {";
		
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice	+ " {";
		}

		if (initialCubeGraph != null) {
			getCompatibleCubes_query += "GRAPH <" + initialCubeGraph + "> {";
		}
		
		getCompatibleCubes_query+=
				"<"+initialCube.getURI()+"> qb:measureCompatible|opencube:measureCompatible ?cube." +
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
					ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));					
				}
				
				//Add the first instance of the dimension (regardless of the language)
				if(!compatibleCubes.contains(ldr)){
					compatibleCubes.add(ldr);
				}else{
					//If ignore language
					if(ignoreLang){
						//First en then everything else
						if(ldr.getLanguage().equals("en")){
							compatibleCubes.remove(ldr);
							compatibleCubes.add(ldr);
						}
					}else{
						for(LDResource exisitingLdr:compatibleCubes){
							//Find the existing dimension that has the same URI (different language)
							if(exisitingLdr.equals(ldr)){
								//The new ldr has the preferred language
								if(ldr.getLanguage().equals(lang)){
									compatibleCubes.remove(ldr);
									compatibleCubes.add(ldr);
								//The new ldr has the default language and the existing does 
								//not have the preferred language
								}else if (ldr.getLanguage().equals(defaultlang)&&
									!exisitingLdr.getLanguage().equals(lang)){
									compatibleCubes.remove(ldr);
									compatibleCubes.add(ldr);
								}
							}
						}
					}
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
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"+ i + ".";
			// If there is a code list check the code list
			if (dimensionsConceptSchemes.get(dim) != null
					&& dimensionsConceptSchemes.get(dim).size() > 0) {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes.get(dim)) {
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
					+ "?measurecomp" + i + " qb:measure <" + measure.getURI()+ ">.}";
		}

		getCompatibleCubes_query += "}";
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);
		List<LDResource> potentialCompatibleCubes = new ArrayList<LDResource>();

		// The result is cube -> all values of the expansion dimension
		HashMap<LDResource, List<LDResource>> compatibleCubes = new HashMap<LDResource, List<LDResource>>();
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
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
		
		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();
		List<LDResource> correctDims = new ArrayList<LDResource>(cubeDimensions);		

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
						if (!CubeSPARQL.askDimensionLevelInDataCube(
								"<"+ cube.getURI() + ">", level,
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
						List<LDResource> dimValues = originalCubeDimensionsValues.get(dim);
						List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues.get(dim);

						// For the expansion dimension we need the complement size to be >0
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
						// For all other dimensions (except expansion) we need 100% overlap
						} else {
							List<LDResource> intersection = new ArrayList<LDResource>(dimValues);
							intersection.retainAll(compatibleCubeDimValues);
							double overlap = (double) intersection.size()/ (double) dimValues.size();
							// If overlap is under the theshold -> cubes are not compatible
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
						List<LDResource> measureIntersection = new ArrayList<LDResource>(cubeMeasures);
						measureIntersection.retainAll(compMeasures);
						double meausureOverlap = (double) measureIntersection.size() / (double) cubeMeasures.size();

						if (meausureOverlap >= overlapThreshold) {

							// RETURN ONLY THE NEW VALUES FOR EACH CUBE FOR THE SELECTED DIM
							List<LDResource> newValues = new ArrayList<LDResource>(
									compatibleCubeDimensionsValues.get(expansionDimension));
							newValues.removeAll(originalCubeDimensionsValues.get(expansionDimension));
							compatibleCubes.put(cube, newValues);
						}
					}
				}
			}
		}
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
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"+ i + ".";

			// If there is a code list check the code list
			if (dimensionsConceptSchemes.get(dim) != null
					&& dimensionsConceptSchemes.get(dim).size() > 0) {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"+ conceptScheme.getURI() + ">.";
				}
			// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"	+ dim.getURI() + ">.";
			}
			getCompatibleCubes_query += "}";
			i++;
		}

		i = 1;
		// Measures
		for (LDResource measure : cubeMeasures) {
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{"
					+ "?dsd qb:component ?measurecomp" + i + "."
					+ "?measurecomp" + i + " qb:measure <" + measure.getURI()+ ">.}";
		}

		getCompatibleCubes_query += "}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);
		List<LDResource> potentialCompatibleCubes = new ArrayList<LDResource>();

		// The result is cube -> all values of the expansion dimension
		int numberOfCompatibleCubes = 0;		
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
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
		
		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();
		List<LDResource> correctDims = new ArrayList<LDResource>(cubeDimensions);
		
		for (LDResource cube : potentialCompatibleCubes) {
			// The above query returns all compatible cubes including the original cube
			if (!cube.equals(selectedCube)) {
				// Get the original cube dimension values
				if (originalCubeDimensionsValues.keySet().isEmpty()) {
					originalCubeDimensionsValues = CubeHandlingUtils
							.getDimsValues(cubeDimensions,
									"<" + selectedCube.getURI() + ">",
									selectedCubeGraph, selectedCubeDSDgraph,
									false, selectedLanguage, defaultLang,
									ignoreLang, SPARQLservice);					
				}

				// Get the compatible Cube Graph
				String cubeGraph = CubeSPARQL.getCubeSliceGraph(
						"<" + cube.getURI() + ">", SPARQLservice);

				// Get the compatible Cube Structure graph
				String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"
						+ cube.getURI() + ">", cubeGraph, SPARQLservice);				
				
				boolean levelFound = true;
				Map<LDResource,List<LDResource>> commonLevelsPerDim=new HashMap<LDResource, List<LDResource>>();
				
				// check the potential compatible cube if it has the same levels at dimensions
				for (LDResource dim : cubeDimensions) {					
					//If there are levels at the original cube
					if (dimensionsLevels.get(dim).size()>0){
						List<LDResource> commonLevels=new ArrayList<LDResource>();
						for (LDResource level : dimensionsLevels.get(dim)) {
							if (CubeSPARQL.askDimensionLevelInDataCube(
									"<"+ cube.getURI() + ">", level,
									cubeGraph, cubeDSDGraph, SPARQLservice)) {
								commonLevels.add(level);
							}
						}					
						commonLevelsPerDim.put(dim, commonLevels);						
						//No common levels found
						if(commonLevels.size()==0){
							levelFound=false;
							break;
						}						
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
						List<LDResource> dimValues = originalCubeDimensionsValues.get(dim);
						List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues.get(dim);

						// For the expansion dimension we need the complement size to be >0
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
							List<LDResource> intersection = new ArrayList<LDResource>(dimValues);
							intersection.retainAll(compatibleCubeDimValues);
							double overlap = (double) intersection.size()
									/ (double) dimValues.size();
							// If overlap is under the theshold -> cubes are not compatible
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

						List<LDResource> measureIntersection = new ArrayList<LDResource>(cubeMeasures);
						measureIntersection.retainAll(compMeasures);
						double meausureOverlap = (double) measureIntersection.size() / (double) cubeMeasures.size();

						if (meausureOverlap >= overlapThreshold) {
							// create random link specification
							Random rand = new Random();
							long rnd = Math.abs(rand.nextLong());
							String linkSpecification = "<http://opencube-project.eu/linkSpecification_"	+ rnd + ">";

							// Add new link
							String create_link_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
									+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
									+ "PREFIX opencube: <http://opencube-project.eu/> "
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
									+ " rdf:type opencube:LinkSpecification. " + "<"
									+ selectedCube.getURI()
									+ "> opencube:dimensionValueCompatible "
									+ linkSpecification + "."
									+ linkSpecification
									+ " opencube:compatibleCube <" + cube.getURI()
									+ ">." + linkSpecification
									+ " opencube:compatibleDimension <"
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
	
	// Create Links for all add value to level compatible cubes
		// Compatible:
		// - have the dimensions of the original cube
		// - the dimensions have the same skos:ConceptScheme
		// - the dimension instances are at the same xkos:ClassificationLevel
		// - All the dimension (except the expansion) have 100% overlap
		// - The expansion dimension has complement size > 0
		// - The measures have overlap 100%
		public static int linkAddValueToLevelCompatibleCubesAndLevels(
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
				getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"	+ i + ".";

				// If there is a code list check the code list
				if (dimensionsConceptSchemes.get(dim) != null
						&& dimensionsConceptSchemes.get(dim).size() > 0) {
					getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"+ i + ".";
					for (LDResource conceptScheme : dimensionsConceptSchemes
							.get(dim)) {
						getCompatibleCubes_query += "?dim" + i + " qb:codeList <"
								+ conceptScheme.getURI() + ">.";
					}
				// if no code list exists check the dimension URI
				} else {
					getCompatibleCubes_query += "?comp" + i + " qb:dimension <"	+ dim.getURI() + ">.";
				}
				getCompatibleCubes_query += "}";
				i++;
			}

			i = 1;

			// Measures
			for (LDResource measure : cubeMeasures) {
				getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{"
						+ "?dsd qb:component ?measurecomp" + i + "."
						+ "?measurecomp" + i + " qb:measure <" + measure.getURI()+ ">.}";
			}

			getCompatibleCubes_query += "}";

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getCompatibleCubes_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);
			List<LDResource> potentialCompatibleCubes = new ArrayList<LDResource>();

			// The result is cube -> all values of the expansion dimension
			int numberOfCompatibleCubes = 0;			
			try {
				while (res.hasNext()) {
					BindingSet bindingSet = res.next();
					LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
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

			HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();
			Map<LDResource, List<LDResource>>originalLevelsValues=new HashMap<LDResource, List<LDResource>>();
			List<LDResource> correctDims = new ArrayList<LDResource>(cubeDimensions);
			
			for (LDResource cube : potentialCompatibleCubes) {
				// The above query returns all compatible cubes including the original cube
				if (!cube.equals(selectedCube)) {
					// Get the original cube dimension values
					if (originalCubeDimensionsValues.keySet().isEmpty()) {
						originalCubeDimensionsValues = CubeHandlingUtils
								.getDimsValues(cubeDimensions,
										"<" + selectedCube.getURI() + ">",
										selectedCubeGraph, selectedCubeDSDgraph,
										false, selectedLanguage, defaultLang,
										ignoreLang, SPARQLservice);
						
						//Get all the values for each level (level -> values[])
						//To get the values use the originalCubeDimensionsValues
						for(LDResource dim:originalCubeDimensionsValues.keySet()){
							for(LDResource value:originalCubeDimensionsValues.get(dim)){
								//if there is a level at the value put the value at the list						
								if(value.getLevel()!=null){
									LDResource thisLevel=new LDResource(value.getLevel());
									List<LDResource> currentLevelValues=originalLevelsValues.get(thisLevel);
									//If there are no values for the level yet
									if(currentLevelValues==null){
										currentLevelValues=new ArrayList<LDResource>();
									}
									currentLevelValues.add(value);
									Collections.sort(currentLevelValues);
									originalLevelsValues.put(thisLevel, currentLevelValues);
								}
							}
						}							
					}
					// Get the compatible Cube Graph
					String cubeGraph = CubeSPARQL.getCubeSliceGraph("<" + cube.getURI() + ">", SPARQLservice);

					// Get the compatible Cube Structure graph
					String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"
							+ cube.getURI() + ">", cubeGraph, SPARQLservice);				
					
					boolean levelFound = true;
					Map<LDResource,List<LDResource>> commonLevelsPerDim=new HashMap<LDResource, List<LDResource>>();
					
					// check the potential compatible cube if it has the same levels at dimensions
					for (LDResource dim : cubeDimensions) {						
						//If there are levels at the original cube
						if (dimensionsLevels.get(dim).size()>0){
							List<LDResource> commonLevels=new ArrayList<LDResource>();
							for (LDResource level : dimensionsLevels.get(dim)) {
								if (CubeSPARQL.askDimensionLevelInDataCube(
										"<"+ cube.getURI() + ">", level,
										cubeGraph, cubeDSDGraph, SPARQLservice)) {
									commonLevels.add(level);
								}
							}					
							commonLevelsPerDim.put(dim, commonLevels);						
							//No common levels found
							if(commonLevels.size()==0){
								levelFound=false;
								break;
							}						
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
						
						Map<LDResource, List<LDResource>>compatibleLevelsValues=new HashMap<LDResource, List<LDResource>>();
						
						//Get all the values for each level (level -> values[])
						//To get the values use the compatibleCubeDimensionsValues
						for(LDResource dim:compatibleCubeDimensionsValues.keySet()){
							for(LDResource value:compatibleCubeDimensionsValues.get(dim)){
								//if there is a level at the value put the value at the list						
								if(value.getLevel()!=null){
									LDResource thisLevel=new LDResource(value.getLevel());
									List<LDResource> currentLevelValues=compatibleLevelsValues.get(thisLevel);
									//If there are no values for the level yet
									if(currentLevelValues==null){
										currentLevelValues=new ArrayList<LDResource>();
									}
									currentLevelValues.add(value);
									Collections.sort(currentLevelValues);
									compatibleLevelsValues.put(thisLevel, currentLevelValues);
								}
							}
						}					

						boolean isDimensionCompatible = true;
						// check if there is an intersection (at least one) at the values of each dimension
						for (LDResource dim : correctDims) {
							// For the expansion dimension we need the complement size to be >0
							if (dim.equals(expansionDimension)) {
								List<LDResource> dimValues = originalCubeDimensionsValues.get(dim);
								List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues.get(dim);
								List<LDResource> dimensionComplement = new ArrayList<LDResource>(compatibleCubeDimValues);
								dimensionComplement.removeAll(dimValues);

								// If the expansion dimension complement is empty ->
								// cubes are not compatible
								if (dimensionComplement.size() == 0) {
									isDimensionCompatible = false;
									break;
								}

							// For all other dimensions (except expansion) we need 100% overlap:
							//- if no levels exist - overlap at the whole dimension
							//- if levels exist - overlap at the common levels
							} else {
								if(dimensionsLevels.get(dim).size()>0){
									List<LDResource> commonLevels=commonLevelsPerDim.get(dim);
									for(LDResource commonLevel:commonLevels){
										List<LDResource> levelValues = originalLevelsValues.get(commonLevel);
										List<LDResource> compatibleCubeLevelValues = compatibleLevelsValues.get(commonLevel);
										List<LDResource> intersection = new ArrayList<LDResource>(levelValues);
										intersection.retainAll(compatibleCubeLevelValues);
										double overlap = (double) intersection.size()/ (double) levelValues.size();
										if (overlap < overlapThreshold) {
											isDimensionCompatible = false;
											break;
										}								
									}
									
									if(!isDimensionCompatible){
										break;								
									}
									
								}else{
									List<LDResource> dimValues = originalCubeDimensionsValues.get(dim);
									List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues.get(dim);
									List<LDResource> intersection = new ArrayList<LDResource>(dimValues);
									intersection.retainAll(compatibleCubeDimValues);
									double overlap = (double) intersection.size()/ (double) dimValues.size();
									if (overlap < overlapThreshold) {
										isDimensionCompatible = false;
										break;
									}							
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
							List<LDResource> measureIntersection = new ArrayList<LDResource>(
									cubeMeasures);
							measureIntersection.retainAll(compMeasures);
							double meausureOverlap = (double) measureIntersection
									.size() / (double) cubeMeasures.size();
							if (meausureOverlap >= overlapThreshold) {
								// create random link specification
								Random rand = new Random();
								long rnd = Math.abs(rand.nextLong());
								String linkSpecification = "<http://opencube-project.eu/linkSpecification_"	+ rnd + ">";

								// Add new link
								String create_link_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
										+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
										+ "PREFIX opencube: <http://opencube-project.eu/> "
										+ "INSERT DATA  {";

								if (SPARQLservice != null) {
									create_link_query += "SERVICE " + SPARQLservice	+ " {";
								}

								if (selectedCubeGraph != null) {
									create_link_query += "GRAPH <" + selectedCubeGraph + "> {";
								}

								// cube1 -> dimensionValueCompatible ->
								// linkspecification
								// linkspecification -> compatibleCube -> cube2
								// linkspecification -> compatibleDimension ->
								// expansionDimension
								create_link_query += linkSpecification
										+ " rdf:type opencube:LinkSpecification. " + "<"
										+ selectedCube.getURI()
										+ "> opencube:dimensionValueCompatible "
										+ linkSpecification + "."
										+ linkSpecification
										+ " opencube:compatibleCube <" + cube.getURI()
										+ ">." + linkSpecification
										+ " opencube:compatibleDimension <"
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
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"+ i + ".";

			// If there is a code list check the code list
			if (dimensionsConceptSchemes.get(dim) != null
					&& dimensionsConceptSchemes.get(dim).size() > 0) {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"
							+ conceptScheme.getURI() + ">.";
				}
			// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"	+ dim.getURI() + ">.";
			}
			getCompatibleCubes_query += "}";
			i++;
		}
		i = 1;
		// Measures
		for (LDResource measure : cubeMeasures) {
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?measurecomp"
					+ i	+ "."+ "?measurecomp"+ i+ " qb:measure <"+ measure.getURI() + ">.}";
		}

		getCompatibleCubes_query += "}";
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);
		List<LDResource> potentialCompatibleCubes = new ArrayList<LDResource>();
		HashMap<LDResource, HashMap<LDResource, List<LDResource>>> compatibleCubes = new HashMap<LDResource, HashMap<LDResource, List<LDResource>>>();
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
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
		
		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();
		List<LDResource> correctDims = new ArrayList<LDResource>(cubeDimensions);
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
			// The above query returns all compatible cubes including the original cube
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
						if (!CubeSPARQL.askDimensionLevelInDataCube(
								"<"+ cube.getURI() + ">", level,
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
						List<LDResource> dimValues = originalCubeDimensionsValues.get(dim);
						List<LDResource> compatibleCubeDimValues = compatibleCubeDimensionsValues.get(dim);
						List<LDResource> intersection = new ArrayList<LDResource>(dimValues);
						intersection.retainAll(compatibleCubeDimValues);
						List<LDResource> dimensionComplement = new ArrayList<LDResource>(
								compatibleCubeDimValues);
						dimensionComplement.removeAll(dimValues);
						double overlap = (double) intersection.size()
								/ (double) dimValues.size();
						cubeLine += "," + overlap + ","	+ dimensionComplement.size();						
					}

					// check measure
					// Get the Cube measure
					List<LDResource> compMeasures = CubeSPARQL
							.getDataCubeMeasure("<" + cube.getURI() + ">",
									cubeGraph, cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQLservice);					

					List<LDResource> measureIntersection = new ArrayList<LDResource>(cubeMeasures);
					measureIntersection.retainAll(compMeasures);
					double meausureOverlap = (double) measureIntersection.size() / (double) cubeMeasures.size();
					List<LDResource> measureComplement = new ArrayList<LDResource>(compMeasures);
					measureComplement.removeAll(cubeMeasures);
					cubeLine += "," + meausureOverlap + ","	+ measureComplement.size();
					writer.println(cubeLine);					
				}
			}
		}
		writer.close();
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
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"+ i + ".";

			// If there is a code list check the code list
			if (dimensionsConceptSchemes.get(dim) != null
					&& dimensionsConceptSchemes.get(dim).size() > 0) {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension ?dim"+ i + ".";
				for (LDResource conceptScheme : dimensionsConceptSchemes
						.get(dim)) {
					getCompatibleCubes_query += "?dim" + i + " qb:codeList <"+ conceptScheme.getURI() + ">.";
				}
				// if no code list exists check the dimension URI
			} else {
				getCompatibleCubes_query += "?comp" + i + " qb:dimension <"	+ dim.getURI() + ">.";
			}
			getCompatibleCubes_query += "}";
			i++;
		}
		i = 1;
		// Measures
		for (LDResource measure : cubeMeasures) {
			getCompatibleCubes_query += "GRAPH ?cubeDSDgraph{?dsd qb:component ?measurecomp"
					+ i	+ "."+ "?measurecomp"+ i+ " qb:measure <"+ measure.getURI() + ">.}";
		}
		getCompatibleCubes_query += "}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);
		HashMap<LDResource, HashMap<LDResource, List<LDResource>>> clusterCubesAndDims = new HashMap<LDResource, HashMap<LDResource, List<LDResource>>>();
		List<LDResource> potentialCompatibleCubes = new ArrayList<LDResource>();
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());

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
			String cubeGraph = CubeSPARQL.getCubeSliceGraph("<" + cube.getURI()	+ ">", SPARQLservice);

			// Get Cube Structure graph
			String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
					"<" + cube.getURI() + ">", cubeGraph, SPARQLservice);

			// Get all Cube dimensions
			List<LDResource> compatibleCubeDimensions = CubeSPARQL
					.getDataCubeDimensions("<" + cube.getURI() + ">",
							cubeGraph, cubeDSDGraph, selectedLanguage,
							defaultLang, ignoreLang, SPARQLservice);

			// the same number of dimensions
			HashMap<LDResource, List<LDResource>> compatibleCubeDimensionsValues = CubeHandlingUtils
					.getDimsValues(compatibleCubeDimensions,
							"<" + cube.getURI() + ">", cubeGraph, cubeDSDGraph,
							false, selectedLanguage, defaultLang, ignoreLang,
							SPARQLservice);

			clusterCubesAndDims.put(cube, compatibleCubeDimensionsValues);			
		}
		return clusterCubesAndDims;
	}	

	public static String mergeCubesAddMeasure(LDResource originalCube,
			LDResource expanderCube, String selectedLanguage,
			String defaultLang, boolean ignoreLang, String SPARQLservice) {
	
		/////////// ORIGINAL CUBE ///////////////////////////
	
		String originalCubeURI = "<" + originalCube.getURI() + ">";
	
		// Get Original Cube Graph
		String originalCubeGraph = CubeSPARQL.getCubeSliceGraph(originalCubeURI, SPARQLservice);
	
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
	
		String newDSD_URI = "<http://opencube-project.eu/dsd_" + rnd+ ">";
		String newCube_URI = "<http://opencube-project.eu/cube_" + rnd + ">";
		String newCubeGraph_URI = "<http://opencube-project.eu/graph_" + rnd + ">";
	
		// Add new DSD
		// ADD THE NEW DSD AT THE ORIGINAL CUBE DSD GRAPH
		String create_new_merged_dsd_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";
	
		if (SPARQLservice != null) {
			create_new_merged_dsd_query += "SERVICE " + SPARQLservice + " {";
		}
	
		if (originalCubeDSDGraph != null) {
			create_new_merged_dsd_query += "GRAPH <" + originalCubeDSDGraph + "> {";
		}
	
		create_new_merged_dsd_query += newDSD_URI+ " rdf:type qb:DataStructureDefinition.";
	
		// Add dimensions to DSD
		for (LDResource ldr : originalCubeDimensions) {
			rnd = Math.abs(rand.nextLong());
			String newComponentSpecification_URI =
					"<http://opencube-project.eu/componentSpecification_" + rnd + ">";
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
			String newComponentSpecification_URI = 
					"<http://opencube-project.eu/componentSpecification_" + rnd + ">";
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
			getOriginalCubeObservations_query += "SERVICE " + SPARQLservice	+ " {";
		}
	
		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "GRAPH <" + originalCubeGraph+ "> {";
		}
	
		getOriginalCubeObservations_query += "?obs qb:dataSet "+ originalCubeURI + ".";
	
		i = 1;
		for (LDResource ldr : originalCubeDimensions) {
			getOriginalCubeObservations_query += "?obs <" + ldr.getURI()+ "> ?dim" + i + ".";
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
				String newObservation_URI = "<http://opencube-project.eu/observation_" + obsCount + ">";
				create_new_cube_query += newObservation_URI
						+ " rdf:type qb:Observation." + newObservation_URI
						+ " qb:dataSet " + newCube_URI + ".";
	
				i = 1;
				for (LDResource ldr : originalCubeDimensions) {
					String dimValue = bindingSet.getValue("dim" + i).stringValue();
					// Is URI
					if (dimValue.contains("http")) {
						create_new_cube_query += 
								newObservation_URI + " <"+ ldr.getURI() + "> <" + dimValue + ">.";	
					// Is literal
					} else {
						create_new_cube_query +=
								newObservation_URI + " <"+ ldr.getURI() + "> \"" + dimValue + "\".";
					}
					i++;
				}
	
				i = 1;
				for (LDResource ldr : originalCubeMeasures) {
					String measureValue = bindingSet.getValue("measure" + i).stringValue();
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
						create_new_cube_query += "SERVICE " + SPARQLservice	+ " {";
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
		ArrayList<LDResource> expandedMeasures = new ArrayList<LDResource>(expanderCubeMeasures);		
		i = 1;
		// Add measure variables to query
		for (LDResource ldr : expandedMeasures) {
			getExpanderCubeObservations_query += "?measure" + i + " ";
			i++;
		}
	
		getExpanderCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "SERVICE " + SPARQLservice	+ " {";
		}
	
		if (expanderCubeGraph != null) {
			getExpanderCubeObservations_query += "GRAPH <" + expanderCubeGraph	+ "> {";
		}
	
		getExpanderCubeObservations_query += "?obs qb:dataSet "	+ expanderCubeURI + ".";
	
		i = 1;
		for (LDResource ldr : expanderCubeDimensions) {
			getExpanderCubeObservations_query += "?obs <" + ldr.getURI() + "> ?dim" + i + ".";
			i++;
		}
	
		i = 1;
		for (LDResource m : expandedMeasures) {
			getExpanderCubeObservations_query += "?obs <" + m.getURI()+ "> ?measure" + i + ".";
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
				String newObservation_URI = "<http://opencube-project.eu/observation_" + obsCount + ">";
				create_new_cube_query += newObservation_URI
						+ " rdf:type qb:Observation." + newObservation_URI
						+ " qb:dataSet " + newCube_URI + ".";	
				i = 1;
				for (LDResource ldr : expanderCubeDimensions) {
					String dimValue = bindingSet.getValue("dim" + i).stringValue();
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
				for (LDResource ldr : expandedMeasures) {
					String measureValue = bindingSet.getValue("measure" + i).stringValue();
					// Is URI
					if (measureValue.contains("http")) {
						create_new_cube_query +=
								newObservation_URI + " <"+ ldr.getURI() + "> <" + measureValue + ">.";	
					// Is literal
					} else {
						create_new_cube_query += 
								newObservation_URI + " <"+ ldr.getURI() + "> \"" + measureValue + "\".";
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

		String newDSD_URI = "<http://opencube-project.eu/dsd_" + rnd+ ">";
		String newCube_URI = "<http://opencube-project.eu/cube_" + rnd+ ">";
		String newCubeGraph_URI = "<http://opencube-project.eu/graph_"+ rnd + ">";

		// Add new DSD
		// ADD THE NEW DSD AT THE ORIGINAL CUBE DSD GRAPH
		String create_new_merged_dsd_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_merged_dsd_query += "SERVICE " + SPARQLservice + " {";
		}

		if (originalCubeDSDGraph != null) {
			create_new_merged_dsd_query += "GRAPH <" + originalCubeDSDGraph	+ "> {";
		}

		create_new_merged_dsd_query += newDSD_URI	+ " rdf:type qb:DataStructureDefinition.";

		// Add dimensions to DSD
		for (LDResource ldr : originalCubeDimensions) {
			rnd = Math.abs(rand.nextLong());
			String newComponentSpecification_URI = 
					"<http://opencube-project.eu/componentSpecification_"+ rnd + ">";
			
			create_new_merged_dsd_query +=
					newDSD_URI + " qb:component "+ newComponentSpecification_URI + "."	+
				    newComponentSpecification_URI + " qb:dimension <"	+ ldr.getURI() + ">.";
		}
		
		// Add measures to DSD
		for (LDResource m : originalCubeMeasures) {
			rnd = Math.abs(rand.nextLong());
			String newComponentSpecification_URI = 
					"<http://opencube-project.eu/componentSpecification_"+ rnd + ">";
			create_new_merged_dsd_query += 
					newDSD_URI + " qb:component "+ newComponentSpecification_URI + "."+
			        newComponentSpecification_URI + " qb:measure <"+ m.getURI() + ">.";
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
		String getOriginalCubeObservations_query = 
				"PREFIX qb: <http://purl.org/linked-data/cube#>"
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
			getOriginalCubeObservations_query += "SERVICE " + SPARQLservice	+ " {";
		}

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "GRAPH <" + originalCubeGraph + "> {";
		}

		getOriginalCubeObservations_query += "?obs qb:dataSet "	+ originalCubeURI + ".";

		i = 1;
		for (LDResource ldr : originalCubeDimensions) {
			getOriginalCubeObservations_query += "?obs <" + ldr.getURI()+ "> ?dim" + i + ".";
			i++;
		}

		i = 1;
		for (LDResource m : originalCubeMeasures) {
			getOriginalCubeObservations_query += "?obs <" + m.getURI()+ "> ?measure" + i + ".";
			i++;
		}

		getOriginalCubeObservations_query += "}";

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getOriginalCubeObservations_query);

		// CREATE NEW MERGED CUBE
		String create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_cube_query += "SERVICE " + SPARQLservice + " {";
		}

		create_new_cube_query += "GRAPH " + newCubeGraph_URI + " {"
				+ newCube_URI + " rdf:type qb:DataSet."
				+ newCube_URI + " qb:structure " + newDSD_URI + ".";

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
				String newObservation_URI =
						"<http://opencube-project.eu/observation_"+ obsCount + ">";
				create_new_cube_query += newObservation_URI	+ " rdf:type qb:Observation." + 
						newObservation_URI 	+ " qb:dataSet " + newCube_URI + ".";

				i = 1;
				for (LDResource ldr : originalCubeDimensions) {
					String dimValue = bindingSet.getValue("dim" + i).stringValue();
					// Is URI
					if (dimValue.contains("http")) {
						create_new_cube_query += 
								newObservation_URI + " <" + ldr.getURI() + "> <" + dimValue + ">.";

					// Is literal
					} else {
						create_new_cube_query +=
								newObservation_URI + " <" + ldr.getURI() + "> \"" + dimValue + "\".";
					}
					i++;
				}

				i = 1;
				for (LDResource ldr : originalCubeMeasures) {
					String measureValue = bindingSet.getValue("measure" + i).stringValue();
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

		i = 1;
		// Add measure variables to query
		for (LDResource ldr : expanderCubeMeasures) {
			getExpanderCubeObservations_query += "?measure" + i + " ";
			i++;
		}

		getExpanderCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "SERVICE " + SPARQLservice	+ " {";
		}

		if (expanderCubeGraph != null) {
			getExpanderCubeObservations_query += "GRAPH <" + expanderCubeGraph + "> {";
		}

		getExpanderCubeObservations_query += "?obs qb:dataSet "	+ expanderCubeURI + ".";

		i = 1;
		for (LDResource ldr : expanderCubeDimensions) {
			getExpanderCubeObservations_query += 
					"?obs <" + ldr.getURI()	+ "> ?dim" + i + ".";
			i++;
		}

		i = 1;
		for (LDResource m : expanderCubeMeasures) {
			getExpanderCubeObservations_query += 
					"?obs <" + m.getURI()+ "> ?measure" + i + ".";
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
			expansionDimNewValues.removeAll(originalCubeAllDimensionsValues.get(expansionDim));

			int expansionDimIndex = expanderCubeDimensions.indexOf(expansionDim) + 1;

			for (BindingSet bindingSet : bs) {

				// Get the value of the expansion dim of the observation
				LDResource expansionValueLdr = new LDResource(bindingSet
						.getValue("dim" + expansionDimIndex).stringValue());

				// If the observation has a NEW value at the expansion dimension
				if (expansionDimNewValues.contains(expansionValueLdr)) {

					// Observation URI
					String newObservation_URI =
							"<http://opencube-project.eu/observation_"+ obsCount + ">";
					
					create_new_cube_query += newObservation_URI
							+ " rdf:type qb:Observation." + newObservation_URI
							+ " qb:dataSet " + newCube_URI + ".";

					i = 1;
					for (LDResource ldr : expanderCubeDimensions) {
						String dimValue = bindingSet.getValue("dim" + i).stringValue();						
						// Is URI
						if (dimValue.contains("http")) {
							create_new_cube_query +=
									newObservation_URI + " <"+ ldr.getURI() + "> <" + dimValue + ">.";
						// Is literal
						} else {
							create_new_cube_query +=
									newObservation_URI + " <"+ ldr.getURI() + "> \"" + dimValue + "\".";
						}
						i++;
					}

					i = 1;
					for (LDResource ldr : expanderCubeMeasures) {
						String measureValue = bindingSet.getValue("measure" + i).stringValue();						
						// Is URI
						if (measureValue.contains("http")) {
							create_new_cube_query += 
									newObservation_URI + " <"+ ldr.getURI() + "> <" + measureValue + ">.";
						// Is literal
						} else {
							create_new_cube_query +=
									newObservation_URI + " <"+ ldr.getURI() + "> \"" + measureValue	+ "\".";
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
							create_new_cube_query += "SERVICE " + SPARQLservice	+ " {";
						}

						create_new_cube_query += "GRAPH " + newCubeGraph_URI+ " {";
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
		
		List<Literal> originalCubeLabels=CubeSPARQL.getLabels(
				originalCubeURI, originalCubeGraph, SPARQLservice);
		
		/////////// EXPANDER CUBE ///////////////////////////

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
		
		List<Literal> expanderCubeLabels=CubeSPARQL.getLabels(
				expanderCubeURI, expanderCubeGraph, SPARQLservice);
		
		// create random DSD, Cube URI and Cube Graph URI
		Random rand = new Random();
		long rnd = Math.abs(rand.nextLong());

	
		String newCube_URI = "<http://opencube-project.eu/cube_" + rnd+ ">";
	
		// GET ORIGINAL CUBE OBSERVATIONS
		String getOriginalCubeObservations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "Select ?obs ";
		int i = 1;

		getOriginalCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "SERVICE " + SPARQLservice	+ " {";
		}

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "GRAPH <" + originalCubeGraph + "> {";
		}

		getOriginalCubeObservations_query += "?obs qb:dataSet "	+ originalCubeURI + ".";

		getOriginalCubeObservations_query += "}";

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getOriginalCubeObservations_query);

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
		
		if(originalCubeLabels.size()>0){
			//Add labels for each language
			for(Literal l:originalCubeLabels){
				if(l.getLanguage()!=null){
					boolean found=false;
					for(Literal l_ext:expanderCubeLabels){
						//if the expander cube labels has language and is the same as
						if(l_ext.getLanguage()!=null&&
									l.getLanguage().equals(l_ext.getLanguage())){
							create_new_cube_query+=newCube_URI + "rdfs:label \"Merged:"+
									l.getLabel()+" & "+l_ext.getLabel()+"\"@"+l.getLanguage()+".";
							found=true;
						}						
					}
					//If no common language combine the first label from the expander cube
					if(!found){
						create_new_cube_query+=newCube_URI + "rdfs:label \"Merged:"+
								l.getLabel()+" & "+
								expanderCubeLabels.get(0).getLabel()+"\"@"+l.getLanguage()+".";
					}
				}else{
					create_new_cube_query+=newCube_URI + "rdfs:label \"Merged:"+
							l.getLabel()+" & "+	expanderCubeLabels.get(0).getLabel()+"\".";
				}			
			}		
		}else{
			create_new_cube_query+=newCube_URI + "rdfs:label \"Merged:"+
					originalCube.getURIorLabel()+" & "+	expanderCube.getURIorLabel()+"\".";
		}
		
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
				create_new_cube_query +=  "<"+bindingSet.getValue("obs")+ "> qb:dataSet " + newCube_URI + ".";			
				// If |observations|= 100 execute insert
				if (create_new_cube_query.length()>100000) {
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
				+ "Select ?obs ";
		i = 1;

		// Add dimension variables to query
		for (LDResource ldr : expanderCubeDimensions) {
			getExpanderCubeObservations_query += "?dim" + i + " ";
			i++;
		}

		i = 1;
		// Add measure variables to query
		for (LDResource ldr : expanderCubeMeasures) {
			getExpanderCubeObservations_query += "?measure" + i + " ";
			i++;
		}

		getExpanderCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "SERVICE " + SPARQLservice	+ " {";
		}

		if (expanderCubeGraph != null) {
			getExpanderCubeObservations_query += "GRAPH <" + expanderCubeGraph+ "> {";
		}

		getExpanderCubeObservations_query += "?obs qb:dataSet "	+ expanderCubeURI + ".";

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

			int expansionDimIndex = expanderCubeDimensions.indexOf(expansionDim) + 1;

			for (BindingSet bindingSet : bs) {
				// Get the value of the expansion dim of the observation
				LDResource expansionValueLdr = new LDResource(bindingSet
						.getValue("dim" + expansionDimIndex).stringValue());

				// If the observation has a NEW value at the expansion dimension
				if (expansionDimNewValues.contains(expansionValueLdr)) {					
					String newObservation_URI = "<"+bindingSet.getValue("obs").stringValue()+">";
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

					i = 1;
					for (LDResource ldr : expanderCubeMeasures) {
						String measureValue = bindingSet.getValue("measure" + i).stringValue();
						// Is URI
						if (measureValue.contains("http")) {
							create_new_cube_query += 
									newObservation_URI + " <"+ ldr.getURI() + "> <" + measureValue+ ">.";

						// Is literal
						} else {
							create_new_cube_query += 
									newObservation_URI + " <"+ ldr.getURI() + "> \"" + measureValue	+ "\".";
						}
						i++;
					}

					// If |observations|= 100 execute insert
					//MAX SPARQL SIZE 200000 195000
					if (create_new_cube_query.length()>100000) {
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
							create_new_cube_query += "SERVICE " + SPARQLservice	+ " {";
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
		String newCube_URI = "<http://opencube-project.eu/cube_" + rnd+ ">";

		// GET ORIGINAL CUBE OBSERVATIONS
		String getOriginalCubeObservations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "Select ?obs ";
		int i = 1;

		getOriginalCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "SERVICE " + SPARQLservice	+ " {";
		}

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "GRAPH <" + originalCubeGraph	+ "> {";
		}

		getOriginalCubeObservations_query += "?obs qb:dataSet "	+ originalCubeURI + ".";	
		getOriginalCubeObservations_query += "}";

		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getOriginalCubeObservations_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getOriginalCubeObservations_query);

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
				if (create_new_cube_query.length()>100000) {
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
						create_new_cube_query += "SERVICE " + SPARQLservice	+ " {";
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
			getExpanderCubeObservations_query += "SERVICE " + SPARQLservice	+ " {";
		}

		if (expanderCubeGraph != null) {
			getExpanderCubeObservations_query += "GRAPH <" + expanderCubeGraph+ "> {";
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
			for (BindingSet bindingSet : bs) {
				// Get the value of the expansion dim of the observation
				LDResource expansionValueLdr = new LDResource(bindingSet.getValue("expvalue").stringValue());

				// If the observation has a NEW value at the expansion dimension
				if (expansionDimNewValues.contains(expansionValueLdr)) {
					create_new_cube_query +=  "<"+bindingSet.getValue("obs")+ "> qb:dataSet " + newCube_URI + ".";				
					// If |observations|= 100 execute insert
					//MAX SPARQL SIZE 200000
					if (create_new_cube_query.length()>100000) {
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
							create_new_cube_query += "SERVICE " + SPARQLservice+ " {";
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
		
	public static String mergeCubesAddMeasureSingleObservation(LDResource originalCube,
			LDResource expanderCube, String selectedLanguage,
			String defaultLang, boolean ignoreLang, String SPARQLservice) {
	
		/////////// ORIGINAL CUBE ///////////////////////////	
		String originalCubeURI = "<" + originalCube.getURI() + ">";
	
		// Get Original Cube Graph
		String originalCubeGraph = CubeSPARQL.getCubeSliceGraph(originalCubeURI, SPARQLservice);
	
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
		
		List<Literal> originalCubeLabels=CubeSPARQL.getLabels(
				originalCubeURI, originalCubeGraph, SPARQLservice);
	
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
		
		List<Literal> expanderCubeLabels=CubeSPARQL.getLabels(
				expanderCubeURI, expanderCubeGraph, SPARQLservice);
	
		// create random DSD, Cube URI and Cube Graph URI
		Random rand = new Random();
		long rnd = Math.abs(rand.nextLong());
	
		String newDSD_URI = "<http://opencube-project.eu/dsd" + rnd+ ">";
		String newCube_URI = "<http://opencube-project.eu/cube_" + rnd + ">";
	
		// Add new DSD
		// ADD THE NEW DSD AT THE ORIGINAL CUBE DSD GRAPH
		String create_new_merged_dsd_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";
	
		if (SPARQLservice != null) {
			create_new_merged_dsd_query += "SERVICE " + SPARQLservice + " {";
		}
	
		if (originalCubeDSDGraph != null) {
			create_new_merged_dsd_query += "GRAPH <" + originalCubeDSDGraph + "> {";
		}
	
		create_new_merged_dsd_query += newDSD_URI+ " rdf:type qb:DataStructureDefinition.";
	
		// Add dimensions to DSD
		for (LDResource ldr : originalCubeDimensions) {
			rnd = Math.abs(rand.nextLong());
			String newComponentSpecification_URI =
					"<http://opencube-project.eu/componentSpecification_" + rnd + ">";
			create_new_merged_dsd_query += newDSD_URI + " qb:component "
					+ newComponentSpecification_URI + "."
					+ newComponentSpecification_URI + " qb:dimension <"
					+ ldr.getURI() + ">.";
		}
	
		Set<LDResource> mergedMeasures = new HashSet<LDResource>(originalCubeMeasures);
		mergedMeasures.addAll(expanderCubeMeasures);
		// Add measures to DSD
		for (LDResource m : mergedMeasures) {
			rnd = Math.abs(rand.nextLong());
			String newComponentSpecification_URI = 
					"<http://opencube-project.eu/componentSpecification_" + rnd + ">";
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
			getOriginalCubeObservations_query += "SERVICE " + SPARQLservice	+ " {";
		}
	
		if (originalCubeGraph != null) {
			getOriginalCubeObservations_query += "GRAPH <" + originalCubeGraph	+ "> {";
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
	
		TupleQueryResult res = QueryExecutor.executeSelect(getOriginalCubeObservations_query);
	
		// CREATE NEW MERGED CUBE
		String create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";
	
		if (SPARQLservice != null) {
			create_new_cube_query += "SERVICE " + SPARQLservice + " {";
		}
	
		create_new_cube_query += "GRAPH <" + originalCubeGraph + "> {"
				+ newCube_URI + " rdf:type qb:DataSet." + newCube_URI
				+ " qb:structure " + newDSD_URI + ".";
		
		if(originalCubeLabels.size()>0){
			//Add labels for each language
			for(Literal l:originalCubeLabels){
				if(l.getLanguage()!=null){
					boolean found=false;
					for(Literal l_ext:expanderCubeLabels){
						//if the expander cube labels has language and is the same as
						if(l_ext.getLanguage()!=null&&
									l.getLanguage().equals(l_ext.getLanguage())){
							create_new_cube_query+=newCube_URI + "rdfs:label \"Merged:"+
									l.getLabel()+" & "+l_ext.getLabel()+"\"@"+l.getLanguage()+".";
							found=true;
						}else if(l_ext.getLanguage()==null){
							create_new_cube_query+=newCube_URI + "rdfs:label \"Merged:"+
									l.getLabel()+" & "+expanderCube.getURIorLabel()+"\"@"+l.getLanguage()+".";
						}
					}
					//If no common language combine the first label from the expander cube
					if(!found){
						create_new_cube_query+=newCube_URI + "rdfs:label \"Merged:"+
								l.getLabel()+" & "+
								expanderCubeLabels.get(0).getLabel()+"\"@"+l.getLanguage()+".";
					}
				}else{
					create_new_cube_query+=newCube_URI + "rdfs:label \"Merged:"+
							l.getLabel()+" & "+	expanderCubeLabels.get(0).getLabel()+"\".";
				}			
			}		
		}else{
			create_new_cube_query+=newCube_URI + "rdfs:label \"Merged:"+
					originalCube.getURIorLabel()+" & "+	expanderCube.getURIorLabel()+"\".";
		}
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
				String newObservation_URI = "<http://opencube-project.eu/observation_" + obsCount + ">";
				create_new_cube_query += newObservation_URI
						+ " rdf:type qb:Observation." + newObservation_URI
						+ " qb:dataSet " + newCube_URI + ".";
	
				i = 1;
				for (LDResource ldr : originalCubeDimensions) {
					String dimValue = bindingSet.getValue("dim" + i).stringValue();
					// Is URI
					if (dimValue.contains("http")) {
						create_new_cube_query += 
								newObservation_URI + " <"+ ldr.getURI() + "> <" + dimValue + ">.";	
					// Is literal
					} else {
						create_new_cube_query +=
								newObservation_URI + " <"+ ldr.getURI() + "> \"" + dimValue + "\".";
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
						create_new_cube_query += "SERVICE " + SPARQLservice	+ " {";
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
		ArrayList<LDResource> expandedMeasures = new ArrayList<LDResource>(expanderCubeMeasures);
		expandedMeasures.removeAll(originalCubeMeasures);
		i = 1;
		// Add measure variables to query
		for (LDResource ldr : expandedMeasures) {
			getExpanderCubeObservations_query += "?measure" + i + " ";
			i++;
		}
	
		getExpanderCubeObservations_query += "where{";
		if (SPARQLservice != null) {
			getExpanderCubeObservations_query += "SERVICE " + SPARQLservice	+ " {";
		}
	
		if (expanderCubeGraph != null) {
			getExpanderCubeObservations_query += "GRAPH <" + expanderCubeGraph	+ "> {";
		}
	
		getExpanderCubeObservations_query += "?obs qb:dataSet "	+ expanderCubeURI + ".";
	
		i = 1;
		for (LDResource ldr : expanderCubeDimensions) {
			getExpanderCubeObservations_query += "?obs <" + ldr.getURI() + "> ?dim" + i + ".";
			i++;
		}
	
		i = 1;
		for (LDResource m : expandedMeasures) {
			getExpanderCubeObservations_query += "?obs <" + m.getURI()+ "> ?measure" + i + ".";
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
			
		try {
	
			// Store expander cube observations
			List<BindingSet> bs = new ArrayList<BindingSet>();
	
			while (res.hasNext()) {
				bs.add(res.next());
			}
	
			// INSERT NEW MEAURES TO MERGED CUBE
			//Insert the new measures to the existing observations
			for (BindingSet bindingSet : bs) {							
				create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
						+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
						+ "INSERT {";
			
				if (SPARQLservice != null) {
					create_new_cube_query += "SERVICE " + SPARQLservice + " {";
				}
			
				create_new_cube_query += "GRAPH <" + originalCubeGraph + "> {";
				
				i = 1;
				for (LDResource ldr : expandedMeasures) {
					String measureValue = bindingSet.getValue("measure" + i).stringValue();
					// Is URI
					if (measureValue.contains("http")) {
						create_new_cube_query +="?obs <"+ ldr.getURI() + "> <" + measureValue + ">.";
	
					// Is literal
					} else {
						create_new_cube_query +="?obs <"+ ldr.getURI() + "> \"" + measureValue + "\".";
					}
					i++;
				}
				
				create_new_cube_query+="}}";
				
				if (SPARQLservice != null) {
					create_new_cube_query += "}";
				}
				
				create_new_cube_query+="where{ ";				
				if (SPARQLservice != null) {
					create_new_cube_query += "SERVICE " + SPARQLservice + " {";
				}				
				create_new_cube_query += "GRAPH <" + originalCubeGraph + "> {";	
				create_new_cube_query += "?obs rdf:type qb:Observation." 
				+ "?obs qb:dataSet " + newCube_URI + ".";
	
				i = 1;
				for (LDResource ldr : expanderCubeDimensions) {
					String dimValue = bindingSet.getValue("dim" + i)
							.stringValue();
					// Is URI
					if (dimValue.contains("http")) {
						create_new_cube_query += "?obs <"+ ldr.getURI() + "> <" + dimValue + ">.";	
					// Is literal
					} else {
						create_new_cube_query += "?obs <"+ ldr.getURI() + "> \"" + dimValue + "\".";
					}
					i++;
				}
	
				create_new_cube_query+="}}";				
				if (SPARQLservice != null) {
					create_new_cube_query += "}";
				}	
				QueryExecutor.executeUPDATE(create_new_cube_query);	
			}
	
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
	
		
		return newCube_URI;
	}
}
