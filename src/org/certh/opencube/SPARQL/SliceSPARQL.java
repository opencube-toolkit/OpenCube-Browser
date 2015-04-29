package org.certh.opencube.SPARQL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.certh.opencube.utils.LDResource;
import org.openrdf.model.Literal;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
 
public class SliceSPARQL {

//	private static boolean globalDSD = false;
//	private static boolean notime = false;

	// Get all the fixed dimensions of a slice
	// Input: The sliceURI, sliceGraph, SPARQL service
	// The slice Graph and SPARQL service can be null if not available
	public static List<LDResource> getSliceFixedDimensions(String sliceURI,
			String sliceGraph, String cubeDSDGraph, String lang, 
			String defaultlang, boolean ignoreLang, String SPARQLservice) {

		String getSliceDimensions_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "select  distinct ?dim ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getSliceDimensions_query += "SERVICE " + SPARQLservice + " {";
		}

		// If a slice graph is defined
		if (sliceGraph != null) {
			getSliceDimensions_query += "GRAPH <" + sliceGraph + "> {";
		}

		getSliceDimensions_query += sliceURI + " qb:sliceStructure ?slicekey. "
				+ "?slicekey qb:componentProperty  ?dim.";

		// If slice graph is defined
		if (sliceGraph != null) {
			getSliceDimensions_query += "}";
		}

		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getSliceDimensions_query += "GRAPH <" + cubeDSDGraph + "> {";
		}
		
		getSliceDimensions_query+="?dim qb:concept ?cons." +
				"OPTIONAL{?cons skos:prefLabel|rdfs:label ?label.}}";
		
		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getSliceDimensions_query += "}";
		}

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getSliceDimensions_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getSliceDimensions_query);
		List<LDResource> sliceDimensions = new ArrayList<LDResource>();

		try {

			while (res.hasNext()) {
				
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dim").stringValue());

				// check if there is a label (rdfs:label or skos:prefLabel)
				if (bindingSet.getValue("label") != null) {
					ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
				}
				
				//Add the first instance of the dimension (regardless of the language)
				if(!sliceDimensions.contains(ldr)){
					sliceDimensions.add(ldr);
				}else{
					//If ignore language
					if(ignoreLang){
						//First en then everything else
						if(ldr.getLanguage().equals("en")){
							sliceDimensions.remove(ldr);
							sliceDimensions.add(ldr);
						}
					}else{
						for(LDResource exisitingLdr:sliceDimensions){
							//Find the existing dimension that has the same URI (different language)
							if(exisitingLdr.equals(ldr)){
								//The new ldr has the preferred language
								if(ldr.getLanguage().equals(lang)){
									sliceDimensions.remove(ldr);
									sliceDimensions.add(ldr);
								//The new ldr has the default language and the existing does 
								//not have the preferred language
								}else if (ldr.getLanguage().equals(defaultlang)&&
									!exisitingLdr.getLanguage().equals(lang)){
									sliceDimensions.remove(ldr);
									sliceDimensions.add(ldr);
								}
							}
						}
					}
				}						
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return sliceDimensions;
	}

	// Get the values for all fixed dimensions of a slice
	// Input: The fixed slice dimensions, The sliceURI, sliceGraph, SPARQL service
	// The slice Graph and SPARQL service can be null if not available
	public static HashMap<LDResource, LDResource> getSliceFixedDimensionsValues(
			List<LDResource> sliceFixedDimensions, String sliceURI,
			String sliceGraph, String cubeDSDGraph, String lang,
			String defaultlang, boolean ignoreLang, String SPARQLservice) {

		HashMap<LDResource, LDResource> sliceFixedDimensionsValues = new HashMap<LDResource, LDResource>();

		for (LDResource ldr : sliceFixedDimensions) {
			String getSliceFixedDimensionValues_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
					+ "select  distinct ?fdimvalue ?label where {";

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getSliceFixedDimensionValues_query += "SERVICE "+ SPARQLservice + " {";
			}

			// If a slice graph is defined
			if (sliceGraph != null) {
				getSliceFixedDimensionValues_query += "GRAPH <" + sliceGraph+ "> {";
			}

			getSliceFixedDimensionValues_query += sliceURI + " <"
					+ ldr.getURI() + "> ?fdimvalue.";

			// If slice graph is defined
			if (sliceGraph != null) {
				getSliceFixedDimensionValues_query += "}";
			}

			getSliceFixedDimensionValues_query += "OPTIONAL{";

			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getSliceFixedDimensionValues_query += "GRAPH <" + cubeDSDGraph+ "> {";
			}
			getSliceFixedDimensionValues_query+=
					"?fdimvalue skos:prefLabel|rdfs:label ?label.}}";
					
			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getSliceFixedDimensionValues_query += "}";
			}

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getSliceFixedDimensionValues_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getSliceFixedDimensionValues_query);

			try {

				while (res.hasNext()) {
					
					
					BindingSet bindingSet = res.next();
					LDResource fdimvalue = new LDResource(bindingSet.getValue("fdimvalue").stringValue());

					// check if there is a label (rdfs:label or skos:prefLabel)
					if (bindingSet.getValue("label") != null) {
						fdimvalue.setLabelLiteral((Literal)bindingSet.getValue("label"));							
					}
					
					//Add the first instance of the dimension (regardless of the language)
					if(!sliceFixedDimensionsValues.keySet().contains(ldr)){
						sliceFixedDimensionsValues.put(ldr, fdimvalue);
					}else{
						//If ignore language
						if(ignoreLang){
							//First en then everything else
							if(fdimvalue.getLanguage().equals("en")){
								sliceFixedDimensionsValues.put(ldr, fdimvalue);
							}
						}else{
							LDResource existingFdimValue=sliceFixedDimensionsValues.get(ldr);
							//The new fdimvalue has the preferred language
							if(fdimvalue.getLanguage().equals(lang)){
								sliceFixedDimensionsValues.put(ldr, fdimvalue);
							//The new fdimvalue has the default language and the existing does 
							//not have the preferred language
							}else if (fdimvalue.getLanguage().equals(defaultlang)&&
										!existingFdimValue.getLanguage().equals(lang)){
								sliceFixedDimensionsValues.put(ldr, fdimvalue);
							}
						}													
					}			
				}
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}
		}

		return sliceFixedDimensionsValues;
	}

	// Get all cube dimensions from a slice
	// Input: The sliceURI, slice Graph, cube DSD graph, SPARQL service
	// The slice Graph, cube DSD graph and SPARQL service can be null if not
	// available
	public static List<LDResource> getDataCubeDimensionsFromSlice(
			String sliceURI, String sliceGraph, String cubeDSDGraph,
			String lang, String defaultlang, boolean ignoreLang,String SPARQLservice) {

		String getSliceMeasure_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "select  distinct ?dim ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getSliceMeasure_query += "SERVICE " + SPARQLservice + " {";
		}

		// If a cube graph is defined
		if (sliceGraph != null) {
			getSliceMeasure_query += "GRAPH <" + sliceGraph + "> {";
		}

		getSliceMeasure_query += sliceURI + "qb:sliceStructure ?sliceKey."
				+ "?dsd qb:sliceKey ?sliceKey.";

		// If a cube DSD graph is defined
		if (sliceGraph != null) {
			getSliceMeasure_query += "}";
		}

		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getSliceMeasure_query += "GRAPH <" + cubeDSDGraph + "> {";
		}
		
		getSliceMeasure_query += "?dsd qb:component  ?cs."
				+ "?cs qb:dimension  ?dim." +
				"OPTIONAL{?dim qb:concept ?cons." +
				"?cons skos:prefLabel|rdfs:label ?label.}}";
			
		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getSliceMeasure_query += "}";
		}

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getSliceMeasure_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getSliceMeasure_query);
		List<LDResource> cubeDimensions = new ArrayList<LDResource>();

		try {
			while (res.hasNext()) {
				
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dim").stringValue());

				// check if there is a label (rdfs:label or skos:prefLabel)
				if (bindingSet.getValue("label") != null) {
					ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
				}
				
				//Add the first instance of the dimension (regardless of the language)
				if(!cubeDimensions.contains(ldr)){
					cubeDimensions.add(ldr);
				}else{
					//If ignore language
					if(ignoreLang){
						//First en then everything else
						if(ldr.getLanguage().equals("en")){
							cubeDimensions.remove(ldr);
							cubeDimensions.add(ldr);
						}
					}else{
						for(LDResource exisitingLdr:cubeDimensions){
							//Find the existing dimension that has the same URI (different language)
							if(exisitingLdr.equals(ldr)){
							//	if(ldr.getLanguage()!=null){
									//The new ldr has the preferred language
									if(ldr.getLanguage().equals(lang)){
										cubeDimensions.remove(ldr);
										cubeDimensions.add(ldr);
									//The new ldr has the default language and the existing does 
									//not have the preferred language
									}else if (ldr.getLanguage().equals(defaultlang)&&
										!exisitingLdr.getLanguage().equals(lang)){
										cubeDimensions.remove(ldr);
										cubeDimensions.add(ldr);
									}
							//	}
							}
						}
					}
				}				
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return cubeDimensions;
	}

	// Get all the measure of a slice
	// Input: The sliceURI, slice Graph,language, SPARQL service
	// The slice Graph and SPARQL service can be null if not available
	public static List<LDResource> getSliceMeasure(String sliceURI,
			String sliceGraph, String cubeDSDGraph,String lang, String defaultlang,
			boolean ignoreLang,String SPARQLservice) {

		String getSliceMeasure_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "select  distinct ?measure ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getSliceMeasure_query += "SERVICE " + SPARQLservice + " {";
		}

		// If a cube graph is defined
		if (sliceGraph != null) {
			getSliceMeasure_query += "GRAPH <" + sliceGraph + "> {";
		}

		getSliceMeasure_query += sliceURI + "qb:sliceStructure ?sliceKey."
				+ "?dsd qb:sliceKey ?sliceKey.";

		// If a cube DSD graph is defined
		if (sliceGraph != null) {
			getSliceMeasure_query += "}";
		}

		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getSliceMeasure_query += "GRAPH <" + cubeDSDGraph + "> {";
		}
		
		getSliceMeasure_query += "?dsd qb:component  ?cs."
				+ "?cs qb:measure  ?measure." +
				"OPTIONAL{?measure qb:concept ?cons. " +
				"?cons skos:prefLabel|rdfs:label ?label.}}";				
		
		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getSliceMeasure_query += "}";
		}

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getSliceMeasure_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getSliceMeasure_query);

		List<LDResource> cubeMeasures = new ArrayList<LDResource>();

		try {
			while (res.hasNext()) {
				
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("measure").stringValue());

				// check if there is a label (rdfs:label or skos:prefLabel)
				if (bindingSet.getValue("label") != null) {
					ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
				}
				
				//Add the first instance of the dimension (regardless of the language)
				if(!cubeMeasures.contains(ldr)){
					cubeMeasures.add(ldr);
				}else{
					//If ignore language
					if(ignoreLang){
						//First en then everything else
						if(ldr.getLanguage().equals("en")){
							cubeMeasures.remove(ldr);
							cubeMeasures.add(ldr);
						}
					}else{
						for(LDResource exisitingLdr:cubeMeasures){
							//Find the existing dimension that has the same URI (different language)
							if(exisitingLdr.equals(ldr)){
								//The new ldr has the preferred language
								if(ldr.getLanguage().equals(lang)){
									cubeMeasures.remove(ldr);
									cubeMeasures.add(ldr);
								//The new ldr has the default language and the existing does 
								//not have the preferred language
								}else if (ldr.getLanguage().equals(defaultlang)&&
									!exisitingLdr.getLanguage().equals(lang)){
									cubeMeasures.remove(ldr);
									cubeMeasures.add(ldr);
								}
							}
						}
					}
				}	
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		
		
		
		return cubeMeasures;
	}

	public static List<LDResource> getDimensionValuesFromSlice(
			String dimensionURI, String sliceURI, String cubeGraph,
			String cubeDSDGraph, String sliceGraph, String lang,
			String defaultlang, boolean ignoreLang,String SPARQLservice) {

		String getDimensionValues_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "select  distinct ?value ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getDimensionValues_query += "SERVICE " + SPARQLservice + " {";
		}

		// If a slice graph is defined
		if (sliceGraph != null) {
			getDimensionValues_query += "GRAPH <" + sliceGraph + "> {";
		}

		getDimensionValues_query += sliceURI + " qb:observation ?observation .";

		// If a slice graph is defined
		if (sliceGraph != null) {
			getDimensionValues_query += "}";
		}

		// If a cube graph is defined
		if (cubeGraph != null) {
			getDimensionValues_query += "GRAPH <" + cubeGraph + "> {";
		}

		getDimensionValues_query += "?observation <" + dimensionURI	+ "> ?value.";

		// If a cube graph is defined
		if (cubeGraph != null) {
			getDimensionValues_query += "}";
		}

		getDimensionValues_query += "OPTIONAL{";
		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getDimensionValues_query += "GRAPH <" + cubeDSDGraph + "> {";
		}
		
		getDimensionValues_query+="?value skos:prefLabel|rdfs:label ?label.}}";
	
		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getDimensionValues_query += "}";
		}

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getDimensionValues_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getDimensionValues_query);
		List<LDResource> dimensionValues = new ArrayList<LDResource>();

		try {
			while (res.hasNext()) {
				
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("value").stringValue());

				// check if there is a label (rdfs:label or skos:prefLabel)
				if (bindingSet.getValue("label") != null) {
					ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
				}
				
				//Add the first instance of the dimension (regardless of the language)
				if(!dimensionValues.contains(ldr)){
					dimensionValues.add(ldr);
				}else{
					//If ignore language
					if(ignoreLang){
						//First en then everything else
						if(ldr.getLanguage().equals("en")){
							dimensionValues.remove(ldr);
							dimensionValues.add(ldr);
						}
					}else{
						for(LDResource exisitingLdr:dimensionValues){
							//Find the existing dimension that has the same URI (different language)
							if(exisitingLdr.equals(ldr)){
								//The new ldr has the preferred language
								if(ldr.getLanguage().equals(lang)){
									dimensionValues.remove(ldr);
									dimensionValues.add(ldr);
								//The new ldr has the default language and the existing does 
								//not have the preferred language
								}else if (ldr.getLanguage().equals(defaultlang)&&
									!exisitingLdr.getLanguage().equals(lang)){
									dimensionValues.remove(ldr);
									dimensionValues.add(ldr);
								}
							}
						}
					}
				}
			}
		} catch (QueryEvaluationException e1) {
			e1.printStackTrace();
			System.out.println(dimensionURI);

		}

		Collections.sort(dimensionValues);
		return dimensionValues;
	}

	// Get the cube URI using as starting point a Slice
	public static String getCubeGraphFromSlice(String sliceURI,
			String sliceGraph, String SPARQLservice) {

		String geCubeGraphFromSlice_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "select distinct ?graph_uri where{";

		if (SPARQLservice != null) {
			geCubeGraphFromSlice_query += "SERVICE " + SPARQLservice + " {";
		}

		if (sliceGraph != null) {
			geCubeGraphFromSlice_query += "GRAPH <" + sliceGraph + "> {";
		}

		geCubeGraphFromSlice_query += "?cube qb:slice " + sliceURI + ". ";

		if (sliceGraph != null) {
			geCubeGraphFromSlice_query += "}";
		}

		geCubeGraphFromSlice_query += " GRAPH ?graph_uri{?cube rdf:type qb:DataSet }}";

		if (SPARQLservice != null) {
			geCubeGraphFromSlice_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(geCubeGraphFromSlice_query);

		String graphURI = null;
		try {
			if (res.hasNext()) {
				graphURI = res.next().getValue("graph_uri").toString();
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return graphURI;
	}

	public static String getCubeStructureGraphFromSlice(String sliceURI,
			String sliceGraph, String SPARQLservice) {

		String getCubeStructureGraphFromSlice_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "select distinct ?graph_uri where{";

		if (SPARQLservice != null) {
			getCubeStructureGraphFromSlice_query += "SERVICE " + SPARQLservice+ " {";
		}

		if (sliceGraph != null) {
			getCubeStructureGraphFromSlice_query += "GRAPH <" + sliceGraph	+ "> {";
		}

		getCubeStructureGraphFromSlice_query += sliceURI
				+ " qb:sliceStructure ?slicekey."
				+ "?cubeDSD qb:sliceKey ?slicekey.";

		if (sliceGraph != null) {
			getCubeStructureGraphFromSlice_query += "}";
		}

		getCubeStructureGraphFromSlice_query += " GRAPH ?graph_uri{"
				+ "?cubeDSD rdf:type qb:DataStructureDefinition }}";

		if (SPARQLservice != null) {
			getCubeStructureGraphFromSlice_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getCubeStructureGraphFromSlice_query);

		String graphURI = null;
		try {
			while (res.hasNext()) {
				graphURI = res.next().getValue("graph_uri").toString();				
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return graphURI;
	}

	// TO DO ADD SPARQL SERVICE
	public static String createCubeSlice(String cubeURI, String cubeGraphURI,
			HashMap<LDResource, LDResource> sliceFixedDimensions,
			List<LDResource> sliceObservation,String SPARQLservice) {

		// create random slice graph
		Random rand = new Random();
		long rnd = Math.abs(rand.nextLong());

		String sliceGraph = "<http://www.fluidops.com/resource/graph_" + rnd+ ">";

		// check if graph already exists
		while (existGraph(sliceGraph)) {
			rnd = Math.abs(rand.nextLong());
			sliceGraph = "<http://www.fluidops.com/resource/graph_" + rnd + ">";
		}

		// create random slice URI
		String sliceURI = "<http://www.fluidops.com/resource/slice_" + rnd+ ">";

		// create random slice key
		String sliceKeyURI = "<http://www.fluidops.com/resource/sliceKey" + rnd	+ ">";

		// INSERT SLICE STRUCTURE QUERY
		String insert_slice_structure_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";
		
		if (SPARQLservice != null) {
			insert_slice_structure_query += "SERVICE " + SPARQLservice + " {";
		}
		
		insert_slice_structure_query+="graph "	+ sliceGraph+ " {"
				+ sliceKeyURI+ " rdf:type qb:SliceKey. ";

		// Add fixed dimensions to slice-key structure
		for (LDResource ldr : sliceFixedDimensions.keySet()) {
			insert_slice_structure_query += sliceKeyURI
					+ " qb:componentProperty <" + ldr.getURI() + ">. ";
		}

		// insert_slice_structure_query += sliceKeyURI + " qb:measure <"+
		// sliceMeasure + ">. ";

		insert_slice_structure_query += sliceURI + " rdf:type qb:Slice."
				+ sliceURI + " qb:sliceStructure " + sliceKeyURI + ". "
				+ cubeURI + " qb:slice " + sliceURI + ". "
				+ "<"+ CubeSPARQL.getCubeDSD(cubeURI, cubeGraphURI)+ "> " 
				+ "qb:sliceKey " + sliceKeyURI + ". ";

		// Add fixed dimensions values to slice
		for (LDResource ldr : sliceFixedDimensions.keySet()) {
			String dimensionValue = "";
			// is URI
			if (sliceFixedDimensions.get(ldr).getURI().contains("http")) {
				dimensionValue = "<" + sliceFixedDimensions.get(ldr).getURI()
						+ ">";

			// Is literal
			} else {
				dimensionValue = "\"" + sliceFixedDimensions.get(ldr).getURI()	+ "\"";
			}
			insert_slice_structure_query += sliceURI + " <" + ldr.getURI()+ "> " + dimensionValue + ".";
		}

		if (SPARQLservice != null) {
			insert_slice_structure_query += "}";
		}
		
		insert_slice_structure_query += "}}";
		QueryExecutor.executeUPDATE(insert_slice_structure_query);

		System.out.println(insert_slice_structure_query);
		// INSERT SLICE DATA QUERY
		String insert_slice_data_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  { ";
		
		
		if (SPARQLservice != null) {
			insert_slice_data_query += "SERVICE " + SPARQLservice + " {";
		}
		
		insert_slice_data_query+="graph " + sliceGraph + " {";

		int i = 0;
		for (LDResource ldr : sliceObservation) {
			i++;
			insert_slice_data_query += sliceURI + " qb:observation <"+ ldr.getURI() + ">.";

			// Execute an INSERT for every 1000 observations
			if (i == 1000) {
				i = 0;
				
				if (SPARQLservice != null) {
					insert_slice_data_query += "}";
				}
				insert_slice_data_query += "}}";
				QueryExecutor.executeUPDATE(insert_slice_data_query);

				// Empty the Insert query and re-initialize it
				insert_slice_data_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
						+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
						+ "INSERT DATA  { ";
				
				if (SPARQLservice != null) {
					insert_slice_data_query += "SERVICE " + SPARQLservice + " {";
				}
				
				insert_slice_data_query+=" graph " + sliceGraph + " {";
			}
		}
		
		if (SPARQLservice != null) {
			insert_slice_data_query += "}";
		}

		insert_slice_data_query += "}}";

		// If there are still observations not inserted
		if (i > 0) {
			QueryExecutor.executeUPDATE(insert_slice_data_query);
		}

		sliceURI = sliceURI.replaceAll("<", "");
		sliceURI = sliceURI.replaceAll(">", "");

		return sliceURI;

	}

	public static List<LDResource> getSliceObservations(
			HashMap<LDResource, LDResource> fixedDimValues, String cubeURI,
			String cubeGraph, String SPARQLservice) {
		List<LDResource> sliceObservations = new ArrayList<LDResource>();

		String getSliceObservations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "Select ?obs where{";

		if (SPARQLservice != null) {
			getSliceObservations_query += "SERVICE " + SPARQLservice + " {";
		}

		if (cubeGraph != null) {
			getSliceObservations_query += "GRAPH <" + cubeGraph + "> {";
		}

		getSliceObservations_query += "?obs qb:dataSet " + cubeURI + ".";

		// Add fixed dimensions to where clause, select the first value of the
		// list of all values
		int j = 1;
		for (LDResource fDim : fixedDimValues.keySet()) {
			getSliceObservations_query += "?obs <" + fDim.getURI() + "> ";
			if (fixedDimValues.get(fDim).getURI().contains("http")) {
				getSliceObservations_query += "<"
						+ fixedDimValues.get(fDim).getURI() + ">.";
			} else {
				getSliceObservations_query += "?value_" + j
						+ ". FILTER(STR(?value_" + j + ")='"
						+ fixedDimValues.get(fDim).getURI() + "')";
			}
			j++;
		}

		getSliceObservations_query += "}";
		
		if (cubeGraph != null) {
			getSliceObservations_query += "}";
		}

		if (SPARQLservice != null) {
			getSliceObservations_query += "}";
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(getSliceObservations_query);

		try {

			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("obs")
						.stringValue());
				sliceObservations.add(ldr);
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return sliceObservations;

	}
	
	//Get the Geo Dimensions of a Slice
	public static LDResource getGeoDimension(String sliceURI,
			String sliceGraph, String cubeDSDGraph, String lang,
			String SPARQLservice) {
		LDResource ldr = null;
		String getGeoDimension_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX sdmxDim: <http://purl.org/linked-data/sdmx/2009/dimension#>"
				+ "PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>  " +

				"SELECT DISTINCT ?uri ?label WHERE {";
		
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getGeoDimension_query += "SERVICE " + SPARQLservice + " {";
		}
		
		// If a cube graph is defined
		if (sliceGraph != null) {
			getGeoDimension_query += "GRAPH <" + sliceGraph + "> {";
		}
		
		getGeoDimension_query += sliceURI + " qb:sliceStructure ?slicekey." +
				"?dsd qb:sliceKey ?slicekey.";
		
		// If a cube graph is defined
		if (sliceGraph != null) {
			getGeoDimension_query += "}";
		}
		
		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getGeoDimension_query += "GRAPH <" + cubeDSDGraph + "> {";
		}
		
		getGeoDimension_query += "?dsd qb:component  ?comp."
				+ "?comp qb:dimension ?uri."
				+ "?uri a ?dimensionType;"
				+ "     a qb:DimensionProperty. "
				+ "OPTIONAL {"
				+ "?uri qb:concept ?concept."
				+ "?concept skos:prefLabel ?label.FILTER(LANGMATCHES(LANG(?label), \""+lang+"\"))"
				+ "}" + "{" + "{ ?uri rdfs:subPropertyOf+ sdmxDim:refArea }"
				+ "UNION" + "{ ?uri a sdmxDim:refArea }" + "}}";

		// If cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getGeoDimension_query += "}";
		}
		
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getGeoDimension_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getGeoDimension_query);
		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				ldr = new LDResource(bindingSet.getValue("uri").stringValue());
				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}
			}
			return ldr;
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	public static List<LDResource> getDataCubeAttributesFromSlice(String sliceURI,
			String sliceGraph, String cubeDSDGraph, String lang, String SPARQLservice) { // Areti

		String getCubeAttributes_query = "PREFIX property: <http://eurostat.linked-statistics.org/property#> "
				+ "PREFIX  qb: <http://purl.org/linked-data/cube#>  "
				+ "PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>  "
				+ "PREFIX concept:  <http://eurostat.linked-statistics.org/concept#> "
				+ "PREFIX dsd:  <http://stats.data-gov.ie/dsd/> "
				+ "select  distinct ?attribute ?label where {";
		
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCubeAttributes_query += "SERVICE " + SPARQLservice + " {";
		}
		
		// If a cube graph is defined
		if (sliceGraph != null) {
			getCubeAttributes_query += "GRAPH <" + sliceGraph + "> {";
		}
		
		getCubeAttributes_query += sliceURI + " qb:sliceStructure ?slicekey." +
				"?dsd qb:sliceKey ?slicekey.";
		
		// If a cube graph is defined
		if (sliceGraph != null) {
			getCubeAttributes_query += "}";
		}
		
		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getCubeAttributes_query += "GRAPH <" + cubeDSDGraph + "> {";
		}
		getCubeAttributes_query += "?dsd qb:component ?comp. "
				+ "?comp qb:attribute ?attribute. "
				+ "OPTIONAL {?attribute qb:concept ?concept. "
				+ "?concept skos:prefLabel ?label."
				+ "FILTER(LANGMATCHES(LANG(?label), \""+lang+"\")) }}";

		// If cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getCubeAttributes_query += "}";
		}

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCubeAttributes_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(getCubeAttributes_query);
		List<LDResource> cubeAttributes = new ArrayList<LDResource>();

		try {
			while (res.hasNext()) {
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet
						.getValue("attribute").stringValue());

				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}
				cubeAttributes.add(ldr);
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return cubeAttributes;
	}

	public static boolean existGraph(String graphURI) {
		String askQuery = "ASK {graph " + graphURI + "{?x ?y ?z}}";
		return QueryExecutor.executeASK(askQuery);

	}

}
