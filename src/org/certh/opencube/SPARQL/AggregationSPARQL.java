package org.certh.opencube.SPARQL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.certh.opencube.utils.LDResource;
import org.openrdf.model.Literal;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

public class AggregationSPARQL {

	public static List<LDResource> getCubesWithNoAggregationSet(){
		
		String getCubesWithNoAggregationSet_query=
				"PREFIX  qb: <http://purl.org/linked-data/cube#>" +
				"select ?dataset where {" +
				"GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet.} " +
				"MINUS{  GRAPH ?aggGraph{ ?dataset qb:aggregationSet ?set}}}";
		
		TupleQueryResult res = QueryExecutor.executeSelect(getCubesWithNoAggregationSet_query);
		
		ArrayList<LDResource> cubesWithNoAggregationSet=new ArrayList<LDResource>();
		
		try {
			while(res.hasNext()){
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
				cubesWithNoAggregationSet.add(ldr);
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
	
		return cubesWithNoAggregationSet;
			 
	}

	public static String createNewAggregationSet(String aggregationGraph,
			String SPARQLservice) {

		// create random aggregation set
		Random rand = new Random();
		long rnd = Math.abs(rand.nextLong());

		String aggregationSetURI = "<http://www.fluidops.com/resource/aggregationSet_"
				+ rnd + ">";

		// INSERT NEW AGGREGATION SET
		String insert_aggregationSet_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			insert_aggregationSet_query += "SERVICE " + SPARQLservice + " {";
		}

		if (aggregationGraph != null) {
			insert_aggregationSet_query += "GRAPH <" + aggregationGraph + "> {";
		}

		insert_aggregationSet_query += aggregationSetURI
				+ " rdf:type qb:AggregationSet.} ";

		if (aggregationGraph != null) {
			insert_aggregationSet_query += "}";
		}

		if (SPARQLservice != null) {
			insert_aggregationSet_query += "}";
		}

		QueryExecutor.executeUPDATE(insert_aggregationSet_query);

		return aggregationSetURI;

	}

	public static void attachCube2AggregationSet(String aggregationSetURI,
			String aggregationSetGraph, String datacubeURI, String SPARQLservice) {

		// Attach cube 2 aggregation set
		String attach_cube2aggregationSet_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		// If sparql service defined
		if (SPARQLservice != null) {
			attach_cube2aggregationSet_query += "SERVICE " + SPARQLservice
					+ " {";
		}

		// if aggregation set graph defined
		if (aggregationSetGraph != null) {
			attach_cube2aggregationSet_query += "GRAPH <" + aggregationSetGraph
					+ "> {";
		}

		attach_cube2aggregationSet_query += datacubeURI + " qb:aggregationSet "
				+ aggregationSetURI + " .} ";

		if (aggregationSetGraph != null) {
			attach_cube2aggregationSet_query += "}";
		}

		if (SPARQLservice != null) {
			attach_cube2aggregationSet_query += "}";
		}

		QueryExecutor.executeUPDATE(attach_cube2aggregationSet_query);

	}

	public static List<LDResource> getAggegationSetDimsFromCube(String cubeURI,
			String cubeDSDGraph, String lang,String defaultlang,boolean ignoreLang,  String SPARQLservice) {

		String getAggegationSetDimsFromCube_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "select  distinct ?dim ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAggegationSetDimsFromCube_query += "SERVICE " + SPARQLservice + " {";
		}

		// If a cube graph is defined
		if (cubeDSDGraph != null) {
			getAggegationSetDimsFromCube_query += "GRAPH <" + cubeDSDGraph	+ "> {";
		}

		getAggegationSetDimsFromCube_query += cubeURI+ " qb:aggregationSet ?set."
				+ "?dataset qb:aggregationSet ?set."
				+ "?dsd qb:component ?comp."
				+ "?comp qb:dimension ?dim."
				+"OPTIONAL{?dim qb:concept ?cons." +
				 "?cons skos:prefLabel|rdfs:label ?label.}";	

		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getAggegationSetDimsFromCube_query += "}";
		}

		getAggegationSetDimsFromCube_query += "{select ?cubeGraph where {GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet.}}}"
				+ "GRAPH ?cubeGraph{?dataset qb:structure ?dsd.}}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAggegationSetDimsFromCube_query += "}";
		}
		
		TupleQueryResult res = QueryExecutor.executeSelect(getAggegationSetDimsFromCube_query);
		List<LDResource> aggegationSetDimensions = new ArrayList<LDResource>();

		try {

			while (res.hasNext()) {
				
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dim").stringValue());

				// check if there is a label (rdfs:label or skos:prefLabel)
				if (bindingSet.getValue("label") != null) {
					ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
				}
				
				//Add the first instance of the dimension (regardless of the language)
				if(!aggegationSetDimensions.contains(ldr)){
					aggegationSetDimensions.add(ldr);
				}else{
					//If ignore language
					if(ignoreLang){
						//First en then everything else
						if(ldr.getLanguage().equals("en")){
							aggegationSetDimensions.remove(ldr);
							aggegationSetDimensions.add(ldr);
						}
					}else{
						for(LDResource exisitingLdr:aggegationSetDimensions){
							//Find the existing dimension that has the same URI (different language)
							if(exisitingLdr.equals(ldr)){
								//The new ldr has the preferred language
								if(ldr.getLanguage().equals(lang)){
									aggegationSetDimensions.remove(ldr);
									aggegationSetDimensions.add(ldr);
								//The new ldr has the default language and the existing does 
								//not have the preferred language
								}else if (ldr.getLanguage().equals(defaultlang)&&
										!exisitingLdr.getLanguage().equals(lang)){
									aggegationSetDimensions.remove(ldr);
									aggegationSetDimensions.add(ldr);
								}
							}
						}
					}
				}			
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return aggegationSetDimensions;

	}
	
	public static HashMap<LDResource, List<LDResource>> getCubeAndDimensionsOfAggregateSet(String cubeURI,
			String cubeDSDGraph, String lang,String defaultlang, boolean ignorelang,String SPARQLservice) {

		String getAggegationSetDimsFromCube_query = "PREFIX  qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "select  distinct ?dataset ?dim ?label where {";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAggegationSetDimsFromCube_query += "SERVICE " + SPARQLservice+ " {";
		}

		// If a cube graph is defined
		if (cubeDSDGraph != null) {
			getAggegationSetDimsFromCube_query += "GRAPH <" + cubeDSDGraph+ "> {";
		}

		getAggegationSetDimsFromCube_query += cubeURI
				+ " qb:aggregationSet ?set."
				+ "?dataset qb:aggregationSet ?set."
				+ "?dsd qb:component ?comp."
				+ "?comp qb:dimension ?dim."
				+"OPTIONAL{?dim qb:concept ?cons." +
				"?cons skos:prefLabel|rdfs:label ?label.}";
			
		// If a cube DSD graph is defined
		if (cubeDSDGraph != null) {
			getAggegationSetDimsFromCube_query += "}";
		}

		getAggegationSetDimsFromCube_query += "{select ?cubeGraph where {GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet.}}}"
				+ "GRAPH ?cubeGraph{?dataset qb:structure ?dsd.}}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getAggegationSetDimsFromCube_query += "}";
		}
		
		TupleQueryResult res = QueryExecutor.executeSelect(getAggegationSetDimsFromCube_query);
		HashMap<LDResource, List<LDResource>> aggregationSetCubeDimensions = new 
				HashMap<LDResource, List<LDResource>>();

		try {
			while (res.hasNext()) {
				
				BindingSet bindingSet = res.next();
				LDResource cube = new LDResource(bindingSet.getValue("dataset").stringValue());
				LDResource dim = new LDResource(bindingSet.getValue("dim").stringValue());

				// check if there is a label (rdfs:label or skos:prefLabel)
				if (bindingSet.getValue("label") != null) {
					dim.setLabelLiteral((Literal)bindingSet.getValue("label"));							
				}			
				
				if (aggregationSetCubeDimensions.get(cube) == null) {
					List<LDResource> cubeDimensions = new ArrayList<LDResource>();
					cubeDimensions.add(dim);
					aggregationSetCubeDimensions.put(cube, cubeDimensions);
				} else {
					List<LDResource> cubeDimensions = aggregationSetCubeDimensions.get(cube);
				
					//Add the first instance of the dimension (regardless of the language)
					if(!cubeDimensions.contains(dim)){
						cubeDimensions.add(dim);
					}else{
						//If ignore language
						if(ignorelang){
							//First en then everything else
							if(dim.getLanguage().equals("en")){
								cubeDimensions.remove(dim);
								cubeDimensions.add(dim);
							}
						}else{
							for(LDResource exisitingDim:cubeDimensions){
								//Find the existing dimension that has the same URI (different language)
								if(exisitingDim.equals(dim)){
									//The new ldr has the preferred language
									if(dim.getLanguage().equals(lang)){
										cubeDimensions.remove(dim);
										cubeDimensions.add(dim);
									//The new ldr has the default language and the existing does 
									//not have the preferred language
									}else if (dim.getLanguage().equals(defaultlang)&&
										!exisitingDim.getLanguage().equals(lang)){
										cubeDimensions.remove(dim);
										cubeDimensions.add(dim);
									}
								}
							}
						}
					}				
					aggregationSetCubeDimensions.put(cube, cubeDimensions);
				}
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return aggregationSetCubeDimensions;
			
	}

	// TO DO::::: HANDLE MULTIPLE MEASURES!!!!!!!!!!!!!!!

	// TO DO: DO SOMETHONG WITH THE RANDOM. CHECK IF EXIST? OR FOR OBSERVATIONS
	// START FROM 1...N
	public static String createCubeForAggregationSet(
			Set<LDResource> dimensions, List<LDResource> measures,
			String originalCubeURI, String originalCubeGraph, String DSDgraph,
			String aggregationSetURI, String SPARQLservice) {

		// create random DSD, Cube URI and Cube Graph URI
		Random rand = new Random();
		long rnd = Math.abs(rand.nextLong());

		String newDSD_URI = "<http://www.fluidops.com/resource/dsd_" + rnd+ ">";
		String newCube_URI = "<http://www.fluidops.com/resource/cube_" + rnd+ ">";
		String newCubeGraph_URI = "<http://www.fluidops.com/resource/graph_"+ rnd + ">";

		// Add new DSD
		String create_new_dsd_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_dsd_query += "SERVICE " + SPARQLservice + " {";
		}

		if (DSDgraph != null) {
			create_new_dsd_query += "GRAPH <" + DSDgraph + "> {";
		}

		create_new_dsd_query += newDSD_URI
				+ " rdf:type qb:DataStructureDefinition.";

		// Add dimensions to DSD
		for (LDResource ldr : dimensions) {
			rnd = Math.abs(rand.nextLong());
			String newComponentSpecification_URI = "<http://www.fluidops.com/resource/componentSpecification_"
					+ rnd + ">";
			create_new_dsd_query += newDSD_URI + " qb:component "
					+ newComponentSpecification_URI + "."
					+ newComponentSpecification_URI + " qb:dimension <"
					+ ldr.getURI() + ">.";
		}

		// Add measures to DSD
		for (LDResource m : measures) {
			rnd = Math.abs(rand.nextLong());
			String newComponentSpecification_URI = "<http://www.fluidops.com/resource/componentSpecification_"
					+ rnd + ">";
			create_new_dsd_query += newDSD_URI + " qb:component "+ newComponentSpecification_URI + "."
					+ newComponentSpecification_URI + " qb:measure <" + m.getURI()+ ">.";
		}

		// Attach Cube to Aggregation Set
		create_new_dsd_query += newCube_URI + " qb:aggregationSet "
				+ aggregationSetURI + " .} ";

		if (DSDgraph != null) {
			create_new_dsd_query += "}";
		}

		if (SPARQLservice != null) {
			create_new_dsd_query += "}";
		}

		QueryExecutor.executeUPDATE(create_new_dsd_query);

		// Create new cube
		String create_new_cube_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "INSERT DATA  {";

		if (SPARQLservice != null) {
			create_new_cube_query += "SERVICE " + SPARQLservice + " {";
		}

		create_new_cube_query += "GRAPH " + newCubeGraph_URI + " {"
				+ newCube_URI + " rdf:type qb:DataSet." + newCube_URI
				+ " qb:structure " + newDSD_URI + ".";

		// Query to get aggregated observations
		String aggregated_observations_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "Select ";
		int i = 1;

		// Add dimension variables to query
		for (LDResource ldr : dimensions) {
			aggregated_observations_query += "?dim" + i + " ";
			i++;
		}

		aggregated_observations_query += "(sum(xsd:decimal(?measure))as ?aggregatedMeasure)where{";

		if (SPARQLservice != null) {
			aggregated_observations_query += "SERVICE " + SPARQLservice + " {";
		}

		if (originalCubeGraph != null) {
			aggregated_observations_query += "GRAPH <" + originalCubeGraph
					+ "> {";
		}

		aggregated_observations_query += "?obs qb:dataSet " + originalCubeURI
				+ ".";

		i = 1;
		for (LDResource ldr : dimensions) {
			aggregated_observations_query += "?obs <" + ldr.getURI() + "> ?dim"
					+ i + ".";
			i++;
		}

		for (LDResource m : measures) {
			aggregated_observations_query += "?obs <" + m.getURI() + "> ?measure.";
			i++;
		}

		aggregated_observations_query += "}";

		if (originalCubeGraph != null) {
			aggregated_observations_query += "}";
		}

		if (SPARQLservice != null) {
			aggregated_observations_query += "}";
		}

		// Group by dimensions to get aggregated value
		aggregated_observations_query += "GROUP BY";

		i = 1;
		for (LDResource ldr : dimensions) {
			aggregated_observations_query += " ?dim" + i;
			i++;
		}

		TupleQueryResult res = QueryExecutor
				.executeSelect(aggregated_observations_query);

		int count = 0;
		try {

			// Store aggregated results
			List<BindingSet> bs = new ArrayList<BindingSet>();

			while (res.hasNext()) {
				bs.add(res.next());
			}

			// Create new observations for the new aggregated cube. Insert cubes
			// in sets of 100
			for (BindingSet bindingSet : bs) {

				rnd = Math.abs(rand.nextLong());

				// Observation URI
				String newObservation_URI = "<http://www.fluidops.com/resource/observation_"+ rnd + ">";
				create_new_cube_query += newObservation_URI
						+ " rdf:type qb:Observation." + newObservation_URI
						+ " qb:dataSet " + newCube_URI + ".";
				i = 1;
				for (LDResource ldr : dimensions) {
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

				String measure = bindingSet.getValue("aggregatedMeasure")
						.stringValue();

				if (measure.contains("http")) {
					create_new_cube_query += newObservation_URI + " <"
							+ measures.get(0).getURI() + "> <" + measure + ">.";

				// Is literal
				} else {
					create_new_cube_query += newObservation_URI + " <"
							+ measures.get(0).getURI() + "> \"" + measure + "\".";
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
