package org.certh.opencube.SPARQL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.certh.opencube.utils.LDResource;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

public class CubeSPARQL {
	
	//private static boolean globalDSD = false;
	//private static boolean notime = false;
	
		// Get all the dimensions of a data cube
		// Input: The cubeURI, cubeGraph, SPARQL service
		// The cube Graph, cube DSD graph and SPARQL service can be null if not available
		public static List<LDResource> getDataCubeDimensions(String dataCubeURI,
				String cubeGraph, String cubeDSDGraph, String lang, String defaultlang,
				boolean ignoreLang,	String SPARQLservice) {

			String getCubeDimensions_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
					+ "select  distinct ?dim ?label where {";

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getCubeDimensions_query += "SERVICE " + SPARQLservice + " {";
			}

			// If a cube graph is defined
			if (cubeGraph != null) {
				getCubeDimensions_query += "GRAPH <" + cubeGraph + "> {";
			}

			getCubeDimensions_query += dataCubeURI + " qb:structure ?dsd.";

			// If a cube graph is defined
			if (cubeGraph != null) {
				getCubeDimensions_query += "}";
			}

			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getCubeDimensions_query += "GRAPH <" + cubeDSDGraph + "> {";
			}
			
			getCubeDimensions_query += "?dsd qb:component  ?cs."
					+ "?cs qb:dimension ?dim." +
					"OPTIONAL{?dim qb:concept ?cons.?cons skos:prefLabel|rdfs:label ?label.}}"; 
						
			// If cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getCubeDimensions_query += "}";
			}

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getCubeDimensions_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getCubeDimensions_query);
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
		
		// Get all the measure of a data cube
		// Input: The cubeURI, cubeGraph, label language, SPARQL service
		// The cube Graph and SPARQL service can be null if not available
		public static List<LDResource> getDataCubeMeasure(String dataCubeURI,
				String cubeGraph, String cubeDSDGraph, String lang, String defaultlang,
				boolean ignoreLang,String SPARQLservice) {

			String getCubeMeasure_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
					+ "select  distinct ?dim ?label where {";

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getCubeMeasure_query += "SERVICE " + SPARQLservice + " {";
			}

			// If a cube graph is defined
			if (cubeGraph != null) {
				getCubeMeasure_query += "GRAPH <" + cubeGraph + "> {";
			}

			getCubeMeasure_query += dataCubeURI + "qb:structure ?dsd.";

			// If a cube graph is defined
			if (cubeGraph != null) {
				getCubeMeasure_query += "}";
			}

			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getCubeMeasure_query += "GRAPH <" + cubeDSDGraph + "> {";
			}

			getCubeMeasure_query += "?dsd qb:component  ?cs."
					+ "?cs qb:measure  ?dim."+
					"OPTIONAL{?dim qb:concept ?cons.?cons skos:prefLabel|rdfs:label ?label.}}";
						
			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getCubeMeasure_query += "}";
			}

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getCubeMeasure_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getCubeMeasure_query);

			List<LDResource> cubeMeasures = new ArrayList<LDResource>();

			try {
				while (res.hasNext()) {
										
					BindingSet bindingSet = res.next();
					LDResource ldr = new LDResource(bindingSet.getValue("dim").stringValue());

					// check if there is a label (rdfs:label or skos:prefLabel)
					if (bindingSet.getValue("label") != null) {
						ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
					}
					
					//Add only once a dimension (not one for each available label)
					if(!cubeMeasures.contains(ldr)){
						cubeMeasures.add(ldr);
					}else{
						if(ignoreLang){
							//First en then everything else
							if(ldr.getLanguage().equals("en")){
								cubeMeasures.remove(ldr);
								cubeMeasures.add(ldr);
							}
						}else{
							for(LDResource exisitingLdr:cubeMeasures){
								if(exisitingLdr.equals(ldr)){
									//The new ldr has the preferred language
									if(ldr.getLanguage().equals(lang)){
										cubeMeasures.remove(ldr);
										cubeMeasures.add(ldr);
									//The new ldr has the default language
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
		
		public static List<LDResource> getDimensionValues(String dimensionURI,
				String cubeURI, String cubeGraph, String cubeDSDGraph,
				String lang, String defaultlang,boolean ignoreLang, String SPARQLservice) {

			String getDimensionValues_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
					+ "select  distinct ?value ?label where {";

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getDimensionValues_query += "SERVICE " + SPARQLservice + " {";
			}

			// If a cube graph is defined
			if (cubeGraph != null) {
				getDimensionValues_query += "GRAPH <" + cubeGraph + "> {";
			}

			getDimensionValues_query += "?observation qb:dataSet " + cubeURI + "."
					+ "?observation <" + dimensionURI + "> ?value.";

			// If a cube graph is defined
			if (cubeGraph != null) {
				getDimensionValues_query += "}";
			}	
			
			getDimensionValues_query += "OPTIONAL{";
			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getDimensionValues_query += "GRAPH <" + cubeDSDGraph + "> {";
			}
		
			getDimensionValues_query += "?value skos:prefLabel|rdfs:label ?label}}";
			
			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getDimensionValues_query += "}";
			}	

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getDimensionValues_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getDimensionValues_query);
			List<LDResource> dimensionValues = new ArrayList<LDResource>();

			try {
				while (res.hasNext()) {
					BindingSet bindingSet = res.next();
					LDResource ldr = new LDResource(bindingSet.getValue("value").stringValue());

					// check if there is a label (rdfs:label or skos:prefLabel)
					if (bindingSet.getValue("label") != null) {
						ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
					}
					
					//Add only once a dimension (not one for each available label)
					if(!dimensionValues.contains(ldr)){
						dimensionValues.add(ldr);
					}else{
						if(ignoreLang){
							//First en then everything else
							if(ldr.getLanguage().equals("en")){
								dimensionValues.remove(ldr);
								dimensionValues.add(ldr);
							}
						}else{
							for(LDResource exisitingLdr:dimensionValues){
								if(exisitingLdr.equals(ldr)){
									//The new ldr has the preferred language
									if(ldr.getLanguage().equals(lang)){
										dimensionValues.remove(ldr);
										dimensionValues.add(ldr);
									//The new ldr has the default language
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
		
		// ASK if a cube dimension has a specific value
		// (check if exists an observation with this value)
		public static boolean askDimensionValueInDataCube(String dimensionURI,
				String value, String cubeURI, String SPARQLservice) {
			String askDimensionValueInDataCube_query = "ASK where{";

			if (SPARQLservice != null) {
				askDimensionValueInDataCube_query += "SERVICE " + SPARQLservice
						+ " {";
			}
			askDimensionValueInDataCube_query += 
					" ?obs <http://purl.org/linked-data/cube#dataSet> "	+ cubeURI+ "."
					+ "?obs <"+ dimensionURI+ "> <"	+ value	+ ">}";

			if (SPARQLservice != null) {
				askDimensionValueInDataCube_query += "}";
			}
		
			return QueryExecutor.executeASK(askDimensionValueInDataCube_query);
		}

		public static List<LDResource> getDimensionValuesFromCodeList(
				String dimensionURI, String cubeURI, String cubeDSDGraph,
				String lang, String defaultlang, boolean ignoreLang, String SPARQLservice) {

			String getDimensionValues_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
					//+ "PREFIX skos: <http://www.w3.org/2004/02/skos#>"
					+ "select  ?value ?label where {";

			if (SPARQLservice != null) {
				getDimensionValues_query += "SERVICE " + SPARQLservice + " {";
			}

			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getDimensionValues_query += "GRAPH <" + cubeDSDGraph + "> {";
			}

			getDimensionValues_query += "<"	+ dimensionURI+ "> qb:codeList ?cd."
					+ "?cd skos:hasTopConcept ?value." +
					"OPTIONAL{?value skos:prefLabel|rdfs:label ?label}}";
					
			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getDimensionValues_query += "}";
			}

			if (SPARQLservice != null) {
				getDimensionValues_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getDimensionValues_query);
			List<LDResource> dimensionValues = new ArrayList<LDResource>();
			
			try {
				while (res.hasNext()) {
									
					BindingSet bindingSet = res.next();
					LDResource ldr = new LDResource(bindingSet.getValue("value").stringValue());

					// check if there is a label (rdfs:label or skos:prefLabel)
					if (bindingSet.getValue("label") != null) {
						ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
					}
					
					//Add only once a dimension (not one for each available label)
					if(!dimensionValues.contains(ldr)){
						dimensionValues.add(ldr);
					}else{
						if(ignoreLang){
							//First en then everything else
							if(ldr.getLanguage().equals("en")){
								dimensionValues.remove(ldr);
								dimensionValues.add(ldr);
							}
						}else{
							for(LDResource exisitingLdr:dimensionValues){
								if(exisitingLdr.equals(ldr)){
									//The new ldr has the preferred language
									if(ldr.getLanguage().equals(lang)){
										dimensionValues.remove(ldr);
										dimensionValues.add(ldr);
									//The new ldr has the default language
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
			}

			Collections.sort(dimensionValues);
			return dimensionValues;
		}

	public static String getCubeSliceGraph(String cubeURI, String SPARQLservice) {

			String geCubeGraph_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
					+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
					+ "select distinct ?graph_uri where{";

			if (SPARQLservice != null) {
				geCubeGraph_query += "SERVICE " + SPARQLservice + " {";
			}

			geCubeGraph_query += " GRAPH ?graph_uri{ {" + cubeURI + " rdf:type qb:DataSet }" +
					"UNION {"+cubeURI + " rdf:type qb:Slice}}}";

			if (SPARQLservice != null) {
				geCubeGraph_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(geCubeGraph_query);

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
		
		public static String getCubeStructureGraph(String cubeURI,
				String cubeGraph, String SPARQLservice) {

			String geCubeGraph_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
					+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
					+ "select distinct ?graph_uri where{";

			if (SPARQLservice != null) {
				geCubeGraph_query += "SERVICE " + SPARQLservice + " {";
			}

			if (cubeGraph != null) {
				geCubeGraph_query += "GRAPH <" + cubeGraph + "> {";
			}

			geCubeGraph_query += cubeURI + "qb:structure ?dsd. ";

			if (cubeGraph != null) {
				geCubeGraph_query += "}";
			}

			geCubeGraph_query += "GRAPH ?graph_uri{"
					+ "?dsd rdf:type qb:DataStructureDefinition }}";

			if (SPARQLservice != null) {
				geCubeGraph_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(geCubeGraph_query);

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
		
		public static String getCubeDSD(String cubeURI, String cubeGraphURI) {

			String get_cube_dsd = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "select ?dsd where{";

			if (cubeGraphURI != null) {
				get_cube_dsd += "GRAPH <" + cubeGraphURI + ">{";
			}

			get_cube_dsd += cubeURI + "qb:structure ?dsd.} ";

			if (cubeGraphURI != null) {
				get_cube_dsd += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(get_cube_dsd);

			String cubeDSD = "";

			try {
				while (res.hasNext()) {

					BindingSet bindingSet = res.next();
					cubeDSD = bindingSet.getValue("dsd").stringValue();
				}
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}

			return cubeDSD;

		}
		
		public static List<String> getType(String cubeSliceURI, String cubeSliceGraph,
				String SPARQLservice) {

			String getCubeSliceType_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
					+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
					+ "select ?type where{";

			if (SPARQLservice != null) {
				getCubeSliceType_query += "SERVICE " + SPARQLservice + " {";
			}

			// If a cube/slice graph is defined
			if (cubeSliceGraph != null) {
				getCubeSliceType_query += "GRAPH <" + cubeSliceGraph + "> {";
			}

			getCubeSliceType_query += cubeSliceURI + " rdf:type ?type}";

			// If a cube/slice graph is defined
			if (cubeSliceGraph != null) {
				getCubeSliceType_query += "}";
			}

			if (SPARQLservice != null) {
				getCubeSliceType_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getCubeSliceType_query);

			List<String> types = new ArrayList<String>();
			try {
				while (res.hasNext()) {
					types.add(res.next().getValue("type").toString());
				}
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}

			return types;
		}
		
		
		public static List<LDResource> getDataCubeAttributes(String dataCubeURI,
				String cubeGraph, String cubeDSDGraph, String lang,String defaultlang,
				boolean ignoreLang,	String SPARQLservice) { // Areti

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
			if (cubeGraph != null) {
				getCubeAttributes_query += "GRAPH <" + cubeGraph + "> {";
			}
			
			getCubeAttributes_query += dataCubeURI + " qb:structure ?dsd.";
			
			// If a cube graph is defined
			if (cubeGraph != null) {
				getCubeAttributes_query += "}";
			}
			
			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getCubeAttributes_query += "GRAPH <" + cubeDSDGraph + "> {";
			}
			getCubeAttributes_query += "?dsd qb:component ?comp. "
					+ "?comp qb:attribute ?attribute. "
					+ "OPTIONAL {?attribute qb:concept ?concept. "
					+ "?concept skos:prefLabel|rdfs:label ?label.}}";

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
					LDResource ldr = new LDResource(bindingSet.getValue("attribute").stringValue());

					// check if there is a label (rdfs:label or skos:prefLabel)
					if (bindingSet.getValue("label") != null) {
						ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
					}
					
					//Add only once a attribute (not one for each available label)
					if(!cubeAttributes.contains(ldr)){
						cubeAttributes.add(ldr);
					}else{
						if(ignoreLang){
							//First en then everything else
							if(ldr.getLanguage().equals("en")){
								cubeAttributes.remove(ldr);
								cubeAttributes.add(ldr);
							}
						}else{
							for(LDResource exisitingLdr:cubeAttributes){
								if(exisitingLdr.equals(ldr)){
									//The new ldr has the preferred language
									if(ldr.getLanguage().equals(lang)){
										cubeAttributes.remove(ldr);
										cubeAttributes.add(ldr);
									//The new ldr has the default language
									}else if (ldr.getLanguage().equals(defaultlang)&&
										!exisitingLdr.getLanguage().equals(lang)){
										cubeAttributes.remove(ldr);
										cubeAttributes.add(ldr);
									}
								}
							}
						}
					}			
				}
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}
			return cubeAttributes;
		}
		
		
		//NEED TO CHECK QUERY
		public static List<LDResource> getAttributeValues(String attributeURI,
				String dataCubeURI, String cubeGraph, String cubeDSDGraph,
				String lang,String defaultlang,	boolean ignoreLang, String SPARQLservice) {

			String getAttributeValues_query = "PREFIX qb: <http://purl.org/linked-data/cube#>" // Areti
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
					+ "select  distinct ?value ?label where {";
			
			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getAttributeValues_query += "SERVICE " + SPARQLservice + " {";
			}
			
			// If a cube graph is defined
			if (cubeGraph != null) {
				getAttributeValues_query += "GRAPH <" + cubeGraph + "> {";
			}
			
			getAttributeValues_query += dataCubeURI + " qb:structure ?dsd.";
			
			// If a cube graph is defined
			if (cubeGraph != null) {
				getAttributeValues_query += "}";
			}
			
			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getAttributeValues_query += "GRAPH <" + cubeDSDGraph + "> {";
			}
			
			getAttributeValues_query += "?observation qb:dataSet "	+ dataCubeURI + "."
					+ "?observation <"+ attributeURI+ "> ?value."
					+ "OPTIONAL {?value skos:prefLabel|rdfs:label ?label. }}";

			// If cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getAttributeValues_query += "}";
			}

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getAttributeValues_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getAttributeValues_query);
			List<LDResource> attributeValues = new ArrayList<LDResource>();

			try {
				while (res.hasNext()) {
					
					BindingSet bindingSet = res.next();
					LDResource ldr = new LDResource(bindingSet.getValue("value").stringValue());

					// check if there is a label (rdfs:label or skos:prefLabel)
					if (bindingSet.getValue("label") != null) {
						ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
					}
					
					//Add only once an atribute value (not one for each available label)
					if(!attributeValues.contains(ldr)){
						attributeValues.add(ldr);
					}else{
						if(ignoreLang){
							//First en then everything else
							if(ldr.getLanguage().equals("en")){
								attributeValues.remove(ldr);
								attributeValues.add(ldr);
							}
						}else{
							for(LDResource exisitingLdr:attributeValues){
								if(exisitingLdr.equals(ldr)){
									//The new ldr has the preferred language
									if(ldr.getLanguage().equals(lang)){
										attributeValues.remove(ldr);
										attributeValues.add(ldr);
									//The new ldr has the default language
									}else if (ldr.getLanguage().equals(defaultlang)&&
										!exisitingLdr.getLanguage().equals(lang)){
										attributeValues.remove(ldr);
										attributeValues.add(ldr);
									}
								}
							}
						}
					}						
				}
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}
			Collections.sort(attributeValues);
			return attributeValues;
		}		

		
		public static LDResource getGeoDimension(String dataCubeURI,
				String cubeGraph, String cubeDSDGraph, String lang, String defaultlang,
				boolean ignoreLang, String SPARQLservice) {
			
			String getGeoDimension_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
					+ "PREFIX sdmxDim: <http://purl.org/linked-data/sdmx/2009/dimension#>"
					+ "PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>  " 
					+ "PREFIX sdmxCon: <http://purl.org/linked-data/sdmx/2009/concept#>"
					+ "SELECT DISTINCT ?uri ?label WHERE {";
			
			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getGeoDimension_query += "SERVICE " + SPARQLservice + " {";
			}
			
			// If a cube graph is defined
			if (cubeGraph != null) {
				getGeoDimension_query += "GRAPH <" + cubeGraph + "> {";
			}
			
			getGeoDimension_query += dataCubeURI + " qb:structure ?dsd.";
			
			// If a cube graph is defined
			if (cubeGraph != null) {
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
					+ "?concept skos:prefLabel|rdfs:label ?label." 
					+ "}" +					
             " {{{ ?uri rdfs:subPropertyOf sdmxDim:refArea }" +
              		"UNION{?uri a sdmxDim:refArea} " +
              		"UNION {?uri qb:concept sdmxCon:refArea}}}}";

			// If cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getGeoDimension_query += "}";
			}
			
			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getGeoDimension_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getGeoDimension_query);
			LDResource ldr = null;
			try {
				while (res.hasNext()) {
					
					BindingSet bindingSet = res.next();
					LDResource newldr = new LDResource(bindingSet.getValue("uri").stringValue());

					// check if there is a label (rdfs:label or skos:prefLabel)
					if (bindingSet.getValue("label") != null) {
						newldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
					}
					
					if(ldr!=null){
						if(ignoreLang){
							//First en then everything else
							if(newldr.getLanguage().equals("en")){
								ldr=newldr;
							}
						}else{
							//The new ldr has the preferred language
							if(newldr.getLanguage().equals(lang)){
								ldr=newldr;
								//The new ldr has the default language
							}else if (newldr.getLanguage().equals(defaultlang)&&
								!ldr.getLanguage().equals(lang)){
								ldr=newldr;
							}
						}
					}else{
						ldr=newldr;
					}
				}
				return ldr;
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}
			return null;
		}
		
		//TODO get language from other properties other than the skos:preflabel
		
		//Get the available languages 
		public static List<String> getAvailableCubeLanguages(String cubeDSDGraph, String SPARQLservice){
		
			List<String> availableLanguages=new ArrayList<String>();
			String getAvailableCubeLanguages_query="PREFIX skos: <http://www.w3.org/2004/02/skos/core#>" +
					"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
					"select distinct (lang(?label) as ?lang) where {";
			
			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getAvailableCubeLanguages_query += "SERVICE " + SPARQLservice + " {";
			}
			

			// If a cube graph is defined
			if (cubeDSDGraph != null) {
				getAvailableCubeLanguages_query += "GRAPH <" + cubeDSDGraph + "> {";
			}
			
			getAvailableCubeLanguages_query+="?x skos:prefLabel ?label }";
			
			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
					getAvailableCubeLanguages_query += "}";
			}
			
			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getAvailableCubeLanguages_query += "}";
			}
			
			TupleQueryResult res = QueryExecutor.executeSelect(getAvailableCubeLanguages_query);
			try {
				while (res.hasNext()) {
					BindingSet bindingSet = res.next();
					if(!bindingSet.getValue("lang").stringValue().equals("")){
						availableLanguages.add(bindingSet.getValue("lang").stringValue());
					}
				}
				
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}
			
			
			return availableLanguages;
		
		}

		
		// Get all the xkos:ClassificationLevel from the schema
		public static List<LDResource> getDimensionLevelsFromSchema(String dimensionURI,
				String cubeDSDGraph, String lang, String defaultlang,
				boolean ignoreLang, String SPARQLservice) {

			String getDimensionLevels_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" 
					+ "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>" 
					+ "PREFIX xkos:<http://rdf-vocabulary.ddialliance.org/xkos#>"
					+ "select  distinct ?level ?label where {";

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getDimensionLevels_query += "SERVICE " + SPARQLservice + " {";
			}

			// If a cube graph is defined
			if (cubeDSDGraph != null) {
				getDimensionLevels_query += "GRAPH <" + cubeDSDGraph + "> {";
			}

			getDimensionLevels_query += "<"+dimensionURI +">  qb:codeList ?codelist." +
					"?codelist xkos:levels ?levellist." +
					"?levellist rdf:rest*/rdf:first ?level." +
					"OPTIONAL{?level skos:prefLabel|rdfs:label ?label.}}";
						
			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getDimensionLevels_query += "}";
			}

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getDimensionLevels_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getDimensionLevels_query);

			List<LDResource> dimensionLevels = new ArrayList<LDResource>();

			try {
				while (res.hasNext()) {

					BindingSet bindingSet = res.next();
					LDResource ldr = new LDResource(bindingSet.getValue("level").stringValue());

					// check if there is a label (rdfs:label or skos:prefLabel)				
					if (bindingSet.getValue("label") != null) {
						ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
					}
					
					//Add only once a level (not one for each available label)
					if(!dimensionLevels.contains(ldr)){
						dimensionLevels.add(ldr);
					}else{
						if(ignoreLang){
							//First en then everything else
							if(ldr.getLanguage().equals("en")){
								dimensionLevels.remove(ldr);
								dimensionLevels.add(ldr);
							}
						}else{
							for(LDResource exisitingLdr:dimensionLevels){
								if(exisitingLdr.equals(ldr)){
									//The new ldr has the preferred language
									if(ldr.getLanguage().equals(lang)){
										dimensionLevels.remove(ldr);
										dimensionLevels.add(ldr);
									//The new ldr has the default language
									}else if (ldr.getLanguage().equals(defaultlang)&&
										!exisitingLdr.getLanguage().equals(lang)){
										dimensionLevels.remove(ldr);
										dimensionLevels.add(ldr);
									}
								}
							}
						}
					}			
				}						
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}
			return dimensionLevels;
		}
		
		// Get all the xkos:ClassificationLevel from the schema
		public static List<LDResource> getDimensionLevels(String cubeURI,String dimensionURI,
						String cubeDSDGraph, String cubeGraph,
						String lang, String defaultlang,
						boolean ignoreLang,String SPARQLservice) {

			String getDimensionLevels_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" 
					+ "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>" 
					+ "PREFIX xkos:<http://rdf-vocabulary.ddialliance.org/xkos#>"
					+ "select  distinct ?level ?label where {";

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getDimensionLevels_query += "SERVICE " + SPARQLservice + " {";
			}

			// If a cube graph is defined
			if (cubeGraph != null) {
				getDimensionLevels_query += "GRAPH <" + cubeGraph + "> {";
			}

			getDimensionLevels_query += "?obs qb:dataSet "+cubeURI+"." +
					"?obs <"+dimensionURI+"> ?value." ;
							
			// If a cube graph is defined
			if (cubeGraph != null) {
				getDimensionLevels_query += "}";
			}	
					
					
			// If a cube DSD graph is defined
			if (cubeDSDGraph != null) {
				getDimensionLevels_query += "GRAPH <" + cubeDSDGraph + "> {";
			}

			getDimensionLevels_query +=  "?level skos:member ?value." +
					"OPTIONAL{?level skos:prefLabel|rdfs:label ?label.}}";
										
			// If a cube graph is defined
			if (cubeDSDGraph != null) {
				getDimensionLevels_query += "}";
			}	
									
			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getDimensionLevels_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getDimensionLevels_query);
			List<LDResource> dimensionLevels = new ArrayList<LDResource>();

			try {
				while (res.hasNext()) {
					BindingSet bindingSet = res.next();
					LDResource ldr = new LDResource(bindingSet.getValue("level").stringValue());
					
					// check if there is a label (rdfs:label or skos:prefLabel)
					if (bindingSet.getValue("label") != null) {
						ldr.setLabelLiteral((Literal)bindingSet.getValue("label"));							
					}
					
					//Add only once a dimension (not one for each available label)
					if(!dimensionLevels.contains(ldr)){
						dimensionLevels.add(ldr);
					}else{
						if(ignoreLang){
							//First en then everything else
							if(ldr.getLanguage().equals("en")){
								dimensionLevels.remove(ldr);
								dimensionLevels.add(ldr);
							}
						}else{
							for(LDResource exisitingLdr:dimensionLevels){
								if(exisitingLdr.equals(ldr)){
									//The new ldr has the preferred language
									if(ldr.getLanguage().equals(lang)){
										dimensionLevels.remove(ldr);
										dimensionLevels.add(ldr);
									//The new ldr has the default language
									}else if (ldr.getLanguage().equals(defaultlang)&&
										!exisitingLdr.getLanguage().equals(lang)){
										dimensionLevels.remove(ldr);
										dimensionLevels.add(ldr);
									}
								}
							}
						}
					}			
				}
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}
			return dimensionLevels;
		}
		
		
		// Get all the skos:ConceptScheme of a cube dimension
		public static List<LDResource> getDimensionConceptScheme(String dimensionURI,
								String cubeDSDGraph, String lang,
								String defaultlang, boolean ignoreLang,
								String SPARQLservice) {

			String getDimensionConceptScheme_query = "PREFIX qb: <http://purl.org/linked-data/cube#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" 
					+ "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>" 
					+ "PREFIX xkos:<http://rdf-vocabulary.ddialliance.org/xkos#>"
					+ "select  ?conceptScheme where {";

			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getDimensionConceptScheme_query += "SERVICE " + SPARQLservice + " {";
			}

			// If a cube graph is defined
			if (cubeDSDGraph != null) {
				getDimensionConceptScheme_query += "GRAPH <" + cubeDSDGraph + "> {";
			}

			getDimensionConceptScheme_query += "<"+dimensionURI+"> qb:codeList ?conceptScheme.}";
				
			// If a cube graph is defined
			if (cubeDSDGraph != null) {
				getDimensionConceptScheme_query += "}";
			}					
													
			// If a SPARQL service is defined
			if (SPARQLservice != null) {
				getDimensionConceptScheme_query += "}";
			}

			TupleQueryResult res = QueryExecutor.executeSelect(getDimensionConceptScheme_query);
			List<LDResource> dimensionConceptSchemes = new ArrayList<LDResource>();

			try {
				while (res.hasNext()) {
					BindingSet bindingSet = res.next();
					LDResource ldr = new LDResource(bindingSet.getValue("conceptScheme").stringValue());
					if(!dimensionConceptSchemes.contains(ldr)){
						dimensionConceptSchemes.add(ldr);
					}
				}
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}
			return dimensionConceptSchemes;
		}	
}
