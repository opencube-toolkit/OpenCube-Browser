package org.certh.opencube.SPARQL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.certh.opencube.utils.CubeHandlingUtils;
import org.certh.opencube.utils.LDResource;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;


public class SelectionSPARQL_Eurostat {
	
public static HashMap<LDResource, HashMap<LDResource, List<LDResource>>> allCubeDimensionValues=new 
			HashMap<LDResource, HashMap<LDResource, List<LDResource>>>();

public static long memoryHits=0;
	
public static List<LDResource> getAllAvailableCubes(String SPARQLservice){
		
		String getAllAvailableCubes_query=
				  "PREFIX  qb: <http://purl.org/linked-data/cube#>" 
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+"select ?dataset ?label where {";
		
		getAllAvailableCubes_query+="GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet " +
				"OPTIONAL{?dataset rdfs:label ?label.}} }";
		
		TupleQueryResult res = QueryExecutor.executeSelect_direct(getAllAvailableCubes_query, SPARQLservice);
		
		ArrayList<LDResource> allCubes=new ArrayList<LDResource>();
		
		try {
			while(res.hasNext()){
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
				if(bindingSet.getValue("label")!=null){
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}
				allCubes.add(ldr);
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
	
		return allCubes;			 
	}

public static Map<LDResource,Integer> getAllAvailableCubesAndDimCount(String SPARQLservice){
	
	String getAllAvailableCubesAndDimCount_query=
			  "PREFIX  qb: <http://purl.org/linked-data/cube#>" 
			+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" 
			+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
			+"select distinct ?dataset (COUNT(?dim) AS ?dimcount) ?label where {";
	
	getAllAvailableCubesAndDimCount_query+="GRAPH ?cubeGraph{" +
			"?dataset rdf:type qb:DataSet.?dataset qb:structure ?dsd." +
			"OPTIONAL{?dataset rdfs:label ?label.}}" +
			"GRAPH ?cubeDSDgraph{?dsd qb:component ?comp.?comp qb:dimension ?dim.}}";
	
		getAllAvailableCubesAndDimCount_query+="GROUP by ?dataset ?label";
	
		TupleQueryResult res = QueryExecutor.executeSelect_direct(
					getAllAvailableCubesAndDimCount_query, SPARQLservice);
	
	Map<LDResource, Integer> allCubesAndDimCount=new HashMap<LDResource, Integer>();
	
	try {
		while(res.hasNext()){
			BindingSet bindingSet = res.next();
			LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
			if(bindingSet.getValue("label")!=null){
				ldr.setLabel(bindingSet.getValue("label").stringValue());
			}
			allCubesAndDimCount.put(ldr,Integer.parseInt(bindingSet.getValue("dimcount").stringValue()));
		}
	} catch (QueryEvaluationException e) {
		e.printStackTrace();
	}

	return allCubesAndDimCount;			 
}

	//Get all compatible cubes
	//Compatible: - have the dimensions of the original cube
    //			  - the dimension instances are at the same xkos:ClassificationLevel
    //			  - there exist common values at each dimension
	public static HashMap<LDResource,List<LDResource>> getMeasureCompatibleCubes(
			LDResource selectedCube, String selectedCubeGraph, String selectedCubeDSDgraph,
			Map<LDResource,Integer> allCubesAndDimCount,
			List<LDResource> cubeDimensions, HashMap<LDResource, List<LDResource>> dimensionsLevels,
			HashMap<LDResource,List<LDResource>> dimensionsConceptSchemes,
			String selectedLanguage,String defaultLang, boolean ignoreLang,String SPARQLservice){
				
		String getCompatibleCubes_query=
				"PREFIX  qb: <http://purl.org/linked-data/cube#>" +
				"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"+
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
				"select distinct ?dataset ?label where {";
		
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice + " { ";
		}
		
		getCompatibleCubes_query+="GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet." +
				"?dataset qb:structure ?dsd " +
				"OPTIONAL{?dataset rdfs:label ?label}}";
		
		int i=1;
		int j=1;
		for(LDResource dim:cubeDimensions){
			getCompatibleCubes_query+="GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"+i+".";
										
			//If there is a code list check the code list
			if(dimensionsConceptSchemes.get(dim)!=null &&dimensionsConceptSchemes.get(dim).size()>0){
				getCompatibleCubes_query+="?comp"+i+" qb:dimension ?dim"+i+".";
				for(LDResource conceptScheme:dimensionsConceptSchemes.get(dim)){
					getCompatibleCubes_query+= "?dim"+i+" qb:codeList <"+conceptScheme.getURI()+">.";
				}
			//if no code list exists check the dimension URI
			}else{
				getCompatibleCubes_query+="?comp"+i+" qb:dimension <"+dim.getURI()+">.";
			}
			
			getCompatibleCubes_query+="}";
	//		for(LDResource level:dimensionsLevels.get(dim)){
	//			getCompatibleCubes_query+="GRAPH ?cubeGraph{?obs"+j+" qb:dataSet ?dataset." +
						//"?obs"+j+" <"+dim.getURI()+"> ?value"+j+".}" +
	//					"?obs"+j+" ?dim"+i+" ?value"+j+".}" +
	//					"GRAPH ?cubeAggregationDSDgraph{<"+level.getURI()+"> skos:member ?value"+j+".}";
	//			j++;
	//		}			
			i++;		
		}
		
		getCompatibleCubes_query+="}";
			
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}
		
		TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);
		
		List<LDResource> compatibleCubes=new ArrayList<LDResource>();
		
		HashMap<LDResource,List<LDResource>> compatibleCubesMeasures=
														new HashMap<LDResource, List<LDResource>>();
		try {
			while(res.hasNext()){
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
				
				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}
				
				if(allCubesAndDimCount.get(ldr)==cubeDimensions.size()){
					compatibleCubes.add(ldr);
				}
				
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
					
		HashMap<LDResource, List<LDResource>> originalCubeDimensionsValues = new HashMap<LDResource, List<LDResource>>();
		for(LDResource cube:compatibleCubes){
			if(!cube.equals(selectedCube)){
				if(originalCubeDimensionsValues.keySet().isEmpty()){
					originalCubeDimensionsValues=CubeHandlingUtils.getDimsValues(
										cubeDimensions, "<"+selectedCube.getURI()+">", selectedCubeGraph, selectedCubeDSDgraph,
										false, selectedLanguage,defaultLang,ignoreLang, SPARQLservice);
				}
				// Get Cube Graph
				String cubeGraph = CubeSPARQL.getCubeSliceGraph("<"+cube.getURI()+">",SPARQLservice);
				
				// Get Cube Structure graph
				String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"+cube.getURI()+">",
						cubeGraph, SPARQLservice);
				
				boolean levelFound=true;
				//check the potential compatible cube if it has the same levels at dimensions
				for(LDResource dim:cubeDimensions){
					for(LDResource level:dimensionsLevels.get(dim)){
						if(!askDimensionLevelInDataCube(cube, level, cubeGraph, cubeDSDGraph, SPARQLservice)){
							levelFound=false;
							break;
						}
					}
					if(!levelFound){
						break;				
					}					
				}
				
				//If it has the same dimension levels
				if(levelFound){
								
					// Get all Cube dimensions
					List<LDResource> compatibleCubeDimensions = CubeSPARQL.getDataCubeDimensions("<"+cube.getURI()+">",
							cubeGraph, cubeDSDGraph, selectedLanguage,defaultLang,ignoreLang,SPARQLservice);
					
					//the same number of dimensions 
					HashMap<LDResource, List<LDResource>> compatibleCubeDimensionsValues=
								CubeHandlingUtils.getDimsValues(compatibleCubeDimensions, 
										"<"+cube.getURI()+">", cubeGraph, cubeDSDGraph,false,
										selectedLanguage,defaultLang,ignoreLang, SPARQLservice);
					
					boolean isCompatible=true;
					//check if there is an intersection at the values of each dimension
					for(LDResource dim:cubeDimensions){
						List<LDResource> dimValues=originalCubeDimensionsValues.get(dim);
						System.out.println(compatibleCubeDimensionsValues.keySet());
						List<LDResource> compatibleCubeDimValues=compatibleCubeDimensionsValues.get(dim);
						boolean found=false;
						
						for(LDResource value:dimValues){
							if(compatibleCubeDimValues.contains(value)){
								found=true;
							}
						}
						if(!found){
							isCompatible=false;
							break;
						}
					}
					
					if(isCompatible){
						// Get the Cube measure
						// The cube measures
						List<LDResource> cubeMeasures =CubeSPARQL.getDataCubeMeasure(
								"<"+cube.getURI()+">", cubeGraph, cubeDSDGraph,
								selectedLanguage, defaultLang,ignoreLang, SPARQLservice);
						compatibleCubesMeasures.put(cube,cubeMeasures);
					}
				}
			}
		}

		return compatibleCubesMeasures;
		
	}

	//Get all compatible cubes
	//Compatible: - have the dimensions of the original cube
	//			  - the dimensions have the same skos:ConceptScheme
	//			  - the dimension instances are at the same xkos:ClassificationLevel
	public static List<LDResource> geAddValueToLevelCompatibleCubes(
			Map<LDResource, Integer> allCubesAndDimCount,
			LDResource selectedcube,
			List<LDResource> cubeDimensions, HashMap<LDResource, List<LDResource>> dimensionsLevels,
			List<LDResource> cubeMeasures,
			HashMap<LDResource,List<LDResource>> dimensionsConceptSchemes,
			String selectedLanguage, String defaultLang,boolean ignoreLang, String SPARQLservice){
				
		String getCompatibleCubes_query=
				"PREFIX  qb: <http://purl.org/linked-data/cube#>" +
				"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"+
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
				"select distinct ?dataset ?label where {";
		
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "SERVICE " + SPARQLservice + " { ";
		}
		
		getCompatibleCubes_query+="GRAPH ?cubeGraph{?dataset rdf:type qb:DataSet." +
				"?dataset qb:structure ?dsd " +
				"OPTIONAL{?dataset rdfs:label ?label}}";
		
		int i=1;
		int j=1;
		
		for(LDResource dim:cubeDimensions){
			
			getCompatibleCubes_query+="GRAPH ?cubeDSDgraph{?dsd qb:component ?comp"+i+".";
			
			//If there is a code list check the code list
			if(dimensionsConceptSchemes.get(dim)!=null &&dimensionsConceptSchemes.get(dim).size()>0){
				getCompatibleCubes_query+="?comp"+i+" qb:dimension ?dim"+i+".";
				for(LDResource conceptScheme:dimensionsConceptSchemes.get(dim)){
					getCompatibleCubes_query+= "?dim"+i+" qb:codeList <"+conceptScheme.getURI()+">.";
				}
			//if no code list exists check the dimension URI
			}else{
				getCompatibleCubes_query+="?comp"+i+" qb:dimension <"+dim.getURI()+">.";
			}
			
			getCompatibleCubes_query+="}";
			
			for(LDResource level:dimensionsLevels.get(dim)){
				getCompatibleCubes_query+="GRAPH ?cubeGraph{?obs"+j+" qb:dataSet ?dataset." +
						//"?obs"+j+" <"+dim.getURI()+"> ?value"+j+".}" +
						"?obs"+j+" ?dim"+i+" ?value"+j+".}" +
						"GRAPH ?cubeAggregationDSDgraph{<"+level.getURI()+"> skos:member ?value"+j+".}";
				j++;
			}			
			i++;		
		}
		
		i=1;
		
		for(LDResource measure:cubeMeasures){
			getCompatibleCubes_query+="GRAPH ?cubeDSDgraph{?dsd qb:component ?measurecomp"+i+"." +
					"?measurecomp"+i+" qb:measure <"+measure.getURI()+">.}";
			
		}		
		
		getCompatibleCubes_query+="}";
				
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			getCompatibleCubes_query += "}";
		}
			
		TupleQueryResult res = QueryExecutor.executeSelect(getCompatibleCubes_query);
		
		List<LDResource> potentialCompatibleCubes=new ArrayList<LDResource>();
			
		List<LDResource> compatibleCubes=new ArrayList<LDResource>();
		try {
			while(res.hasNext()){
				BindingSet bindingSet = res.next();
				LDResource ldr = new LDResource(bindingSet.getValue("dataset").stringValue());
					
				if (bindingSet.getValue("label") != null) {
					ldr.setLabel(bindingSet.getValue("label").stringValue());
				}
					
				potentialCompatibleCubes.add(ldr);
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
						
		boolean checkValues=false;
		
		// Get Cube Graph
		String cubeGraph = null;
		
		// Get Cube Structure graph
		String cubeDSDGraph =  null;
				
		HashMap<LDResource, List<LDResource>> cubeDimensionsValues= null;
		
		for(LDResource cube:potentialCompatibleCubes){
						
				//the same number of dimensions 
			if(allCubesAndDimCount.get(cube)==cubeDimensions.size()
					&& !cube.equals(selectedcube)  ){
				
				if(checkValues){
					//first time
					if(cubeGraph==null){
						//Get it only the first time
						if(cubeDimensionsValues==null && 
								allCubeDimensionValues.get(selectedcube)!=null){
							cubeDimensionsValues=allCubeDimensionValues.get(selectedcube);
							memoryHits++;
						}else{
							// Get Cube Graph
							cubeGraph = CubeSPARQL.getCubeSliceGraph("<"+selectedcube.getURI()+">",SPARQLservice);
							
							// Get Cube Structure graph
							cubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"+selectedcube.getURI()+">",
								cubeGraph, SPARQLservice);
												
							
							cubeDimensionsValues=CubeHandlingUtils.getDimsValues(cubeDimensions, 
											"<"+selectedcube.getURI()+">", cubeGraph, cubeDSDGraph,true,
											selectedLanguage,defaultLang,ignoreLang, SPARQLservice);
							allCubeDimensionValues.put(selectedcube, cubeDimensionsValues);
						}
					}
					
					HashMap<LDResource, List<LDResource>> compatibleCubeDimensionsValues=null;
					if(allCubeDimensionValues.get(cube)!=null){
						compatibleCubeDimensionsValues=allCubeDimensionValues.get(cube);
						memoryHits++;
					}else{
						// Get Cube Graph
						String compatibleCubeGraph = CubeSPARQL.getCubeSliceGraph("<"+cube.getURI()+">",SPARQLservice);
						
						// Get Cube Structure graph
						String compatibleCubeDSDGraph = CubeSPARQL.getCubeStructureGraph("<"+cube.getURI()+">",
								cubeGraph, SPARQLservice);
						
						
						// Get all Cube dimensions
						List<LDResource> compatibleCubeDimensions = CubeSPARQL.getDataCubeDimensions(
								"<"+cube.getURI()+">",compatibleCubeGraph, compatibleCubeDSDGraph, selectedLanguage,defaultLang,ignoreLang,
								SPARQLservice);
						
						
						compatibleCubeDimensionsValues=	CubeHandlingUtils.getDimsValues(
										compatibleCubeDimensions,"<"+cube.getURI()+">",
										compatibleCubeGraph, compatibleCubeDSDGraph,true,
										selectedLanguage,defaultLang,ignoreLang, SPARQLservice);
						
						allCubeDimensionValues.put(cube, compatibleCubeDimensionsValues);
					}
					
					boolean isCompatible=true;
					for(LDResource dim:cubeDimensions){
						List<LDResource> dimValues=cubeDimensionsValues.get(dim);
						List<LDResource> compatibleDimValues=compatibleCubeDimensionsValues.get(dim);
						if(!(compatibleDimValues.containsAll(dimValues)&&
								compatibleDimValues.size()==dimValues.size())){
							isCompatible=false;
							break;
						}
					}
					if(isCompatible){
						compatibleCubes.add(cube);
					}
				
				
				}else{			
					compatibleCubes.add(cube);
				}
			}			
		}
					
		return compatibleCubes;
		
	}
	
	// ASK if a cube contains data at a specific dimension level
	// (check if exists an observation with this value)
	public static boolean askDimensionLevelInDataCube(LDResource cube,
			LDResource dimensionLevel, String cubeGraph, String cubeDSDgraph, String SPARQLservice) {
		String askDimensionLevelInDataCube_query = "" +
				"PREFIX  qb: <http://purl.org/linked-data/cube#>" +
				"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>" +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
				"ASK where{";

		if (SPARQLservice != null) {
			askDimensionLevelInDataCube_query += "SERVICE " + SPARQLservice	+ " {";
		}
		
		if(cubeGraph!=null){
			askDimensionLevelInDataCube_query += "GRAPH <" + cubeGraph + "> {";
		}
		
		askDimensionLevelInDataCube_query += "?obs qb:dataSet <"+cube.getURI()+">." +
				"?obs ?dim ?value.";
		
		if(cubeGraph!=null){
			askDimensionLevelInDataCube_query += "}";
		}
		
		if(cubeDSDgraph!=null){
			askDimensionLevelInDataCube_query += "GRAPH <" + cubeDSDgraph + "> {";
		}
		
		askDimensionLevelInDataCube_query += "<"+dimensionLevel.getURI()+"> skos:member ?value.}";
		
		if(cubeDSDgraph!=null){
			askDimensionLevelInDataCube_query += "}";
		}
		
		if (SPARQLservice != null) {
			askDimensionLevelInDataCube_query += "}";
		}
			
		return QueryExecutor.executeASK(askDimensionLevelInDataCube_query);
	}
	
	
	/*public static String createCubeForAddValueToLevel(
			Set<LDResource> dimensions, List<LDResource> measures,
			String originalCubeURI, String originalCubeGraph, String DSDgraph,
			String aggregationSetURI, String SPARQLservice) {}*/

}
