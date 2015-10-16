package org.certh.opencube.SPARQL;

import java.util.HashMap;
import java.util.List;

import org.certh.opencube.utils.LDResource;
import org.openrdf.query.TupleQueryResult;

public class CubeBrowserSPARQL {
	
	public static TupleQueryResult get2DVisualsiationValues(
			List<LDResource> visualDims,
			HashMap<LDResource, LDResource> fixedDims, List<LDResource> selectedMeasures,
			HashMap<LDResource, List<LDResource>> alldimensionvalues,
			String cubeURI, String cubeGraph, String SPARQLservice) {

		String sparql_query = "Select ";
		int i = 1;
		// Add variables ?dim to SPARQL query
		for (LDResource vDim : visualDims) {
			sparql_query += "?dim" + i + " ";
			i++;
		}
		
		i=1;
		// Add variables ?dim to SPARQL query
		for (LDResource meas : selectedMeasures) {
			sparql_query += "?measure_" + meas.getLastPartOfURI().replaceAll("-", "_") + " ";
			i++;
		}

		// Select observations of a specific cube (cubeURI)
		sparql_query += "?obs where{";
		if (SPARQLservice != null) {
			sparql_query += "SERVICE " + SPARQLservice + " {";
		}
		if (cubeGraph != null) {
			sparql_query += "GRAPH <" + cubeGraph + "> {";
		}
		sparql_query += "?obs <http://purl.org/linked-data/cube#dataSet> "
				+ cubeURI + ".";

		i = 1;
		// Add free dimensions to where clause
		for (LDResource vDim : visualDims) {
			sparql_query += "?obs <" + vDim.getURI() + "> " + "?dim" + i + ". ";
			i++;
		}

		// Add fixed dimensions to where clause, select the first value of the
		// list of all values
		int j = 1;
		for (LDResource fDim : fixedDims.keySet()) {
			sparql_query += "?obs <" + fDim.getURI() + "> ";
			if (fixedDims.get(fDim).getURI().contains("http")) {
				sparql_query += "<" + fixedDims.get(fDim).getURI() + ">.";
			} else {
				sparql_query += "?value_" + j + ". FILTER(STR(?value_" + j
						+ ")='" + fixedDims.get(fDim).getURI() + "')";
			}
			j++;

		}

		j=1;
		for(LDResource meas:selectedMeasures){
			sparql_query += "?obs  <" + meas.getURI() + "> ?measure_"+
					meas.getLastPartOfURI().replaceAll("-", "_")+".";
			j++;
		}
		sparql_query+="}";
		if (cubeGraph != null) {
			sparql_query += "}";
		}
		if (SPARQLservice != null) {
			sparql_query += "}";
		}
		TupleQueryResult res = QueryExecutor.executeSelect(sparql_query);
		return res;

	}
	
	//NEED TO TEST!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	public static TupleQueryResult get2DVisualsiationValuesAndLevels(
			List<LDResource> visualDims,
			HashMap<LDResource, LDResource> fixedDims, List<LDResource> selectedMeasures,
			HashMap<LDResource, List<LDResource>> alldimensionvalues,
			HashMap<LDResource, List<LDResource>> selectedDimLevels, 
			HashMap<LDResource, List<LDResource>> allCubesDimensionsLevels,
			String cubeURI, String cubeGraph, String cubeDSDgraph,
			String SPARQLservice) {

		String sparql_query = "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> " +
					"PREFIX xkos:<http://rdf-vocabulary.ddialliance.org/xkos#>"+
					"Select ";
		int i = 1;
		// Add variables ?dim to SPARQL query
		for (LDResource vDim : visualDims) {
			sparql_query += "?dim" + i + " ";
			List<LDResource> dimLevels2Use=selectedDimLevels.get(vDim);
			
			if(dimLevels2Use!=null&&dimLevels2Use.size()>0){
				sparql_query+="?level" + i + " ?parent"+i+" ";
			}
			i++;
		}
		
		i=1;
		// Add variables ?dim to SPARQL query
		for (LDResource meas : selectedMeasures) {
			sparql_query += "?measure_" + meas.getLastPartOfURI().replaceAll("-","_") + " ";
			i++;
		}

		// Select observations of a specific cube (cubeURI)
		sparql_query += "?obs where{";

		if (SPARQLservice != null) {
			sparql_query += "SERVICE " + SPARQLservice + " {";
		}

		if (cubeGraph != null) {
			sparql_query += "GRAPH <" + cubeGraph + "> {";
		}

		sparql_query += "?obs <http://purl.org/linked-data/cube#dataSet> "
				+ cubeURI + ".";

		i = 1;
		// Add free dimensions to where clause
		for (LDResource vDim : visualDims) {
			sparql_query += "?obs <" + vDim.getURI() + "> " + "?dim" + i + ". ";						
			i++;
		}

		// Add fixed dimensions to where clause, select the first value of the
		// list of all values
		int j = 1;
		for (LDResource fDim : fixedDims.keySet()) {
			sparql_query += "?obs <" + fDim.getURI() + "> ";
			if (fixedDims.get(fDim).getURI().contains("http")) {
				sparql_query += "<" + fixedDims.get(fDim).getURI() + ">.";
			} else {
				sparql_query += "?value_" + j + ". FILTER(STR(?value_" + j
						+ ")='" + fixedDims.get(fDim).getURI() + "')";
			}
			j++;
		}

		j=1;
		for(LDResource meas:selectedMeasures){
			sparql_query += "?obs  <" + meas.getURI() + "> ?measure_"+
					meas.getLastPartOfURI().replaceAll("-", "_")+".";
			j++;
		}
		
		if (cubeGraph != null) {
			sparql_query += "}";
		}		
		
		boolean haslevels=false;
		for (LDResource vDim : visualDims) {
			List<LDResource> dimLevels2Use=selectedDimLevels.get(vDim);
			
			if(dimLevels2Use!=null&&dimLevels2Use.size()>0){
				haslevels=true;
				break;
			}
			
		}
		
		//needed for virtuoso - an empty GRAPH{} is created
		if(haslevels){
			if (cubeDSDgraph != null) {
				sparql_query += "GRAPH <" + cubeDSDgraph + "> {";
			}
	
			i = 1;
			// Add free dimensions to where clause
			for (LDResource vDim : visualDims) {				
				List<LDResource> allDimLevels=allCubesDimensionsLevels.get(vDim);				
				List<LDResource> dimLevels2Use=selectedDimLevels.get(vDim);				
				if(dimLevels2Use!=null&&dimLevels2Use.size()>0){					
					if(dimLevels2Use.size()==1){
						sparql_query+="?level"+i+" skos:member ?dim" + i + "."+
								"OPTIONAL{?dim"+i+" xkos:isPartOf|skos:broader ?parent"+i+"}";
					}else{
						int levelDistance=Math.abs(
								allDimLevels.indexOf(dimLevels2Use.get(0))-allDimLevels.indexOf(dimLevels2Use.get(1)));						
						sparql_query+="?level"+i+" skos:member ?dim" + i + ".";								
						if(levelDistance==1){
							sparql_query+="OPTIONAL{?dim"+i+" xkos:isPartOf|skos:broader ?parent"+i+"}";
						}else if (levelDistance==2){
							sparql_query+="OPTIONAL{?dim"+i+" xkos:isPartOf|skos:broader ?p0." +
									"?p0  xkos:isPartOf|skos:broader ?parent"+i+"}";
						}else if (levelDistance==3){
							sparql_query+="OPTIONAL{?dim"+i+" xkos:isPartOf|skos:broader ?p0." +
									"?p0  xkos:isPartOf|skos:broader ?p1."+
									"?p1  xkos:isPartOf|skos:broader ?parent"+i+"}";
						}else if (levelDistance==4){
							sparql_query+="OPTIONAL{?dim"+i+" xkos:isPartOf|skos:broader ?p0." +
									"?p0  xkos:isPartOf|skos:broader ?p1."+
									"?p1  xkos:isPartOf|skos:broader ?p2."+
									"?p2  xkos:isPartOf|skos:broader ?parent"+i+"}";
						}else if (levelDistance==5){
							sparql_query+="OPTIONAL{?dim"+i+" xkos:isPartOf|skos:broader ?p0." +
									"?p0  xkos:isPartOf|skos:broader ?p1."+
									"?p1  xkos:isPartOf|skos:broader ?p2."+
									"?p2  xkos:isPartOf|skos:broader ?p3."+
									"?p3  xkos:isPartOf|skos:broader ?parent"+i+"}";
						}
					}								
				}						
				i++;
			}
			
			if (cubeDSDgraph != null) {
				sparql_query += "}";
			}
		}
		
		sparql_query+="}";

		if (SPARQLservice != null) {
			sparql_query += "}";
		}
		TupleQueryResult res = QueryExecutor.executeSelect(sparql_query);
		return res;
	}

	public static TupleQueryResult get2DVisualsiationValuesFromSlice(
			List<LDResource> visualDims,
			HashMap<LDResource, LDResource> fixedDims, List<LDResource> selectedMeasures,
			HashMap<LDResource, List<LDResource>> alldimensionvalues,
			String sliceURI, String sliceGraph, String cubeGraph,
			String SPARQLservice) {

		String sparql_query = "Select ";
		int i = 1;
		// Add variables ?dim to SPARQL query
		for (LDResource vDim : visualDims) {
			sparql_query += "?dim" + i + " ";
			i++;
		}

		// Select observations of a specific cube (cubeURI)
		i=1;
		// Add variables ?dim to SPARQL query
		for (LDResource meas : selectedMeasures) {
			sparql_query += "?measure" + i + " ";
			i++;
		}

		// Select observations of a specific cube (cubeURI)
		sparql_query += "?obs where{";
		if (SPARQLservice != null) {
			sparql_query += "SERVICE " + SPARQLservice + " {";
		}

		if (sliceGraph != null) {
			sparql_query += "GRAPH <" + sliceGraph + "> {";
		}

		sparql_query += sliceURI
				+ " <http://purl.org/linked-data/cube#observation> ?obs.";

		if (sliceGraph != null) {
			sparql_query += "}";
		}

		if (cubeGraph != null) {
			sparql_query += "GRAPH <" + cubeGraph + "> {";
		}

		i = 1;
		// Add free dimensions to where clause
		for (LDResource vDim : visualDims) {
			sparql_query += "?obs <" + vDim.getURI() + "> " + "?dim" + i + ". ";
			i++;
		}

		// Add fixed dimensions to where clause, select the first value of the
		// list of all values
		int j = 1;
		for (LDResource fDim : fixedDims.keySet()) {
			sparql_query += "?obs <" + fDim.getURI() + "> ";
			if (fixedDims.get(fDim).getURI().contains("http")) {
				sparql_query += "<" + fixedDims.get(fDim).getURI() + ">.";
			} else {
				sparql_query += "?value_" + j + ". FILTER(STR(?value_" + j
						+ ")='" + fixedDims.get(fDim).getURI() + "')";
			}
			j++;
		}
		
		j=1;
		for(LDResource meas:selectedMeasures){
			sparql_query += "?obs  <" + meas.getURI() + "> ?measure"+j+".";
			j++;
		}
		sparql_query+="}";
		if (cubeGraph != null) {
			sparql_query += "}";
		}

		if (SPARQLservice != null) {
			sparql_query += "}";
		}

		TupleQueryResult res = QueryExecutor.executeSelect(sparql_query);
		return res;
	}
}
