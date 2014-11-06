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
			sparql_query += "?measure" + i + " ";
			i++;
		}

		// Select observations of a specific cube (cubeURI)
	//	sparql_query += "?measure ?obs where{";
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
			sparql_query += "?obs  <" + meas.getURI() + "> ?measure"+j+".";
			j++;
		}
		sparql_query+="}";
		
		//sparql_query += "?obs  <" + selectedMeasure.getURI() + "> ?measure.} ";

		if (cubeGraph != null) {
			sparql_query += "}";
		}

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
	//	sparql_query += "?measure ?obs where{";
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
		
		//sparql_query += "?obs  <" + selectedMeasure.getURI() + "> ?measure.} ";

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
