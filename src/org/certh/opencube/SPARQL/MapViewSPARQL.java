package org.certh.opencube.SPARQL;

import java.util.HashMap;
import java.util.List;

import org.certh.opencube.utils.LDResource;
import org.openrdf.query.TupleQueryResult;

public class MapViewSPARQL {

	public static TupleQueryResult getDVisualsiationValues(
			List<LDResource> visualDims, LDResource geodimension,
			HashMap<LDResource, LDResource> fixedDims, List<LDResource> measures,
			List<LDResource> cubeAttributes, String dataCubeURI,
			String cubeGraph, String cubeDSDGraph,String lang, String SPARQLservice) {

		String sparql_query = "PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#> ";
		sparql_query += "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>";
		sparql_query += "PREFIX  qb: <http://purl.org/linked-data/cube#> Select distinct ?obs";

		int i = 1;
		// Add variables ?dim to SPARQL query
		// for (LDResource vDim : visualDims) {
		for (LDResource fDim : fixedDims.keySet()) {
			sparql_query += "?dim" + i + " ";

			i++;
		}

		i = 1;
		// add variables ?attr to SPARQL query
		for (LDResource fAttr : cubeAttributes) { // Areti
			sparql_query += "?attr" + i + " ";
			i++;
		}
		// Select observations of a specific cube (cubeURI)
		sparql_query += "?geolabel ?measure where{ ";
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			sparql_query += "SERVICE " + SPARQLservice + " {";
		}
		// If a cube graph is defined
		if (cubeGraph != null) {
			sparql_query += "GRAPH <" + cubeGraph + "> {";
		}

		sparql_query += "?obs <http://purl.org/linked-data/cube#dataSet> "	+ dataCubeURI + ".";
		
		sparql_query += "?obs <" + measures.get(0).getURI() + "> ?measure. ";

		i = 1;
		// Add free dimensions to where clause
		String geoval="";
		for (LDResource vDim : visualDims) {
			if(vDim.equals(geodimension)){
				geoval="?dim" + i;
			}
			sparql_query += "?obs <" + vDim.getURI() + "> " + "?dim" + i + ". ";
			i++;
		}

		// Add fixed dimensions to where clause, select the first value of the
		// list of all values
		for (LDResource fDim : fixedDims.keySet()) {
			sparql_query += "?obs <" + fDim.getURI() + ">";
			if (fixedDims.get(fDim).getURI().toString().contains("http"))
				sparql_query += "<" + fixedDims.get(fDim).getURI() + ">.";
			else
				sparql_query += "?obs2" + ". FILTER (STR(?obs2) = '"
						+ fixedDims.get(fDim).getURI() + "')";
		}

		// If a cube graph is defined
		if (cubeGraph != null) {
			sparql_query += "}";
		}

		// Add attributes to where clause
		i = 1;
		for (LDResource fAttr : cubeAttributes) {
			sparql_query += "OPTIONAL {";

			// If a cube graph is defined
			if (cubeGraph != null) {
				sparql_query += "GRAPH <" + cubeGraph + "> {";
			}

			sparql_query += "?obs <" + fAttr.getURI() + "> " + "?attribute" + i	+ ". ";

			// If a cube graph is defined
			if (cubeGraph != null) {
				sparql_query += "}";
			}

			// If a cube graph is defined
			if (cubeDSDGraph != null) {
				sparql_query += "GRAPH <" + cubeDSDGraph + "> {";
			}
			sparql_query += "?attribute" + i + " skos:prefLabel ?attr" + i
					+ ". FILTER(LANGMATCHES(LANG(?attr" + i + "), \""+lang+"\"))}";

			// If a cube graph is defined
			if (cubeDSDGraph != null) {
				sparql_query += "}";
			}
			i++;
		}

		// If a cube graph is defined
		if (cubeDSDGraph != null) {
			sparql_query += "GRAPH <" + cubeDSDGraph + "> {";
		}

		sparql_query += geoval+" skos:prefLabel ?geolabel. ";
		//sparql_query += "?geovalue skos:prefLabel ?geolabel. ";

		sparql_query += "FILTER(LANGMATCHES(LANG(?geolabel), \""+lang+"\"))";

		// If a cube graph is defined
		if (cubeDSDGraph != null) {
			sparql_query += "}";
		}

		// if(!serviceURI.equals(null))
		sparql_query += "}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			sparql_query += "}";
		}
		// /////////////////////////////

		TupleQueryResult res = QueryExecutor.executeSelect(sparql_query);
		System.out.println("query is " + sparql_query);
		return res;

	}
	
	
	public static TupleQueryResult getDVisualsiationValuesFromSlice(
			List<LDResource> visualDims, LDResource geodimension,
			HashMap<LDResource, LDResource> fixedDims, List<LDResource> measures,
			List<LDResource> cubeAttributes, String sliceURI, String sliceGraph,
			String cubeGraph, String cubeDSDGraph, String lang,String SPARQLservice) {

		String sparql_query = "PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#> ";
		sparql_query += "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>";
		sparql_query += "PREFIX  qb: <http://purl.org/linked-data/cube#> Select distinct ?obs ";

		int i = 1;
		// Add variables ?dim to SPARQL query
		// for (LDResource vDim : visualDims) {
		for (LDResource fDim : fixedDims.keySet()) {
			sparql_query += "?dim" + i + " ";

			i++;
		}

		i = 1;
		// add variables ?attr to SPARQL query
		for (LDResource fAttr : cubeAttributes) { // Areti
			sparql_query += "?attr" + i + " ";
			i++;
		}
		// Select observations of a specific cube (cubeURI)
		sparql_query += "?geolabel ?measure where{ ";
		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			sparql_query += "SERVICE " + SPARQLservice + " {";
		}
		// If a slice graph is defined
		if (sliceGraph != null) {
			sparql_query += "GRAPH <" + sliceGraph + "> {";
		}
	
		sparql_query += sliceURI +" qb:observation ?obs.";
		
		// If a cube graph is defined
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
		for (LDResource fDim : fixedDims.keySet()) {
			sparql_query += "?obs <" + fDim.getURI() + ">";
			if (fixedDims.get(fDim).getURI().toString().contains("http"))
				sparql_query += "<" + fixedDims.get(fDim).getURI() + ">.";
			else
				sparql_query += "?obs2" + ". FILTER (STR(?obs2) = '"
						+ fixedDims.get(fDim).getURI() + "')";
		}

		// If a cube graph is defined
		if (cubeGraph != null) {
			sparql_query += "}";
		}

		// Add attributes to where clause
		i = 1;
		for (LDResource fAttr : cubeAttributes) {
			sparql_query += "OPTIONAL {";

			// If a cube graph is defined
			if (cubeGraph != null) {
				sparql_query += "GRAPH <" + cubeGraph + "> {";
			}

			sparql_query += "?obs <" + fAttr.getURI() + "> " + "?attribute" + i
					+ ". ";

			// If a cube graph is defined
			if (cubeGraph != null) {
				sparql_query += "}";
			}

			// If a cube graph is defined
			if (cubeDSDGraph != null) {
				sparql_query += "GRAPH <" + cubeDSDGraph + "> {";
			}
			sparql_query += "?attribute" + i + " skos:prefLabel ?attr" + i
					+ ". FILTER(LANGMATCHES(LANG(?attr" + i + "), \""+lang+"\"))}";

			// If a cube graph is defined
			if (cubeDSDGraph != null) {
				sparql_query += "}";
			}
			i++;
		}

		// If a cube graph is defined
		if (cubeGraph != null) {
			sparql_query += "GRAPH <" + cubeGraph + "> {";
		}

		sparql_query += "?obs <"
				+ geodimension.getURI() + "> ?geovalue.";

		sparql_query += "?obs <" + measures.get(0).getURI() + "> ?measure. ";

		if (cubeGraph != null) {
			sparql_query += "}";
		}

		// If a cube graph is defined
		if (cubeDSDGraph != null) {
			sparql_query += "GRAPH <" + cubeDSDGraph + "> {";
		}
		sparql_query += "?geovalue skos:prefLabel ?geolabel. ";

		sparql_query += "FILTER(LANGMATCHES(LANG(?geolabel), \""+lang+"\"))";

		// If a cube graph is defined
		if (cubeDSDGraph != null) {
			sparql_query += "}";
		}

		// if(!serviceURI.equals(null))
		sparql_query += "}";

		// If a SPARQL service is defined
		if (SPARQLservice != null) {
			sparql_query += "}";
		}
		// /////////////////////////////

		TupleQueryResult res = QueryExecutor.executeSelect(sparql_query);
		System.out.println("query is " + sparql_query);
		return res;

	}
	

}
