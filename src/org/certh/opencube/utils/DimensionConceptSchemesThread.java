package org.certh.opencube.utils;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.certh.opencube.SPARQL.CubeSPARQL;

/**
 * 
 * @author user
 */
public class DimensionConceptSchemesThread implements
		Callable<HashMap<LDResource, List<LDResource>>> {

	private String cubeDSDGraph;
	private LDResource dimension;
	private String SPARQLservice;
	private String lang;
	private String defaultLang;
	private boolean ignoreLang;
	
	public DimensionConceptSchemesThread(LDResource dimension,  String cubeDSDGraph,String lang,
			String defaultLang, boolean ignoreLang, String SPARQLservice) {
		this.cubeDSDGraph = cubeDSDGraph;
		this.dimension = dimension;
		this.SPARQLservice = SPARQLservice;
		this.lang=lang;
		this.defaultLang=defaultLang;	
		this.ignoreLang=ignoreLang;

	}

	@Override
	public HashMap<LDResource, List<LDResource>> call() throws Exception {
					
		List<LDResource> dimConceptScheme= CubeSPARQL.getDimensionConceptScheme(
				dimension.getURI(),	cubeDSDGraph, lang,  defaultLang,ignoreLang, SPARQLservice);
	
		HashMap<LDResource,List<LDResource>> dimensionsConceptSchemes=
										new HashMap<LDResource, List<LDResource>>();
		dimensionsConceptSchemes.put(dimension, dimConceptScheme);
				
		return dimensionsConceptSchemes;
	}

}
