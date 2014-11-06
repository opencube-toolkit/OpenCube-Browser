package org.certh.opencube.utils;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.certh.opencube.SPARQL.CubeSPARQL;

/**
 * 
 * @author user
 */
public class DimensionLevelsThread implements
		Callable<HashMap<LDResource, List<LDResource>>> {

	private String cubeURI;
	private String cubeGraph;
	private String cubeDSDGraph;
	private LDResource dimension;
	private String SPARQLservice;
	private String lang;
	private String defaultLang;
	private boolean ignoreLang;

	
	
	
	public DimensionLevelsThread(LDResource dimension, String cubeURI,
			String cubeGraph, String cubeDSDGraph,String lang,
			String defaultLang, boolean ignoreLang, String SPARQLservice) {
		this.cubeURI = cubeURI;
		this.cubeGraph = cubeGraph;
		this.cubeDSDGraph = cubeDSDGraph;
		this.dimension = dimension;
		this.SPARQLservice = SPARQLservice;
		this.lang=lang;
		this.defaultLang=defaultLang;	
		this.ignoreLang=ignoreLang;

	}

	@Override
	public HashMap<LDResource, List<LDResource>> call() throws Exception {
					
		List<LDResource> dimensionLevels= CubeSPARQL.getDimensionLevels(cubeURI, dimension.getURI(),
				cubeDSDGraph, cubeGraph, lang,  defaultLang,ignoreLang, SPARQLservice);
				
		HashMap<LDResource, List<LDResource>> dimensionsAndLevels = new HashMap<LDResource, List<LDResource>>();
		dimensionsAndLevels.put(dimension, dimensionLevels);
		return dimensionsAndLevels;
		
	}

}
