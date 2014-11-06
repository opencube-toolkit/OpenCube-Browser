package org.certh.opencube.utils;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.certh.opencube.SPARQL.CubeSPARQL;
import org.certh.opencube.SPARQL.SliceSPARQL;

/**
 * 
 * @author user
 */
public class DimensionValuesThread implements
		Callable<HashMap<LDResource, List<LDResource>>> {

	private String cubeURI;
	private String cubeGraph;
	private String cubeDSDGraph;
	private LDResource dimension;
	private boolean useCodeLists;
	private boolean ignoreLang;
	private String SPARQLservice;
	private String sliceGraph;
	private String lang;
	private String defaultLang;

	public DimensionValuesThread(LDResource dimension, String cubeURI,
			String cubeGraph, String cubeDSDGraph, boolean useCodeLists,
			String lang,String defaultLang, boolean ignoreLang,String SPARQLservice) {
		this.cubeURI = cubeURI;
		this.cubeGraph = cubeGraph;
		this.cubeDSDGraph = cubeDSDGraph;
		this.dimension = dimension;
		this.useCodeLists = useCodeLists;
		this.SPARQLservice = SPARQLservice;
		this.lang=lang;
		this.defaultLang=defaultLang;	
		this.ignoreLang=ignoreLang;

	}

	public DimensionValuesThread(LDResource dimension, String cubeURI,
			String cubeGraph, String cubeDSDGraph, String sliceGraph,
			boolean useCodeLists, String lang, String defaultLang,boolean ignoreLang,
			String SPARQLservice) {
		this.cubeURI = cubeURI;
		this.cubeGraph = cubeGraph;
		this.cubeDSDGraph = cubeDSDGraph;
		this.sliceGraph = sliceGraph;
		this.dimension = dimension;
		this.useCodeLists = useCodeLists;
		this.SPARQLservice = SPARQLservice;
		this.lang=lang;
		this.defaultLang=defaultLang;
		this.ignoreLang=ignoreLang;

	}

	@Override
	public HashMap<LDResource, List<LDResource>> call() throws Exception {
		List<LDResource> vDimValues = null;
		if (useCodeLists) {
			vDimValues = CubeSPARQL.getDimensionValuesFromCodeList(
					dimension.getURI(), cubeURI, cubeDSDGraph, lang,defaultLang,ignoreLang,	SPARQLservice);
			if (vDimValues.isEmpty()) {
				if (sliceGraph == null) {
					vDimValues = CubeSPARQL.getDimensionValues(
							dimension.getURI(), cubeURI, cubeGraph,
							cubeDSDGraph, lang,defaultLang,ignoreLang,SPARQLservice);
				} else {
					vDimValues = SliceSPARQL.getDimensionValuesFromSlice(
							dimension.getURI(), cubeURI, cubeGraph,
							cubeDSDGraph, sliceGraph, lang, defaultLang,ignoreLang,SPARQLservice);
				}
			}
		} else {
			if (sliceGraph == null) {
				vDimValues = CubeSPARQL.getDimensionValues(dimension.getURI(),
						cubeURI, cubeGraph, cubeDSDGraph, lang,defaultLang, ignoreLang,SPARQLservice);
			} else {
				vDimValues = SliceSPARQL.getDimensionValuesFromSlice(
						dimension.getURI(), cubeURI, cubeGraph, cubeDSDGraph,
						sliceGraph, lang,defaultLang,ignoreLang,SPARQLservice);

			}
		}

		HashMap<LDResource, List<LDResource>> dimensionsAndValues = new HashMap<LDResource, List<LDResource>>();
		dimensionsAndValues.put(dimension, vDimValues);
		return dimensionsAndValues;
	}

}
