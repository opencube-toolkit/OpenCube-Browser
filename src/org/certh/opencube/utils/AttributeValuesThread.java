package org.certh.opencube.utils;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.certh.opencube.SPARQL.CubeSPARQL;

/**
 * 
 * @author user
 */
public class AttributeValuesThread implements
		Callable<HashMap<LDResource, List<LDResource>>> {

	private String cubeURI;
	private LDResource attribute;
	private String serviceURI;
	private String cubeGraph;
	private String cubeDSDGraph;
	private String lang;
	private String defaultLang;
	private boolean ignorelang;

	public AttributeValuesThread(LDResource attribute, String cubeURI,
			String cubeGraph, String cubeDSDGraph, String lang, String defaultLang,
			boolean ignoreLang,String serviceURI) {
		this.cubeURI = cubeURI;
		this.serviceURI = serviceURI;
		this.attribute = attribute;
		this.cubeGraph = cubeGraph;
		this.cubeDSDGraph = cubeDSDGraph;
		this.lang=lang;
		this.defaultLang=defaultLang;
		this.ignorelang=ignoreLang;
	}

	@Override
	public HashMap<LDResource, List<LDResource>> call() throws Exception {
		List<LDResource> vAttrValues = CubeSPARQL.getAttributeValues(
				attribute.getURI(), cubeURI, cubeGraph, cubeDSDGraph,lang,defaultLang,ignorelang,
				serviceURI);
		HashMap<LDResource, List<LDResource>> attributesAndValues = new HashMap<LDResource, List<LDResource>>();
		attributesAndValues.put(attribute, vAttrValues);
		return attributesAndValues;
	}
}
