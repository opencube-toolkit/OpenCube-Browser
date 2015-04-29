package org.certh.opencube.utils;

import java.util.List;
import java.util.Map;

//A class to represent a Data Cube
public class DataCube {

	private List<LDResource> dimensions;
	private List<LDResource> measures;
	private LDResource cube;
	private String cubeGraph;
	private String cubeDSDGraph;
	
	//A list of levels for each dimension
	private Map<LDResource, List<LDResource>> dimensionsLevels;
	
	//A list of concept schemes for each dimension
	private Map<LDResource, List<LDResource>> dimensionsConceptSchemes;
	
	public DataCube(String cubeURI) {
		this.cube = new LDResource(cubeURI);
	}
	
	public DataCube(LDResource cube) {
		this.cube = cube;
	}
	
	public List<LDResource> getDimensions() {
		return dimensions;
	}
	
	public void setDimensions(List<LDResource> dimensions) {
		this.dimensions = dimensions;
	}
	
	public List<LDResource> getMeasures() {
		return measures;
	}
	
	public void setMeasures(List<LDResource> measures) {
		this.measures = measures;
	}
	
	public String getCubeURI() {
		return cube.getURI();
	}
	
	public void setCubeURI(String cubeURI) {
		this.cube.setURI(cubeURI);
	}
	
	public String getCubeGraph() {
		return cubeGraph;
	}
	
	public void setCubeGraph(String cubeGraph) {
		this.cubeGraph = cubeGraph;
	}
	
	public String getCubeDSDGraph() {
		return cubeDSDGraph;
	}
	
	public void setCubeDSDGraph(String cubeDSDGraph) {
		this.cubeDSDGraph = cubeDSDGraph;
	}
	
	public Map<LDResource, List<LDResource>> getDimensionsLevels() {
		return dimensionsLevels;
	}
	
	public void setDimensionsLevels(
			Map<LDResource, List<LDResource>> dimensionsLevels) {
		this.dimensionsLevels = dimensionsLevels;
	}
	
	public Map<LDResource, List<LDResource>> getDimensionsConceptSchemes() {
		return dimensionsConceptSchemes;
	}
	
	public void setDimensionsConceptSchemes(
			Map<LDResource, List<LDResource>> dimensionsConceptSchemes) {
		this.dimensionsConceptSchemes = dimensionsConceptSchemes;
	}
	
}

 