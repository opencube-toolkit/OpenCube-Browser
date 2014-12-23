package org.certh.opencube.selection;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.certh.opencube.SPARQL.CubeSPARQL;
import org.certh.opencube.SPARQL.SelectionSPARQL_Eurostat;
import org.certh.opencube.utils.CubeHandlingUtils;
import org.certh.opencube.utils.LDResource;

import com.fluidops.ajax.components.FCheckBox;
import com.fluidops.ajax.components.FComponent;
import com.fluidops.ajax.components.FContainer;
import com.fluidops.iwb.model.ParameterConfigDoc;
import com.fluidops.iwb.model.TypeConfigDoc;
import com.fluidops.iwb.widget.AbstractWidget;
import com.fluidops.iwb.widget.config.WidgetBaseConfig;

/**
 * On some wiki page add
 * 
 * <code>
 * = Test my demo widget =
 * 
 * <br/>
 * {{#widget: org.certh.opencube.selection.CubeSelection
 * |sparqlService='<http://localhost:3031/dcbrowser/query>'
 *  }}
 * 
 * </code>
 * 
 */

@TypeConfigDoc("The OpenCube Cube Selection enables the selection of cube dimensions and"
		+ "measures to visualize.")
public class EurostatCompatibity extends
		AbstractWidget<EurostatCompatibity.Config> {

	// The central container
	private FContainer cnt;

	private List<LDResource> cubeDimensions = new ArrayList<LDResource>();

	// The cube measures
	private List<LDResource> cubeMeasures = new ArrayList<LDResource>();

	// Selected dimensions
	private List<LDResource> selectedDimensions = new ArrayList<LDResource>();

	// A map with the Dimension URIs and the corresponding Check boxes
	private HashMap<LDResource, FCheckBox> mapDimURIcheckBox = new HashMap<LDResource, FCheckBox>();

	// The graph of the cube
	private String cubeGraph = null;

	// The graph of the cube structure
	private String cubeDSDGraph = null;

	HashMap<LDResource, List<LDResource>> dimensionsLevels = new HashMap<LDResource, List<LDResource>>();

	HashMap<LDResource, List<LDResource>> dimensionsConceptSchemes = new HashMap<LDResource, List<LDResource>>();

	private String selectedCubeURI = null;


	private List<LDResource> compatibleAddValue2Level = new ArrayList<LDResource>();

	private Map<LDResource, Integer> allCubesAndDimCount = new HashMap<LDResource, Integer>();

	private String selectedLanguage = "en";

	private String defaultLang = "en";
	
	private HashMap<LDResource, List<LDResource>> allCompatibleCubes=new HashMap<LDResource, List<LDResource>>();
	
	// The SPARQL service to get data (not required)
		//private String SPARQL_Service="<http://localhost:8888/sparql>";
	
	//USE THE EUROSTAT STARQL ENDPOINT!!!!!
	private String SPARQL_Service="<http://eurostat.linked-statistics.org/sparql>";

	// Ignore multiple languages
	private boolean ignoreLang=true;
		
	public static class Config extends WidgetBaseConfig {
		@ParameterConfigDoc(desc = "SPARQL service to forward queries", required = false)
		public String sparqlService;

		@ParameterConfigDoc(desc = "The default language", required = false)
		public String defaultLang;

		@ParameterConfigDoc(desc = "Use multiple languages", required = false)
		public boolean ignoreLang;
	}

	@Override
	protected FComponent getComponent(String id) {

		// Central container
		cnt = new FContainer(id);

		// Get all cubes from the store and the number of dimensions they have
		allCubesAndDimCount = SelectionSPARQL_Eurostat
				.getAllAvailableCubesAndDimCount(SPARQL_Service);

		System.out.println("All Eurostat cubes: "+allCubesAndDimCount.keySet().size());
		long cubeCount=1;
		long totalMatches=0;
		PrintWriter writer=null;
		try {
			writer = new PrintWriter("eurostat_matches.txt");
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		
		LDResource cube=null;
		for (LDResource mycube : allCubesAndDimCount.keySet()) {
				cube=mycube;
		
			System.out.println("Checking cube: "+cubeCount +"  ("+cube.getURI()+")");
				
			selectedCubeURI = "<" + cube.getURI() + ">";
			// Get Cube/Slice Graph
			cubeGraph = CubeSPARQL.getCubeSliceGraph(selectedCubeURI,SPARQL_Service);

			// Get Cube Structure graph
			cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(selectedCubeURI,cubeGraph, SPARQL_Service);

			// Thread to get Measures
			Thread dimsThread = new Thread(new Runnable() {
				public void run() {
					// Get all Cube dimensions
					cubeDimensions = CubeSPARQL.getDataCubeDimensions(
							selectedCubeURI, cubeGraph, cubeDSDGraph,
							selectedLanguage, defaultLang, ignoreLang,
							SPARQL_Service);
					
					//REMOVE FREQ AND UNTI DIMENSIONS
					LDResource freq=new LDResource("http://eurostat.linked-statistics.org/property#FREQ");
					LDResource unit=new LDResource("http://eurostat.linked-statistics.org/property#unit");
					cubeDimensions.remove(freq);
					cubeDimensions.remove(unit);
				}
			});

			// Thread to get Measures
			Thread measuresThread = new Thread(new Runnable() {
				public void run() {
					// Get the Cube measure
					cubeMeasures = CubeSPARQL
							.getDataCubeMeasure(selectedCubeURI, cubeGraph,
									cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQL_Service);
				}
			});

			// start thread
			dimsThread.start();
			measuresThread.start();

			// wait until thread finish
			try {
				dimsThread.join();
				measuresThread.join();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			// Thread to get dimension levels
			Thread dimLevelsThread = new Thread(new Runnable() {
				public void run() {
					dimensionsLevels = CubeHandlingUtils.getDimensionsLevels(
							selectedCubeURI, cubeDimensions, cubeDSDGraph,
							cubeGraph, selectedLanguage, defaultLang,
							ignoreLang, SPARQL_Service);
				}
			});

			// Thread to get dimension concept schemes
			Thread dimConceptSchemesThread = new Thread(new Runnable() {
				public void run() {
					dimensionsConceptSchemes = CubeHandlingUtils
							.getDimensionsConceptSchemes(cubeDimensions,
									cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQL_Service);
				}
			});

			// Start threads
			dimLevelsThread.start();
			dimConceptSchemesThread.start();

			// wait until thread finished
			try {
				dimLevelsThread.join();
				dimConceptSchemesThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			

			compatibleAddValue2Level = SelectionSPARQL_Eurostat
					.geAddValueToLevelCompatibleCubes(allCubesAndDimCount,cube,
							cubeDimensions,
							dimensionsLevels, cubeMeasures,
							dimensionsConceptSchemes, selectedLanguage,
							defaultLang, ignoreLang, SPARQL_Service);
			
			totalMatches+=compatibleAddValue2Level.size();
			System.out.println("Matches for cube "+cubeCount+": "+compatibleAddValue2Level.size());
			System.out.println("Current total matches: "+totalMatches);	
			System.out.println("Cubes stored: "+SelectionSPARQL_Eurostat.allCubeDimensionValues.size());
			System.out.println("Total memory hits: "+SelectionSPARQL_Eurostat.memoryHits);
			
			allCompatibleCubes.put(cube, compatibleAddValue2Level);

			//Write to file: NUMBER_OF_DIMS,COMPATIBLE_CUBES,CUBE_URI
			writer.println(allCubesAndDimCount.get(cube)+","+compatibleAddValue2Level.size()+","+cube.getURI());
			
	//		if(cubeCount==10)break;
			cubeCount++;		
		}

		writer.close();
		
		
		PrintWriter writerMatches=null;
		try {
			writerMatches = new PrintWriter("eurostat_combinations.txt");
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
		}
		long matches=0;
		//FOR EACH CUBE WRITE MYCUBE--> OTHER_COMPATIBLE_CUBE
		for(LDResource mycube:allCompatibleCubes.keySet()){
			List<LDResource> compatible=allCompatibleCubes.get(mycube);
			writerMatches.println("Matches: "+compatible.size());
			for(LDResource comp:compatible){
				if(!mycube.getURI().equals(comp.getURI())){
					writerMatches.println(mycube.getURI()+" --> "+comp.getURI());
					matches++;
				}
			}
			writerMatches.println();
			writerMatches.println("----------------------------------------------------------");
			writerMatches.println();
		}
	
		writerMatches.println("Matches found: "+matches);
		writerMatches.close();

		return cnt;
	}

	@Override
	public String getTitle() {
		return "Cube Selection widget";
	}

	@Override
	public Class<?> getConfigClass() {
		return Config.class;
	}
}