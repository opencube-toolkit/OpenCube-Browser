package org.certh.opencube.cubebrowser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.certh.opencube.SPARQL.AggregationSPARQL;
import org.certh.opencube.SPARQL.CubeBrowserSPARQL;
import org.certh.opencube.SPARQL.CubeSPARQL;
import org.certh.opencube.SPARQL.SelectionSPARQL;
import org.certh.opencube.SPARQL.SliceSPARQL;
import org.certh.opencube.utils.CubeHandlingUtils;
import org.certh.opencube.utils.DrillDownObservation;
import org.certh.opencube.utils.LDResource;
import org.openrdf.model.Literal;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.fluidops.ajax.components.FButton;
import com.fluidops.ajax.components.FCheckBox;
import com.fluidops.ajax.components.FComboBox;
import com.fluidops.ajax.components.FComponent;
import com.fluidops.ajax.components.FContainer;
import com.fluidops.ajax.components.FDialog;
import com.fluidops.ajax.components.FGrid;
import com.fluidops.ajax.components.FHTML;
import com.fluidops.ajax.components.FLabel;
import com.fluidops.ajax.components.FRadioButtonGroup;
import com.fluidops.ajax.components.FTable;
import com.fluidops.ajax.components.FTable.FilterPos;
import com.fluidops.ajax.helper.HtmlString;
import com.fluidops.ajax.models.FTableModel;
import com.fluidops.iwb.model.ParameterConfigDoc;
import com.fluidops.iwb.model.TypeConfigDoc;
import com.fluidops.iwb.widget.AbstractWidget;
import com.fluidops.iwb.widget.config.WidgetBaseConfig;
import com.fluidops.util.Pair;

/**
 * On some wiki page add
 * 
 * <code>
 * = OLAP Browser Test page =
 * 
 * <br/>
 * {{#widget: org.certh.opencube.cubebrowser.DataCubeBrowserPlusPlus|asynch='true' }}
 * 
 * </code>
 * 
 */

@TypeConfigDoc("The OpenCube OLAP Browser enables the exploration of an RDF data cube" +
		" by presenting each time a two-dimensional slice of the cube as a table.")
public class DataCubeBrowserPlusPlus extends AbstractWidget<DataCubeBrowserPlusPlus.Config> {

	// The top container to show the check boxes with the available aggregation set dimensions
	private FContainer selectCubeContainer = new FContainer("selectCubeContainer");

	// The top container to show the check boxes with the available aggregation set dimensions
	private FContainer aggSetDimContainer = new FContainer("aggSetDimContainer");

	// The left container to show the combo boxes with the visual dimensions
	private FContainer leftcontainer = new FContainer("leftcontainer");

	// The right container to show the combo boxes with the fixed dimensions values
	private FContainer rightcontainer = new FContainer("rightcontainer");

	// The right container to show the combo boxes with the fixed dimensions values
	private FContainer languagecontainer = new FContainer("languagecontainer");

	// The measures container to show the combo box with the cube measures
	private FContainer measurescontainer = new FContainer("measurescontainer");
	
	// The measures container to show the combo box with the cube measures
	private FContainer compatibleCubeContainer = new FContainer("compatibleCubeContainer");
	
	private FRadioButtonGroup addMeasuresAttributeValues_radioButtonGroup;

	// An FGrid to show fixed cube dimensions
	private FGrid fixedDimGrid = new FGrid("fixedDimGrid");

	// All the cube dimensions
	private List<LDResource> cubeDimensions = new ArrayList<LDResource>();

	// The selected cube measure to visualize
	private List<LDResource> selectedMeasures = new ArrayList<LDResource>();

	// All the dimensions of all cubes of the same aggregation set
	private List<LDResource> aggregationSetDims = new ArrayList<LDResource>();

	// All the dimensions per cube of the aggregation set
	private List<HashMap<LDResource, List<LDResource>>> cubeDimsOfAggregationSet = new ArrayList<HashMap<LDResource, List<LDResource>>>();

	// The cube dimensions to visualize (2 dimensions)
	private List<LDResource> visualDimensions = new ArrayList<LDResource>();
	
	// The selected cube dimensions to use for visualization (visual dims + fixed dims)
	private List<LDResource> selectedDimenisons = new ArrayList<LDResource>();
	
	// The SELECTED levels of the cube. Map dim-> levels[]
	private HashMap<LDResource, List<LDResource>> selectedDimLevels = new HashMap<LDResource, List<LDResource>>();

	// The fixed dimensions
	private List<LDResource> fixedDimensions = new ArrayList<LDResource>();

	// The slice fixed dimensions
	private List<LDResource> sliceFixedDimensions = new ArrayList<LDResource>();

	// All the cube observations - to be used to create a slice
	private List<LDResource> sliceObservations = new ArrayList<LDResource>();

	// A map (cube - dimension URI - dimension values) with all cube dimension values
	private HashMap<LDResource, List<LDResource>> allDimensionsValues = new HashMap<LDResource, List<LDResource>>();

	// A map (cube - level URI - dimension values) with all cube dimension values
	private HashMap<LDResource, List<LDResource>> allLevelsValues = new HashMap<LDResource, List<LDResource>>();
	
	// The selected value for each fixed dimension
	private HashMap<LDResource, LDResource> fixedDimensionsSelectedValues = new HashMap<LDResource, LDResource>();

	// The selected value for each fixed dimension
	private HashMap<LDResource, LDResource> sliceFixedDimensionsValues = new HashMap<LDResource, LDResource>();

	// A map with the corresponding components for each fixed cube dimension
	private HashMap<LDResource, List<FComponent>> dimensionURIfcomponents = new HashMap<LDResource, List<FComponent>>();

	// A map with the Aggregation Set Dimension URIs and the corresponding Check boxes
	private HashMap<LDResource, FCheckBox> mapDimURIcheckBox = new HashMap<LDResource, FCheckBox>();
	
	// A map with the Aggregation Set Dimension URIs and the corresponding DIMENSION LEVELS Check boxes
	private HashMap<LDResource, List<FCheckBox>> mapDimLevelscheckBoxes = new HashMap<LDResource, List<FCheckBox>>();

	// The cube measures
	private List<LDResource> cubeMeasures = new ArrayList<LDResource>();
	private HashMap<String,List<LDResource>> measuresPerCube=new HashMap<String, List<LDResource>>();
	
	// A map with the Cube Measure URIs and the corresponding Check boxes
	private HashMap<LDResource, FCheckBox> mapMeasureURIcheckBox = new HashMap<LDResource, FCheckBox>();

	// The cube URI to visualize (required)
	private List<String> cubeSliceURIs = new ArrayList<String>();
	
	// The cube URI to visualize (required)
	private List<String> originalCubeSliceURIs = new ArrayList<String>();

	// The SPARQL service to get data (not required)
	private String SPARQL_service = "";

	// The SPARQL service to get data (not required)
	private String defaultLang;

	// The graph of the cube
	private HashMap<String, String> cubeGraphs = new HashMap<String, String>();

	// The graph of the cube structure
	private HashMap<String, String> cubeDSDGraphs = new HashMap<String, String>();

	// The graph of the slice
	private HashMap<String, String> sliceGraphs = new HashMap<String, String>();

	// The table model for visualization of the cube
	private FTable ftable = new FTable("ftable");

	// True if URI is type qb:DataSet
	private boolean isCube;

	// True if URI is type qb:Slice
	private boolean isSlice;

	private HashMap<String, String> allCubeSliceTypes = new HashMap<String, String>();

	// True if code list will be used to get the cube dimension values
	private boolean useCodeLists;

	// True if widget is loaded for the first time
	private boolean isFirstLoad = true;

	// The central container
	private FContainer cnt = null;
	
	// All the dimensions of all cubes of the same aggregation set
	private List<LDResource> dimsOfaggregationSet2Show = new ArrayList<LDResource>();
	
	private boolean isFirstCubeLoad=true;
	
	FGrid leftGrid=new FGrid("leftGrid");
	FGrid overalGrid=new FGrid("overalgrid");
	FGrid midleGrid=new FGrid("midleGrid");
	FGrid midlemidleGrid=new FGrid("midlemidleGrid");

	// The available languages of the cube
	private List<String> availableLanguages = new ArrayList<String>();

	// The selected language
	private String selectedLanguage;

	// Ignore multiple languages
	private boolean ignoreLang;
	
	//used to identify when a new aggregation has been selected to show compatible cubes
	private boolean newcubeselected;

	private List<LDResource> cubeDimsFromSlice = null;

	private List<LDResource> allCubes = new ArrayList<LDResource>();
	
	private HashMap<LDResource, List<LDResource>> allCubesDimensionsLevels = new HashMap<LDResource, List<LDResource>>();
	
	//cube -> measures[]
	private HashMap<LDResource, List<LDResource>> measurecompatible=
			new HashMap<LDResource, List<LDResource>>();
	
	//dim -> cube -> values[]
	private HashMap<LDResource,HashMap<LDResource, List<LDResource>>> dimensioncompatible=
			new HashMap<LDResource,HashMap<LDResource, List<LDResource>>>();
	
	//private HashMap<LDResource, List<LDResource>> allCubesSelectedDimensionsLevels = new HashMap<LDResource, List<LDResource>>();

	private String[] measureColors = { "black", "CornflowerBlue", "LimeGreen", "Tomato",
			"Orchid","MediumVioletRed","Gold","DarkGoldenRod","DarkGray","DarkRed",
			"MidnightBlue","Aqua","DimGray","FireBrick","DarkRed","SandyBrown","YellowGreen",
			"RosyBrown","PapayaWhip","IndianRed","Khaki","Lime","Maroon","SandyBrown","Thistle"};

	private String cubeSliceURI = "";
	
	private LDResource selectedCubeFromDropDown=null;
	
	public String mergeParam;

	public static class Config extends WidgetBaseConfig {
		@ParameterConfigDoc(desc = "The data cube URI to visualise", required = false)
		public String dataCubeURIs;

		@ParameterConfigDoc(desc = "Use code lists to get dimension values", required = false)
		public boolean useCodeLists;

		@ParameterConfigDoc(desc = "SPARQL service to forward queries", required = false)
		public String sparqlService;

		@ParameterConfigDoc(desc = "The default language", required = false)
		public String defaultLang;

		@ParameterConfigDoc(desc = "Use multiple languages", required = false)
		public boolean ignoreLang;
		
		@ParameterConfigDoc(desc = "Defines they way to merged multiple cubes (Values:measure, dimvalue)", required = false)
		public String mergeParam;
	}

	@Override
	protected FComponent getComponent(String id) {

		final Config config = get();

		// Central container
		cnt = new FContainer(id);

		if(config.dataCubeURIs!=null){
			// Get the cube URI from the widget configuration
			cubeSliceURIs = Arrays.asList(config.dataCubeURIs.split("\\s*,\\s*"));
		}
		
		for(int i=0;i<cubeSliceURIs.size();i++){
			if(!cubeSliceURIs.get(i).contains("<")){
				String updatedURI="<"+cubeSliceURIs.get(i)+">";
				cubeSliceURIs.set(i, updatedURI);
			}
		}
		
		if(cubeSliceURIs.size()>0){
			//remove < and > from the URI. The cubes combobox URI are without < and >
			String selectedCubeURI=cubeSliceURIs.get(0); 
			selectedCubeURI=selectedCubeURI.replaceAll("<","");
			selectedCubeURI=selectedCubeURI.replaceAll(">","");
			selectedCubeFromDropDown=new LDResource(selectedCubeURI);
		}
		
		mergeParam=config.mergeParam;

		originalCubeSliceURIs=new ArrayList<String>(cubeSliceURIs);
	
		// Get the SPARQL service (if any) from the cube configuration
		SPARQL_service = config.sparqlService;

		// Get the use code list parameter from the widget config
		useCodeLists = config.useCodeLists;

		// Use multiple languages, get parameter from widget configuration
		ignoreLang = config.ignoreLang;

		if (!ignoreLang) {
			defaultLang = config.defaultLang;

			if (defaultLang == null || defaultLang.equals("")) {
				defaultLang = com.fluidops.iwb.util.Config.getConfig()
						.getPreferredLanguage();
			}
			selectedLanguage = defaultLang;
		}

		allCubes = SelectionSPARQL.getMaximalCubesAndSlices(selectedLanguage,
				selectedLanguage,ignoreLang,SPARQL_service);
		
		newcubeselected=false;
		
		// Prepare and show the widget
		populateCentralContainer();

		isFirstLoad = false;

		return cnt;
	}

	private void populateCentralContainer() {

		leftGrid=new FGrid("leftGrid");
		overalGrid=new FGrid("overalgrid");
		midleGrid=new FGrid("midleGrid");
		midlemidleGrid=new FGrid("midlemidleGrid");

		aggregationSetDims=new ArrayList<LDResource>();
		cubeGraphs = new HashMap<String, String>();
		allCubeSliceTypes = new HashMap<String, String>();
		cubeDSDGraphs = new HashMap<String, String>();
		sliceGraphs = new HashMap<String, String>();
		availableLanguages = new ArrayList<String>();
		cubeDimensions = new ArrayList<LDResource>();
		sliceFixedDimensions = new ArrayList<LDResource>();
		cubeDimsFromSlice = new ArrayList<LDResource>();
		cubeMeasures = new ArrayList<LDResource>();
		allDimensionsValues = new HashMap<LDResource, List<LDResource>>();
		cubeDimsOfAggregationSet = new ArrayList<HashMap<LDResource, List<LDResource>>>();
		allCubesDimensionsLevels = new HashMap<LDResource, List<LDResource>>();

		long startTime = System.currentTimeMillis();
		// There is at least one cubeSlice
		if (cubeSliceURIs.size() > 0) {

			for (String cubeSliceURI : cubeSliceURIs) {
				// Get Cube/Slice Graph
				cubeGraphs.put(cubeSliceURI, CubeSPARQL.getCubeSliceGraph(
						cubeSliceURI, SPARQL_service));

				allCubeSliceTypes.put(cubeSliceURI, CubeHandlingUtils
						.getTypeString(cubeSliceURI,
								cubeGraphs.get(cubeSliceURI), SPARQL_service));
			}

			if (allCubeSliceTypes.size() > 0) {

				// All URIs are rdf:type qb:DataSet
				isCube = allCubeSliceTypes.containsValue("cube")
						&& !allCubeSliceTypes.containsValue("slice")
						&& !allCubeSliceTypes.containsValue("error");
				
				// All URIs are rdf:type qb:Slice
				isSlice = !allCubeSliceTypes.containsValue("cube")
						&& allCubeSliceTypes.containsValue("slice")
						&& !allCubeSliceTypes.containsValue("error");
			} else {
				isCube = false;
				isSlice = false;
			}

			// If all the URI are valid cube or slice URI
			if (isCube || isSlice) {
				if (isCube) {
					
					boolean getMeasures=true;
					boolean getDimvalues=true;
					for (String cubeSliceURItmp : cubeSliceURIs) {

						cubeSliceURI = cubeSliceURItmp;

						// Get Cube Structure graph
						cubeDSDGraphs.put(cubeSliceURI, CubeSPARQL
								.getCubeStructureGraph(cubeSliceURI,
										cubeGraphs.get(cubeSliceURI),
										SPARQL_service));

						if (!ignoreLang) {
							// Get the available languages of labels

							// Use set to have one instance of each language
							HashSet<String> languagesSet = new HashSet<String>(availableLanguages);
							languagesSet.addAll(CubeSPARQL
									.getAvailableCubeLanguages(
											cubeDSDGraphs.get(cubeSliceURI),
											SPARQL_service));

							availableLanguages = new ArrayList<String>(languagesSet);

							// get the selected language to use
							selectedLanguage = CubeHandlingUtils
									.getSelectedLanguage(availableLanguages,
											selectedLanguage);
						}

						HashSet<LDResource> cubeDimensionsSet = new HashSet<LDResource>();
																	
						// Get all Cube dimensions
						cubeDimensionsSet.addAll(CubeSPARQL
								.getDataCubeDimensions(cubeSliceURI,
										cubeGraphs.get(cubeSliceURI),
										cubeDSDGraphs.get(cubeSliceURI),
										selectedLanguage, defaultLang,
										ignoreLang, SPARQL_service));
						
						cubeDimensionsSet.addAll(cubeDimensions);

						cubeDimensions = new ArrayList<LDResource>(cubeDimensionsSet);
						
						if(isFirstCubeLoad){
							dimsOfaggregationSet2Show=AggregationSPARQL.getAggegationSetDimsFromCube(
								cubeSliceURI, cubeDSDGraphs.get(cubeSliceURI), selectedLanguage,
								defaultLang, ignoreLang, SPARQL_service);
							dimsOfaggregationSet2Show.retainAll(cubeDimensions);
							isFirstCubeLoad=false;						
						}
						
						if(getMeasures){						
							// Thread to get cube dimensions
							Thread measuresThread = new Thread(new Runnable() {
								public void run() {
									// Get the Cube measure
									HashSet<LDResource> cubeMeasuresSet = new HashSet<LDResource>(cubeMeasures);
									
									List<LDResource> currentCubeMeasures=CubeSPARQL
											.getDataCubeMeasure(
													cubeSliceURI,
													cubeGraphs.get(cubeSliceURI),
													cubeDSDGraphs.get(cubeSliceURI),
													selectedLanguage, defaultLang,
													ignoreLang, SPARQL_service);									
									
									cubeMeasuresSet.addAll(currentCubeMeasures);
	
									cubeMeasures = new ArrayList<LDResource>(cubeMeasuresSet);
									
									measuresPerCube.put(cubeSliceURI, currentCubeMeasures);
								}
							});	
							
							measuresThread.start();
							
							try {
								measuresThread.join();								
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						//If merge cubes with add value to dimension then all cubes have the same measures.
						}else{
							measuresPerCube.put(cubeSliceURI, cubeMeasures);
						}
												
						if(getDimvalues){
							// Thread to get cube dimension levels
							Thread dimensionsLevelsThread = new Thread(new Runnable() {
								public void run() {
									
									//get the cube dims
									List<LDResource> thisCubeDims=CubeSPARQL.getDataCubeDimensions(
											cubeSliceURI, cubeGraphs.get(cubeSliceURI),
											cubeDSDGraphs.get(cubeSliceURI),
											selectedLanguage, defaultLang,
											ignoreLang, SPARQL_service);
															
									//for each cube dim get the levels (if exist)
									for(LDResource ldr:thisCubeDims){
										//get levels from schema
										List<LDResource> currentDimLevelsWithLables=
												
										CubeSPARQL.getDimensionLevelsFromSchema(
										ldr.getURI(), cubeDSDGraphs.get(cubeSliceURI),
										selectedLanguage, defaultLang, ignoreLang, SPARQL_service);
																			
										//The ordered levels do not have labels
										List<LDResource> orderedCurrentDimLevels=
												CubeSPARQL.getOrderedDimensionLevelsFromSchema(
												ldr.getURI(), cubeDSDGraphs.get(cubeSliceURI),
												 SPARQL_service);
										
										//Ordered levels with labels
										List<LDResource> orderedDimLevelsWithLabel=new ArrayList<LDResource>();
										for(LDResource orderedLDR:orderedCurrentDimLevels){
											int levelIndex=currentDimLevelsWithLables.indexOf(orderedLDR);
											if(levelIndex!=-1){
												LDResource levelWithLabel=currentDimLevelsWithLables.get(levelIndex);
												orderedDimLevelsWithLabel.add(levelWithLabel);
											}
										}
									
										List<LDResource> allDimLevels = new ArrayList<LDResource>();
										
										//If there are already levels for the Dimensions
										if(allCubesDimensionsLevels.get(ldr)!=null){
											allDimLevels.addAll(allCubesDimensionsLevels.get(ldr));
										}
										
										//have only one instance for each level (no duplicates)
										for(LDResource current_ldr:orderedDimLevelsWithLabel){
											if(!allDimLevels.contains(current_ldr)){
												allDimLevels.add(current_ldr);
											}
										}
									
										List<LDResource> finalAllDimLevels = new ArrayList<LDResource>();
										for(LDResource level:allDimLevels){
											//if the Schema level exists at the data
											if(CubeSPARQL.askDimensionLevelInDataCube(
													cubeSliceURI,level, 
													cubeGraphs.get(cubeSliceURI),
													cubeDSDGraphs.get(cubeSliceURI),SPARQL_service)){
												finalAllDimLevels.add(level);
											}
										}

										allCubesDimensionsLevels.put(ldr, finalAllDimLevels);	
									}
								}
							});
							
							dimensionsLevelsThread.start();
							
							try {
								dimensionsLevelsThread.join();								
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}

						// Thread to get cube dimensions
						Thread aggregationSetDimsThread = new Thread(
								new Runnable() {
									public void run() {
										// Get all the dimensions of the  aggregation
										// set the cube belongs to
										HashSet<LDResource> aggregationSetDimsSet = 
												new HashSet<LDResource>(aggregationSetDims);
										aggregationSetDimsSet.addAll(AggregationSPARQL
												.getAggegationSetDimsFromCube(
														cubeSliceURI,
														cubeDSDGraphs.get(cubeSliceURI),
														selectedLanguage,
														defaultLang,
														ignoreLang,
														SPARQL_service));
										aggregationSetDims = new ArrayList<LDResource>(
												aggregationSetDimsSet);
									}
						});

						// Thread to get cube dimensions
						Thread cubeDimsOfAggregationSetThread = new Thread(new Runnable() {
									public void run() {
										// Get all the dimensions per cube of the aggregations set
										cubeDimsOfAggregationSet.add(AggregationSPARQL
												.getCubeAndDimensionsOfAggregateSet2(
														cubeSliceURI,
														cubeDSDGraphs.get(cubeSliceURI),
														selectedLanguage,
														defaultLang,
														ignoreLang,
														SPARQL_service));
									}
								});

						if(getDimvalues){
							// Thread to get cube dimension values
							Thread dimensionsValuesThread = new Thread(
									new Runnable() {
										public void run() {
											// Get values for each cube dimension
											HashMap<LDResource, List<LDResource>> thisCubeDimensionsValues = CubeHandlingUtils
													.getDimsValues(
															cubeDimensions,
															cubeSliceURI,
															cubeGraphs.get(cubeSliceURI),
															cubeDSDGraphs.get(cubeSliceURI),
															useCodeLists,
															selectedLanguage,
															defaultLang,
															ignoreLang,
															SPARQL_service);
	
											for (LDResource dim : thisCubeDimensionsValues.keySet()) {
	
												HashSet<LDResource> cubeDimValuesSet;
												if (allDimensionsValues.get(dim) != null) {
													cubeDimValuesSet = new HashSet<LDResource>(
															allDimensionsValues.get(dim));
												} else {
													cubeDimValuesSet = new HashSet<LDResource>();
												}										
	
												cubeDimValuesSet.addAll(thisCubeDimensionsValues.get(dim));
										
												List<LDResource> sortedcubeDimValues= new ArrayList<LDResource>(cubeDimValuesSet);
												Collections.sort(sortedcubeDimValues);
												allDimensionsValues.put(dim,sortedcubeDimValues);
											}
										}
							});
							dimensionsValuesThread.start();
							
							try {
								dimensionsValuesThread.join();								
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							
						}
				
						aggregationSetDimsThread.start();
						cubeDimsOfAggregationSetThread.start();
				
						try {
							aggregationSetDimsThread.join();
							cubeDimsOfAggregationSetThread.join();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						
						if(mergeParam!=null&&mergeParam.equals("dimvalue")){
							getMeasures=false;
						}else if(mergeParam!=null&&mergeParam.equals("measure")){
							getDimvalues=false;
						}
					}				
					
					boolean correctNumOfLevels=true;
					
					for(LDResource d:selectedDimenisons){
						List<LDResource> thisDimSelectedLeves=selectedDimLevels.get(d);
						List<LDResource> thisDimAllLevels=allCubesDimensionsLevels.get(d);
						
						if(thisDimAllLevels.size()>0 && 
								(thisDimSelectedLeves==null ||thisDimSelectedLeves.size()==0) ){
							correctNumOfLevels=false;
							break;
						}
					}													
					
					//Show compatible cubes when
					//- a new aggregated cube has been selected by selecting a dimension
					//- only one cube is visualized (e.g. when combine 2 cubes no
					//  compatible cubes will be shown)
					//- At least one measure has already been selected
					if(newcubeselected&& cubeSliceURIs.size()==1&&selectedMeasures.size()>0 &&
							correctNumOfLevels&& selectedDimenisons.size()>0){
						
						String tmpCubeURI=cubeSliceURIs.get(0);
						
						String tmpURIwithoutBrackets=tmpCubeURI.replaceAll("<", "");
						tmpURIwithoutBrackets=tmpURIwithoutBrackets.replaceAll(">","");
						LDResource selectedCube=new LDResource(tmpURIwithoutBrackets);
						
						measurecompatible =	SelectionSPARQL.getLinkAddMeasureCompatibleCubes(
										selectedCube, cubeGraphs.get(tmpCubeURI),
										cubeDSDGraphs.get(tmpCubeURI),
										selectedLanguage,
										defaultLang, ignoreLang, SPARQL_service);
						
						dimensioncompatible=new HashMap<LDResource, HashMap<LDResource,List<LDResource>>>();
						
						for(LDResource dim:cubeDimensions){
							
							HashMap<LDResource, List<LDResource>> dimComp=
									SelectionSPARQL.getLinkAddValueToLevelCompatibleCubes(
											selectedCube, cubeGraphs.get(tmpCubeURI),
											cubeDSDGraphs.get(tmpCubeURI),
											dim, selectedLanguage,
											defaultLang, ignoreLang, SPARQL_service);

							//If compatible cubes are identified
							if(dimComp.keySet().size()>0){
								dimensioncompatible.put(dim,dimComp);	
							}													
						}												
					}
					
				// See when 2 slices are compatible
				// ADD DIM LEVELS OF SLICE 					
				} else if (isSlice) {					

					for (String cubeSliceURItmp : cubeSliceURIs) {

						cubeSliceURI = cubeSliceURItmp;

						// The slice graph is the graph of the URI computed above
						sliceGraphs.put(cubeSliceURI,cubeGraphs.get(cubeSliceURI));

						// Get the cube graph from the slice
						cubeGraphs.put(cubeSliceURI, SliceSPARQL
								.getCubeGraphFromSlice(cubeSliceURI,
										sliceGraphs.get(cubeSliceURI),
										SPARQL_service));

						// Get Cube Structure graph from slice
						cubeDSDGraphs.put(cubeSliceURI, SliceSPARQL
								.getCubeStructureGraphFromSlice(cubeSliceURI,
										sliceGraphs.get(cubeSliceURI),
										SPARQL_service));

						if (!ignoreLang) {
							// Get the available languages of/ labels

							// Use set to have one instance of each language
							HashSet<String> languagesSet = new HashSet<String>(
									availableLanguages);

							languagesSet.addAll(CubeSPARQL
									.getAvailableCubeLanguages(
											cubeDSDGraphs.get(cubeSliceURI),
											SPARQL_service));

							availableLanguages = new ArrayList<String>(
									languagesSet);

							// get the selected language to use
							selectedLanguage = CubeHandlingUtils
									.getSelectedLanguage(availableLanguages,
											selectedLanguage);
						}

						Thread sliceFixedDimensionsThread = new Thread(	new Runnable() {
									public void run() {
										// Get slice fixed dimensions
										// Use set to have one instance of fixed dim
										HashSet<LDResource> sliceFixedDimensionsSet = new HashSet<LDResource>(
												sliceFixedDimensions);

										sliceFixedDimensionsSet.addAll(SliceSPARQL
												.getSliceFixedDimensions(
														cubeSliceURI,
														sliceGraphs.get(cubeSliceURI),
														cubeDSDGraphs.get(cubeSliceURI),
														selectedLanguage,
														defaultLang,
														ignoreLang,
														SPARQL_service));

										sliceFixedDimensions = new ArrayList<LDResource>(
												sliceFixedDimensionsSet);

									}
						});

						Thread cubeDimsFromSliceThread = new Thread(
								new Runnable() {
									public void run() {
										// Get all cube dimensions
										// Use set to have one instance of fixed dim
										HashSet<LDResource> cubeDimsFromSliceSet = new HashSet<LDResource>(
												cubeDimsFromSlice);

										cubeDimsFromSliceSet.addAll(SliceSPARQL
												.getDataCubeDimensionsFromSlice(
														cubeSliceURI,
														sliceGraphs.get(cubeSliceURI),
														cubeDSDGraphs.get(cubeSliceURI),
														selectedLanguage,
														defaultLang,
														ignoreLang,
														SPARQL_service));

										cubeDimsFromSlice = new ArrayList<LDResource>(
												cubeDimsFromSliceSet);

									}
						});

						Thread cubeMeasuresThread = new Thread(new Runnable() {
							public void run() {
								// Get the Cube measures
								HashSet<LDResource> cubeMeasuresSet = new HashSet<LDResource>(
										cubeMeasures);

								cubeMeasuresSet.addAll(SliceSPARQL
										.getSliceMeasure(
												cubeSliceURI,
												sliceGraphs.get(cubeSliceURI),
												cubeDSDGraphs.get(cubeSliceURI),
												selectedLanguage, defaultLang,
												ignoreLang, SPARQL_service));

								cubeMeasures = new ArrayList<LDResource>(cubeMeasuresSet);
						
							}
						});

						sliceFixedDimensionsThread.start();
						cubeDimsFromSliceThread.start();
						cubeMeasuresThread.start();

						try {
							sliceFixedDimensionsThread.join();
							cubeDimsFromSliceThread.join();
							cubeMeasuresThread.join();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

						// The slice visual dimensions are: (all cube dims) - (slice fixed dims)
						cubeDimensions = cubeDimsFromSlice;
						cubeDimensions.removeAll(sliceFixedDimensions);

						Thread sliceFixedDimensionsValuesThread = new Thread(
								new Runnable() {
									public void run() {
										sliceFixedDimensionsValues.putAll(SliceSPARQL
												.getSliceFixedDimensionsValues(
														sliceFixedDimensions,
														cubeSliceURI,
														sliceGraphs.get(cubeSliceURI),
														cubeDSDGraphs.get(cubeSliceURI),
														selectedLanguage,
														defaultLang,
														ignoreLang,
														SPARQL_service));
									}
						});

						Thread allDimensionsValuesThread = new Thread(
								new Runnable() {
									public void run() {

										HashMap<LDResource, List<LDResource>> thisSliceDimensionsValues = CubeHandlingUtils
												.getDimsValuesFromSlice(
														cubeDimensions,
														cubeSliceURI,
														cubeGraphs.get(cubeSliceURI),
														cubeDSDGraphs.get(cubeSliceURI),
														sliceGraphs.get(cubeSliceURI),
														useCodeLists,
														selectedLanguage,
														defaultLang,
														ignoreLang,
														SPARQL_service);

										for (LDResource dim : thisSliceDimensionsValues.keySet()) {

											HashSet<LDResource> cubeDimValuesSet;
											if (allDimensionsValues.get(dim) != null) {
												cubeDimValuesSet = new HashSet<LDResource>(
														allDimensionsValues.get(dim));
											} else {
												cubeDimValuesSet = new HashSet<LDResource>();
											}

											cubeDimValuesSet.addAll(thisSliceDimensionsValues.get(dim));

											List<LDResource> sortedSliceDimValues=
													new ArrayList<LDResource>(cubeDimValuesSet);
											Collections.sort(sortedSliceDimValues);
											allDimensionsValues.put(dim,sortedSliceDimValues);
										}
									}
						});

						sliceFixedDimensionsValuesThread.start();
						allDimensionsValuesThread.start();

						try {
							sliceFixedDimensionsValuesThread.join();
							allDimensionsValuesThread.join();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}

				allLevelsValues=new HashMap<LDResource, List<LDResource>>();
				
				//Get all the values for each level (level -> values[])
				//To get the values use the allDimensionsValues
				for(LDResource dim:allDimensionsValues.keySet()){
					for(LDResource value:allDimensionsValues.get(dim)){
						//if there is a level at the value put the value at the list
				
						if(value.getLevel()!=null){
							LDResource thisLevel=new LDResource(value.getLevel());
							List<LDResource> currentLevelValues=allLevelsValues.get(thisLevel);
							//If there are no values for the level yet
							if(currentLevelValues==null){
								currentLevelValues=new ArrayList<LDResource>();
							}
							currentLevelValues.add(value);
							Collections.sort(currentLevelValues);
							allLevelsValues.put(thisLevel, currentLevelValues);
						}
					}
				}				
				
				// top container styling
				
				selectCubeContainer.addStyle("border-style", "solid");
				selectCubeContainer.addStyle("border-width", "1px");
				selectCubeContainer.addStyle("padding", "10px");
				selectCubeContainer.addStyle("border-radius", "5px");
				selectCubeContainer.addStyle("width", "1050px ");
				selectCubeContainer.addStyle("border-color", "#C8C8C8 ");
				selectCubeContainer.addStyle("display", "table-cell ");
				selectCubeContainer.addStyle("vertical-align", "middle ");
				selectCubeContainer.addStyle("align", "center");
				selectCubeContainer.addStyle("text-align", "left");
				
				if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
					FLabel allcubes_label = new FLabel("allcubes_label",
							"<b>Please select a cube to visualize:<b>");
					selectCubeContainer.add(allcubes_label);
				}else if(selectedLanguage.equals("nl")){
					FLabel allcubes_label = new FLabel("allcubes_label",
							"<b>Selecteer een kubus:<b>");
					selectCubeContainer.add(allcubes_label);
				}else if(selectedLanguage.equals("fr")){
					FLabel allcubes_label = new FLabel("allcubes_label",
							"<b>Veuillez sélectionner un cube:<b>");
					selectCubeContainer.add(allcubes_label);
				}else if(selectedLanguage.equals("de")){
					FLabel allcubes_label = new FLabel("allcubes_label",
							"<b>Bitte wählen Sie einen Würfel zu visualisieren:<b>");
					selectCubeContainer.add(allcubes_label);
				}
				
				// Add Combo box with cube URIs 
				FComboBox cubesCombo = new  FComboBox("cubesCombo") {
				  
					@Override public void onChange() {
						
						 isFirstCubeLoad=true;
						 newcubeselected=false;
						 cubeSliceURIs=new ArrayList<String>();
						 cubeSliceURIs.add("<"+ ((LDResource) this.getSelected().get(0)).getURI()+ ">");
							  
						 selectedCubeFromDropDown=(LDResource) this.getSelected().get(0);
						 originalCubeSliceURIs=cubeSliceURIs;
												  
						 cnt.removeAll(); 
						 overalGrid.removeAll();
						 leftGrid.removeAll();					  
						 leftcontainer.removeAll();
						 rightcontainer.removeAll();
						 aggSetDimContainer.removeAll();
						 dimensionURIfcomponents.clear();
						 languagecontainer.removeAll(); 
						 measurescontainer.removeAll();
						 selectCubeContainer.removeAll();
							  					  
						 // Initialize everything for the new cube 
						 selectedMeasures = new  ArrayList<LDResource>();
						 selectedDimenisons=new ArrayList<LDResource>();
						 aggregationSetDims = new  ArrayList<LDResource>();
						 cubeDimsOfAggregationSet = new ArrayList<HashMap<LDResource, List<LDResource>>>();
						 visualDimensions =  new ArrayList<LDResource>();
						 fixedDimensions = new  ArrayList<LDResource>(); 
						 sliceFixedDimensions = new  ArrayList<LDResource>(); 
						 fixedDimensionsSelectedValues = new  HashMap<LDResource, LDResource>();
						 sliceFixedDimensionsValues  = new HashMap<LDResource, LDResource>();
						 mapDimURIcheckBox =  new HashMap<LDResource, FCheckBox>();
						 mapMeasureURIcheckBox =  new HashMap<LDResource, FCheckBox>();
						 selectedDimLevels=new HashMap<LDResource, List<LDResource>>();
							  
						 // show the cube 
						 populateCentralContainer();
			}};				  			  
				  
			// populate cubes combo box 
			for (LDResource cube : allCubes) { 
			  if (cube.getURIorLabel() != null) {
					   cubesCombo.addChoice(cube.getURIorLabel(), cube);
				  }
			}
				  
			 //set  preselected cube
			 if (selectedCubeFromDropDown!=null) {
				  cubesCombo.setPreSelected(selectedCubeFromDropDown);
			 } 
			 selectCubeContainer.add(cubesCombo);
				 
			// Add top container if it is a cube not slice
			if (isCube) {
			
				// top container styling
				aggSetDimContainer.addStyle("border-style", "solid");
				aggSetDimContainer.addStyle("border-width", "1px");
				aggSetDimContainer.addStyle("padding", "10px");
				aggSetDimContainer.addStyle("border-radius", "5px");
				aggSetDimContainer.addStyle("border-color", "#C8C8C8 ");
				aggSetDimContainer.addStyle("display", "table-cell ");
				aggSetDimContainer.addStyle("vertical-align", "middle ");
				aggSetDimContainer.addStyle("width", "350px ");
				aggSetDimContainer.addStyle("margin-left", "auto");
				aggSetDimContainer.addStyle("margin-right", "auto");
				aggSetDimContainer.addStyle("text-align", "left");
									
				// If an aggregation set has already been created
				if (aggregationSetDims.size() > 0) {

					if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
						FLabel OLAPbrowsing_label = new FLabel(	"OLAPbrowsing_label",
							"<b>Dimensions</b></br>");									
						aggSetDimContainer.add(OLAPbrowsing_label);
					}else if(selectedLanguage.equals("nl")){
						FLabel OLAPbrowsing_label = new FLabel("OLAPbrowsing_label",
								"<b>Dimensies</b></br>");										
						aggSetDimContainer.add(OLAPbrowsing_label);
					}else if(selectedLanguage.equals("fr")){
						FLabel OLAPbrowsing_label = new FLabel("OLAPbrowsing_label",
								"<b>Dimensions</b></br>");
						aggSetDimContainer.add(OLAPbrowsing_label);
					}else if(selectedLanguage.equals("de")){
						FLabel OLAPbrowsing_label = new FLabel("OLAPbrowsing_label",
								"<b>Dimensionen</b></br>");
						aggSetDimContainer.add(OLAPbrowsing_label);
					}
					
					int aggregationDim = 1;

					// Show Aggregation set dimensions
					for (LDResource aggdim : dimsOfaggregationSet2Show) {

						// show one check box for each aggregation set dimension
						FCheckBox aggrDimCheckBox = new FCheckBox("aggregation_" + aggregationDim,
								aggdim.getURIorLabel()) {

							public void onClick() {

								// Get all selected aggregation set dimensions for browsing
								List<LDResource> aggregationSetSelectedDims = new ArrayList<LDResource>();
								selectedDimenisons=new ArrayList<LDResource>();
								for (LDResource aggSetDimURI : mapDimURIcheckBox.keySet()) {
									FCheckBox check = mapDimURIcheckBox.get(aggSetDimURI);

									// Get selected dimensions
									if (check.checked) {
										aggregationSetSelectedDims.add(aggSetDimURI);
										selectedDimenisons.add(aggSetDimURI);										
									}else{
										selectedDimLevels.remove(aggSetDimURI);
									}
								}								

								// Identify the cube of the aggregation set that
								// contains exactly the dimension selected to be browsed
								cubeSliceURIs = new ArrayList<String>();

								//The first set of cubes is from the cube we need to handle first
								// to merge with measures or dimvalues.
								//We need this to add the Aggregated cube with the right order
								for(HashMap<LDResource, List<LDResource>> cubeDimsOfOneAggSet:cubeDimsOfAggregationSet){								
									for (LDResource cube : cubeDimsOfOneAggSet.keySet()) {
										List<LDResource> cubeDims = cubeDimsOfOneAggSet.get(cube);
										if ((cubeDims.size() == aggregationSetSelectedDims.size())
												&& cubeDims.containsAll(aggregationSetSelectedDims)) {
											System.out.println("NEW CUBE URI: "	+ cube.getURI());
	
											// The new cube(s) to visualize
											cubeSliceURIs.add("<" + cube.getURI()+ ">");
										}
									}
								}
								
								if(cubeSliceURIs.size()==0){
									cubeSliceURIs=originalCubeSliceURIs;
								}else{									
									originalCubeSliceURIs=cubeSliceURIs;
								}						
								
								//if aggregation set cubes have been found
								// clear the previous visualization and create a new one for the new cube(s)
								cnt.removeAll();
								overalGrid.removeAll();
								leftGrid.removeAll();									
								aggSetDimContainer.removeAll();
								leftcontainer.removeAll();
								rightcontainer.removeAll();
								dimensionURIfcomponents.clear();
								languagecontainer.removeAll();
								measurescontainer.removeAll();
								selectCubeContainer.removeAll();
	
								measurecompatible=new HashMap<LDResource, List<LDResource>>();
								dimensioncompatible=new HashMap<LDResource, HashMap<LDResource,List<LDResource>>>();
								newcubeselected=true;
								
								// show the cube
								populateCentralContainer();
							}							
						};

						// set as checked if the dimension is contained at the selected cube
						aggrDimCheckBox.setChecked(selectedDimenisons.contains(aggdim));
						mapDimURIcheckBox.put(aggdim, aggrDimCheckBox);
						aggSetDimContainer.add(aggrDimCheckBox);

						aggregationDim++;
				
						if(selectedDimenisons.contains(aggdim)){
							List<FCheckBox> dimLevels_checkBoxes=new ArrayList<FCheckBox>();
							
							FContainer thisDimLevelsContainer = new FContainer("thisDimLevelsContainer_"+aggregationDim);
							thisDimLevelsContainer.addStyle("border-style", "solid");
							thisDimLevelsContainer.addStyle("border-width", "1px");
							thisDimLevelsContainer.addStyle("padding", "10px");
							thisDimLevelsContainer.addStyle("border-radius", "10px");
							thisDimLevelsContainer.addStyle("border-color", "#C8C8C8 ");
							thisDimLevelsContainer.addStyle("display", "table-cell ");
							thisDimLevelsContainer.addStyle("vertical-align", "middle ");
							thisDimLevelsContainer.addStyle("margin-left", "auto");
							thisDimLevelsContainer.addStyle("margin-right", "auto");
							thisDimLevelsContainer.addStyle("text-align", "left");						
							
							if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
								FLabel selectLevels_label = new FLabel(
										"selectLevels_label_"+aggregationDim,
										"<p><i>(Select at most two levels)</i></p>");
								thisDimLevelsContainer.add(selectLevels_label);
							}else if(selectedLanguage.equals("nl")){
								FLabel selectLevels_label = new FLabel(
										"selectLevels_label_"+aggregationDim,
										"<p><i>(Selecteer maximaal 2 niveaus)</i></p>");
								thisDimLevelsContainer.add(selectLevels_label);
							}else if(selectedLanguage.equals("fr")){
								FLabel selectLevels_label = new FLabel(
										"selectLevels_label_"+aggregationDim,
										"<p><i>(Sélectionnez au maximum 2 niveaux)</i></p>");
								thisDimLevelsContainer.add(selectLevels_label);
							}else if(selectedLanguage.equals("de")){
								FLabel selectLevels_label = new FLabel(
										"selectLevels_label_"+aggregationDim,
										"<p><i>(Bitte wählen Sie höchstens 2 Ebenen)</i></p>");
								thisDimLevelsContainer.add(selectLevels_label);
							}					
							
							for(LDResource level:allCubesDimensionsLevels.get(aggdim)){
								FCheckBox dimLevelCheckBox = new FCheckBox(
										"level_" + aggregationDim,level.getURIorLabel()) {
									
									public void onClick() {
										
										selectedDimLevels=new HashMap<LDResource, List<LDResource>>();
										
										//Get the selected levels for each dimension 
										for (LDResource aggSetDimURI : mapDimLevelscheckBoxes.keySet()) {
											List<FCheckBox> dimLevelCheckList = mapDimLevelscheckBoxes.get(aggSetDimURI);
											
											//If there are levels at the Dim
											if(dimLevelCheckList!=null && dimLevelCheckList.size()>0){
												//The Selected Dimension Levels as is now
												List<LDResource> thisDimSelectedLevels=new ArrayList<LDResource>();
												
												//All the existing Dimension Levels
												List<LDResource> thisDimAllLevels=allCubesDimensionsLevels.get(aggSetDimURI);
												if(thisDimAllLevels!=null){
													//Check all checkboxes of levels for a dim if are checked
													for(FCheckBox check:dimLevelCheckList){
														//If is checked add the level to the selected
														if (check.checked) {
															//search the Level LDResource at the existing levels															
															for(LDResource l:thisDimAllLevels){
																//The checkbox has the same value (URI) with the level
																if(l.getURIorLabel().equals(check.getLabel())){
																	//Add the LDResource level at the selected and break
																	thisDimSelectedLevels.add(l);
																	break;
																}
															}													
														}
													}
												}
												selectedDimLevels.put(aggSetDimURI, thisDimSelectedLevels);													
											}
										}
										
										boolean correctNumOfLevels=true;
										boolean correctTotalLevels=true;
										
										for(LDResource d:selectedDimLevels.keySet()){
											List<LDResource> thisDimSelectedLeves=selectedDimLevels.get(d);
											List<LDResource> thisDimAllLevels=allCubesDimensionsLevels.get(d);
											
											if(thisDimSelectedLeves.size()>2 ){
												correctNumOfLevels=false;
												
												//remove the added levels from the selected dim levels
												for(LDResource l:thisDimSelectedLeves){
													//The checkbox has the same value (URI) with the level
													if(l.getURIorLabel().equals(this.getLabel())){
														//Add the LDResource level at the selected and break
														thisDimSelectedLeves.remove(l);
														selectedDimLevels.put(d, thisDimSelectedLeves);
														break;
													}
												}	
												break;
										
											}										
										}
										
										boolean foundOneDimWith2Levels=false;
										for(LDResource d:selectedDimLevels.keySet()){
											List<LDResource> thisDimSelectedLeves=selectedDimLevels.get(d);
											
											if(thisDimSelectedLeves.size()>1){
												if(!foundOneDimWith2Levels){
													foundOneDimWith2Levels=true;
												}else{
													correctTotalLevels=false;
													//remove the added levels from the selected dim levels
													for(LDResource l:thisDimSelectedLeves){
														//The checkbox has the same value (URI) with the level
														if(l.getURIorLabel().equals(this.getLabel())){
															//Add the LDResource level at the selected and break
															thisDimSelectedLeves.remove(l);
															selectedDimLevels.put(d, thisDimSelectedLeves);
															break;
														}
													}	
													break;
												}
											}									
										}
										
										if(correctNumOfLevels && correctTotalLevels ){
											cnt.removeAll();
											overalGrid.removeAll();
											leftGrid.removeAll();									
											aggSetDimContainer.removeAll();
											leftcontainer.removeAll();
											rightcontainer.removeAll();
											dimensionURIfcomponents.clear();
											languagecontainer.removeAll();
											measurescontainer.removeAll();
											selectCubeContainer.removeAll();
			
											// show the cube
											populateCentralContainer();	
										}else if(!correctNumOfLevels) {
											
											if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
												FDialog.showMessage(this.getPage(),
														"Please select at most 2 levels",
														"Please select at most 2 levels", "ok");
											} else if(selectedLanguage.equals("nl")){
													FDialog.showMessage(this.getPage(),
															"Selecteer maximaal 2 niveaus",
															"Selecteer maximaal 2 niveaus", "ok");
											} else if(selectedLanguage.equals("fr")){
													FDialog.showMessage(this.getPage(),
															"Sélectionnez au maximum 2 niveaux ",
															"Sélectionnez au maximum 2 niveaux ", "ok");
											} else if(selectedLanguage.equals("de")){
													FDialog.showMessage(this.getPage(),
															"Bitte wählen Sie höchstens 2 Ebenen",
															"Bitte wählen Sie höchstens 2 Ebenen", "ok");
											}			
											
											this.setChecked(false);
											
										}else if(!correctTotalLevels) {
											
											if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
												FDialog.showMessage(this.getPage(),
														"Select multiple levels at only one dimension",
														"Select multiple levels at only one dimension", "ok");
											} else if(selectedLanguage.equals("nl")){
													FDialog.showMessage(this.getPage(),
															"Selecteer meerdere niveaus slechts één dimensie",
															"Selecteer meerdere niveaus slechts één dimensie", "ok");
											} else if(selectedLanguage.equals("fr")){
													FDialog.showMessage(this.getPage(),
															"Sélectionner plusieurs niveaux à une seule dimension",
															"Sélectionner plusieurs niveaux à une seule dimension", "ok");
											} else if(selectedLanguage.equals("de")){
													FDialog.showMessage(this.getPage(),
															"Wählen Sie mehrere Ebenen auf nur eine dimension",
															"Wählen Sie mehrere Ebenen auf nur eine dimension", "ok");
											}			
											
											this.setChecked(false);											
										}
									}								
								};
								
								if(selectedDimLevels.get(aggdim)!=null){
									dimLevelCheckBox.setChecked(selectedDimLevels.get(aggdim).contains(level));
								}														
								
								dimLevels_checkBoxes.add(dimLevelCheckBox);															
								thisDimLevelsContainer.add(dimLevelCheckBox);
								aggregationDim++;
						}		
						
						//if there are levels added	(there is one FLabel added by default)						
						if(thisDimLevelsContainer.getAllComponents().size()>1){
							mapDimLevelscheckBoxes.put(aggdim, dimLevels_checkBoxes);
							aggSetDimContainer.add(thisDimLevelsContainer);
						}
					}
				  }
				} else {
					FLabel notOLAP = new FLabel("notOLAP", "<b>OLAP-like browsing is not "
									+ "supported for this cube<b>");
					aggSetDimContainer.add(notOLAP);
				}

				} else if (isSlice) {

				}

				// //////////Measures container//////////////////

				// measure container styling
				measurescontainer.addStyle("border-style", "solid");
				measurescontainer.addStyle("border-width", "1px");
				measurescontainer.addStyle("padding", "10px");
				measurescontainer.addStyle("border-radius", "5px");
				measurescontainer.addStyle("border-color", "#C8C8C8 ");
				measurescontainer.addStyle("width", "350px ");
				measurescontainer.addStyle("display", "table-cell ");
				measurescontainer.addStyle("align", "center");
				measurescontainer.addStyle("text-align", "left");
				
				
				if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
					FLabel measure_label = new FLabel("measure_lb",	"<b>Measures</b></br>");
					measurescontainer.add(measure_label);
				}else if(selectedLanguage.equals("nl")){
					FLabel measure_label = new FLabel("measure_lb",	"<b>Metingen</b></br>");
					measurescontainer.add(measure_label);
				}else if(selectedLanguage.equals("fr")){
					FLabel measure_label = new FLabel("measure_lb",	"<b>Mesures</b></br>");
					measurescontainer.add(measure_label);
				}else if(selectedLanguage.equals("de")){
					FLabel measure_label = new FLabel("measure_lb",	"<b>Messungen</b></br>");
					measurescontainer.add(measure_label);
				}
				
				int colorIndex = 0;
				// Show Aggregation set dimensions
				int measureCount = 1;
				for (LDResource measure : cubeMeasures) {

					// show one check box for each measure
					FCheckBox measureCheckBox = new FCheckBox("measure_"
							+ measureCount, "<font color=\""
							+ measureColors[colorIndex] + "\">"
							+ measure.getURIorLabel() + "</font>") {

						public void onClick() {

							// Get all selected aggregation set dimensions for browsing
							selectedMeasures = new ArrayList<LDResource>();
							for (LDResource measureURI : mapMeasureURIcheckBox.keySet()) {
								FCheckBox check = mapMeasureURIcheckBox.get(measureURI);
								// Get selected dimensions
								if (check.checked) {
									selectedMeasures.add(measureURI);
								}
							}

							cnt.removeAll();
							overalGrid.removeAll();
							leftGrid.removeAll();
							aggSetDimContainer.removeAll();
							leftcontainer.removeAll();
							rightcontainer.removeAll();
							dimensionURIfcomponents.clear();
							languagecontainer.removeAll();
							measurescontainer.removeAll();
							selectCubeContainer.removeAll();
							// show the cube
							populateCentralContainer();
						}
					};

					colorIndex++;

					// set as checked if the dimension is contained at the selected cube
					measureCheckBox.setChecked(selectedMeasures.contains(measure));
					mapMeasureURIcheckBox.put(measure, measureCheckBox);
					measurescontainer.add(measureCheckBox);
					measureCount++;
				}

				// //////////Language container//////////////////

				if (!ignoreLang) {
					// language container styling
					languagecontainer.addStyle("border-style", "solid");
					languagecontainer.addStyle("border-width", "1px");
					languagecontainer.addStyle("padding", "10px");
					languagecontainer.addStyle("border-radius", "5px");
					languagecontainer.addStyle("border-color", "#C8C8C8 ");
					languagecontainer.addStyle("margin-left", "auto");
					languagecontainer.addStyle("margin-right", "auto");
					languagecontainer.addStyle("text-align", "left");
					languagecontainer.addStyle("width", "350px ");
					languagecontainer.addStyle("height", "60px");
					languagecontainer.addStyle("display", "table-cell ");
					languagecontainer.addStyle("vertical-align", "middle ");
					
					if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
					FLabel datalang_label = new FLabel(
							"datalang",	"<b>Language</b></br>"
									+ "Select the language of the visualized data:");
					languagecontainer.add(datalang_label);
					}else if(selectedLanguage.equals("nl")){
						FLabel datalang_label = new FLabel(
								"datalang",	"<b>Taal</b></br>"
										+ "Selecteer de taal van de gevisualiseerde data:");
						languagecontainer.add(datalang_label);
					}else if(selectedLanguage.equals("fr")){
						FLabel datalang_label = new FLabel(
								"datalang",	"<b>Langue</b></br>"
										+ "Sélectionnez la langue de la visualisation des données:");
						languagecontainer.add(datalang_label);
					}else if(selectedLanguage.equals("de")){
						FLabel datalang_label = new FLabel(
								"datalang",	"<b>Sprache</b></br>"
										+ "Wählen Sie die Sprache der visualisierten Daten:");
						languagecontainer.add(datalang_label);
					}
					
					
					// Add Combo box for language selection
					FComboBox datalang_combo = new FComboBox("datalang_combo") {
						@Override
						public void onChange() {
							selectedLanguage = this.getSelected().get(0).toString();
							
							allCubes = SelectionSPARQL.getMaximalCubesAndSlices(selectedLanguage,
									selectedLanguage,ignoreLang,SPARQL_service);
											
							List<LDResource> tmp_dimsOfaggregationSet2Show=AggregationSPARQL.getAggegationSetDimsFromCube(
										cubeSliceURI, cubeDSDGraphs.get(cubeSliceURI), selectedLanguage,
										defaultLang, ignoreLang, SPARQL_service);
							tmp_dimsOfaggregationSet2Show.retainAll(dimsOfaggregationSet2Show);
							dimsOfaggregationSet2Show=	tmp_dimsOfaggregationSet2Show;				
							
							cnt.removeAll();
							overalGrid.removeAll();
							leftGrid.removeAll();							
							aggSetDimContainer.removeAll();
							leftcontainer.removeAll();
							rightcontainer.removeAll();
							dimensionURIfcomponents.clear();
							languagecontainer.removeAll();
							measurescontainer.removeAll();
							selectCubeContainer.removeAll();
							// show the cube
							populateCentralContainer();
						}
					};

					// populate language combo box
					for (String lang : availableLanguages) {
						datalang_combo.addChoice(lang, lang);
					}
					datalang_combo.setPreSelected(selectedLanguage);
					languagecontainer.add(datalang_combo);
				}

				// /////Keep the previous combo box selections/////////
				
				if (cubeDimensions.size() > 1 && selectedDimenisons.size()>0) {

					// The visual dimensions has been removed
					if (!cubeDimensions.contains(visualDimensions.get(0))) {
						if (visualDimensions.get(1).equals(
								cubeDimensions.get(0))) {
							visualDimensions.remove(0);
							visualDimensions.add(0, cubeDimensions.get(1));
						} else {
							visualDimensions.remove(0);
							visualDimensions.add(0, cubeDimensions.get(0));
						}
					}

					// There are 2 visual dimensions
					if (visualDimensions.size() > 1) {
						// If the previous dimension does not exist any more
						// (has been removed by the user)
						if (!cubeDimensions.contains(visualDimensions.get(1))) {
							if (visualDimensions.get(0).equals(cubeDimensions.get(0))) {
								visualDimensions.remove(1);
								visualDimensions.add(1, cubeDimensions.get(1));
							} else {
								visualDimensions.remove(1);
								visualDimensions.add(1, cubeDimensions.get(0));
							}
						}

					// One visual dimension
					} else {
						if (visualDimensions.get(0).equals(
								cubeDimensions.get(0))) {
							visualDimensions.add(1, cubeDimensions.get(1));
						} else {
							visualDimensions.add(1, cubeDimensions.get(0));
						}

					}
				} else {
					visualDimensions = cubeDimensions;
				}
				
				if(selectedDimenisons.size()>0){										
					// Get the fixed cube dimensions
					fixedDimensions = CubeHandlingUtils.getFixedDimensions(
							cubeDimensions, visualDimensions);

					// Selected values for the fixed dimensions
					fixedDimensionsSelectedValues = CubeHandlingUtils
							.getFixedDimensionsRandomSelectedValues(
									allDimensionsValues, fixedDimensions,
									fixedDimensionsSelectedValues);
				}
			
				if((selectedMeasures.size()>0 && selectedDimenisons.size()>0&&isCube)||
						(isSlice&&selectedMeasures.size()>0)){
					List<BindingSet> allResults = new ArrayList<BindingSet>();
	
					List<String> reverseCubeSliceURIs = new ArrayList<String>(cubeSliceURIs);
	
					// We need the observations of the first cube to be shown when duplicates exist
					Collections.reverse(reverseCubeSliceURIs);
					for (String tmpCubeSliceURI : reverseCubeSliceURIs) {
						TupleQueryResult res = null;
						// Get query tuples for visualization						
						if (isCube) {							
							List<LDResource> tmpMeasures=new ArrayList<LDResource>(
									measuresPerCube.get(tmpCubeSliceURI));
							tmpMeasures.retainAll(selectedMeasures);
										
							if(tmpMeasures.size()>0){
								res = CubeBrowserSPARQL
										.get2DVisualsiationValuesAndLevels(visualDimensions,
												fixedDimensionsSelectedValues,
												tmpMeasures, allDimensionsValues,
												selectedDimLevels,  allCubesDimensionsLevels,
												tmpCubeSliceURI,
												cubeGraphs.get(tmpCubeSliceURI),
												cubeDSDGraphs.get(tmpCubeSliceURI),
												SPARQL_service);
							}
						
						//Need to see the measures
						} else if (isSlice) {					
							
							res = CubeBrowserSPARQL
									.get2DVisualsiationValuesFromSlice(
											visualDimensions,
											fixedDimensionsSelectedValues,
											selectedMeasures, allDimensionsValues,
											tmpCubeSliceURI,
											sliceGraphs.get(tmpCubeSliceURI),
											cubeGraphs.get(tmpCubeSliceURI),
											SPARQL_service);
						}
	
						//there are results - tmpMeasures.size()>0
						if(res!=null){
							// collect all result from all cubes
							try {
								while (res.hasNext()) {
									allResults.add(res.next());
								}
							} catch (QueryEvaluationException e) {
								e.printStackTrace();
							}
						}
					}				
					
					// Create an FTable model based on the query tuples
					FTableModel tm = create2DCubeTableModelWithLevels(allResults,
							allDimensionsValues, visualDimensions);
	
					// Initialize FTable
					ftable.setShowCSVExport(true);
					ftable.setNumberOfRows(10);
					ftable.setEnableFilter(true);
					ftable.setOverFlowContainer(true);
					ftable.setFilterPos(FilterPos.TOP);
					ftable.setSortable(false);
					ftable.setModel(tm);
				}
				
				// //////////Left container//////////////////

				// Show left container if there are 2 visual dimensions
				if (visualDimensions.size() == 2) {

					// left container styling
					leftcontainer.addStyle("border-style", "solid");
					leftcontainer.addStyle("border-width", "1px");
					leftcontainer.addStyle("padding", "10px");
					leftcontainer.addStyle("border-radius", "5px");
					leftcontainer.addStyle("border-color", "#C8C8C8 ");
					leftcontainer.addStyle("width", "350px ");
					leftcontainer.addStyle("display", "table-cell ");
					leftcontainer.addStyle("align", "center");
					leftcontainer.addStyle("text-align", "left");

					FGrid visualDimGrid = new FGrid("visualDimGrid");

					ArrayList<FComponent> dim1Array = new ArrayList<FComponent>();

					// Add label for Dim1 (column headings)
					if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
						FLabel dim1Label = new FLabel("dim1Label","<b>Columns:<b>");
						dim1Array.add(dim1Label);
					}else if(selectedLanguage.equals("nl")){
						FLabel dim1Label = new FLabel("dim1Label","<b>Kolommen:<b>");
						dim1Array.add(dim1Label);
					}else if(selectedLanguage.equals("fr")){
						FLabel dim1Label = new FLabel("dim1Label","<b>Colonnes:<b>");
						dim1Array.add(dim1Label);
					}else if(selectedLanguage.equals("de")){
						FLabel dim1Label = new FLabel("dim1Label",	"<b>Columns:<b>");
						dim1Array.add(dim1Label);
					}
					
					// Add Combo box for Dim1
					final FComboBox dim1Combo = new FComboBox("dim1Combo") {
						@Override
						public void onChange() {

							// Get the URI of the 1st selected dimension
							List<String> d1Selected = this
									.getSelectedAsString();

							// Get the URI of the 2nd selected dimension
							List<String> d2Selected = null;
							for (FComponent fc : leftcontainer
									.getAllComponents()) {
								if (fc.getId().contains("_dim2Combo")) {
									d2Selected = ((FComboBox) fc)
											.getSelectedAsString();

									// If both combo boxes have the same selected value
									// Select randomly another value for d2
									if (d1Selected.get(0).equals(d2Selected.get(0))) {
										List<Pair<String, Object>> d2choices = 
												((FComboBox) fc).getChoices();
										for (Pair<String, Object> pair : d2choices) {
											if (!pair.snd.toString().equals(d2Selected.get(0))) {
												d2Selected.clear();
												d2Selected.add(pair.snd	.toString());
												((FComboBox) fc).setPreSelected(pair.snd.toString());
												((FComboBox) fc).populateView();
												break;
											}
										}
									}
									break;
								}
							}
							setVisualDimensions(d1Selected, d2Selected);
						
							if((isCube&&selectedMeasures.size()>0 &&selectedDimenisons.size()>0)||
									(isSlice&&selectedMeasures.size()>0)){
								showCube();
							}
						}
					};

					// populate Dim1 combo box
					for (LDResource ldr : cubeDimensions) {
						if (ldr.getURIorLabel().length() > 35) {
							dim1Combo.addChoice(ldr.getURIorLabel().substring(0, 35)
									+ "...", ldr.getURI());
						} else {
							dim1Combo.addChoice(ldr.getURIorLabel(), ldr.getURI());
						}						
					}

					dim1Combo.setPreSelected(visualDimensions.get(0).getURI());
					dim1Array.add(dim1Combo);

					ArrayList<FComponent> dim2Array = new ArrayList<FComponent>();

					// Add label for Dim2 (column headings)					
					if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
						FLabel dim2Label = new FLabel("dim2Label","<b>Rows:</b>");
						dim2Array.add(dim2Label);
					}else if(selectedLanguage.equals("nl")){
						FLabel dim2Label = new FLabel("dim2Label",	"<b>Rijen:</b>");
						dim2Array.add(dim2Label);
					}else if(selectedLanguage.equals("fr")){
						FLabel dim2Label = new FLabel("dim2Label",	"<b>Rangées:</b>");
						dim2Array.add(dim2Label);
					}else if(selectedLanguage.equals("de")){
						FLabel dim2Label = new FLabel("dim2Label","<b>Reihen:</b>");
						dim2Array.add(dim2Label);
					}
					
					// Add Combo box for Dim2
					final FComboBox dim2Combo = new FComboBox("dim2Combo") {
						@Override
						public void onChange() {

							// Get the URI of the 2nd selected dimension
							List<String> d2Selected = this.getSelectedAsString();

							// Get the URI of the 1st selected dimension
							List<String> d1Selected = null;
							for (FComponent fc : leftcontainer.getAllComponents()) {
								if (fc.getId().contains("_dim1Combo")) {
									d1Selected = ((FComboBox) fc)
											.getSelectedAsString();

									// Both combo boxes have the same selected value
									// Select randomly another value for d2
									if (d1Selected.get(0).equals(d2Selected.get(0))) {
										List<Pair<String, Object>> d1choices =
												((FComboBox) fc).getChoices();
										for (Pair<String, Object> pair : d1choices) {
											if (!pair.snd.toString().equals(d1Selected.get(0))) {
												d1Selected.clear();
												d1Selected.add(pair.snd.toString());
												((FComboBox) fc).setPreSelected(pair.snd.toString());
												((FComboBox) fc).populateView();
												break;
											}
										}
									}
									break;
								}
							}
							setVisualDimensions(d1Selected, d2Selected);
							
							if((isCube&&selectedMeasures.size()>0 &&selectedDimenisons.size()>0)||
									(isSlice&&selectedMeasures.size()>0)){
								showCube();
							}
						}
					};

					// populate Dim2 combo box
					for (LDResource ldr : cubeDimensions) {
						if (ldr.getURIorLabel().length() > 35) {
							dim2Combo.addChoice(ldr.getURIorLabel().substring(0, 35)
									+ "...", ldr.getURI());
						} else {
							dim2Combo.addChoice(ldr.getURIorLabel(), ldr.getURI());
						}
						
					}

					dim2Combo.setPreSelected(visualDimensions.get(1).getURI());
					dim2Array.add(dim2Combo);

					visualDimGrid.addRow(dim1Array);
					visualDimGrid.addRow(dim2Array);
					leftcontainer.add(visualDimGrid);
				}

				// //////////Right container//////////////////

				// Right container styling
				rightcontainer.addStyle("border-style", "solid");
				rightcontainer.addStyle("border-width", "1px");
				rightcontainer.addStyle("padding", "10px");
				rightcontainer.addStyle("border-radius", "5px");
				rightcontainer.addStyle("border-color", "#C8C8C8 ");
				rightcontainer.addStyle("width", "350px ");
				rightcontainer.addStyle("display", "table-cell ");
				rightcontainer.addStyle("align", "center");
				rightcontainer.addStyle("text-align", "left");
						
				if (!sliceFixedDimensionsValues.isEmpty()
						|| fixedDimensions.size() > 0) {
					
					if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
						FLabel otheropts = new FLabel("Options",
								"<b>Filter:</b></br>");
						rightcontainer.add(otheropts);
						} else if(selectedLanguage.equals("nl")){
							FLabel otheropts = new FLabel("Options",
									"<b>Filter:</b></br>");
							rightcontainer.add(otheropts);
						} else if(selectedLanguage.equals("fr")){
							FLabel otheropts = new FLabel("Options",
									"<b>Filtre:</b></br>");
							rightcontainer.add(otheropts);
						} else if(selectedLanguage.equals("de")){
							FLabel otheropts = new FLabel("Options",
									"<b>Filter:</b></br>");
							rightcontainer.add(otheropts);
						}					
				}

				fixedDimGrid = new FGrid("fixedDimGrid");
				// If it is a slice and there are fixed dimension values
				if (!sliceFixedDimensionsValues.isEmpty()) {

					// An FGrid to show visual and fixed cube dimensions

					int sliceFixedValues = 1;
					for (LDResource sliceFixedDim : sliceFixedDimensionsValues.keySet()) {
						ArrayList<FComponent> farray = new ArrayList<FComponent>();

						// Add the label for the fixed cube dimension
						FLabel fDimLabel = new FLabel("sliceFixedName_"
								+ sliceFixedValues, "<u>"
								+ sliceFixedDim.getURIorLabel() + ": </u>");
						farray.add(fDimLabel);
						LDResource fDimValue = sliceFixedDimensionsValues.get(sliceFixedDim);

						FLabel fDimValueLabel = null;
						if (fDimValue.getURIorLabel().length() > 60) {
							fDimValueLabel = new FLabel("sliceFixedValue_"
									+ sliceFixedValues, fDimValue.getURIorLabel().substring(0, 60) + "...");
						} else {
							fDimValueLabel = new FLabel("sliceFixedValue_"
									+ sliceFixedValues,	fDimValue.getURIorLabel());
						}

						farray.add(fDimValueLabel);
						fixedDimGrid.addRow(farray);

						sliceFixedValues++;
					}
				}

				// If there are fixed dimensions
				if (fixedDimensions.size() > 0) {
					// Add labels and combo boxed for the fixed cube dimensions
					addFixedDimensions();
				}
				rightcontainer.add(fixedDimGrid);
								
				
				////////// Compatible Cube Container ///////////////
				
				boolean correctNumOfLevels=true;
				
				for(LDResource d:selectedDimenisons){
					List<LDResource> thisDimSelectedLeves=selectedDimLevels.get(d);
					List<LDResource> thisDimAllLevels=allCubesDimensionsLevels.get(d);
					
					if(thisDimAllLevels.size()>0 && 
							(thisDimSelectedLeves==null ||thisDimSelectedLeves.size()==0) ){
						correctNumOfLevels=false;
						break;
					}
				}													
				
				
			
				//Only one cube selected to show
				if(cubeSliceURIs.size()==1 && newcubeselected &&
						selectedMeasures.size()>0 && correctNumOfLevels&&
						selectedDimenisons.size()>0&&
						(dimensioncompatible.keySet().size()>0||
						 measurecompatible.keySet().size()>0)){		
					
					compatibleCubeContainer=new FContainer("compatibleCubeContainer");
					compatibleCubeContainer.addStyle("border-style", "solid");
					compatibleCubeContainer.addStyle("border-width", "1px");
					compatibleCubeContainer.addStyle("padding", "10px");
					compatibleCubeContainer.addStyle("border-radius", "5px");
					compatibleCubeContainer.addStyle("border-color", "#C8C8C8 ");
					compatibleCubeContainer.addStyle("width", "700px ");
					compatibleCubeContainer.addStyle("display", "table-cell ");
					compatibleCubeContainer.addStyle("align", "center");
					compatibleCubeContainer.addStyle("text-align", "left");
					
					FLabel compatibleCubes_label = new FLabel(
							"compatibleCubes_label",
							"<b>Based on the current selections the presented cube " +
							"can be expanded in the following ways: <b>");
					compatibleCubeContainer.add(compatibleCubes_label);
					
					addMeasuresAttributeValues_radioButtonGroup = new FRadioButtonGroup(
							"compAttributeValuesRadio");
										
					int i=1;					
					
					String tmpCubeURI=cubeSliceURIs.get(0);
					
					String tmpURIwithoutBrackets=tmpCubeURI.replaceAll("<", "");
					tmpURIwithoutBrackets=tmpURIwithoutBrackets.replaceAll(">","");
					LDResource selectedCube=new LDResource(tmpURIwithoutBrackets);
					
					
					for(LDResource dim:dimensioncompatible.keySet()){
						HashMap<LDResource,List<LDResource>> compatibleCubeValues=dimensioncompatible.get(dim);
						for (LDResource compCube : compatibleCubeValues.keySet()) {
							// show the dimension values only of compatible cubes
							if (!compCube.equals(selectedCube)) {
								String compCubeAttributeValues_str = "<b>Add values to: </b>"
												+dim.getURIorLabel()+" ("+compCube.getURIorLabel()+")";

								compCubeAttributeValues_str+="<a id=\"show_value_id"+i+"\" " +
										"onclick=\"document.getElementById('spoiler_value_id"+i+"').style.display=''; " +
										"document.getElementById('show_value_id"+i+"').style.display='none';\" " +
										"class=\"link\">[Show dimension values]</a>" +
										"<span id=\"spoiler_value_id"+i+"\" style=\"display: none\">" +
										"<a onclick=\"document.getElementById('spoiler_value_id"+i+"').style.display='none';" +
										" document.getElementById('show_value_id"+i+"').style.display='';\"" +
										" class=\"link\">[Hide dimension values]</a><br>";
								
								// show the measures of the compatible cubes
								compCubeAttributeValues_str += "<ol>";
								for (LDResource compAttributeValue : compatibleCubeValues.get(compCube)) {
									compCubeAttributeValues_str += "<li>"
											+ compAttributeValue.getURIorLabel() + "</li>";
								}
								compCubeAttributeValues_str += "</ol><br>";
								
								compCubeAttributeValues_str+="</span>";								
								
								addMeasuresAttributeValues_radioButtonGroup.addRadioButton(
										compCubeAttributeValues_str, compCube.getURI());
								i++;
							}
						}					
					}					
					
					for (LDResource compCube : measurecompatible.keySet()) {
						// show the measures only of compatible cubes
						if (!compCube.equals(selectedCube)) {
							String compCubeMeasures = "<b>Add measures</b> ("
									+ compCube.getURIorLabel()+ ")";								

							compCubeMeasures+="<a id=\"show_measure_id"+i+"\" " +
									"onclick=\"document.getElementById('spoiler_measure_id"+i+"').style.display=''; " +
									"document.getElementById('show_measure_id"+i+"').style.display='none';\" " +
									"class=\"link\">[Show measures]</a>" +
									"<span id=\"spoiler_measure_id"+i+"\" style=\"display: none\">" +
									"<a onclick=\"document.getElementById('spoiler_measure_id"+i+"').style.display='none';" +
									" document.getElementById('show_measure_id"+i+"').style.display='';\"" +
									" class=\"link\">[Hide measures]</a><br>";
							
							
							// show the measures of the compatible cubes
							compCubeMeasures += "<ol>";
							for (LDResource compCubeMeasure : measurecompatible.get(compCube)) {
								compCubeMeasures += "<li>"
										+ compCubeMeasure.getURIorLabel() + "</li>";
							}
							compCubeMeasures += "</ol><br>";
							compCubeMeasures+="</span>";	
							addMeasuresAttributeValues_radioButtonGroup.addRadioButton(
									compCubeMeasures, compCube.getURI());
							i++;
						}
					}									

					//By default checked the 1st button
					compatibleCubeContainer.add(addMeasuresAttributeValues_radioButtonGroup);		
					
					
					FButton mergeAttributeValuesCubes_button = new FButton(
							"mergeAttributeValueCubes", "Expand...") {
						@Override
						public void onClick() {
							//a cube is selected for expansion
							if(addMeasuresAttributeValues_radioButtonGroup.checked!=null){
								LDResource expanderCube = new LDResource(
										addMeasuresAttributeValues_radioButtonGroup.checked.value);
								
								DataCubeBrowserPlusPlus browserPlus = new DataCubeBrowserPlusPlus();
								browserPlus.setPageContext(pc);
								DataCubeBrowserPlusPlus.Config browserPlusConfig = browserPlus.get();
								browserPlusConfig.asynch = true;
								browserPlusConfig.dataCubeURIs=cubeSliceURIs.get(0)+",<"+
										expanderCube.getURI()+">";
								
								//Merge the 2 cube to Add value to dimension
								if(addMeasuresAttributeValues_radioButtonGroup.
										checked.getLabel().contains("Add values")){
									browserPlusConfig.mergeParam="dimvalue";

								//Merge the 2 cube to Add measures
								}else if(addMeasuresAttributeValues_radioButtonGroup.
										checked.getLabel().contains("Add measures")){
									browserPlusConfig.mergeParam="measure";
								}														
								
								browserPlus.setConfig(browserPlusConfig);
								cnt.removeAll();
								cnt.add(browserPlus.getComponentUAE("myid"));					
								cnt.populateView();
							}							
						}
					};
					
					compatibleCubeContainer.add(mergeAttributeValuesCubes_button);
					
				}				
				
				////               UI                ///////////////////////	
				
				ArrayList<FComponent> leftArray=new ArrayList<FComponent>();									
						
				// add language container if there are more than 1 language
				if (!ignoreLang && availableLanguages.size() > 1) {
					leftArray.add(languagecontainer);
					leftGrid.addRow(leftArray);
				}


				//Add dimensions
				if (isCube) {
					leftArray=new ArrayList<FComponent>();
					leftArray.add(aggSetDimContainer);
					leftGrid.addRow(leftArray);
				}
								
				//Add measures
				leftArray=new ArrayList<FComponent>();
				leftArray.add(measurescontainer);
				leftGrid.addRow(leftArray);
				
				if(isCube){
					if(selectedDimenisons.size()>0){
						// Show visual dimensions panel if there are 2 visual dimension
						if (visualDimensions.size() == 2) {
							leftArray=new ArrayList<FComponent>();
							leftArray.add(leftcontainer);
							leftGrid.addRow(leftArray);
						}
						
						// Show fixed dimensions panel if there are any
						if ((fixedDimensions.size() > 0)|| (!sliceFixedDimensionsValues.isEmpty())) {
							leftArray=new ArrayList<FComponent>();
							leftArray.add(rightcontainer);
							leftGrid.addRow(leftArray);
						}						
					}	
				}else if (isSlice){
					// Show visual dimensions panel if there are 2 visual dimension
					if (visualDimensions.size() == 2) {
						leftArray=new ArrayList<FComponent>();
						leftArray.add(leftcontainer);
						leftGrid.addRow(leftArray);
					}
					
					// Show fixed dimensions panel if there are any
					if ((fixedDimensions.size() > 0)|| (!sliceFixedDimensionsValues.isEmpty())) {
						leftArray=new ArrayList<FComponent>();
						leftArray.add(rightcontainer);
						leftGrid.addRow(leftArray);
					}
				}
				
				// Show the create slice button if there are fixed dimensions
				// i.e. a slice is not already visualized
				//SUPPORT IF ON ECUBE IS PRESENTED ... NO MULTIPLE CUBES FOR NOW
				if(selectedMeasures.size()>0 &&selectedDimenisons.size()>0){
					if (fixedDimensions.size() > 0 && isCube && cubeSliceURIs.size()==1) {
						FContainer bottomcontainer = new FContainer("bottomcontainer");
	
						// Bottom container styling
						bottomcontainer.addStyle("border-style", "solid");
						bottomcontainer.addStyle("border-width", "1px");
						bottomcontainer.addStyle("padding", "10px");
						bottomcontainer.addStyle("border-radius", "5px");
						bottomcontainer.addStyle("width", "990px ");
						bottomcontainer.addStyle("border-color", "#C8C8C8 ");
						bottomcontainer.addStyle("display", "table-cell ");
						bottomcontainer.addStyle("vertical-align", "middle ");
						bottomcontainer.addStyle("align", "center");
						bottomcontainer.addStyle("text-align", "left");
	
						FLabel bottomLabel = new FLabel(
								"bottomlabel",
								"<b>Slice</b></br>"
										+ "Store a two-dimensional slice of the cube as" +
										"  it is presented in the browser </br>");
	
						bottomcontainer.add(bottomLabel);
	
						// Button to create slice
						FButton createSlice = new FButton("Create Slice", "createSlice") {
	
							@Override
							public void onClick() {
								String sliceURI = SliceSPARQL.createCubeSlice(
										cubeSliceURIs.get(0), cubeGraphs.get(cubeSliceURIs.get(0)),
										fixedDimensionsSelectedValues,
										sliceObservations, SPARQL_service);
								String message = "A new slice with the following URI has been created: "
										+ sliceURI;
	
								FDialog.showMessage(this.getPage(),
										"New Slice created", message, "ok");
							}
						};
	
						bottomcontainer.add(createSlice);					
						leftArray=new ArrayList<FComponent>();
						leftArray.add(bottomcontainer);				
					}
				}				
			
				
				ArrayList<FComponent> midlemidleArray=new ArrayList<FComponent>();				
				
				if(isCube){
					if(selectedMeasures.size()>0 &&selectedDimenisons.size()>0 && correctNumOfLevels){
						ftable.addStyle("width", "700px ");
						midlemidleArray.add(ftable);
					}else{
						FContainer placeholderout=new FContainer("placeholderout");							
						FContainer placeholder=new FContainer("placeholder");						
						placeholder.addStyle("border-style", "dashed");
						placeholder.addStyle("border-width", "4px");
						placeholder.addStyle("padding", "10px");
						placeholder.addStyle("border-radius", "5px");
						placeholder.addStyle("border-color", "#C8C8C8");
						placeholder.addStyle("background-color", "#F1F1F1");			
						placeholder.addStyle("height", "300px ");
						placeholder.addStyle("display", "table-cell ");
						placeholder.addStyle("align", "center");
						placeholder.addStyle("width", "700px ");
						
						if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
							FLabel placeholderlabel = new FLabel(
								"placeholderlabel",	"<b>Select at least one " +
										"Dimension and one Measure to browse.</b>");
							placeholder.add(placeholderlabel);
							placeholderout.add(placeholder);
							midlemidleArray.add(placeholderout);
						}else if(selectedLanguage.equals("nl")){
							FLabel placeholderlabel = new FLabel(
									"placeholderlabel",	"<b>Selecteer minstens één dimensie en één meting.</b>");
							placeholder.add(placeholderlabel);
							placeholderout.add(placeholder);
							midlemidleArray.add(placeholderout);
						}else if(selectedLanguage.equals("fr")){
							FLabel placeholderlabel = new FLabel(
									"placeholderlabel",	"<b>Sélectionnez au moins une dimension et une mesure.</b>");
							placeholder.add(placeholderlabel);
							placeholderout.add(placeholder);
							midlemidleArray.add(placeholderout);
						} else if(selectedLanguage.equals("de")){
							FLabel placeholderlabel = new FLabel(
									"placeholderlabel",	"<b>Wählen Sie mindestens eine Dimension und eine Messung.</b>");
							placeholder.add(placeholderlabel);
							placeholderout.add(placeholder);
							midlemidleArray.add(placeholderout);
						}
						
					}
				}else if (isSlice){
					
					if(selectedMeasures.size()>0 ){
						ftable.addStyle("width", "700px ");
						midlemidleArray.add(ftable);
					}else{
						FContainer placeholderout=new FContainer("placeholderout");
											
						FContainer placeholder=new FContainer("placeholder");
						
						placeholder.addStyle("border-style", "dashed");
						placeholder.addStyle("border-width", "4px");
						placeholder.addStyle("padding", "10px");
						placeholder.addStyle("border-radius", "5px");
						placeholder.addStyle("border-color", "#C8C8C8");
						placeholder.addStyle("background-color", "#F1F1F1");			
						placeholder.addStyle("height", "300px ");
						placeholder.addStyle("display", "table-cell ");
						placeholder.addStyle("align", "center");
						placeholder.addStyle("width", "700px ");
						
						FLabel placeholderlabel = new FLabel(
								"placeholderlabel",	"<b>Selected at least one " +
										"Measure to browse.</b>");
								
						placeholder.add(placeholderlabel);
						placeholderout.add(placeholder);
						midlemidleArray.add(placeholderout);						
					}
									
				}					
				midlemidleGrid.addRow(midlemidleArray);	
						
							
				//show compatible cubes if any
				if(cubeSliceURIs.size()==1 && newcubeselected &&
						selectedMeasures.size()>0 && correctNumOfLevels &&
						selectedDimenisons.size()>0&&
						(dimensioncompatible.keySet().size()>0||
								 measurecompatible.keySet().size()>0)){
					
					
					FLabel comlatibleExist_label=new FLabel("comlatibleExist_label",
							"<b>&#10004; Expansions available.</b>");
					
					comlatibleExist_label.addStyle("background-color", "#90EE90");			
					comlatibleExist_label.addStyle("width", "700px ");
					comlatibleExist_label.addStyle("padding", "10px");
					comlatibleExist_label.addStyle("height", "20px ");
					comlatibleExist_label.addStyle("display", "table-cell ");
					comlatibleExist_label.addStyle("align", "center");
										
					midlemidleArray=new ArrayList<FComponent>();
					midlemidleArray.add(getNewLineComponent(false));
					midlemidleGrid.addRow(midlemidleArray);	
					
					midlemidleArray=new ArrayList<FComponent>();
					midlemidleArray.add(comlatibleExist_label);
					midlemidleGrid.addRow(midlemidleArray);	
									
					midlemidleArray=new ArrayList<FComponent>();
					midlemidleArray.add(compatibleCubeContainer);
					midlemidleGrid.addRow(midlemidleArray);						
				}	
				
				ArrayList<FComponent> midleArray=new ArrayList<FComponent>();
				midleArray.add(leftGrid);
				midleArray.add(midlemidleGrid);				
				
				midleGrid.addStyle("display", "table-cell ");
				midleGrid.addStyle("vertical-align", "top");
				midleGrid.addRow(midleArray);
				
				ArrayList<FComponent> overalArray=new ArrayList<FComponent>();
				overalArray=new ArrayList<FComponent>();
				overalArray.add(selectCubeContainer);
				overalGrid.addRow(overalArray);		
				
				overalArray=new ArrayList<FComponent>();
				overalArray.add(midleGrid);
						 
				overalGrid.addRow(overalArray);			
				
				cnt.add(overalGrid);				
				
				
			// //////// Not a valid cube or Slice URI /////////////
			} else {

				String message = "The URIs <b>";

				for (String uri : cubeSliceURIs) {
					uri = uri.replaceAll("<", "");
					uri = uri.replaceAll(">", "");
					message += uri + " , ";
				}

				message += "</b> are not valid cube or slice URIs.";
				FLabel invalidURI_label = new FLabel("invalidURI", message);
				cnt.add(invalidURI_label);

			}

			// If this is not the first load of the widget
			if (!isFirstLoad) {
				cnt.populateView();
			}

			long stopTime = System.currentTimeMillis();
			long elapsedTime = stopTime - startTime;
			System.out.println("Total browser Time: " + elapsedTime);

		// No cube has been yet selected
		} else {

			// top container styling
			selectCubeContainer.addStyle("border-style", "solid");
			selectCubeContainer.addStyle("border-width", "1px");
			selectCubeContainer.addStyle("padding", "10px");
			selectCubeContainer.addStyle("border-radius", "5px");
			selectCubeContainer.addStyle("width", "1050px ");
			selectCubeContainer.addStyle("border-color", "#C8C8C8 ");
			selectCubeContainer.addStyle("display", "table-cell ");
			selectCubeContainer.addStyle("vertical-align", "middle ");
			selectCubeContainer.addStyle("align", "center");
			selectCubeContainer.addStyle("text-align", "left");

			
			if(selectedLanguage.equals("en")||selectedLanguage.equals("")){
				FLabel allcubes_label = new FLabel("allcubes_label",
				"<b>Please select a cube to visualize:<b>");
				selectCubeContainer.add(allcubes_label);
			}else if(selectedLanguage.equals("nl")){
				FLabel allcubes_label = new FLabel("allcubes_label",
							"<b>Selecteer een kubus:<b>");
				selectCubeContainer.add(allcubes_label);
			}else if(selectedLanguage.equals("fr")){
				FLabel allcubes_label = new FLabel("allcubes_label",
						"<b>Veuillez sélectionner un cube:<b>");
				selectCubeContainer.add(allcubes_label);
			}else if(selectedLanguage.equals("de")){
				FLabel allcubes_label = new FLabel("allcubes_label",
						"<b>Bitte wählen Sie einen Würfel zu visualisieren:<b>");
				selectCubeContainer.add(allcubes_label);
			}			
	
			// Add Combo box with cube URIs
			FComboBox cubesCombo = new FComboBox("cubesCombo") {
				@Override
				public void onChange() {
					isFirstCubeLoad=true;
					newcubeselected=false;
					measurecompatible=new HashMap<LDResource, List<LDResource>>();
					dimensioncompatible=new HashMap<LDResource, HashMap<LDResource,List<LDResource>>>();
					
					cubeSliceURIs=new ArrayList<String>();
					cubeSliceURIs.add("<"+ ((LDResource) this.getSelected().get(0)).getURI()+ ">");
										
					selectedCubeFromDropDown=(LDResource) this.getSelected().get(0);
					originalCubeSliceURIs=cubeSliceURIs;
					cnt.removeAll();
					overalGrid.removeAll();
					leftGrid.removeAll();					
					leftcontainer.removeAll();
					rightcontainer.removeAll();
					dimensionURIfcomponents.clear();
					languagecontainer.removeAll();
					measurescontainer.removeAll();
					selectCubeContainer.removeAll();

					// Initialize everything for the new cube
					mapDimURIcheckBox = new HashMap<LDResource, FCheckBox>();
					selectedMeasures = new ArrayList<LDResource>();
					selectedDimenisons=new ArrayList<LDResource>();
					aggregationSetDims = new ArrayList<LDResource>();
					cubeDimsOfAggregationSet = new ArrayList<HashMap<LDResource, List<LDResource>>>();
					visualDimensions = new ArrayList<LDResource>();
					fixedDimensions = new ArrayList<LDResource>();
					sliceFixedDimensions = new ArrayList<LDResource>();
					fixedDimensionsSelectedValues = new HashMap<LDResource, LDResource>();
					sliceFixedDimensionsValues = new HashMap<LDResource, LDResource>();
					mapMeasureURIcheckBox = new HashMap<LDResource, FCheckBox>();
					selectedDimLevels=new HashMap<LDResource, List<LDResource>>();
									
					// show the cube
					populateCentralContainer();

				}
			};

			// populate cubes combo box
			for (LDResource cube : allCubes) {
				 if (cube.getURIorLabel() != null) {
						   cubesCombo.addChoice(cube.getURIorLabel(), cube);
				 }
			}			
			selectCubeContainer.add(cubesCombo);
			cnt.add(selectCubeContainer);
		}
	}

	// Set visual and fixed dimensions based on user selection of combo boxes
	private void setVisualDimensions(List<String> d1Selected,List<String> d2Selected) {
		// A tmp list to store the new dimensions for visualization
		List<LDResource> tmpvisualDimensions = new ArrayList<LDResource>();
		tmpvisualDimensions.add(null);
		tmpvisualDimensions.add(null);

		// The the visual dimensions from the combo boxes
		for (LDResource ldres : cubeDimensions) {
			// The first dimension
			if (ldres.getURI().equals(d1Selected.get(0))) {
				tmpvisualDimensions.set(0, ldres);
			}

			// The second dimension
			if (ldres.getURI().equals(d2Selected.get(0))) {
				tmpvisualDimensions.set(1, ldres);
			}
		}

		// Update the Global visual dimensions
		visualDimensions.clear();
		visualDimensions = tmpvisualDimensions;

		//Only if there are more than two dimension
		if(selectedDimenisons.size()>2){
			Collection<FComponent> allcomp = rightcontainer.getAllComponents();
			for (FComponent comp : allcomp) {
				if (comp.getId().contains("fixedDimGrid")) {
					rightcontainer.removeAndRefresh(comp);
				}
			}
	
			fixedDimGrid = new FGrid("fixedDimGrid");
	
			// Tmp Fixed dimensions
			List<LDResource> tmpFixedDimensions = CubeHandlingUtils
					.getFixedDimensions(cubeDimensions, visualDimensions);
	
			// Update Global fixed dimensions
			fixedDimensions.clear();
			fixedDimensions = tmpFixedDimensions;
	
			// If it is a slice and there are fixed dimension values
			if (!sliceFixedDimensionsValues.isEmpty()) {
	
				// An FGrid to show visual and fixed cube dimensions
				for (LDResource sliceFixedDim : sliceFixedDimensionsValues.keySet()) {
	
					ArrayList<FComponent> farray = new ArrayList<FComponent>();
					Random rnd = new Random();
					Long rndLong = Math.abs(rnd.nextLong());
					// Add the label for the fixed cube dimension
					FLabel fDimLabel = new FLabel(sliceFixedDim.getURIorLabel()
							+ "_" + rndLong + "_name", "<u>"
							+ sliceFixedDim.getURIorLabel() + ": </u>");
	
					farray.add(fDimLabel);
	
					// rightcontainer.add(fDimLabel);
	
					LDResource fDimValue = sliceFixedDimensionsValues.get(sliceFixedDim);
					FLabel fDimValueLabel = new FLabel(fDimValue.getURIorLabel()
							+ "_" + rndLong + "_value", fDimValue.getURIorLabel());
	
					farray.add(fDimValueLabel);
					fixedDimGrid.addRow(farray);
				}
			}
	
			// if there are fixed dimensions
			if (fixedDimensions.size() > 0) {
	
				// Tmp Selected values for the fixed dimensions
				HashMap<LDResource, LDResource> tmpFixedDimensionsSelectedValues = CubeHandlingUtils
						.getFixedDimensionsRandomSelectedValues(
								allDimensionsValues, fixedDimensions,
								fixedDimensionsSelectedValues);
	
				// Update global selected values
				fixedDimensionsSelectedValues.clear();
				fixedDimensionsSelectedValues = tmpFixedDimensionsSelectedValues;
	
				// Add labels and combo boxed for the fixed cube dimensions
				addFixedDimensions();
			}
			rightcontainer.addAndRefresh(fixedDimGrid);
		}
	}

	// Add labels and combo boxed for the fixed cube dimensions
	// Input: refresh, true: when refresh is needed i.e. not at the
	// initialization, false when refresh is not need i.e. at the initialization
	private void addFixedDimensions() {

		dimensionURIfcomponents.clear();
		int fixedDims = 1;
		// Add a Label - Combo box for each fixed cube dimension
		for (LDResource fDim : fixedDimensions) {

			ArrayList<FComponent> farray = new ArrayList<FComponent>();

			List<FComponent> dimComponents = new ArrayList<FComponent>();

			// Add the label for the fixed cube dimension
			FLabel fDimLabel = new FLabel("fixedDimLabel_" + fixedDims, fDim.getURIorLabel() + ":");
			dimComponents.add(fDimLabel);

			farray.add(fDimLabel);

			// Add the combo box for the fixed cube dimension
			FComboBox fDimCombo = new FComboBox("fixedDimCombo_" + fixedDims) {
				@Override
				public void onChange() {
					if((isCube&&selectedMeasures.size()>0 &&selectedDimenisons.size()>0)||
							(isSlice&&selectedMeasures.size()>0)){
						showCube();
					}
				}
			};
			
			// Populate the combo box with the values of the fixed cube dimension			
			List<LDResource> dimLevels=allCubesDimensionsLevels.get(fDim);
			
			//The dimension has levels
			//Show the values of the selected levels
			if(dimLevels!=null && dimLevels.size()>0){
				List<LDResource> thisDimSelectedLevels=	selectedDimLevels.get(fDim);
				if(thisDimSelectedLevels!=null){
					for(LDResource level:thisDimSelectedLevels){
						List<LDResource> levelValues=allLevelsValues.get(level);
						for(LDResource levelVal:levelValues){
							// Show the first 60 chars if label too long
							if (levelVal.getURIorLabel().length() > 35) {
								fDimCombo.addChoice(levelVal.getURIorLabel().substring(0, 35)
									+ "...", levelVal.getURI());
							} else {
								fDimCombo.addChoice(levelVal.getURIorLabel(), levelVal.getURI());
							}
						}
					}
				}				
			}else{
				for (LDResource ldr : allDimensionsValues.get(fDim)) {
					// Show the first 60 chars if label too long
					if (ldr.getURIorLabel().length() > 35) {
						fDimCombo.addChoice(ldr.getURIorLabel().substring(0, 35)
							+ "...", ldr.getURI());
					} else {
						fDimCombo.addChoice(ldr.getURIorLabel(), ldr.getURI());
					}
				}	
			}
			

			if (fixedDimensionsSelectedValues.get(fDim) != null) {
				// Combo box pre-selected value
				fDimCombo.setPreSelected(fixedDimensionsSelectedValues.get(fDim).getURI());
			}

			dimComponents.add(fDimCombo);

			farray.add(fDimCombo);

			fixedDimGrid.addRow(farray);
			dimensionURIfcomponents.put(fDim, dimComponents);
			fixedDims++;
		}
	}

	// Show the data cube
	private void showCube() {

		fixedDimensionsSelectedValues.clear();

		// Get the selected value for each fixed dimension
		for (LDResource dimres : dimensionURIfcomponents.keySet()) {
			List<FComponent> dimComponents = dimensionURIfcomponents.get(dimres);
			String selectedValue = ((FComboBox) dimComponents.get(1))
					.getSelectedAsString().get(0);
			List<LDResource> selectedDimValues = allDimensionsValues.get(dimres);
			for (LDResource dimValue : selectedDimValues) {
				if (dimValue.getURI().equals(selectedValue)) {
					fixedDimensionsSelectedValues.put(dimres, dimValue);
				}
			}
		}		

		List<BindingSet> allResults = new ArrayList<BindingSet>();

		for (String tmpCubeSliceURI : cubeSliceURIs) {
			TupleQueryResult res = null;
			if (isCube) {
				
				List<LDResource> tmpMeasures=new ArrayList<LDResource>(
						measuresPerCube.get(tmpCubeSliceURI));
				tmpMeasures.retainAll(selectedMeasures);
				// Get query tuples for visualization
				res = CubeBrowserSPARQL.get2DVisualsiationValuesAndLevels(visualDimensions,
										fixedDimensionsSelectedValues, tmpMeasures,
										allDimensionsValues,selectedDimLevels,allCubesDimensionsLevels,
										tmpCubeSliceURI,cubeGraphs.get(tmpCubeSliceURI), 
										cubeDSDGraphs.get(tmpCubeSliceURI),
										SPARQL_service);					
			} else if (isSlice) {
				
				//Need to do sth with measures as above
				res = CubeBrowserSPARQL.get2DVisualsiationValuesFromSlice(
						visualDimensions, fixedDimensionsSelectedValues,
						selectedMeasures, allDimensionsValues, tmpCubeSliceURI,
						sliceGraphs.get(tmpCubeSliceURI), cubeGraphs.get(tmpCubeSliceURI),
						SPARQL_service);
			}
						
			
			try {
				while (res.hasNext()) {
					allResults.add(res.next());
				}
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}
		}		
		
		FTableModel newTableModel = create2DCubeTableModelWithLevels(allResults,
							allDimensionsValues, visualDimensions);
		ftable.setModel(newTableModel);
		ftable.populateView();

	}

	// Create a table model from the Tuple query result in order to visualize
	private FTableModel create2DCubeTableModel(
			List<BindingSet> res,
			HashMap<LDResource, List<LDResource>> dimensions4VisualisationValues,
			List<LDResource> visualDimensions) {

		// Get all values for 1st visual dimension
		List<LDResource> dim1 = dimensions4VisualisationValues.get(visualDimensions.get(0));

		List<LDResource> dim2 = null;

		// If there are 2 visual dimensions
		// Get all values for 2nd visual dimension
		if (visualDimensions.size() == 2) {

			dim2 = dimensions4VisualisationValues.get(visualDimensions.get(1));
		}

		LDResource[][] v2DCube = null;

		// Set the table dimensions
		// If there are 2 visual dimensions
		if (visualDimensions.size() == 2) {
			v2DCube = new LDResource[dim2.size()][dim1.size()];

		// One visual dimension
		} else {
			v2DCube = new LDResource[dim1.size()][1];
		}

		FTableModel tm = new FTableModel();

		// Add all visual observations to a list - to create a slice later on
		sliceObservations.clear();

		// If there are 2 visual dimensions
		if (visualDimensions.size() == 2) {

			// The first column of 1st row - the name of the 2nd dimension
			if(visualDimensions.get(1).getURIorLabel().length()>30){				
				tm.addColumn(visualDimensions.get(1).getURIorLabel().substring(0,30)+"...");
			}else{
				tm.addColumn(visualDimensions.get(1).getURIorLabel());				
			}
						
			// The rest columns of the first row - the values of the 1st dimension
			for (LDResource dim1Val : dim1) {
				tm.addColumn(dim1Val.getURIorLabel());				
			}

		// If there is 1 visual dimensions
		} else {
			// The first column of 1st row - the name of the 2nd dimension
			if(visualDimensions.get(0).getURIorLabel().length()>30){
				tm.addColumn(visualDimensions.get(0).getURIorLabel().substring(0,30)+"...");
			}else{
				tm.addColumn(visualDimensions.get(0).getURIorLabel());
			}
			tm.addColumn("Measure");
		}
		
		// for each cube observation
		int k = 1;
		for (BindingSet bindingSet : res) {

			// get Dim1 value
			String dim1Val = bindingSet.getValue("dim1").stringValue();
			LDResource r1 = new LDResource(dim1Val);

			String measure = "";
			// get measures
			for (LDResource meas : selectedMeasures) {
				if(bindingSet.getValue("measure_" + meas.getLastPartOfURI())!=null){
					measure += "<font color=\""
							+ measureColors[cubeMeasures.indexOf(meas)] + "\">"
							+ "<p align=\"right\">";
					
					String measurevalue=bindingSet.getValue("measure_" + meas.getLastPartOfURI()).stringValue();
					//Show 1 decimal if decimals exist
					if(measurevalue.contains(".")){
						if((measurevalue.length()-measurevalue.indexOf("."))>=3){
							measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
						}
					}		
					
					measure	+= measurevalue+" </p></font> ";				
				}
				k++;				
			}

			// get observation URI
			String obsURI = bindingSet.getValue("obs").stringValue();
			LDResource obs =new LDResource(obsURI);
			obs.setLabel(measure);
			
			//An observation returned may not contain any measure
			//e.g. there are two cubes and only the measures of the 1st is selected for
			//visualization... then an observation may be returned but not contain any
			// measure to visualize
			if(!measure.equals("")){
				// If there are 2 visual dimensions
				if (visualDimensions.size() == 2) {
					// get Dim2 value
					String dim2Val = bindingSet.getValue("dim2").stringValue();
					LDResource r2 = new LDResource(dim2Val);
	
					// Add the observation to the corresponding (row, column) position of the table
					LDResource currentObs=v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)];
					if(currentObs!=null){
						if(obs.getLabel().contains("color")){
							String color1=obs.getLabel().substring(
									obs.getLabel().indexOf("color"),obs.getLabel().indexOf("color")+9);
							if(!currentObs.getLabel().contains(color1)){
								obs.setLabel(currentObs.getLabel()+obs.getLabel());
							}
						}
					}
					v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)] = obs;
				} else {
					LDResource currentObs=v2DCube[dim1.indexOf(r1)][0];
					if(currentObs!=null){
						if(obs.getLabel().contains("color")){
							String color1=obs.getLabel().substring(
									obs.getLabel().indexOf("color"),obs.getLabel().indexOf("color")+9);
							if(!currentObs.getLabel().contains(color1)){
								obs.setLabel(currentObs.getLabel()+obs.getLabel());
							}
						}
					}
					v2DCube[dim1.indexOf(r1)][0] = obs;
				}
			
				// add observation to potential slice
				sliceObservations.add(obs);
			}
		}

		// If there are 2 visual dimensions
		if (visualDimensions.size() == 2) {
			// populate the FTableModel based on the v2DCube created
			
			List<LDResource> dimlLevels=allCubesDimensionsLevels.get(visualDimensions.get(1));
			
			for (int i = 0; i < dim2.size(); i++) {				
				Object[] data = new Object[dim1.size() + 1];
				
				
				// Add row header (values in first column)
				data[0] = getFHTMLFromLDResource(dim2.get(i),"HTMLrow_"+i);
						
				// Add observations
				for (int j = 1; j <= dim1.size(); j++) {
					data[j] = getFHTMLFromLDResource(v2DCube[i][j - 1],"HTMLobs_"+i+"_"+j);
				}
				tm.addRow(data);	
				
				
			}

		// If there is 1 visual dimensions
		} else {
			
			//Get the dimension levels
			List<LDResource> dimlLevels=allCubesDimensionsLevels.get(visualDimensions.get(0));
			
			// Add observations
			for (int j = 0; j < dim1.size(); j++) {
				Object[] data = new Object[2];
				
				// Add row header (values in first column)
				data[0] = getFHTMLFromLDResource(dim1.get(j),"HTMLrow_"+j);
				data[1] = getFHTMLFromLDResource(v2DCube[j][0],"HTMLobs_"+j);		
				tm.addRow(data);	
				
				
			}
		}		
				
		return tm;
	}

	
	
	// Create a table model from the Tuple query result in order to visualize
	private FTableModel create2DCubeTableModelWithLevels(
				List<BindingSet> res,
				HashMap<LDResource, List<LDResource>> dimensions4VisualisationValues,
				List<LDResource> visualDimensions) {

			// Get all values for 1st visual dimension
			List<LDResource> dim1 = null;
			List<LDResource> dim2 = null;

			DrillDownObservation[][] v2DCube = null;

			//Ordered selected levels. First Upper level, second other level
			List<LDResource> dim1SelectedLevels=new ArrayList<LDResource>();
			List<LDResource> dim2SelectedLevels=new ArrayList<LDResource>();
			
			// Set the table dimensions
			// If there are 2 visual dimensions
			if (visualDimensions.size() == 2) {
				List<LDResource> dim1DimensionLevels=allCubesDimensionsLevels.get(visualDimensions.get(0));
				List<LDResource> dim2DimensionLevels=allCubesDimensionsLevels.get(visualDimensions.get(1));
				
				//Get Dim1 size
				if(dim1DimensionLevels!=null&&dim1DimensionLevels.size()>0){
					
					if(selectedDimLevels.get(visualDimensions.get(0))!=null){
						dim1SelectedLevels=new ArrayList<LDResource>(selectedDimLevels.get(visualDimensions.get(0))) ;
					}else{
						dim1SelectedLevels=new ArrayList<LDResource>() ;
					}
				
					//There is at least one selected level
					//The size of the v2DCube is the size of the upper level values size
					if(dim1SelectedLevels.size()>0){
						if(dim1SelectedLevels.size()==1){
							dim1=allLevelsValues.get(dim1SelectedLevels.get(0));
						}else{
							//First selected level broader than the other 
							if(dim1DimensionLevels.indexOf(dim1SelectedLevels.get(0))<
							dim1DimensionLevels.indexOf(dim1SelectedLevels.get(1))){
								dim1=allLevelsValues.get(dim1SelectedLevels.get(0));
							}else{
								dim1=allLevelsValues.get(dim1SelectedLevels.get(1));
								
								//Switch position
								LDResource tmpldr=dim1SelectedLevels.get(0);
								dim1SelectedLevels.add(0, dim1SelectedLevels.get(1));
								dim1SelectedLevels.add(1, tmpldr);
								
							}
						}
					}else{
						dim1 = dimensions4VisualisationValues.get(visualDimensions.get(0));				
					}
				//Assume there are only 2 selected levels
				}else{
					dim1 = dimensions4VisualisationValues.get(visualDimensions.get(0));
				}
								
				//Get Dim2 size
				if(dim2DimensionLevels!=null&&dim2DimensionLevels.size()>0){
					
					if(selectedDimLevels.get(visualDimensions.get(1))!=null){
						dim2SelectedLevels=new ArrayList<LDResource>(selectedDimLevels.get(visualDimensions.get(1))) ;
					}else{
						dim2SelectedLevels=new ArrayList<LDResource>() ;
					}					
				
					//There is at least one selected level
					if(dim2SelectedLevels.size()>0){
						if(dim2SelectedLevels.size()==1){
							dim2=allLevelsValues.get(dim2SelectedLevels.get(0));
						}else{						
							//First selected level broader than the other 
							if(dim2DimensionLevels.indexOf(dim2SelectedLevels.get(0))<
							dim2DimensionLevels.indexOf(dim2SelectedLevels.get(1))){
								dim2=allLevelsValues.get(dim2SelectedLevels.get(0));
							}else{
								dim2=allLevelsValues.get(dim2SelectedLevels.get(1));
								
								//Switch position
								LDResource tmpldr=dim2SelectedLevels.get(0);
								dim2SelectedLevels.add(0, dim2SelectedLevels.get(1));
								dim2SelectedLevels.add(1, tmpldr);
							}
						}
					}else{
						dim2 = dimensions4VisualisationValues.get(visualDimensions.get(1));						
					}
					
				//Assume there are only 2 selected levels
				}else{
					dim2 = dimensions4VisualisationValues.get(visualDimensions.get(1));
				}		
				
				v2DCube = new DrillDownObservation[dim2.size()][dim1.size()];

			// One visual dimension
			} else {
				
				List<LDResource> dim1DimensionLevels=allCubesDimensionsLevels.get(visualDimensions.get(0));
				
				//Get Dim1 size
				if(dim1DimensionLevels!=null&&dim1DimensionLevels.size()>0){
					if(selectedDimLevels.get(visualDimensions.get(0))!=null){
						dim1SelectedLevels=new ArrayList<LDResource>(selectedDimLevels.get(visualDimensions.get(0))) ;
					}else{
						dim1SelectedLevels=new ArrayList<LDResource>() ;
					}
					
					//There is at least one selected level
					if(dim1SelectedLevels.size()>0){
						if(dim1SelectedLevels.size()==1){
							dim1=allLevelsValues.get(dim1SelectedLevels.get(0));
						}else{	
							//First selected level broader than the other 
							if(dim1DimensionLevels.indexOf(dim1SelectedLevels.get(0))<
							dim1DimensionLevels.indexOf(dim1SelectedLevels.get(1))){
								dim1=allLevelsValues.get(dim1SelectedLevels.get(0));
							}else{
								dim1=allLevelsValues.get(dim1SelectedLevels.get(1));
								
								//Switch position
								LDResource tmpldr=dim1SelectedLevels.get(0);
								dim1SelectedLevels.add(0, dim1SelectedLevels.get(1));
								dim1SelectedLevels.add(1, tmpldr);
							}
						}
					}else{
						dim1 = dimensions4VisualisationValues.get(visualDimensions.get(0));											
					}
					//Assume there are only 2 selected levels
				}else{
					dim1 = dimensions4VisualisationValues.get(visualDimensions.get(0));
				}								
				v2DCube = new DrillDownObservation[dim1.size()][1];
			}

			FTableModel tm = new FTableModel();

			// Add all visual observations to a list - to create a slice later on
			sliceObservations.clear();

			// If there are 2 visual dimensions
			if (visualDimensions.size() == 2) {

				// The first column of 1st row - the name of the 2nd dimension
				if(visualDimensions.get(1).getURIorLabel().length()>30){				
					tm.addColumn(visualDimensions.get(1).getURIorLabel().substring(0,30)+"...");
				}else{
					String label=visualDimensions.get(1).getURIorLabel();
					for(int i=0;i<(60-label.length());i++){
						label+=" ";
					}
					tm.addColumn(label);				
				}
							
				// The rest columns of the first row - the values of the 1st dimension
				for (LDResource dim1Val : dim1) {
					tm.addColumn(dim1Val.getURIorLabel());				
				}

			// If there is 1 visual dimensions
			} else {
				// The first column of 1st row - the name of the 2nd dimension
				if(visualDimensions.get(0).getURIorLabel().length()>30){
					tm.addColumn(visualDimensions.get(0).getURIorLabel().substring(0,30)+"...");
				}else{
					tm.addColumn(visualDimensions.get(0).getURIorLabel());
				}
				
				String measures=" ";
				tm.addColumn(measures);
			}			
			
			// for each cube observation
			int k = 1;
			for (BindingSet bindingSet : res) {

				DrillDownObservation dobs=new DrillDownObservation();
				
				// get Dim1 value
				String dim1Val = bindingSet.getValue("dim1").stringValue();
				LDResource r1 = new LDResource(dim1Val);
				
				// get Dim1 level
				String dim1Level = null;
				if(bindingSet.getValue("level1")!=null){
					dim1Level=bindingSet.getValue("level1").stringValue();
				}
								
				// get Dim1 parent
				String dim1parent = null;
				if(bindingSet.getValue("parent1")!=null){
					dim1parent=bindingSet.getValue("parent1").stringValue();
				}
					
				Map<LDResource, String> obsMeasureValueMap=new HashMap<LDResource, String>(); 
			
				// get measures
				for (LDResource meas : selectedMeasures) {
					if(bindingSet.getValue("measure_" +
				meas.getLastPartOfURI().replaceAll("-", "_"))!=null){
						obsMeasureValueMap.put(meas,
								bindingSet.getValue(
										"measure_" + meas.getLastPartOfURI().replaceAll("-", "_")).stringValue());
					}					
				}
				
				if (visualDimensions.size() == 2) {

					// get Dim2 value
					String dim2Val = bindingSet.getValue("dim2").stringValue();
					LDResource r2 = new LDResource(dim2Val);
					
					// get Dim2 level
					String dim2Level = null;
					if(bindingSet.getValue("level2")!=null){
						dim2Level=bindingSet.getValue("level2").stringValue();
					}
																			
					// get Dim2 parent
					String dim2parent = null;
					if(bindingSet.getValue("parent2")!=null){
						dim2parent=bindingSet.getValue("parent2").stringValue();
					}	
										
					//ASSUME ONLY 1 DIM HAS LEVELS		
					//OR ONLY ONE DIM HAS MORE THAN ONE LEVEL SELECTED
										
					//DIM1 HAS LEVELS and more than 1 level is selected
					//OR DIM1 HAS LEVELS and more than 1 level is selected AND DIM2 has levels and at most 
					//1 level is selected
					if((dim1Level!=null && dim2Level==null && dim1SelectedLevels.size()>1)||
							(dim1Level!=null && dim2Level!=null && dim1SelectedLevels.size()>1 &&dim2SelectedLevels.size()<=1)){
						
						//Check if level is the upper or down level
						LDResource dim1level_ldr=new LDResource(dim1Level);
						LDResource dim2level_ldr=new LDResource(dim2Level);
						
						//Dim1 1 is the upper level
						//AND Dim2 has no levels OR dim2 has the one selected level
						if(dim1level_ldr.equals(dim1SelectedLevels.get(0))&&
								(dim2Level==null ||(dim2Level!=null&&dim2level_ldr.equals(dim2SelectedLevels.get(0))))){
							
							if(dim2.indexOf(r2)!=-1&&dim1.indexOf(r1)!=-1){
								
								dobs=v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)];
								if(dobs==null){
									dobs=new DrillDownObservation();
								}
								dobs.Level1measureValueMap.putAll(obsMeasureValueMap);
								
								v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)]=dobs;
							}
						
						//Dim 1 is the down level
						//AND Dim2 has no levels OR dim2 has the one selected level
						}else if(dim1level_ldr.equals(dim1SelectedLevels.get(1))
								&&dim1parent!=null &&
								(dim2Level==null ||(dim2Level!=null&&dim2level_ldr.equals(dim2SelectedLevels.get(0))))){
							
							
								LDResource p1=new LDResource(dim1parent);
							if(dim2.indexOf(r2)!=-1 && dim1.indexOf(p1)!=-1){
								dobs=v2DCube[dim2.indexOf(r2)][dim1.indexOf(p1)];
								if(dobs==null){
									dobs=new DrillDownObservation();
								}
								
								Map<LDResource,String> level2Measures=dobs.Level2MeasureDimensionValueMap.get(r1);
								if(level2Measures==null){
									level2Measures=new HashMap<LDResource, String>();
								}
								level2Measures.putAll(obsMeasureValueMap);
								dobs.Level2MeasureDimensionValueMap.put(r1, level2Measures);
								v2DCube[dim2.indexOf(r2)][dim1.indexOf(p1)]=dobs;
							}
						}									
										
					//DIM2 HAS LEVELS and more than one level is selected
					}else if((dim2Level!=null &&dim1Level==null && dim2SelectedLevels.size()>1)||
							(dim2Level!=null && dim1Level!=null && dim2SelectedLevels.size()>1 &&dim1SelectedLevels.size()<=1)){
						
						//Check if level is the upper or down level
						LDResource dim2level_ldr=new LDResource(dim2Level);
						LDResource dim1level_ldr=new LDResource(dim1Level);
						
						//Dim1 1 is the upper level
						if(dim2level_ldr.equals(dim2SelectedLevels.get(0))&&
								(dim1Level==null ||(dim1Level!=null&&dim1level_ldr.equals(dim1SelectedLevels.get(0))))){
							
							if(dim2.indexOf(r2)!=-1 && dim1.indexOf(r1)!=-1){
								dobs=v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)];
								if(dobs==null){
									dobs=new DrillDownObservation();
								}
								dobs.Level1measureValueMap.putAll(obsMeasureValueMap);							
								v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)]=dobs;	
							}
						
						//Dim 1 is the down level
						}else if(dim2level_ldr.equals(dim2SelectedLevels.get(1))
								&& dim2parent!=null &&
								(dim1Level==null ||(dim1Level!=null&&dim1level_ldr.equals(dim1SelectedLevels.get(0))))){
							LDResource p2=new LDResource(dim2parent);
							
							if(dim2.indexOf(p2)!=-1 && dim1.indexOf(r1)!=-1){
								dobs=v2DCube[dim2.indexOf(p2)][dim1.indexOf(r1)];
								if(dobs==null){
									dobs=new DrillDownObservation();
								}
								
								Map<LDResource,String> level2Measures=dobs.Level2MeasureDimensionValueMap.get(r2);/////////////
								if(level2Measures==null){
									level2Measures=new HashMap<LDResource, String>();
								}
								level2Measures.putAll(obsMeasureValueMap);
								dobs.Level2MeasureDimensionValueMap.put(r2, level2Measures);
								v2DCube[dim2.indexOf(p2)][dim1.indexOf(r1)]=dobs;	
							}
						}													
						
					//NO DIM HAS LEVELS	or ONLY ONE LEVELS HAS BEEN SELECTED
					}else {
						
						//Only dim1 has levels
						if(dim1Level!=null &&dim2Level==null){
							LDResource dim1level_ldr=new LDResource(dim1Level);
							//Dim1 is the 1 selected level
							if(dim1level_ldr.equals(dim1SelectedLevels.get(0))){
								if(dim2.indexOf(r2)!=-1 && dim1.indexOf(r1)!=-1){
									dobs=v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)];
									if(dobs==null){
										dobs=new DrillDownObservation();
									}
									dobs.Level1measureValueMap.putAll(obsMeasureValueMap);
									
									v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)]=dobs;
								}
							}
						
						//Only dim2 has levels
						}else if(dim2Level!=null &&dim1Level==null){
							LDResource dim2level_ldr=new LDResource(dim2Level);
							//Dim2 is the 1 selected level
							if(dim2level_ldr.equals(dim2SelectedLevels.get(0))){
								if(dim2.indexOf(r2)!=-1 && dim1.indexOf(r1)!=-1){
									dobs=v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)];
									if(dobs==null){
										dobs=new DrillDownObservation();
									}
									dobs.Level1measureValueMap.putAll(obsMeasureValueMap);
									
									v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)]=dobs;
								}
							}
						}else if(dim2Level!=null &&dim1Level!=null){
							
							LDResource dim1level_ldr=new LDResource(dim1Level);
							LDResource dim2level_ldr=new LDResource(dim2Level);
							
							//Dim1 is the 1 selected level AND
							//Dim2 is the 1 selected level
							if(dim2level_ldr.equals(dim2SelectedLevels.get(0))&&
									dim1level_ldr.equals(dim1SelectedLevels.get(0))){
								
								if(dim2.indexOf(r2)!=-1 && dim1.indexOf(r1)!=-1){
									dobs=v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)];
									if(dobs==null){
										dobs=new DrillDownObservation();
									}
									dobs.Level1measureValueMap.putAll(obsMeasureValueMap);
									
									v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)]=dobs;
								}
							}
						
						}else{
							if(dim2.indexOf(r2)!=-1 && dim1.indexOf(r1)!=-1){
								dobs=v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)];
								if(dobs==null){
									dobs=new DrillDownObservation();
								}
								dobs.Level1measureValueMap.putAll(obsMeasureValueMap);
								
								v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)]=dobs;
							}
						}						
					}
				//Visual dimension =1 	
				}else{
					
					//DIM1 HAS LEVELS and more than 1 level is selected
					if(dim1Level!=null && dim1SelectedLevels.size()>1){
						
						LDResource dim1level_ldr=new LDResource(dim1Level);
					
						//Dim1 1 is the upper level
						if(dim1level_ldr.equals(dim1SelectedLevels.get(0))){
							if(dim1.indexOf(r1)!=-1){
								dobs=v2DCube[dim1.indexOf(r1)][0];
								if(dobs==null){
									dobs=new DrillDownObservation();
								}
								dobs.Level1measureValueMap.putAll(obsMeasureValueMap);
								
								v2DCube[dim1.indexOf(r1)][0]=dobs;
							}
					
						//Dim 1 is the down level
						}else if(dim1level_ldr.equals(dim1SelectedLevels.get(1))){
							LDResource p1=new LDResource(dim1parent);
							if(p1!=null && dim1.indexOf(p1)!=-1){							
								dobs=v2DCube[dim1.indexOf(p1)][0];
								if(dobs==null){
									dobs=new DrillDownObservation();
								}
								
								Map<LDResource,String> level2Measures=dobs.Level2MeasureDimensionValueMap.get(r1);
								if(level2Measures==null){
									level2Measures=new HashMap<LDResource, String>();
								}
								level2Measures.putAll(obsMeasureValueMap);
								dobs.Level2MeasureDimensionValueMap.put(r1, level2Measures);
								v2DCube[dim1.indexOf(p1)][0]=dobs;
							}
						}	

					//DIM1 has no levels or ONLY ONE LEVELS HAS BEEN SELECTED
					}else{								
						if(dim1Level!=null){
							LDResource dim1level_ldr=new LDResource(dim1Level);
							
							//Dim1 1 is the 1 selected level
							if(dim1level_ldr.equals(dim1SelectedLevels.get(0))){
								if(dim1.indexOf(r1)!=-1){
									dobs=v2DCube[dim1.indexOf(r1)][0];
									if(dobs==null){
										dobs=new DrillDownObservation();
									}
									dobs.Level1measureValueMap.putAll(obsMeasureValueMap);
									
									v2DCube[dim1.indexOf(r1)][0]=dobs;
								}
							}
						}else{
							if(dim1.indexOf(r1)!=-1){							
								dobs=v2DCube[dim1.indexOf(r1)][0];
								if(dobs==null){
									dobs=new DrillDownObservation();
								}
								dobs.Level1measureValueMap.putAll(obsMeasureValueMap);
								
								v2DCube[dim1.indexOf(r1)][0]=dobs;
							}
						}							
					}
				}
			}
							
			// If there are 2 visual dimensions
			if (visualDimensions.size() == 2) {
				// populate the FTableModel based on the v2DCube created
							
				List<LDResource> dim1Levels=selectedDimLevels.get(visualDimensions.get(0));
				List<LDResource> dim2Levels=selectedDimLevels.get(visualDimensions.get(1));
				
				for (int i = 0; i < dim2.size(); i++) {				
					Object[] data = new Object[dim1.size() + 1];
					
					//The row dimension has levels
					if(dim2Levels!=null&&dim2Levels.size()>1){
						
						ArrayList<String> showIDsToAdd=new ArrayList<String>();
						
						//Use the calculated dim values to get the LDResource with the label
						List<LDResource> rowDimValues=allDimensionsValues.get(visualDimensions.get(1));
						
						Set<LDResource> allLevel2RowDimValues=new HashSet<LDResource>();
						//get all level2 row dimension values
						for (int j = 1; j <= dim1.size(); j++) {
							DrillDownObservation dobs=v2DCube[i][j - 1];
							if(dobs!=null){
								allLevel2RowDimValues.addAll(dobs.Level2MeasureDimensionValueMap.keySet());
							}
						}
						
						List<LDResource> sortedLevel2RowDimValues=new ArrayList<LDResource>(allLevel2RowDimValues);
						Collections.sort(sortedLevel2RowDimValues);
										
						// Add observations
						for (int j = 1; j <= dim1.size(); j++) {
							DrillDownObservation dobs=v2DCube[i][j - 1];
							String measure = "<span id='show_id"+k+"' class=\"link\">" 							
											+ "<p align=\"right\">"
											+"<table align=\"right\">" +
											"<tr>";
							
							if(dobs!=null&&dobs.Level1measureValueMap!=null){
								for(LDResource ldr_measure:selectedMeasures){
									
									String measurevalue=dobs.Level1measureValueMap.get(ldr_measure);
									
									if(measurevalue!=null){
										//Show 1 decimal if decimals exist
										if(measurevalue.contains(".")){
											if((measurevalue.length()-measurevalue.indexOf("."))>=3){
												measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
											}
										}	
									}else{
										measurevalue="-";
									}
															
									measure+="<td align=\"right\"><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
											 measurevalue+" "+"</font></td>";	
								}
								
							}else{
								for(LDResource ldr_measure:selectedMeasures){																								
									measure+="<td align=\"right\"><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
											 "- "+"</font></td>";	
								}	
							}							
							
							measure+=" </tr></table></p> </span>";							
							measure+="<span id='spoiler_row_id"+k+"' style=\"display: none \">" 	
									+ "<p align=\"right\">";
							measure+="<table align=\"right\">" +	"<tr>";
							
							if(dobs!=null&&dobs.Level1measureValueMap!=null){
								for(LDResource ldr_measure:selectedMeasures){									
									String measurevalue=dobs.Level1measureValueMap.get(ldr_measure);									
									if(measurevalue!=null){
										//Show 1 decimal if decimals exist
										if(measurevalue.contains(".")){
											if((measurevalue.length()-measurevalue.indexOf("."))>=3){
												measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
											}
										}		
									}else{
										measurevalue="-";
									}									
									measure+="<td align=\"right\"><b><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
											 measurevalue+" "+"</font></b></td>";										
								}
							}else{
								
								for(LDResource ldr_measure:selectedMeasures){									
									
									measure+="<td align=\"right\"><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
											 "- "+"</font></td>";	
								}									
							}
							
							measure+="</tr>";							
							
							for(LDResource ldr_dim_value:sortedLevel2RowDimValues){
								if(dobs!=null&&dobs.Level2MeasureDimensionValueMap!=null&&
										dobs.Level2MeasureDimensionValueMap.get(ldr_dim_value)!=null){
									Map<LDResource,String> measures4dimVal=dobs.Level2MeasureDimensionValueMap.get(ldr_dim_value);
								
									measure+="<tr>";
									for(LDResource ldr_measure:selectedMeasures){
										if(measures4dimVal!=null&&measures4dimVal.get(ldr_measure)!=null){	
											measure+="<td align=\"right\">"+
													"<font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">";
											
											String measurevalue=measures4dimVal.get(ldr_measure);
											//Show 1 decimal if decimals exist
											if(measurevalue.contains(".")){
												if((measurevalue.length()-measurevalue.indexOf("."))>=3){
													measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
												}
											}												
											measure+=measurevalue+"</font></td>";
										}else{
											measure+="<td align=\"right\">"+
													"<font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"
													+"-</font></td>";
										}									
									}									
								}else{
									for(LDResource ldr_measure:selectedMeasures){									
										
										measure+="<td align=\"right\"><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
												 "- "+"</font></td>";	
									}	
								}
								measure+="</tr>";
							}
							measure+="</table></p></span>";
							k++;
							
							FHTML html_string = new FHTML("HTMLobs_"+i+"_"+j,measure);
							data[j]=html_string;														
							showIDsToAdd.addAll(getShowIdValues(measure));
						}										
						
						String valueToadd="<a id='show"+i+"' onclick=\"" +
								"document.getElementById('spoiler"+i+"').style.display=''; " +
								"document.getElementById('show"+i+"').style.display='none';";
						
						for(String num:showIDsToAdd){
							valueToadd+="document.getElementById('spoiler_row_id"+num+"').style.display=''; " +
								"document.getElementById('show_id"+num+"').style.display='none';";
						}
						
						valueToadd+="\" class=\"link\"> [+]</a>" +
								"<span id='spoiler"+i+"' style=\"display: none\">" +
								"<a onclick=\""+
								"document.getElementById('spoiler"+i+"').style.display='none';" +
								" document.getElementById('show"+i+"').style.display='';";
						
						for(String num:showIDsToAdd){
							valueToadd+="document.getElementById('spoiler_row_id"+num+"').style.display='none';" +
								" document.getElementById('show_id"+num+"').style.display='';";
						}
												
						valueToadd+="\" class=\"link\"> [-] </a><table>";
						
						for(LDResource rowDimValue:sortedLevel2RowDimValues){
							//Use the calculated dim values to get the LDResource with the label
							int valueIndex=rowDimValues.indexOf(rowDimValue);
							valueToadd+="<tr><td>"+rowDimValues.get(valueIndex).getURIorLabel()+"</td></tr>";									
						}						
						
						valueToadd+="</table></span>";
																				
						data[0] = new FHTML("HTMLrow_"+i, dim2.get(i).getURIorLabel()+valueToadd);
						tm.addRow(data);			
					
					//The column dimension has levels
					}else if(dim1Levels!=null&&dim1Levels.size()>1){
													
						// Add observations
						for (int j = 1; j <= dim1.size(); j++) {							
							
							//Use the calculated dim values to get the LDResource with the label
							List<LDResource> colDimValues=allDimensionsValues.get(visualDimensions.get(0));
							
							Set<LDResource> allLevel2ColDimValues=new HashSet<LDResource>();
							//get all level2 row dimension values
							for (int m = 0; m < dim2.size(); m++) {
								DrillDownObservation dobs=v2DCube[m][j - 1];
								if(dobs!=null){
									allLevel2ColDimValues.addAll(dobs.Level2MeasureDimensionValueMap.keySet());
								}
							}		
							
							List<LDResource> sortedLevel2ColDimValues=new ArrayList<LDResource>(allLevel2ColDimValues);
							Collections.sort(sortedLevel2ColDimValues);											
							
							DrillDownObservation dobs=v2DCube[i][j - 1];
							String measure="";
							if(dobs!=null){
							
								measure = "<span id='show_id"+k+"' class=\"link\">" 							
												+ "<p align=\"right\">"
												+"<table align=\"right\">" +
												"<tr>";
								
								for(LDResource ldr_measure:selectedMeasures){
									
									String measurevalue=dobs.Level1measureValueMap.get(ldr_measure);
									if(measurevalue!=null){
										//Show 1 decimal if decimals exist
										if(measurevalue.contains(".")){
											if((measurevalue.length()-measurevalue.indexOf("."))>=3){
												measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
											}
										}
									}else{
										measurevalue="-";										
									}
															
									measure+="<td align=\"right\"><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
											 measurevalue+" "+"</font></td>";	
								}
								
								measure+=" </tr></table></p> </span>";								
								measure+="<span id='spoiler_col_id"+k+"' style=\"display: none  \">" 	
										+ "<p align=\"right\">";
								measure+="<table align=\"right\"><tr>";
								
								for(LDResource colDimValue:sortedLevel2ColDimValues){
									//Use the calculated dim values to get the LDResource with the label
									int valueIndex=colDimValues.indexOf(colDimValue);
									measure+="<td>"+colDimValues.get(valueIndex).getURIorLabel()+"</td>";									
								}	
								
								measure+="</tr>";														
								
								for(LDResource ldr_measure:selectedMeasures){
									measure+="<tr>";
									for(LDResource ldr_dim_value:sortedLevel2ColDimValues){
										if(dobs!=null&&dobs.Level2MeasureDimensionValueMap!=null&&
												dobs.Level2MeasureDimensionValueMap.get(ldr_dim_value)!=null){	
											
											Map<LDResource,String> measures4dimVal=dobs.Level2MeasureDimensionValueMap.get(ldr_dim_value);
											String measurevalue=measures4dimVal.get(ldr_measure);
									
											measure+="<td align=\"right\">"+
														"<font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">";
																								
											//Show 1 decimal if decimals exist
											if(measurevalue!=null&&measurevalue.contains(".")){
												if((measurevalue.length()-measurevalue.indexOf("."))>=3){
													measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
												}
											}	
												
											if(measurevalue!=null){
												measure+=measurevalue+"</font></td>";
											}else{
												measure+="-</font></td>";
											}
										}else{
											measure+="<td align=\"right\">"+
													"<font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"
													+"-</font></td>";
										}									
									}
									
									String measurevalue=dobs.Level1measureValueMap.get(ldr_measure);
									
									if(measurevalue!=null){
										//Show 1 decimal if decimals exist
										if(measurevalue.contains(".")){
											if((measurevalue.length()-measurevalue.indexOf("."))>=3){
												measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
											}
										}		
																
										measure+="<td align=\"right\"><b><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
												 measurevalue+" "+"</font></b></td>";	
									}else{
										measure+="<td align=\"right\"><b><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
												 "-</font></b></td>";	
									}
									
									measure+="</tr>";								
								}
								measure+=	"</table></p></span>";
								k++;
							}else{								
								measure+="<table align=\"right\"><tr>";										
								for(LDResource ldr_measure:selectedMeasures){									
									
									measure+="<td align=\"right\"><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
											 "- "+"</font></td>";	
								}	
								measure+=" </tr></table>";
							}
							
							FHTML html_string = new FHTML("HTMLobs_"+i+"_"+j,measure);
							data[j]=html_string;					
						}													
																				
						data[0] = new FHTML("HTMLrow_"+i, dim2.get(i).getURIorLabel());
						tm.addRow(data);	
								
					//no levels exist or only one level selected
					}else{
						// Add observations
						for (int j = 1; j <= dim1.size(); j++) {
							DrillDownObservation dobs=v2DCube[i][j - 1];
							String measure = "<table align=\"right\"><tr>";
							
							if(dobs!=null){
								for(LDResource ldr_measure:selectedMeasures){									
									String measurevalue=dobs.Level1measureValueMap.get(ldr_measure);									
									if(measurevalue!=null){
										//Show 1 decimal if decimals exist
										if(measurevalue.contains(".")){
											if((measurevalue.length()-measurevalue.indexOf("."))>=3){
												measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
											}
										}	
									}else{
										measurevalue="-";
									}															
									measure+="<td align=\"right\"><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
											 measurevalue+" "+"</font></td>";	
								}
							}else{
								for(LDResource ldr_measure:selectedMeasures){									
									
									measure+="<td align=\"right\"><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
											 "- "+"</font></td>";	
								}								
							}
							
							measure+=" </tr></table></p>";
							FHTML html_string = new FHTML("HTMLobs_"+i+"_"+j,measure);
							data[j]=html_string;	
						}
						data[0] = new FHTML("HTMLrow_"+i, dim2.get(i).getURIorLabel());
						tm.addRow(data);									
					}
				}				
							
				if (dim1Levels!=null && dim1Levels.size()>1) {										
					//Use the calculated dim values to get the LDResource with the label
					List<LDResource> colDimValues=allDimensionsValues.get(visualDimensions.get(0));
									
					Object[] data = new Object[dim1.size() + 1];
					data[0]="";
					for (int j = 1; j <= dim1.size(); j++) {				
								
						ArrayList<String> showIDsToAdd=new ArrayList<String>();
												
						Set<LDResource> allLevel2ColDimValues=new HashSet<LDResource>();
												
						for (int i = 0; i < dim2.size(); i++) {tm.getValueAt(i, j).toString();
							showIDsToAdd.addAll(getShowIdValues(((FHTML)tm.getValueAt(i, j)).getValue()));
							
							DrillDownObservation dobs=v2DCube[i][j - 1];
							if(dobs!=null){
								allLevel2ColDimValues.addAll(dobs.Level2MeasureDimensionValueMap.keySet());
							}							
						}	
						
						List<LDResource> sortedLevel2ColDimValues=new ArrayList<LDResource>(allLevel2ColDimValues);
						Collections.sort(sortedLevel2ColDimValues);
							
						String valueToadd= "<p align=\"right\">"+
									"<a id='show_column_"+j+"' onclick=\"" +
									"document.getElementById('spoiler_column_"+j+"').style.display=''; " +
									"document.getElementById('show_column_"+j+"').style.display='none';";
							
						for(String num:showIDsToAdd){
							valueToadd+="document.getElementById('spoiler_col_id"+num+"').style.display=''; " +
								"document.getElementById('show_id"+num+"').style.display='none';";
						}
							
						valueToadd+="\" class=\"link\"> [+]</a> </p>" +
								"<span id='spoiler_column_"+j+"' style=\"display: none\">" +
								"<p align=\"right\">"+
								"<a onclick=\""+
								"document.getElementById('spoiler_column_"+j+"').style.display='none';" +
								" document.getElementById('show_column_"+j+"').style.display='';";
							
						for(String num:showIDsToAdd){
							valueToadd+="document.getElementById('spoiler_col_id"+num+"').style.display='none';" +
								" document.getElementById('show_id"+num+"').style.display='';";
						}
							
						valueToadd+="\" class=\"link\"> [-] </a></p>";							
						valueToadd+="</span>";
						
						data[j] = new FHTML ("HTMLcolumn_"+j,valueToadd);
					}	
					tm.insertRow(0, data);		
				}	

			// If there is 1 visual dimensions
			}else {
			
				// populate the FTableModel based on the v2DCube created				
				List<LDResource> dim1Levels=selectedDimLevels.get(visualDimensions.get(0));

				for (int i = 0; i < dim1.size(); i++) {				
					Object[] data = new Object[dim1.size() + 1];
					
					//The 1 visual dimension has levels
					if(dim1Levels!=null&&dim1Levels.size()>1){
						
						ArrayList<String> showIDsToAdd=new ArrayList<String>();
						
						//Use the calculated dim values to get the LDResource with the label
						List<LDResource> rowDimValues=allDimensionsValues.get(visualDimensions.get(0));
						
						Set<LDResource> allLevel2RowDimValues=new HashSet<LDResource>();
						DrillDownObservation dobs=v2DCube[i][0];
						if(dobs!=null){
							allLevel2RowDimValues.addAll(dobs.Level2MeasureDimensionValueMap.keySet());
							
							
							List<LDResource> sortedLevel2RowDimValues=new ArrayList<LDResource>(allLevel2RowDimValues);
							Collections.sort(sortedLevel2RowDimValues);
											
							
							String measure = "<span id='show_id"+k+"' class=\"link\">" 							
												+ "<p align=\"right\">"
												+"<table align=\"right\"><tr>";
								
							for(LDResource ldr_measure:dobs.Level1measureValueMap.keySet()){									
									String measurevalue=dobs.Level1measureValueMap.get(ldr_measure);
									//Show 1 decimal if decimals exist
									if(measurevalue.contains(".")){
										if((measurevalue.length()-measurevalue.indexOf("."))>=3){
											measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
										}
									}																	
									measure+="<td align=\"right\"><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
											 measurevalue+" "+"</font></td>";	
								}
								
								measure+=" </tr></table></p> </span>";
								
								measure+="<span id='spoiler_row_id"+k+"' style=\"display: none \">" 	
										+ "<p align=\"right\">";
								measure+="<table align=\"right\"><tr>";
								
								for(LDResource ldr_measure:dobs.Level1measureValueMap.keySet()){
									
									String measurevalue=dobs.Level1measureValueMap.get(ldr_measure);
									//Show 1 decimal if decimals exist
									if(measurevalue.contains(".")){
										if((measurevalue.length()-measurevalue.indexOf("."))>=3){
											measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
										}
									}																									
									measure+="<td align=\"right\"><b><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
											 measurevalue+" "+"</font></b></td>";										
								}
								
								measure+="</tr>";							
								
								for(LDResource ldr_dim_value:sortedLevel2RowDimValues){
									Map<LDResource,String> measures4dimVal=dobs.Level2MeasureDimensionValueMap.get(ldr_dim_value);
									measure+="<tr>";
									for(LDResource ldr_measure:selectedMeasures){
										if(measures4dimVal!=null&&measures4dimVal.get(ldr_measure)!=null){	
											measure+="<td align=\"right\">"+
													"<font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">";
											
											String measurevalue=measures4dimVal.get(ldr_measure);
											//Show 1 decimal if decimals exist
											if(measurevalue.contains(".")){
												if((measurevalue.length()-measurevalue.indexOf("."))>=3){
													measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
												}
											}												
											measure+=measurevalue+"</font></td>";													
										}else{
											measure+="<td align=\"right\">"+
													"<font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"
													+"-</font></td>";
										}									
									}
									measure+="</tr>";
									
								}
							measure+=	"</table></p></span>";
							k++;
								
							FHTML html_string = new FHTML("HTMLobs_"+i,measure);
							data[1]=html_string;					
							showIDsToAdd.addAll(getShowIdValues(measure));
														
							String valueToadd="<a id='show"+i+"' onclick=\"" +
									"document.getElementById('spoiler"+i+"').style.display=''; " +
									"document.getElementById('show"+i+"').style.display='none';";
							
							for(String num:showIDsToAdd){
								valueToadd+="document.getElementById('spoiler_row_id"+num+"').style.display=''; " +
									"document.getElementById('show_id"+num+"').style.display='none';";
							}
							
							valueToadd+="\" class=\"link\"> [+]</a>" +
									"<span id='spoiler"+i+"' style=\"display: none\">" +
									"<a onclick=\""+
									"document.getElementById('spoiler"+i+"').style.display='none';" +
									" document.getElementById('show"+i+"').style.display='';";
							
							for(String num:showIDsToAdd){
								valueToadd+="document.getElementById('spoiler_row_id"+num+"').style.display='none';" +
									" document.getElementById('show_id"+num+"').style.display='';";
							}
													
							valueToadd+="\" class=\"link\"> [-] </a><table>";
							
							for(LDResource rowDimValue:sortedLevel2RowDimValues){
								//Use the calculated dim values to get the LDResource with the label
								int valueIndex=rowDimValues.indexOf(rowDimValue);
								valueToadd+="<tr><td>"+rowDimValues.get(valueIndex).getURIorLabel()+"</td></tr>";									
							}						
							
							valueToadd+="</table></span>";
																					
							data[0] = new FHTML("HTMLrow_"+i, dim1.get(i).getURIorLabel()+valueToadd);
							tm.addRow(data);		
						}
					
					//The column dimension has levels
					}else{
						DrillDownObservation dobs=v2DCube[i][0];
						String measure = "<table align=\"right\">" +
											"<tr>";
						
						if(dobs!=null&&dobs.Level1measureValueMap!=null){
							for(LDResource ldr_measure:dobs.Level1measureValueMap.keySet()){
									
								String measurevalue=dobs.Level1measureValueMap.get(ldr_measure);
								//Show 1 decimal if decimals exist
								if(measurevalue.contains(".")){
									if((measurevalue.length()-measurevalue.indexOf("."))>=3){
										measurevalue=measurevalue.substring(0,measurevalue.indexOf(".")+3);
									}
								}																	
								measure+="<td align=\"right\"><font color=\""+ measureColors[cubeMeasures.indexOf(ldr_measure)] + "\">"+
										 measurevalue+" "+"</font></td>";	
							}
						}else{
							measure+="<td align=\"right\">-</td>";
						}
							
						measure+=" </tr></table></p>";
						FHTML html_string = new FHTML("HTMLobs_"+i,measure);
						data[1]=html_string;	
						
						data[0] = new FHTML("HTMLrow_"+i, dim1.get(i).getURIorLabel());
						tm.addRow(data);	
						
					}
				}
			}					
			return tm;
		}
	
	
	private ArrayList<String> getShowIdValues(String htmltext){
		if (htmltext == null  ) {
			return new ArrayList<String>();
		}
		
		ArrayList<String> numbers=new ArrayList<String>();
		
		int fromIndex=0;
		while(htmltext.indexOf("show_id", fromIndex)!=-1){
			int showIdIndex=htmltext.indexOf("show_id", fromIndex);
			int quotationMarkIndex=	htmltext.indexOf("'", showIdIndex);
			String number=htmltext.substring(showIdIndex+7,quotationMarkIndex);
			if(!numbers.contains(number)){
				numbers.add(number);
			}			
			fromIndex=quotationMarkIndex;
		}
		
		return numbers;
		
		
	}
	
	// get an HTML representation of an LDResource
	private FHTML getFHTMLFromLDResource(LDResource ldr, String id) {
		if (ldr == null  ) {
			return new FHTML(id,"<p align=\"right\">-</p>");
		}
		String linktext = ldr.getURIorLabel();
		String html = linktext;
		FHTML html_string = new FHTML(id,html);
		return html_string;

	}

	// Adds a new line to UI
	private FHTML getNewLineComponent(boolean delete) {
		Random rand = new Random();
		FHTML fhtml = null;
		if (delete) {
			fhtml = new FHTML("fhtmlnewlinedelete_" + Math.abs(rand.nextLong()));
		} else {
			fhtml = new FHTML("fhtmlnewline_" + Math.abs(rand.nextLong()));
		}
		fhtml.setValue("<br><br>");
		return fhtml;
	}

	@Override
	public String getTitle() {
		return "Data Cube Browser widget";
	}

	@Override
	public Class<?> getConfigClass() {
		return Config.class;
	}
}
