package org.certh.opencube.cubebrowser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.certh.opencube.SPARQL.AggregationSPARQL;
import org.certh.opencube.SPARQL.CubeBrowserSPARQL;
import org.certh.opencube.SPARQL.CubeSPARQL;
import org.certh.opencube.SPARQL.SelectionSPARQL;
import org.certh.opencube.SPARQL.SliceSPARQL;
import org.certh.opencube.utils.CubeHandlingUtils;
import org.certh.opencube.utils.LDResource;
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
 * = Test my demo widget =
 * 
 * <br/>
 * {{#widget: org.certh.opencube.cubebrowser.DataCubeBrowser|
 *  dataCubeURI= '<http://eurostat.linked-statistics.org/data/cdh_e_fsp>'
 *  |asynch='true' }}
 * 
 * </code>
 * 
 */

@TypeConfigDoc("The OpenCube browser enables the exploration of an RDF Data Cube"
		+ " by presenting each time a two-dimensional slice of the cube as a table.")
public class DataCubeBrowserPlusPlus extends
		AbstractWidget<DataCubeBrowserPlusPlus.Config> {

	// The top container to show the check boxes with the available aggregation
	// set dimensions
	private FContainer selectCubeContainer = new FContainer("selectCubeContainer");

	// The top container to show the check boxes with the available aggregation
	// set dimensions
	private FContainer aggSetDimContainer = new FContainer("aggSetDimContainer");

	// The left container to show the combo boxes with the visual dimensions
	private FContainer leftcontainer = new FContainer("leftcontainer");

	// The right container to show the combo boxes with the fixed dimensions values
	private FContainer rightcontainer = new FContainer("rightcontainer");

	// The right container to show the combo boxes with the fixed dimensions values
	private FContainer languagecontainer = new FContainer("languagecontainer");

	// The measures container to show the combo box with the cube measures
	private FContainer measurescontainer = new FContainer("measurescontainer");

	// An FGrid to show fixed cube dimensions
	private FGrid fixedDimGrid = new FGrid("fixedDimGrid");

	// All the cube dimensions
	private List<LDResource> cubeDimensions = new ArrayList<LDResource>();

	// The selected cube measure to visualize
	private List<LDResource> selectedMeasures = new ArrayList<LDResource>();

	// All the dimensions of all cubes of the same aggregation set
	private List<LDResource> aggregationSetDims = new ArrayList<LDResource>();

	// All the dimensions per cube of the aggregation set
	private HashMap<LDResource, List<LDResource>> cubeDimsOfAggregationSet = new HashMap<LDResource, List<LDResource>>();

	// The cube dimensions to visualize (2 dimensions)
	private List<LDResource> visualDimensions = new ArrayList<LDResource>();
	
	// The selected cube dimensions to use for visualization (visaul dims + fixed dims)
	private List<LDResource> selectedDimenisons = new ArrayList<LDResource>();

	// The fixed dimensions
	private List<LDResource> fixedDimensions = new ArrayList<LDResource>();

	// The slice fixed dimensions
	private List<LDResource> sliceFixedDimensions = new ArrayList<LDResource>();

	// All the cube observations - to be used to create a slice
	private List<LDResource> sliceObservations = new ArrayList<LDResource>();

	// A map (cube - dimension URI - dimension values) with all cube dimension values
	private HashMap<LDResource, List<LDResource>> allDimensionsValues = new HashMap<LDResource, List<LDResource>>();

	// The selected value for each fixed dimension
	private HashMap<LDResource, LDResource> fixedDimensionsSelectedValues = new HashMap<LDResource, LDResource>();

	// The selected value for each fixed dimension
	private HashMap<LDResource, LDResource> sliceFixedDimensionsValues = new HashMap<LDResource, LDResource>();

	// A map with the corresponding components for each fixed cube dimension
	private HashMap<LDResource, List<FComponent>> dimensionURIfcomponents = new HashMap<LDResource, List<FComponent>>();

	// A map with the Aggregation Set Dimension URIs and the corresponding Check boxes
	private HashMap<LDResource, FCheckBox> mapDimURIcheckBox = new HashMap<LDResource, FCheckBox>();

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

	// The available languages of the cube
	private List<String> availableLanguages = new ArrayList<String>();

	// The selected language
	private String selectedLanguage;

	// Ignore multiple languages
	private boolean ignoreLang;

	private List<LDResource> cubeDimsFromSlice = null;

	private List<LDResource> allCubes = new ArrayList<LDResource>();

	private String[] measureColors = { "black", "CornflowerBlue", "LimeGreen", "Tomato",
			"Orchid","MediumVioletRed","Gold","DarkGoldenRod","DarkGray","DarkRed",
			"DarkRed","DarkRed","DarkRed","DarkRed","DarkRed","DarkRed","DarkRed","DarkRed",
			"DarkRed","DarkRed","DarkRed","DarkRed","DarkRed","DarkRed","DarkRed","DarkRed"};

	private String cubeSliceURI = "";
	
	private LDResource selectedCubeFromDropDown=null;

	public static class Config extends WidgetBaseConfig {
		@ParameterConfigDoc(desc = "The data cube URI to visualise", required = true)
		public String dataCubeURIs;

		@ParameterConfigDoc(desc = "Use code lists to get dimension values", required = false)
		public boolean useCodeLists;

		@ParameterConfigDoc(desc = "SPARQL service to forward queries", required = false)
		public String sparqlService;

		@ParameterConfigDoc(desc = "The default language", required = false)
		public String defaultLang;

		@ParameterConfigDoc(desc = "Use multiple languages", required = false)
		public boolean ignoreLang;
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
		
		// cubeSliceURIs = config.dataCubeURIs;
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

		allCubes = SelectionSPARQL.getAllAvailableCubesAndSlices(SPARQL_service);
		// Prepare and show the widget
		populateCentralContainer();

		isFirstLoad = false;

		return cnt;
	}

	private void populateCentralContainer() {

		leftGrid=new FGrid("leftGrid");
		overalGrid=new FGrid("overalgrid");
		midleGrid=new FGrid("midleGrid");

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
	//	aggregationSetDims = new ArrayList<LDResource>();
		allDimensionsValues = new HashMap<LDResource, List<LDResource>>();
		cubeDimsOfAggregationSet = new HashMap<LDResource, List<LDResource>>();

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
			// if (!allCubeSliceTypes.containsValue("error")) {

			if (isCube || isSlice) {
				if (isCube) {
					// The cube graph is the graph of the URI computed above
					// cubeGraph = cubeSliceGraph;

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

						HashSet<LDResource> cubeDimensionsSet = new HashSet<LDResource>(
								cubeDimensions);
						// Get all Cube dimensions
						cubeDimensionsSet.addAll(CubeSPARQL
								.getDataCubeDimensions(cubeSliceURI,
										cubeGraphs.get(cubeSliceURI),
										cubeDSDGraphs.get(cubeSliceURI),
										selectedLanguage, defaultLang,
										ignoreLang, SPARQL_service));

						cubeDimensions = new ArrayList<LDResource>(
								cubeDimensionsSet);
						
						if(isFirstCubeLoad){
							dimsOfaggregationSet2Show=cubeDimensions;
							isFirstCubeLoad=false;
						}
						
						// Thread to get cube dimensions
						Thread measuresThread = new Thread(new Runnable() {
							public void run() {
								// Get the Cube measure

								HashSet<LDResource> cubeMeasuresSet = new HashSet<LDResource>(
										cubeMeasures);
								
								List<LDResource> currentCubeMeasures=CubeSPARQL
										.getDataCubeMeasure(
												cubeSliceURI,
												cubeGraphs.get(cubeSliceURI),
												cubeDSDGraphs.get(cubeSliceURI),
												selectedLanguage, defaultLang,
												ignoreLang, SPARQL_service);
										
									
								
								cubeMeasuresSet.addAll(currentCubeMeasures);

								cubeMeasures = new ArrayList<LDResource>(
										cubeMeasuresSet);
								
								measuresPerCube.put(cubeSliceURI, currentCubeMeasures);

/*								// Get the selected measure to use
								selectedMeasures = CubeHandlingUtils
										.getSelectedMeasure(cubeMeasures,
												selectedMeasures);*/
							}
						});

						// Thread to get cube dimensions
						Thread aggregationSetDimsThread = new Thread(
								new Runnable() {
									public void run() {
										// Get all the dimensions of the  aggregation
										// set the cube belongs to

										HashSet<LDResource> aggregationSetDimsSet = new HashSet<LDResource>(
												aggregationSetDims);
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
//
						// Thread to get cube dimensions
						Thread cubeDimsOfAggregationSetThread = new Thread(new Runnable() {
									public void run() {
										// Get all the dimensions per cube of the
										// aggregations set
										cubeDimsOfAggregationSet.putAll(AggregationSPARQL
												.getCubeAndDimensionsOfAggregateSet(
														cubeSliceURI,
														cubeDSDGraphs.get(cubeSliceURI),
														selectedLanguage,
														defaultLang,
														ignoreLang,
														SPARQL_service));
									}
								});

						// Thread to get cube dimensions
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

										for (LDResource dim : thisCubeDimensionsValues
												.keySet()) {

											HashSet<LDResource> cubeDimValuesSet;
											if (allDimensionsValues.get(dim) != null) {
												cubeDimValuesSet = new HashSet<LDResource>(
														allDimensionsValues.get(dim));
											} else {
												cubeDimValuesSet = new HashSet<LDResource>();
											}

											cubeDimValuesSet.addAll(thisCubeDimensionsValues.get(dim));

											List<LDResource> sortedcubeDimValues=
													new ArrayList<LDResource>(cubeDimValuesSet);
											Collections.sort(sortedcubeDimValues);
											allDimensionsValues.put(dim,sortedcubeDimValues);
										}
									}
								});

						measuresThread.start();
						aggregationSetDimsThread.start();
						cubeDimsOfAggregationSetThread.start();
						dimensionsValuesThread.start();

						try {
							dimensionsValuesThread.join();
							measuresThread.join();
							aggregationSetDimsThread.join();
							cubeDimsOfAggregationSetThread.join();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				} else if (isSlice) {

					// PREPEI NA DOUME POTE 2 SLICES EINAI COMPATIBLE

					for (String cubeSliceURItmp : cubeSliceURIs) {

						cubeSliceURI = cubeSliceURItmp;

						// The slice graph is the graph of the URI computed above
						sliceGraphs.put(cubeSliceURI,
								cubeGraphs.get(cubeSliceURI));

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

						Thread sliceFixedDimensionsThread = new Thread(
								new Runnable() {
									public void run() {
										// Get slice fixed dimensions
										// Use set to have one instance of fixed dim
										HashSet<LDResource> sliceFixedDimensionsSet = new HashSet<LDResource>(
												sliceFixedDimensions);

										sliceFixedDimensionsSet.addAll(SliceSPARQL
												.getSliceFixedDimensions(
														cubeSliceURI,
														sliceGraphs
																.get(cubeSliceURI),
														cubeDSDGraphs
																.get(cubeSliceURI),
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
														sliceGraphs
																.get(cubeSliceURI),
														cubeDSDGraphs
																.get(cubeSliceURI),
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

								cubeMeasures = new ArrayList<LDResource>(
										cubeMeasuresSet);

								// Get the selected measure to use
						/*		selectedMeasures = CubeHandlingUtils
										.getSelectedMeasure(cubeMeasures,
												selectedMeasures);*/
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
														sliceGraphs
																.get(cubeSliceURI),
														cubeDSDGraphs
																.get(cubeSliceURI),
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
				  
				  FLabel allcubes_label = new FLabel("allcubes_label",
				  "<b>Please select a cube to visualize:<b>");
				  
				  selectCubeContainer.add(allcubes_label);
				  
				  // Add Combo box with cube URIs 
				  FComboBox cubesCombo = new  FComboBox("cubesCombo") {
				  
				  @Override public void onChange() {
					
					  isFirstCubeLoad=true;
					  cubeSliceURIs=new ArrayList<String>();
					  cubeSliceURIs.add("<"+ ((LDResource) this.getSelected().get(0)).getURI()+ ">");
					  
					  selectedCubeFromDropDown=(LDResource) this.getSelected().get(0);
					  originalCubeSliceURIs=cubeSliceURIs;
					// cubeSliceURIs = config.dataCubeURIs;
					//originalCubeSliceURIs=new ArrayList<String>(cubeSliceURIs);
					  
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
					  cubeDimsOfAggregationSet = new  HashMap<LDResource, List<LDResource>>();
					  visualDimensions =  new ArrayList<LDResource>();
					  fixedDimensions = new  ArrayList<LDResource>(); 
					  sliceFixedDimensions = new  ArrayList<LDResource>(); 
					  fixedDimensionsSelectedValues = new  HashMap<LDResource, LDResource>();
					  sliceFixedDimensionsValues  = new HashMap<LDResource, LDResource>();
					  mapDimURIcheckBox =  new HashMap<LDResource, FCheckBox>();
					  mapMeasureURIcheckBox =  new HashMap<LDResource, FCheckBox>();
					  
					  // show the cube 
					  populateCentralContainer();
					  
				  } };
				  			  
				  
				  // populate cubes combo box 
				  for (LDResource cube : allCubes) { 
				//	  if(!cube.getURI().contains("http://www.fluidops.com/resource/cube_")
				//			  && !cube.getURI().contains("http://www.fluidops.com/resource/slice_")){
						  if (cube.getLabel() != null) {
							   cubesCombo.addChoice(cube.getLabel(), cube);
						  } else {
							  cubesCombo.addChoice(cube.getURI(), cube); 
						  }
					//  }
				  }
				  
				  //set as preselected the first cube URI IF it is only one
				  if (selectedCubeFromDropDown!=null) {
					   // Remove the "<" and ">"  from  the cube URI 
					  cubesCombo.setPreSelected(selectedCubeFromDropDown);
					  //  LDResource(cubeSliceURIs.get(0).substring(1, cubeSliceURI.length() -  1))); 
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
			//	aggSetDimContainer.addStyle("height", containerHeight+"px ");
				aggSetDimContainer.addStyle("margin-left", "auto");
				aggSetDimContainer.addStyle("margin-right", "auto");
				aggSetDimContainer.addStyle("text-align", "left");
									
				// If an aggregation set has already been created
				if (aggregationSetDims.size() > 0) {

					FLabel OLAPbrowsing_label = new FLabel(
							"OLAPbrowsing_label",
							"<b>Dimensions</b></br>");
									//+ "Summarize observations by adding/removing dimensions: </br>");
					aggSetDimContainer.add(OLAPbrowsing_label);

					int aggregationDim = 1;

					// Show Aggregation set dimensions
					for (LDResource aggdim : dimsOfaggregationSet2Show) {

						// show one check box for each aggregation set
						// dimension
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
									}
								}
								
								

								// Identify the cube of the aggregation set that
								// contains exactly the dimension selected to be browsed
								cubeSliceURIs = new ArrayList<String>();
								for (LDResource cube : cubeDimsOfAggregationSet.keySet()) {
									List<LDResource> cubeDims = cubeDimsOfAggregationSet.get(cube);
									if ((cubeDims.size() == aggregationSetSelectedDims.size())
											&& cubeDims.containsAll(aggregationSetSelectedDims)) {
										System.out.println("NEW CUBE URI: "	+ cube.getURI());

										// The new cube(s) to visualize
										cubeSliceURIs.add("<" + cube.getURI()+ ">");
									}
								}
								
								if(cubeSliceURIs.size()==0){
									cubeSliceURIs=originalCubeSliceURIs;
								}else{									
									originalCubeSliceURIs=cubeSliceURIs;
								}

								//if aggregation set cubes have been found
							//	if(cubeSliceURIs.size()>0){
									// clear the previous visualization
									// and create a new one for the new cube(s)
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
							//	}
							}
						};

						// set as checked if the dimension is contained at the
						// selected cube
						//aggrDimCheckBox.setChecked(cubeDimensions.contains(aggdim));
						aggrDimCheckBox.setChecked(selectedDimenisons.contains(aggdim));
						mapDimURIcheckBox.put(aggdim, aggrDimCheckBox);
						aggSetDimContainer.add(aggrDimCheckBox);

						aggregationDim++;
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
				
				FLabel measure_label = new FLabel("measure_lb",
						"<b>Measures</b></br>");
							//	+ "Select the measures to visualize:");
				measurescontainer.add(measure_label);

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

							// Get all selected aggregation set dimensions for
							// browsing
							selectedMeasures = new ArrayList<LDResource>();
							for (LDResource measureURI : mapMeasureURIcheckBox
									.keySet()) {
								FCheckBox check = mapMeasureURIcheckBox
										.get(measureURI);

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

					// set as checked if the dimension is contained at the
					// selected cube
					measureCheckBox.setChecked(selectedMeasures
							.contains(measure));
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
					

					FLabel datalang_label = new FLabel(
							"datalang",	"<b>Language</b></br>"
									+ "Select the language of the visualized data:");
					languagecontainer.add(datalang_label);

					// Add Combo box for language selection
					FComboBox datalang_combo = new FComboBox("datalang_combo") {
						@Override
						public void onChange() {
							selectedLanguage = this.getSelected().get(0)
									.toString();
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

				// only the first time
				if (visualDimensions.size() == 0) {
					// Select the two dimension with the most values for
					// visualization
					visualDimensions = CubeHandlingUtils
							.getRandomDims4Visualisation(cubeDimensions,
									allDimensionsValues);
				} else if (cubeDimensions.size() > 1) {

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
							if (visualDimensions.get(0).equals(
									cubeDimensions.get(0))) {
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

				// Get the fixed cube dimensions
				fixedDimensions = CubeHandlingUtils.getFixedDimensions(
						cubeDimensions, visualDimensions);

				// Selected values for the fixed dimensions
				fixedDimensionsSelectedValues = CubeHandlingUtils
						.getFixedDimensionsRandomSelectedValues(
								allDimensionsValues, fixedDimensions,
								fixedDimensionsSelectedValues);

				if((selectedMeasures.size()>0 && selectedDimenisons.size()>0&&isCube)||
						(isSlice&&selectedMeasures.size()>0)){
					List<BindingSet> allResults = new ArrayList<BindingSet>();
	
					List<String> reverseCubeSliceURIs = new ArrayList<String>(
							cubeSliceURIs);
	
					// We need the observations of the first cube to be shown when
					// duplicates exist
					Collections.reverse(reverseCubeSliceURIs);
					for (String tmpCubeSliceURI : reverseCubeSliceURIs) {
						TupleQueryResult res = null;
						// Get query tuples for visualization
						
						if (isCube) {
							
							List<LDResource> tmpMeasures=new ArrayList<LDResource>(
									measuresPerCube.get(tmpCubeSliceURI));
							tmpMeasures.retainAll(selectedMeasures);
							
							res = CubeBrowserSPARQL
									.get2DVisualsiationValues(visualDimensions,
											fixedDimensionsSelectedValues,
											tmpMeasures, allDimensionsValues,
											tmpCubeSliceURI,
											cubeGraphs.get(tmpCubeSliceURI),
											SPARQL_service);
							
						} else if (isSlice) {
							
							//PRPEPEI NA KANW OTI ME PANW GIA TA MEASURES
							
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
	
						// collect all result from all cubes
						try {
							while (res.hasNext()) {
								allResults.add(res.next());
							}
						} catch (QueryEvaluationException e) {
							e.printStackTrace();
						}
					}				

					// Create an FTable model based on the query tuples
					FTableModel tm = create2DCubeTableModel(allResults,
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

			//		FLabel fixeddimlabel = new FLabel(
			//				"fixeddimlabel",
			//				"<b>Visual dimensions</b></br> ");
			//				"Select the two dimensions that define the table of the browser:");

			//		leftcontainer.add(fixeddimlabel);

					FGrid visualDimGrid = new FGrid("visualDimGrid");

					ArrayList<FComponent> dim1Array = new ArrayList<FComponent>();

					// Add label for Dim1 (column headings)
					FLabel dim1Label = new FLabel("dim1Label",
							"<b>Columns:<b>");
					dim1Array.add(dim1Label);

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

									// If both combo boxes have the same
									// selected value
									// Select randomly another value for d2
									if (d1Selected.get(0).equals(
											d2Selected.get(0))) {
										List<Pair<String, Object>> d2choices = ((FComboBox) fc)
												.getChoices();
										for (Pair<String, Object> pair : d2choices) {
											if (!pair.snd.toString().equals(
													d2Selected.get(0))) {
												d2Selected.clear();
												d2Selected.add(pair.snd
														.toString());
												((FComboBox) fc)
														.setPreSelected(pair.snd
																.toString());
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
					FLabel dim2Label = new FLabel("dim2Label",
							"<b>Rows:</b>");
					dim2Array.add(dim2Label);

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
										List<Pair<String, Object>> d1choices = ((FComboBox) fc)
												.getChoices();
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
					FLabel otheropts = new FLabel("Options",
							"<b>Filter:</b></br>");
					rightcontainer.add(otheropts);
				}

				fixedDimGrid = new FGrid("fixedDimGrid");
				// If it is a slice and there are fixed dimension values
				if (!sliceFixedDimensionsValues.isEmpty()) {

					// An FGrid to show visual and fixed cube dimensions

					int sliceFixedValues = 1;
					for (LDResource sliceFixedDim : sliceFixedDimensionsValues
							.keySet()) {
						ArrayList<FComponent> farray = new ArrayList<FComponent>();

						// Add the label for the fixed cube dimension
						FLabel fDimLabel = new FLabel("sliceFixedName_"
								+ sliceFixedValues, "<u>"
								+ sliceFixedDim.getURIorLabel() + ": </u>");
						farray.add(fDimLabel);
						LDResource fDimValue = sliceFixedDimensionsValues
								.get(sliceFixedDim);

						FLabel fDimValueLabel = null;
						if (fDimValue.getURIorLabel().length() > 60) {
							fDimValueLabel = new FLabel("sliceFixedValue_"
									+ sliceFixedValues, fDimValue
									.getURIorLabel().substring(0, 60) + "...");
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
				
				
				////               UI                ///////////////////////

			
				ArrayList<FComponent> leftArray=new ArrayList<FComponent>();
				
			
				leftGrid.addStyle("height", "1500px");
								
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
					//	leftGrid.addRow(leftArray);						
					}
				}
				
				ArrayList<FComponent> midleArray=new ArrayList<FComponent>();
				midleArray.add(leftGrid);
				if(isCube){
					if(selectedMeasures.size()>0 &&selectedDimenisons.size()>0){
						ftable.addStyle("height", "1500px");
						ftable.addStyle("width", "700px ");
						midleArray.add(ftable);
					}else{
						FContainer placeholderout=new FContainer("placeholderout");
						placeholderout.addStyle("height", "1500px ");
						
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
								"placeholderlabel",	"<b>Select at least one " +
										"Dimension and one Measure to browse.</b>");
								
						placeholder.add(placeholderlabel);
						placeholderout.add(placeholder);
						midleArray.add(placeholderout);
						
					}
				}else if (isSlice){
					
					if(selectedMeasures.size()>0 ){
						ftable.addStyle("height", "1500px");
						ftable.addStyle("width", "700px ");
						midleArray.add(ftable);
					}else{
						FContainer placeholderout=new FContainer("placeholderout");
						placeholderout.addStyle("height", "1500px ");
						
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
						midleArray.add(placeholderout);
						
					}
									
				}
				midleGrid.addRow(midleArray);		
				
				ArrayList<FComponent> overalArray=new ArrayList<FComponent>();
				
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

			FLabel allcubes_label = new FLabel("allcubes_label",
					"<b>Please select a cube to visualize:<b>");

			selectCubeContainer.add(allcubes_label);

			// Add Combo box with cube URIs
			FComboBox cubesCombo = new FComboBox("cubesCombo") {
				@Override
				public void onChange() {
					isFirstCubeLoad=true;
					cubeSliceURIs=new ArrayList<String>();
					cubeSliceURIs.add("<"+ ((LDResource) this.getSelected().get(0)).getURI()+ ">");
										
					selectedCubeFromDropDown=(LDResource) this.getSelected().get(0);
					originalCubeSliceURIs=cubeSliceURIs;
					cnt.removeAll();
					overalGrid.removeAll();
					leftGrid.removeAll();					
			//		topcontainer.removeAll();
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
					cubeDimsOfAggregationSet = new HashMap<LDResource, List<LDResource>>();
					visualDimensions = new ArrayList<LDResource>();
					fixedDimensions = new ArrayList<LDResource>();
					sliceFixedDimensions = new ArrayList<LDResource>();
					fixedDimensionsSelectedValues = new HashMap<LDResource, LDResource>();
					sliceFixedDimensionsValues = new HashMap<LDResource, LDResource>();
					mapMeasureURIcheckBox = new HashMap<LDResource, FCheckBox>();
									
					// show the cube
					populateCentralContainer();

				}
			};

			// populate cubes combo box
			for (LDResource cube : allCubes) {
				//if(!cube.getURI().contains("http://www.fluidops.com/resource/cube_")
				//		  && !cube.getURI().contains("http://www.fluidops.com/resource/slice_")){
					  if (cube.getLabel() != null) {
						   cubesCombo.addChoice(cube.getLabel(), cube);
					  } else {
						  cubesCombo.addChoice(cube.getURI(), cube); 
					  }
				//  }
			}
			
			selectCubeContainer.add(cubesCombo);

			cnt.add(selectCubeContainer);
		}

	}

	// Set visual and fixed dimensions based on user selection of combo boxes
	private void setVisualDimensions(List<String> d1Selected,
			List<String> d2Selected) {
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
	
					LDResource fDimValue = sliceFixedDimensionsValues
							.get(sliceFixedDim);
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
	// initialization,
	// false when refresh is not need i.e. at the initialization
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
		

			// Populate the combo box with the values of the fixed cube
			// dimension
			for (LDResource ldr : allDimensionsValues.get(fDim)) {
				// Show the first 60 chars if label too long
				if (ldr.getURIorLabel().length() > 35) {
					fDimCombo.addChoice(ldr.getURIorLabel().substring(0, 35)
							+ "...", ldr.getURI());
				} else {
					fDimCombo.addChoice(ldr.getURIorLabel(), ldr.getURI());
				}
			}

			if (fixedDimensionsSelectedValues.get(fDim) != null) {
				// Combo box pre-selected value
				fDimCombo.setPreSelected(fixedDimensionsSelectedValues
						.get(fDim).getURI());
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
			List<FComponent> dimComponents = dimensionURIfcomponents
					.get(dimres);
			String selectedValue = ((FComboBox) dimComponents.get(1))
					.getSelectedAsString().get(0);
			List<LDResource> selectedDimValues = allDimensionsValues
					.get(dimres);
			for (LDResource dimValue : selectedDimValues) {
				if (dimValue.getURI().equals(selectedValue)) {
					fixedDimensionsSelectedValues.put(dimres, dimValue);
				}
			}
		}

		// fixedDimensionsSelectedValues.putAll(tmpFixedDimensionsSelectedValues);

		List<BindingSet> allResults = new ArrayList<BindingSet>();

		for (String tmpCubeSliceURI : cubeSliceURIs) {
			TupleQueryResult res = null;
			if (isCube) {
				
				List<LDResource> tmpMeasures=new ArrayList<LDResource>(
						measuresPerCube.get(tmpCubeSliceURI));
				tmpMeasures.retainAll(selectedMeasures);
				// Get query tuples for visualization
				res = CubeBrowserSPARQL.get2DVisualsiationValues(visualDimensions,
						fixedDimensionsSelectedValues, tmpMeasures,
						allDimensionsValues, tmpCubeSliceURI,
						cubeGraphs.get(tmpCubeSliceURI), SPARQL_service);
			} else if (isSlice) {
				
				//PRPEPEI NA KANW OTI ME PANW GIA TA MEASURES
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

		/*
		 * TupleQueryResult res = null; if (isCube) { // Get query tuples for
		 * visualization res =
		 * CubeBrowserSPARQL.get2DVisualsiationValues(visualDimensions,
		 * fixedDimensionsSelectedValues, selectedMeasures, allDimensionsValues,
		 * cubeSliceURI, cubeGraph, SPARQL_service); } else if (isSlice) { res =
		 * CubeBrowserSPARQL.get2DVisualsiationValuesFromSlice(
		 * visualDimensions, fixedDimensionsSelectedValues, selectedMeasures,
		 * allDimensionsValues, cubeSliceURI, sliceGraph, cubeGraph,
		 * SPARQL_service); }
		 */

		// create table model for visualization
		FTableModel newTableModel = create2DCubeTableModel(allResults,
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
		List<LDResource> dim1 = dimensions4VisualisationValues
				.get(visualDimensions.get(0));

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

			// The rest columns of the first row - the values of the 1st
			// dimension
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
		for (BindingSet bindingSet : res) {

			// BindingSet bindingSet = res.next();

			// get Dim1 value
			String dim1Val = bindingSet.getValue("dim1").stringValue();
			LDResource r1 = new LDResource(dim1Val);

			String measure = "";
			// get measures
			int i = 1;
			for (LDResource meas : selectedMeasures) {
				if(bindingSet.getValue("measure_" + meas.getLastPartOfURI())!=null){
					measure += "<font color=\""
							+ measureColors[cubeMeasures.indexOf(meas)] + "\">"
							+ "<p align=\"right\">"
							+ bindingSet.getValue("measure_" + meas.getLastPartOfURI()).stringValue()
							+ " </p></font> ";
				}
				i++;
			}

			// get observation URI
			String obsURI = bindingSet.getValue("obs").stringValue();
			LDResource obs =new LDResource(obsURI);
			obs.setLabel(measure);
			
			//An observation returned may not contain any measure
			//e.g. there are two cubes and only the measures of the 1st is selected for
			//visualization... then an observation may be returned but not contain any
			// measure to visualise
			if(!measure.equals("")){
				// If there are 2 visual dimensions
				if (visualDimensions.size() == 2) {
					// get Dim2 value
					String dim2Val = bindingSet.getValue("dim2").stringValue();
					LDResource r2 = new LDResource(dim2Val);
	
					// Add the observation to the corresponding (row, column)
					// position of the table
					LDResource currentObs=v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)];
					if(currentObs!=null){
						if(obs.getLabel().contains("color")){
							String color1=obs.getLabel().substring(
									obs.getLabel().indexOf("color"),obs.getLabel().indexOf("color")+9);
							//System.out.println(color1);
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
							System.out.println(color1);
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
			for (int i = 0; i < dim2.size(); i++) {
				Object[] data = new Object[dim1.size() + 1];

				// Add row header (values in first column)
				data[0] = getHTMLStringFromLDResource(dim2.get(i));

				// Add observations
				for (int j = 1; j <= dim1.size(); j++) {
					data[j] = getHTMLStringFromLDResource(v2DCube[i][j - 1]);
				}

				tm.addRow(data);
			}

			// If there is 1 visual dimensions
		} else {

			// Add observations
			for (int j = 0; j < dim1.size(); j++) {
				Object[] data = new Object[2];
				// Add row header (values in first column)
				data[0] = getHTMLStringFromLDResource(dim1.get(j));

				data[1] = getHTMLStringFromLDResource(v2DCube[j][0]);
				tm.addRow(data);
			}

		}
		return tm;
	}

	// get an HTML representation of an LDResource
	private HtmlString getHTMLStringFromLDResource(LDResource ldr) {
		if (ldr == null  ) {
			return new HtmlString("<p align=\"right\">-</p>");
		}
		String linktext = ldr.getURIorLabel();
		String linkURI = ldr.getURI();
		String html = linktext;
		// String html = "<a href=\"" + linkURI + "\">" + linktext + "</a>";

		HtmlString html_string = new HtmlString(html);
		// HtmlString html_string = new HtmlString(linkURI);
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
