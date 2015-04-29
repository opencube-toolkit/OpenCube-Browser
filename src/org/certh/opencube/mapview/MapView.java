
package org.certh.opencube.mapview;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.certh.opencube.SPARQL.AggregationSPARQL;
import org.certh.opencube.SPARQL.CubeSPARQL;
import org.certh.opencube.SPARQL.MapViewSPARQL;
import org.certh.opencube.SPARQL.SliceSPARQL;
import org.certh.opencube.utils.CubeHandlingUtils;
import org.certh.opencube.utils.LDResource;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.fluidops.ajax.FClientUpdate;
import com.fluidops.ajax.components.FButton;
import com.fluidops.ajax.components.FCheckBox;
import com.fluidops.ajax.components.FComboBox;
import com.fluidops.ajax.components.FComponent;
import com.fluidops.ajax.components.FContainer;
import com.fluidops.ajax.components.FDialog;
import com.fluidops.ajax.components.FGrid;
import com.fluidops.ajax.components.FHTML;
import com.fluidops.ajax.components.FLabel;
import com.fluidops.iwb.api.EndpointImpl;
import com.fluidops.iwb.model.ParameterConfigDoc;
import com.fluidops.iwb.model.TypeConfigDoc;
import com.fluidops.iwb.widget.AbstractWidget;
import com.fluidops.iwb.widget.config.WidgetBaseConfig;
import com.google.common.collect.Lists;


/**
 * On some wiki page add
 * 
 * <code>
 *  ==OpenCube Map View==
 * 
 * <br/>
 * stiek.vlaanderen.be/dataset/inburgering#id>' |asynch='true' |mapzoom='BE'  }}
 * {{#widget:org.certh.opencube.mapview.MapView
 * | dataCubeURI = 'Enter the cube URI'
 * |asynch='true'
 * |mapzoom='BE' OR "EU"
 * }}
 * </code>
 * 
 */  
@TypeConfigDoc("The OpenCube Map View enables the visualization on a map of RDF data cubes based on their geospatial dimension.")
public class MapView extends AbstractWidget<MapView.Config> {

	// The URI of the cube
	private String cubeSliceURI;

	// The SPARQL service to get data (not required)
	private String SPARQL_service;

	// True if code list will be used to get the cube dimension values
	private boolean useCodeLists;

	// The graph of the cube
	private String cubeGraph;

	// The graph of the cube structure
	private String cubeDSDGraph;

	// The central container
	private FContainer cnt = null;

	// All the cube dimensions
	private List<LDResource> cubeDimensions = new ArrayList<LDResource>();

	// The geospatial dimension of the cube
	private LDResource geodimension = null;

	// All the cube attributes
	private List<LDResource> cubeAttributes = new ArrayList<LDResource>();

	// The measure of the cube
	private List<LDResource> cubeMeasure = new ArrayList<LDResource>();

	// The values of all the dimensions of the cube
	private HashMap<LDResource, List<LDResource>> allDimensionsValues = new HashMap<LDResource, List<LDResource>>();

	// The selected (from the dropdown lists) values of the fixed dimensions
	private HashMap<LDResource, LDResource> fixedDimensionsSelectedValues = new HashMap<LDResource, LDResource>();

	// The FHTML that contains the maps
	private FHTML map = null;

	// The label of the "type of visualization" dropdown list component
	private FLabel visTypeLabel = null;

	// A list with the supported types of visualization
	private List<String> visualizations = new ArrayList<String>();

	private HashMap<LDResource, List<FComponent>> dimensionURIfcomponents = new HashMap<LDResource, List<FComponent>>();

	// The grid that organizes the dropdown lists
	private FGrid grid = null;
	
	// The grid that organizes the top menu 
	private FGrid topgrid = null;

	// The FCombobox that contains the types of visualizations
	private FComboBox vistypes = null;

	// The FCombobox that contains the values of the dimensions of the cube
	private FComboBox fDimCombo = null;

	// The javascript code that creates the maps
	//	private String javascript = null;

	// Used to define the names of the flabels of the comboboxes
	private int label = 0;

	// Used to define the names of the fcomboboxes
	private int combo = 0;

	// True if URI is type qb:DataSet
	private boolean isCube;

	// True if URI is type qb:Slice
	private boolean isSlice;

	// used to show the create slice button after a map is visualized
	private boolean mapShown = false;
	
	// used to show the create slice button after a map is visualized
	private boolean isFirstLoad = true;

	// True if cube/slice has a geo dimension
	private boolean hasGeoDimension;

	// All the cube observations - to be used to create a slice
	private List<LDResource> sliceObservations = new ArrayList<LDResource>();

	// The graph of the slice
	private String sliceGraph = null;

	// All the dimensions of all cubes of the same aggregation set
	private List<LDResource> aggregationSetDims = new ArrayList<LDResource>();

	// All the dimensions per cube of the aggregation set
	private HashMap<LDResource, List<LDResource>> cubeDimsOfAggregationSet = new HashMap<LDResource, List<LDResource>>();

	// The slice fixed dimensions
	private List<LDResource> sliceFixedDimensions = new ArrayList<LDResource>();

	// The selected value for each fixed dimension
	private HashMap<LDResource, LDResource> sliceFixedDimensionsValues = new HashMap<LDResource, LDResource>();
	
	// A map with the Aggregation Set Dimension URi and the corresponding Check box
	private HashMap<LDResource, FCheckBox> mapDimURIcheckBox = new HashMap<LDResource, FCheckBox>();
	
	// The top container to show the check boxes with the available aggregation
	// set dimensions
	private FContainer topcontainer = new FContainer("topcontainer");
	
	// The visualization type container 
	private FContainer visTypeContainer = new FContainer("visTypeContainer");
	
	// The fixed dimensions container 
	private FContainer fixedDimContainer = new FContainer("fixedDimContainer");
	
	// The right container to show the combo boxes with the fixed dimensions values
	private FContainer languagecontainer = new FContainer("languagecontainer");
	
	//The available languages of the cube
	private List<String> availableLanguages;
		
	//The selected language
	private String selectedLanguage;
	
	
	//The default language
	private String defaultLang;

	private static String mapzoom; 
	
	//Ignore multiple languages
	private boolean ignoreLang;
	
	final FClientUpdate update = new FClientUpdate("javascript");

	public static class Config extends WidgetBaseConfig {
		@ParameterConfigDoc(desc = "The data cube URI to visualise", required = true)
		public String dataCubeURI;

		@ParameterConfigDoc(desc = "Use code lists to get dimension values", required = false)
		public boolean useCodeLists;

		@ParameterConfigDoc(desc = "SPARQL service to forward queries", required = false)
		public String sparqlService;

		@ParameterConfigDoc(desc = "The default language", required = false)
		public String defaultLang;	
		
		@ParameterConfigDoc(desc = "Use multiple languages", required = false)
		public boolean ignoreLang;
		
		@ParameterConfigDoc(desc = "Set map zoom", required = false)
		public String mapzoom;
		
	}

	@Override
	public List<String> jsURLs() {
		String cp = EndpointImpl.api().getRequestMapper().getContextPath();
				
		return Lists
				.newArrayList(
						cp + "//cdn.leafletjs.com/leaflet-0.7.3/leaflet.js",
						cp + "//rawgit.com/simogeo/geostats/master/lib/geostats.js",
						cp + "//leaflet.github.io/Leaflet.heat/dist/leaflet-heat.js",
						cp + "//open.mapquestapi.com/sdk/leaflet/v1.0//mq-map.js?key=Fmjtd%7Cluur2d0znu%2Cbl%3Do5-9a82dw",
						cp + "//open.mapquestapi.com/sdk/leaflet/v1.0/mq-geocoding.js?key=Fmjtd%7Cluur2d0znu%2Cbl%3Do5-9a82dw");
					//	cp + "//www.mapquestapi.com/sdk/leaflet/v1.0/mq-map.js?key=Fmjtd%7Cluur2d0znu%2Cbl%3Do5-9a82dw",
					//	cp + "//www.mapquestapi.com/sdk/leaflet/v1.0/mq-geocoding.js?key=Fmjtd%7Cluur2d0znu%2Cbl%3Do5-9a82dw");
   	}
	
	@Override
	protected FComponent getComponent(String id) {		
		 
		final Config config = get();

		// Central container
		cnt = new FContainer(id);
		
		// Get the cube URI from the widget configuration
		cubeSliceURI = config.dataCubeURI;

		// Get the SPARQL service (if any) from the cube configuration
		SPARQL_service = config.sparqlService;

		// The use code list parameter from the widget config
		useCodeLists = config.useCodeLists;
				
		mapzoom = config.mapzoom;

		if(mapzoom==null || mapzoom.equals("")){
			mapzoom = "EU";
		}
		
		//Use multiple languages, get parameter from widget configuration
		ignoreLang=config.ignoreLang;
		
		if(!ignoreLang){
			defaultLang = config.defaultLang;
			
			if(defaultLang==null || defaultLang.equals("")){
				defaultLang=com.fluidops.iwb.util.Config.getConfig().getPreferredLanguage();
			}
			selectedLanguage=defaultLang;
		}
		
		//populateMapViewContainer();
		addVisualizationTypes();
		cnt.add(visTypeContainer);
						
		return cnt;
	}

	//Populate the Map View container
	private void populateMapViewContainer() {
		
		System.out.println("lalala:"+new File("ppppp").getAbsolutePath());  
		
		// Get Cube/Slice Graph
		String cubeSliceGraph = CubeSPARQL.getCubeSliceGraph(cubeSliceURI, SPARQL_service);

		// Get the type of the URI i.e. cube / slice
		List<String> cubeSliceTypes = CubeSPARQL.getType(cubeSliceURI, cubeSliceGraph,	SPARQL_service);

		if (cubeSliceTypes != null) {
			// The URI corresponds to a data cube
			isCube = cubeSliceTypes.contains("http://purl.org/linked-data/cube#DataSet");

			// The URI corresponds to a cube Slice
			isSlice = cubeSliceTypes.contains("http://purl.org/linked-data/cube#Slice");

		} else {
			isCube = false;
			isSlice = false;
		}

		// If the URI is a valid cube or slice URI
		if (isCube || isSlice) {
			hasGeoDimension = false;

			if (isCube) {

				// The cube graph is the graph of the URI computed above
				cubeGraph = cubeSliceGraph;

				// Get Cube Structure graph
				cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(cubeSliceURI,
						cubeGraph, SPARQL_service);
				
				if(!ignoreLang){
					//Get the available languages of labels
					availableLanguages=CubeSPARQL.getAvailableCubeLanguages(
							cubeDSDGraph,SPARQL_service);
	
					//get the selected language to use
					selectedLanguage=CubeHandlingUtils.getSelectedLanguage(
							availableLanguages, selectedLanguage);
				}

				// Get the geospatial dimension of the cube
				geodimension = CubeSPARQL.getGeoDimension(cubeSliceURI,
						cubeGraph, cubeDSDGraph, selectedLanguage,defaultLang,ignoreLang, SPARQL_service);

				if (geodimension != null) {

					hasGeoDimension = true;

					// Get all Cube dimensions
					cubeDimensions = CubeSPARQL.getDataCubeDimensions(
							cubeSliceURI, cubeGraph, cubeDSDGraph,
							selectedLanguage,defaultLang,ignoreLang,SPARQL_service);

					// Get the Cube measure
					cubeMeasure = CubeSPARQL.getDataCubeMeasure(cubeSliceURI,
							cubeGraph, cubeDSDGraph,selectedLanguage,defaultLang,ignoreLang, SPARQL_service);

					// Get all the dimensions of the aggregation set the cube
					// belongs
					aggregationSetDims = AggregationSPARQL
							.getAggegationSetDimsFromCube(cubeSliceURI,
									cubeDSDGraph, selectedLanguage,defaultLang,ignoreLang,SPARQL_service);

					// Get all the dimensions per cube of the aggregations set
					cubeDimsOfAggregationSet = AggregationSPARQL
							.getCubeAndDimensionsOfAggregateSet(cubeSliceURI,
									cubeDSDGraph, selectedLanguage,defaultLang,ignoreLang,SPARQL_service);

					// Get the attributes of the cube
					cubeAttributes = CubeSPARQL.getDataCubeAttributes(
							cubeSliceURI, cubeGraph, cubeDSDGraph,selectedLanguage,
							defaultLang,ignoreLang,	SPARQL_service);

					// Get values for each cube dimension
					allDimensionsValues = CubeHandlingUtils.getDimsValues(cubeDimensions,
							cubeSliceURI, cubeGraph, cubeDSDGraph, useCodeLists, 
							selectedLanguage,defaultLang,ignoreLang,SPARQL_service);
				}

			} else if (isSlice) {

				// The slice graph is the graph of the URI computed above
				sliceGraph = cubeSliceGraph;

				// Get the cube graph from the slice
				cubeGraph = SliceSPARQL.getCubeGraphFromSlice(cubeSliceURI,	sliceGraph, SPARQL_service);

				// Get Cube Structure graph from slice
				cubeDSDGraph = SliceSPARQL.getCubeStructureGraphFromSlice(
						cubeSliceURI, sliceGraph, SPARQL_service);
				
				if(!ignoreLang){
					//Get the available languages of labels
					availableLanguages=CubeSPARQL.getAvailableCubeLanguages(
							cubeDSDGraph,SPARQL_service);
	
					//get the selected language to use
					selectedLanguage=CubeHandlingUtils.getSelectedLanguage(
							availableLanguages, selectedLanguage);
				}

				// Get the geospatial dimension of the cube
				geodimension = SliceSPARQL.getGeoDimension(cubeSliceURI,
						sliceGraph, cubeDSDGraph,selectedLanguage, SPARQL_service);

				// Get slice fixed dimensions
				sliceFixedDimensions = SliceSPARQL.getSliceFixedDimensions(
						cubeSliceURI, sliceGraph, cubeDSDGraph, selectedLanguage,
						defaultLang,ignoreLang,SPARQL_service);

				// if the slice has a geo dimensions
				// and it is not defined as a fixed value
				if (geodimension != null && !sliceFixedDimensions.contains(geodimension)) {

					hasGeoDimension = true;

					sliceFixedDimensionsValues = SliceSPARQL.getSliceFixedDimensionsValues(
									sliceFixedDimensions, cubeSliceURI,
									sliceGraph, cubeDSDGraph, selectedLanguage, defaultLang,
									ignoreLang,SPARQL_service);

					// Get all cube dimensions
					List<LDResource> cubeDimsFromSlice = SliceSPARQL.getDataCubeDimensionsFromSlice(
							cubeSliceURI, sliceGraph, cubeDSDGraph,
							selectedLanguage,defaultLang,ignoreLang,SPARQL_service);

					// The slice visual dimensions are:(all cube dims) - (slice fixed dims)
					cubeDimensions = cubeDimsFromSlice;
					cubeDimensions.removeAll(sliceFixedDimensions);

					// Get the Cube measure
					cubeMeasure = SliceSPARQL.getSliceMeasure(cubeSliceURI,
							sliceGraph, cubeDSDGraph,selectedLanguage,
							defaultLang,ignoreLang,SPARQL_service);

					// Get the attributes of the cube
					cubeAttributes = SliceSPARQL.getDataCubeAttributesFromSlice(cubeSliceURI,
									sliceGraph, cubeDSDGraph,selectedLanguage,
									SPARQL_service);

					// Get values for each slice dimension
					allDimensionsValues = CubeHandlingUtils.getDimsValuesFromSlice(cubeDimensions,
									cubeSliceURI, cubeGraph, cubeDSDGraph, sliceGraph,
									useCodeLists,selectedLanguage,defaultLang,ignoreLang,SPARQL_service);
				}
			}

			// In case there is no geo-dimension
			if (hasGeoDimension) {

				// Create the FHTML object that will contain the maps
				map = new FHTML("mapping");
				
				topgrid=new FGrid("topgrid");
				ArrayList<FComponent> topfarray = new ArrayList<FComponent>();
				
				addVisualizationTypes();
				
				topfarray.add(visTypeContainer);
				
				// Add top container if it is a cube not slice
				if (isCube) {

					// top container styling
					topcontainer.addStyle("border-style", "solid");
					topcontainer.addStyle("border-width", "1px");
					topcontainer.addStyle("padding", "10px");
					topcontainer.addStyle("border-radius", "5px");
					topcontainer.addStyle("border-color", "#C8C8C8 ");
					topcontainer.addStyle("display", "table-cell ");
					topcontainer.addStyle("vertical-align", "middle ");
					topcontainer.addStyle("margin-left", "auto");
					topcontainer.addStyle("margin-right", "auto");
					topcontainer.addStyle("height", "130px ");
					topcontainer.addStyle("width", "430px ");
					topcontainer.addStyle("text-align", "left");
					
					// If an aggregation set has already been created
					if (aggregationSetDims.size() > 0) {
						
						FLabel OLAPbrowsing_label = new FLabel("OLAPbrowsing_label",
								"<b>Dimensions</b></br>"
										+ "Summarize observations by adding/removing dimensions:</br>");
						topcontainer.add(OLAPbrowsing_label);
						
						// Show Aggregation set dimensions
						for (LDResource aggdim :aggregationSetDims) {

							if (!aggdim.getURI().equals(geodimension.getURI())) {
							
								String checkBoxID=aggdim.getURI();
								
								//remove characters that are supported by the FComponent ID
								checkBoxID=checkBoxID.replaceAll("//","");
								checkBoxID=checkBoxID.replaceAll("#","");
								checkBoxID=checkBoxID.replaceAll(":","");
								checkBoxID=checkBoxID.replaceAll("_","");
								
								//IWB does not support too long IDs
								if(checkBoxID.length()>10){
									//use as ID the last part of the URI (the first part is common to all URIs)
									checkBoxID=checkBoxID.substring(checkBoxID.length()-11,checkBoxID.length());
								}
								// show one check box for each aggregation set dimension
								FCheckBox aggrDimCheckBox = new FCheckBox("aggregation_" + checkBoxID,
										aggdim.getURIorLabel()) {
	
									public void onClick() {
			
	
										// Get all selected aggregation set dimensions
										// for browsing
										List<LDResource> aggregationSetSelectedDims = new ArrayList<LDResource>();
										for (LDResource aggSetDimURI :mapDimURIcheckBox
												.keySet()) {
											FCheckBox check = mapDimURIcheckBox.get(aggSetDimURI);
	
											// Get selected dimensions
											if (check.checked) {
												aggregationSetSelectedDims.add(aggSetDimURI);
											}
	
										}

										//show only cube with geodimension
										aggregationSetSelectedDims.add(geodimension);
	
										// Identify the cube of the aggregation set that
										// contains exactly the dimension selected to be
										// browsed
										for (LDResource cube :cubeDimsOfAggregationSet.keySet()) {
											List<LDResource> cubeDims = cubeDimsOfAggregationSet
													.get(cube);
											if ((cubeDims.size() == aggregationSetSelectedDims.size()) 
													&& cubeDims.containsAll(aggregationSetSelectedDims)) {
												
												System.out.println("NEW CUBE URI:"+ cube.getURI());
	
												// The new cube to visualize
												cubeSliceURI = "<" + cube.getURI()+ ">";
	
												// clear the previous visualization and
												// create a new one for the new cube
												languagecontainer.removeAll();
												fixedDimContainer.removeAll();
												visTypeContainer.removeAll();
												topgrid.removeAll();
												topcontainer.removeAll();
												dimensionURIfcomponents.clear();
												grid.removeAll();
												visualizations.clear();
												cnt.removeAll();
												label = 0;
												combo = 0;
												mapShown=false;
	
												// show the cube
												populateMapViewContainer();
												createMap(this);
												addSliceCreate();
												break;
											}
										}
									}
	
								};
	
								// set as checked if the dimension is contained at the
								// selected cube
								aggrDimCheckBox.setChecked(cubeDimensions.contains(aggdim));
								mapDimURIcheckBox.put(aggdim, aggrDimCheckBox);
								topcontainer.add(aggrDimCheckBox);
							}
						}
					} else {
						FLabel notOLAP = new FLabel("notOLAP",
								"<b>OLAP-like browsing is not supported for this cube<b>");
						topcontainer.add(notOLAP);
					}					
					
					topfarray.add(topcontainer);
					
				}
				
				if(!ignoreLang){
					// language container styling
					languagecontainer.addStyle("border-style", "solid");
					languagecontainer.addStyle("border-width", "1px");
					languagecontainer.addStyle("padding", "10px");
					languagecontainer.addStyle("border-radius", "5px");
					languagecontainer.addStyle("border-color", "#C8C8C8 ");
					languagecontainer.addStyle("margin-left", "auto");
					languagecontainer.addStyle("margin-right", "auto");
					languagecontainer.addStyle("text-align", "left");
					languagecontainer.addStyle("width", "150px ");
					languagecontainer.addStyle("height", "130px ");
											
					FLabel datalang_label = new FLabel("datalang","<b>Language</b></br>" +
									"Select the language of the visualized data:");
					languagecontainer.add(datalang_label);
	
					// Add Combo box for language selection
					 FComboBox datalang_combo = new FComboBox("datalang_combo") {
							@Override
							public void onChange() {
								selectedLanguage=this.getSelected().get(0).toString();
								languagecontainer.removeAll();
								fixedDimContainer.removeAll();
								visTypeContainer.removeAll();
								topgrid.removeAll();
								topcontainer.removeAll();
								dimensionURIfcomponents.clear();
								grid.removeAll();
								visualizations.clear();
								cnt.removeAll();
								label = 0;
								combo = 0;
								mapShown=false;
	
								// show the cube
								populateMapViewContainer();
								createMap(this);
								addSliceCreate();
							}
					};
	
					// populate language combo box
					for (String lang :availableLanguages) {
						datalang_combo.addChoice(lang,lang);
					}
					datalang_combo.setPreSelected(selectedLanguage);
					languagecontainer.add(datalang_combo);
					topfarray.add(languagecontainer);
				}
				
				topgrid.addRow(topfarray);
				cnt.add(topgrid);
							
				// Visualization Type Container styling
				fixedDimContainer.addStyle("border-style", "solid");
				fixedDimContainer.addStyle("border-width", "1px");
				fixedDimContainer.addStyle("padding", "10px");
				fixedDimContainer.addStyle("border-radius", "5px");
				fixedDimContainer.addStyle("border-color", "#C8C8C8 ");
				fixedDimContainer.addStyle("display", "table-cell ");
				fixedDimContainer.addStyle("vertical-align", "middle ");
				fixedDimContainer.addStyle("margin-left", "auto");
				fixedDimContainer.addStyle("margin-right", "auto");
				fixedDimContainer.addStyle("width", "900px ");
				fixedDimContainer.addStyle("text-align", "left");
								
				FHTML text = new FHTML("text");
				text.setValue("<b>Fixed dimensions</b></br>" +
						"Change the values of the fixed dimensions:");
				fixedDimContainer.add(text);

				grid=new FGrid("grid");
				
				// If it is a slice and there are fixed dimension values
				if (!sliceFixedDimensionsValues.isEmpty()) {
								
					for (LDResource sliceFixedDim :sliceFixedDimensionsValues.keySet()) {

						ArrayList<FComponent> farray = new ArrayList<FComponent>();
						Random rnd = new Random();
						Long rndLong = Math.abs(rnd.nextLong());
						
						String id=sliceFixedDim.getURI();
						
						//remove characters that are supported by the FComponent ID
						id=id.replaceAll("//","");
						id=id.replaceAll("#","");
						id=id.replaceAll(":","");
						
						//IWB does not support too long IDs
						if(id.length()>10){
							//use as ID the last part of the URI (the first part is common to all URIs)
							id=id.substring(id.length()-11,id.length());
						}
						
						// Add the label for the fixed cube dimension
						FLabel fDimLabel = new FLabel(id + "_" + rndLong
										+ "_name", "<u>"+ sliceFixedDim.getURIorLabel()+ ":</u>");

						farray.add(fDimLabel);

						// rightcontainer.add(fDimLabel);

						LDResource fDimValue = sliceFixedDimensionsValues
								.get(sliceFixedDim);
						FLabel fDimValueLabel = new FLabel(	id + "_" + rndLong
										+ "_value", fDimValue.getURIorLabel());

						farray.add(fDimValueLabel);
						grid.addRow(farray);
						farray = new ArrayList<FComponent>();
						grid.addRow(farray);
					}
				}
				
				// Selected values for the fixed dimensions
				fixedDimensionsSelectedValues = CubeHandlingUtils
						.getFixedDimensionsRandomSelectedValues(
								allDimensionsValues, cubeDimensions,fixedDimensionsSelectedValues);


				// Dynamically create as comboboxes as the number of the
				// dimensions of the cube (except for the geospatial dimension)
				for (LDResource fDim :cubeDimensions) {

					// Remove the geo dimension from the combo boxes
					if (!fDim.getURI().equals(geodimension.getURI())) {
						List<FComponent> dimComponents = new ArrayList<FComponent>();

						// Add label
						FLabel fDimLabel = null;
						if (fDim.getURIorLabel() != null) {
							fDimLabel = new FLabel((new Integer(label++).toString())
								+ "_label", "<u>"+ fDim.getURIorLabel().replace(".","-") + 
								"</u> ");
						} else
							fDimLabel = new FLabel((new Integer(label++).toString())
											+ "_label", "<b></b> <br>");

						dimComponents.add(fDimLabel);
						fDimLabel.addStyle("float", "left");

						// Add Combobox
						fDimCombo = new FComboBox((new Integer(combo++).toString()) + "_combo") {
							@Override
							public void onChange() {
								createMap(this);
							}
						};

						// Add choices to the combo box
						for (LDResource ldr :allDimensionsValues.get(fDim)) {
							//if label is too long show the first 100 chars
							if(ldr.getURIorLabel().length()>100){
								fDimCombo.addChoice(ldr.getURIorLabel().substring(0,100)+"...",ldr.getURI());
							}else{
								fDimCombo.addChoice(ldr.getURIorLabel(),ldr.getURI());
							}
						}
						
						// Combo box pre-selected value
						fDimCombo.setPreSelected(fixedDimensionsSelectedValues.get(fDim).getURI());

						dimComponents.add(fDimCombo);
						fDimCombo.addStyle("float", "left");
						FContainer newcnt_label = new FContainer("newcntl"+ label++);
						FContainer newcnt_combo = new FContainer("newcntc"+ label++);
						// newcnt_label.addStyle("width", "400px");
						newcnt_label.add(fDimLabel);
						newcnt_combo.add(fDimCombo);
						ArrayList<FComponent> farray = new ArrayList<FComponent>();

						farray.add(newcnt_label);
						farray.add(newcnt_combo);
						grid.addRow(farray);
						grid.render();

						// Add both components to the URI - Component list Map
						dimensionURIfcomponents.put(fDim, dimComponents);
					}
				}
				
				fixedDimContainer.add(grid);
				
				cnt.add(map);
				cnt.add(getNewLineComponent(true));
				cnt.add(fixedDimContainer);
				

			} else {
				String uri = cubeSliceURI.replaceAll("<", "");
				uri = uri.replaceAll(">", "");
				String message = "The cube with the URI <b>" + uri
						+ "</b> has no valid geospatial dimension.";
				FLabel invalidURI_label = new FLabel("invalidURI", message);
				cnt.add(invalidURI_label);
			}

		} else {
			String uri = cubeSliceURI.replaceAll("<", "");
			uri = uri.replaceAll(">", "");
			String message = "The URI <b>" + uri + "</b> is not a valid cube or slice URI.";
			FLabel invalidURI_label = new FLabel("invalidURI", message);
			cnt.add(invalidURI_label);
		}
	}
	
	private void addVisualizationTypes(){
		// Visualization Type Container styling
		visTypeContainer.addStyle("border-style", "solid");
		visTypeContainer.addStyle("border-width", "1px");
		visTypeContainer.addStyle("padding", "10px");
		visTypeContainer.addStyle("border-radius", "5px");
		visTypeContainer.addStyle("border-color", "#C8C8C8 ");
		visTypeContainer.addStyle("display", "table-cell ");
		visTypeContainer.addStyle("height", "130px ");
		visTypeContainer.addStyle("width", "250px ");
		visTypeContainer.addStyle("margin-left", "auto");
		visTypeContainer.addStyle("margin-right", "auto");
		visTypeContainer.addStyle("text-align", "left");
		
		// Add label for the dropdown that selects the type of visualization
		visTypeLabel = new FLabel("dim1Label","<b>Type of map</b></br>" +
				"In order to view the map please select one of the following map types:");
		visTypeLabel.addStyle("float", "left");
		visTypeContainer.add(visTypeLabel);

		// Add the types of the supported visualizations
		visualizations.add(0, "Visualization type");
		visualizations.add(1, "Markers map");
		visualizations.add(2, "Bubble map");
		visualizations.add(3, "Choropleth map");

		// Create the FComboBox with the supported types of visualizations.
		String selected="";
		
		if(vistypes!=null){
		 selected=(String)vistypes.getSelected().get(0);
		}
		
		vistypes = new FComboBox("vistypes") {
			@Override
			public void onChange() {
				if(isFirstLoad){
					visTypeContainer.removeAll();
					visualizations.clear();
					cnt.removeAll();
					populateMapViewContainer();
					isFirstLoad=false;
				}
				createMap(this);
				addSliceCreate();
			}
		};
		
		vistypes.addChoices(visualizations);
		
		if(vistypes!=null){
			vistypes.setPreSelected(selected);
		}
		
		
		visTypeContainer.add(vistypes);
		
		
	}
	
	private void addSliceCreate(){
		// Show the create slice button if there are more that one dimension
		// i.e. one more dimension than the geodimension
		if (cubeDimensions.size() > 1 && !mapShown && isCube) {
			mapShown = true;
			FContainer bottomcontainer = new FContainer("bottomcontainer");

			// Bottom container styling
			bottomcontainer.addStyle("border-style", "solid");
			bottomcontainer.addStyle("border-width", "1px");
			bottomcontainer.addStyle("padding", "10px");
			bottomcontainer.addStyle("border-radius", "5px");
			bottomcontainer.addStyle("width", "900px ");
			bottomcontainer.addStyle("border-color", "#C8C8C8 ");
			bottomcontainer.addStyle("display", "table-cell ");
			bottomcontainer.addStyle("vertical-align","middle ");
			bottomcontainer.addStyle("align", "center");
			bottomcontainer.addStyle("text-align", "left");
			
			FLabel bottomLabel = new FLabel("bottomlabel",
					"<b>Slice</b></br>" +
					"In case you want to create and store a slice of the cube as it is presented " +
					"in the MapView click the button:</br>");
		//	bottomLabel.addStyle("width", "900px ");
			bottomcontainer.add(bottomLabel);

			// Button to create slice
			FButton createSlice = new FButton("createSlice","createSlice") {
				@Override
				public void onClick() {
					String sliceURI = SliceSPARQL.createCubeSlice(cubeSliceURI,
									cubeGraph,fixedDimensionsSelectedValues,sliceObservations,SPARQL_service);
					String message = "A new slice with the following URI has been created:"
							+ sliceURI;
					FDialog.showMessage(this.getPage(),"New Slice created", message, "ok");
				}
			};

			bottomcontainer.add(createSlice);
			cnt.add(getNewLineComponent(false));
			cnt.add(bottomcontainer);
			cnt.populateView();
		}
		
	}
	private void createMap(FComponent comp){
		
		String maptext = "<div id=\"map\" style=\"width:920px; height:400px;\"></div>";
		map.setValue(maptext);

		String attr_labels = "";
		// Create the label of the geo-dimension that will be showed in the markers
		System.out.println("geo label  2 is "+geodimension.getURIorLabel());
		String geo_label = "'"+ geodimension.getURIorLabel().replaceAll("'","\'") + "'";
		System.out.println("geo label is "+geo_label);
		
		// Get all the attribute labels to show in markers
		for (LDResource attres :cubeAttributes) {
			if (attres.getURIorLabel() != null)
				attr_labels += "'" + attres.getURIorLabel().replaceAll("'","\'")+ "',";
			else
				attr_labels += "' '";
		}
		
		if(attr_labels.length()>0){
			attr_labels = attr_labels.substring(0,attr_labels.length() - 1);
		}
		
		String temp = "";
		HashMap<LDResource, LDResource> tmpFixedDimensionsSelectedValues = new HashMap<LDResource, LDResource>();
		for (LDResource dimres :dimensionURIfcomponents.keySet()) {
			System.out.println("dimres"	+ dimres.getURI());
			List<FComponent> dimComponents = dimensionURIfcomponents.get(dimres);
			String selectedValue = ((FComboBox) dimComponents.get(1)).getSelectedAsString().get(0);
			System.out.println("selected "+ selectedValue);
			List<LDResource> selectedDimValues = allDimensionsValues.get(dimres);
			for (LDResource dimValue :selectedDimValues) {
				if (dimValue.getURI().equals(selectedValue)) {
					tmpFixedDimensionsSelectedValues.put(dimres, dimValue);
				}
			}

		}

		fixedDimensionsSelectedValues=tmpFixedDimensionsSelectedValues;

		TupleQueryResult res_values = null;
		if (isCube) {
			// Get query tuples for visualization
			res_values = MapViewSPARQL
					.getDVisualsiationValues(cubeDimensions,geodimension,
							fixedDimensionsSelectedValues,cubeMeasure, cubeAttributes,
							cubeSliceURI, cubeGraph,cubeDSDGraph,selectedLanguage,
							SPARQL_service);
		} else if (isSlice) {
			// Get query tuples for visualization
			res_values = MapViewSPARQL
					.getDVisualsiationValuesFromSlice(cubeDimensions,geodimension,
							fixedDimensionsSelectedValues,cubeMeasure,	cubeAttributes,
							cubeSliceURI, sliceGraph, cubeGraph, cubeDSDGraph, selectedLanguage,
							SPARQL_service);
		}

		int results_size = cubeDimensions.size()+ cubeAttributes.size() + 1;

			
		
		
		Vector<Vector<String>> obsdata = getResults(res_values, results_size);
		System.out.println("results size is"+obsdata.get(0).size());
		int all = obsdata.get(0).size();
		
		
		
		
		// dynamically gather selected values from each dimension combobox
		// dynamically create strings with the selected values for input in the map's bubbles
		for (LDResource fDim :fixedDimensionsSelectedValues.keySet()) {
			if (fDim.getURIorLabel() != null)
				temp += "<b>" + fDim.getURIorLabel()	+ ":</b>";
			else
				temp += "";
			temp += fixedDimensionsSelectedValues.get(fDim).getURIorLabel()	+ "<br />";
		}

		String values = "";
		for (int i = 0; i < obsdata.get(0).size(); i++) {
			values += "'" + temp.replaceAll("'", " ") + "',";
		}
		String newvalues = values.substring(0,values.length() - 1);

		String measurevalues = obsdata.get(results_size - 1).toString().replaceAll("'"," ");
		measurevalues = "'"	+ obsdata.get(results_size - 1).get(0).replaceAll("'"," ")	+ "'";

		String geovalues = obsdata.get(results_size - 2).toString().replaceAll("'"," ");
		geovalues = "'"+ obsdata.get(results_size - 2).get(0).replaceAll("'"," ")+ "'";

		List<String> attributeValues = new ArrayList<String>();

		for (int i = 0; i < cubeAttributes.size(); i++) // Areti
		{
			String tmp2 = obsdata.get(results_size - 3 - i).toString().replaceAll("'"," ");
			tmp2 = "'"+ obsdata.get(results_size - 3 - i).get(0).replaceAll("'"," ") + "'";
			System.out.println("Tmp:::" + tmp2);
			attributeValues.add(tmp2);
		}

		for (int i = 1; i < obsdata.get(results_size - 2).size(); i++) {
			geovalues += ",'"+ obsdata.get(results_size - 2).get(i).replaceAll("'"," ") + "'";
			measurevalues += ",'"+ obsdata.get(results_size - 1).get(i).replaceAll("'"," ") + "'";
			for (int j = 0; j < attributeValues.size(); j++) // Areti
			{
				String tmp = attributeValues.get(j);
				tmp += ",'"	+ obsdata.get(results_size - 3 - j).get(i).replaceAll("'"," ") + "'";
				attributeValues.remove(j);
				attributeValues.add(j, tmp);
			}
		}
		String javascript="";
		// According to the selected value of visualization create the corresponding map
		if (vistypes.getSelectedAsString().toString().contains("Markers")) {
					
			  javascript= getMarkerJavascript(geovalues, measurevalues,
					newvalues, attributeValues,	cubeAttributes.size(), geo_label,attr_labels, all);
			
			update.clientCode = javascript;
			
			comp.addClientUpdate(update);
					javascript ="";
		}
			
		else if (vistypes.getSelectedAsString().toString().contains("Bubble")) {
			  javascript = getBubbleJavascript(geovalues, measurevalues,
					newvalues, attributeValues,cubeAttributes.size(), geo_label,attr_labels);
			update.clientCode = javascript;
			comp.addClientUpdate(update);
			javascript ="";
			
		}
		else if (vistypes.getSelectedAsString().toString().contains("Heat")) {
			
			javascript = getHeatMapJavascript(geovalues, measurevalues, newvalues);
			update.clientCode = javascript;
			comp.addClientUpdate(update);
			javascript ="";
		}
		else if (vistypes.getSelectedAsString().toString().contains("Choropleth")) {
			 javascript = getChoroplethMapJavascript(geovalues, measurevalues,
					newvalues, attributeValues,	cubeAttributes.size(), attr_labels);
			update.clientCode = javascript;
			comp.addClientUpdate(update);
			javascript ="";
		}
				
		// If this is not the first load of the widget
		if (!isFirstLoad) {
			cnt.populateView();				
		}
	}
	@Override
	public String getTitle() {
		return "Data Cube Map View widget";
	}

	@Override
	public Class<?> getConfigClass() {
		return Config.class;
	}

	/*
	 * Input:a TupleQueryResult from which the values of the dimensions will be
	 * retrieved, the number of the queried dimensions Output:a 2D Vector with
	 * the values of the dimensions of the cube
	 */
	public Vector<Vector<String>> getResults(TupleQueryResult res,
			int ndimensions) {
		// The 2D Vector
		Vector<Vector<String>> dimensions = new Vector<Vector<String>>();

		// Create one sub-Vector for each of the dimensions of the cube
		for (int i = 0; i < ndimensions; i++) {
			dimensions.add(i, new Vector<String>());
		}

		sliceObservations.clear();
		try {

			List<String> bindingNames = res.getBindingNames();

			// for all the results of the query get the returned values and put
			// them into the @D Vector
			while (res.hasNext()) {

				BindingSet bindingSet = res.next();
		
				int i = 0;
				for (String bindingName :bindingNames) {
					//Store slice observation for future use i.e. to create slice if needed
					if (bindingName.equals("obs")) {
						LDResource ld = new LDResource(bindingSet.getValue(bindingName).stringValue());
						sliceObservations.add(ld);
					} else {
						String dim1Val = bindingSet.getValue(bindingName).stringValue();
						Vector<String> dimensionsi = dimensions.get(i);
						dimensionsi.add(dim1Val);
						dimensions.set(i, dimensionsi);
						i++;
					}
				}
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		return dimensions;

	}

	/*
	 * Input:Strings with the i) values of the geospatial dimension of the
	 * cube, ii) values of the measure of the cube and iii) values of the rest
	 * of the dimensions of the cube, a list of strings with the values of the
	 * attributes of the cube, the number of the attributes of the cube, a
	 * String with the labels of the geospatial values of the cube and a String
	 * with the labels of the attribute values of the cube Output:A String with
	 * the javascript to create the marker map
	 */
		
	public static String getMarkerJavascript(String geovalue,
			String measurevalue, String dimvalues,
			List<String> attributeValues, int cubeAttributesSize,
			String geo_label, String attr_labels, int iterations) {
		String markermap = null;
		//int nofiterations = iterations/100;
	 
	//    String input = geovalue;
	

		System.out.println("mpla:" + attr_labels);
		
	
		// Create a javascript array with the values of the measure
		markermap = "var measure = new Array();";
		
		// Create a javascript array with the values of the geospatial dimension
		markermap += "var arr = new Array (" + geovalue + "); ";
					
		
		// Create a javasript array with the values of the measure
		markermap += "measure = new Array(" + measurevalue + ");";
	
		// Create a javasript array with the values of the dimensions (except
		// for the geospatial dimension)
		markermap += "observ = new Array(" + dimvalues + ");";
		
		// Create one javascript array for the values of each of the attributes of the cube
		for (int j = 0; j < cubeAttributesSize; j++)
			markermap += "attribute" + j + " = new Array("
					+ attributeValues.get(j) + ");";

		// Create a javascript array with the labels of the attributes of the cube		
		markermap += "attr_labels = new Array(" + attr_labels + ");";

		System.out.println("geovalue "+geovalue);
		
		// Perform the geocoding
		markermap +="var counter = 0;";
		
		
		markermap += "var i,j,temparray, tempmeasure, chunk = 100;";
	
		markermap +="var geova = new Array(" + geovalue +");";
			markermap +="group = [];";	
	
		
		markermap +="var results, k, m=0, all=[], news, features, marker, result, latlng, prop, best, val, map, r, i;";
		//markermap += "map = L.map('map').setView([51.505, -0.09], 13); L.tileLayer('https://{s}.tiles.mapbox.com/v3/{id}/{z}/{x}/{y}.png', {maxZoom:18,attribution:'Map data &copy; <a href=\"http://openstreetmap.org\">OpenStreetMap</a> contributors, ' + '<a href=\"http://creativecommons.org/licenses/by-sa/2.0/\">CC-BY-SA</a>, ' +'Imagery &copy; <a href=\"http://mapbox.com\">Mapbox</a>', id:'examples.map-i86knfo3'}).addTo(map);";
		markermap +="map = L.map('map', {layers: MQ.mapLayer() });";
		markermap +="var count=0;";
		markermap +="var  o = 0;";
		markermap +="news =[];";
		markermap +="for (k=0,j=geova.length; k<j; k+=chunk) {";
		markermap +="temparray = geova.slice(k,k+chunk); ";
		markermap +="tempmeasure = measure.slice(k,k+chunk); ";
		markermap +="";
		markermap +="var temp = temparray;";
		
		markermap +=" (function (n, meas) { ";
		markermap +="setTimeout(function(){";
		
		
		//markermap +="alert(n);";
		markermap += "MQ.geocode().search(n) .on('success', ";
		markermap += "function(e) "
				+ "{ results =e.result;";
			markermap +="for (i = 0; i < results.length; i++) { ";
		//	markermap += "for (i = k; i < ((k/chunk)+1)*results.length; i++) { ";
			
			markermap += "result = results[i].best;  ";
			markermap += "latlng = result.latlng; ";
		//	markermap +="all.push(latlng);";
			
			markermap +="}";
			
		//markermap +="all.push(results);";
		// Get the lat and long
		
					
	//	markermap +="news='new';";
		//markermap +="var t = k+chunk;";
		//markermap +="alert(results.length);";
		//markermap +="}, 100);";
		//markermap +=" }(geova.slice(k,k+chunk)));";
		
	//	markermap +="}";
		
		
	//	markermap +="alert(news);";
		//markermap +="alert(all.length);";
		//markermap +="for (i = 0; i < results.length; i++) { ";
		markermap +="for (i = 0; i < results.length; i++) { ";
	//	markermap += "for (i = k; i < ((k/chunk)+1)*results.length; i++) { ";
		
	markermap += "result = results[i].best;  ";
		markermap += "latlng = result.latlng; ";
		//markermap+="count++;";
		
	//	markermap +="for (i =0; i < all.length; i++) {";
		
		// Create the markers
		
		markermap += "marker = L.marker();";
		markermap += "marker.setLatLng([ latlng.lat, latlng.lng]); ";
	   
	   
	  // markermap += "for (i = k; i < t; i++) { ";
		// Create the content of the markers' popup
		
		markermap += "marker.bindPopup( \"<b>\" + "
				+ geo_label
				+ "+ \":\"+ \"</b>\" + n[i]+  \"<br />\" + observ[o]+ +meas[i]";
		
		for (int j = 0; j < cubeAttributesSize; j++)
			markermap += "+ \"<br />\"+ \"<b>\" +attr_labels[" + j
					+ "] +\":\"+ \"</b>\" +attribute" + j + "[o]";
		
		markermap += ");";
		//markermap +="alert(o);";
		markermap += "o++;";
	//	markermap +="m++;";
		//markermap += "if (m < geova.length) {";
		markermap += "var popup = marker._popup;";
		markermap += "group.push(marker); } ";
				
		markermap += "features = L.featureGroup(group).addTo(map); ";
		//markermap +="map.setView(results[1].best.latlng, 8);";
		//markermap += " } ";
		markermap += "map.fitBounds(features.getBounds()); ";
		markermap += "popup.update();";
		
		markermap += "L.DomUtil.get('info').innerHTML = html;";
		
			markermap += "});";
					
			
	//			markermap +="}; } (counter), 2000);";
		//markermap +="alert(results.length);";
		//markermap +="alert(m);";
		//markermap +="alert('counter is '+count);";
		
				markermap +="}, 100);";
				markermap +=" })(geova.slice(k,k+chunk), measure.slice(k,k+chunk));";
			//	markermap +="counter++;";
		markermap +="}";
		
		return markermap;
		
	}

	/*
	 * Input:Strings with the i) values of the geospatial dimension of the
	 * cube, ii) values of the measure of the cube and iii) values of the rest
	 * of the dimensions of the cube, a list of strings with the values of the
	 * attributes of the cube, the number of the attributes of the cube, a
	 * String with the labels of the geospatial values of the cube and a String
	 * with the labels of the attribute values of the cube Output:A String with
	 * the javascript to create the bubble map
	 */
	public static String getBubbleJavascript(String geovalue,
			String measurevalue, String dimvalues,
			List<String> attributeValues, int cubeAttributesSize,
			String geo_label, String attr_labels) {

		// The javascript code to create the bubble map
		String bubblemap = null;

		// Create a javascript array with the values of the geospatial dimension
		bubblemap = " arr = new Array (" + geovalue + "); ";

		// Create a javascript array with the values of the measure
		bubblemap += "measure = new Array(" + measurevalue + ");";

		// Create a javasript array with the values of the dimensions (except
		//for the geospatial dimension)
		bubblemap += "observ = new Array(" + dimvalues + ");";

		// Create one javascript array for the values of each of the attributes of the cube
		for (int j = 0; j < cubeAttributesSize; j++)
			bubblemap += "attribute" + j + " = new Array("
					+ attributeValues.get(j) + ");";

		// Create a javascript array with the labels of the attributes of the cube
		bubblemap += "attr_labels = new Array(" + attr_labels + ");";
		bubblemap +="var counter = 0;";
		bubblemap += "var i,j,temparray, chunk = 100;";
		
		bubblemap +="var geova = new Array(" + geovalue +");";
		bubblemap +="group = [];";	

		bubblemap +="var results, b = 0, features, marker, result, latlng, prop, best, val, map, r, i;";
		//bubblemap += "map = L.map('map').setView([51.505, -0.09], 13); L.tileLayer('https://{s}.tiles.mapbox.com/v3/{id}/{z}/{x}/{y}.png', {maxZoom:18,attribution:'Map data &copy; <a href=\"http://openstreetmap.org\">OpenStreetMap</a> contributors, ' + '<a href=\"http://creativecommons.org/licenses/by-sa/2.0/\">CC-BY-SA</a>, ' +'Imagery &copy; <a href=\"http://mapbox.com\">Mapbox</a>', id:'examples.map-i86knfo3'}).addTo(map);";		
		bubblemap +="map = L.map('map', {layers: MQ.mapLayer() });";
		
		//bubblemap +="var rand = Math.floor((Math.random() * 1000) + 1);";
		//bubblemap +="eval('var myvar' + rand + ' = 0;');";
	//	bubblemap +="\"myvar\"+rand=0;";
		//bubblemap +="alert(eval('myvar'+rand));";
		
		
		bubblemap +="for (k=0,j=geova.length; k<j; k+=chunk) {";
		//bubblemap +="for (k=0,j=geova.length; 98<j-k; k+=chunk) {";
		bubblemap +="counter++;";
		bubblemap +="temparray = geova.slice(k,k+chunk); ";
		bubblemap +="var something = \"new\"+counter;";
		//bubblemap +="alert(something);";
		
		//bubblemap +=" (function (n) { ";
		
		bubblemap +=" (function (n, meas) { ";
		bubblemap +="setTimeout(function(){";
		// Perform the geocoding
		bubblemap += "MQ.geocode().search(n) .on('success', ";
		bubblemap += "function(e) {results = e.result; ";
	//			+ "var results = e.result, html = '', group = [], features, marker, result, latlng, prop, best, val, map, r, i;  ";
	//	bubblemap += "map = L.map('map').setView([51.505, -0.09], 13); L.tileLayer('https://{s}.tiles.mapbox.com/v3/{id}/{z}/{x}/{y}.png', {maxZoom:18,attribution:'Map data &copy; <a href=\"http://openstreetmap.org\">OpenStreetMap</a> contributors, ' + '<a href=\"http://creativecommons.org/licenses/by-sa/2.0/\">CC-BY-SA</a>, ' +'Imagery &copy; <a href=\"http://mapbox.com\">Mapbox</a>', id:'examples.map-i86knfo3'}).addTo(map);";

		// Get the lat and long
		bubblemap += "for (i = 0; i < results.length; i++) { ";
		
		bubblemap += "result = results[i].best;  ";
		bubblemap += "latlng = result.latlng; ";

		// Create the bubble markers
		bubblemap += "marker = L.circleMarker();";
		bubblemap += "marker.setLatLng([ latlng.lat, latlng.lng]) ";
		bubblemap += ".bindPopup( \"<b>\" + " + geo_label
				+ "+ \":\"+ \"</b>\" + n[i] + \"<br />\" + observ[b]+ +meas[i]";
		for (int j = 0; j < cubeAttributesSize; j++)
			bubblemap += "+ \"<br />\"+ \"<b>\" +attr_labels[" + j
					+ "] +\":\"+ \"</b>\" +attribute" + j + "[b]";
		bubblemap += ");";
		bubblemap += "b++;";
	//	bubblemap +="eval('myvar' + rand + ' ++ ;');";
        bubblemap +="var radius = [];";
        bubblemap +="var min = Math.min.apply(Math, measure);";
        bubblemap +="var max = Math.max.apply(Math, measure);";
        bubblemap +="radius[i] = 100*((meas[i]-min)/(max-min));";
		bubblemap += "marker.setRadius(radius[i]);";
		bubblemap += "group.push(marker); } ";
		bubblemap += "features = L.featureGroup(group).addTo(map); ";
		//bubblemap +="map.setView(results[1].best.latlng, 8);";
		bubblemap += "map.fitBounds(features.getBounds()); ";
		bubblemap += "L.DomUtil.get('info').innerHTML = html; });";
		
		bubblemap +="}, 100);";
		bubblemap +=" })(geova.slice(k,k+chunk), measure.slice(k,k+chunk));";
		
		bubblemap +="}";
		
		return bubblemap;
	}

	/*
	 * Input:Strings with the i) values of the geospatial dimension of the
	 * cube, ii) values of the measure of the cube and iii) values of the rest
	 * of the dimensions of the cube, Output:A String with the javascript to
	 * create the heat map
	 */
	public static String getHeatMapJavascript(String geovalue,
			String measurevalue, String dimvalues) {

		// The javascript code to create the heat map
		String heatmap = null;

		// A javascript array with the proper values to create the heat map
		heatmap = "var heatarray=[];";

		// Create a javascript array with the values of the geospatial dimension
		heatmap += " arr = new Array (" + geovalue + "); ";

		// Create a javascript array with the values of the measure
		heatmap += "measure = new Array(" + measurevalue + ");";

		// Perform the geocoding
		heatmap += "MQ.geocode().search([" + geovalue + "]) .on('success', ";
		heatmap += "function(e) {var results = e.result, html = '', group = [], features, marker, result, latlng, prop, best, val, map, r, i;  ";

		// Create the tiles
		heatmap += "var tiles = L.tileLayer('https://{s}.tiles.mapbox.com/v3/{id}/{z}/{x}/{y}.png', {maxZoom:18,attribution:'Map data &copy; <a href=\"http://openstreetmap.org\">OpenStreetMap</a> contributors, ' + '<a href=\"http://creativecommons.org/licenses/by-sa/2.0/\">CC-BY-SA</a>, ' +'Imagery &copy; <a href=\"http://mapbox.com\">Mapbox</a>', id:'examples.map-20v6611k'});";
		heatmap += "map = L.map('map').setView([51.505, -0.09], 3); ";
		heatmap += "map.addLayer(tiles);";

		// Make heataarray a 2D array
		heatmap += "for (var i=0;i<results.length;i++) {";
		heatmap += "heatarray[i] = [];}";

		// Put into heatarray:i) the lat of ech location, ii) the lng of each
		// location and iii) the corresponding measure
		heatmap += "for (i = 0; i < results.length; i++) {";
		heatmap += "result = results[i].best;  ";
		heatmap += "latlng = result.latlng; ";
		heatmap += "heatarray[i][0] = latlng.lat;";
		heatmap += "heatarray[i][1] = latlng.lng;";
		heatmap += "heatarray[i][2] = measure[i];}";
		heatmap += "var heat = L.heatLayer(heatarray);";
		heatmap += "heat.setOptions({gradient:{0.45:\"rgb(0,0,255)\", 0.55:\"rgb(0,255,255)\", 0.65:\"rgb(0,255,0)\", 0.95:\"yellow\", 1.0:\"rgb(255,0,0)\"}});";
		heatmap += "heat.addTo(map);";
		heatmap += "L.DomUtil.get('info').innerHTML = html; });";

		return heatmap;
	}

	/*
	 * Input:Strings with the i) values of the geospatial dimension of the
	 * cube, ii) values of the measure of the cube and iii) values of the rest
	 * of the dimensions of the cube, Output:A String with the javascript to
	 * create the choropleth map
	 */
	public static String getChoroplethMapJavascript(String geovalue,
			String measurevalue, String dimvalues,
			List<String> attributeValues, int cubeAttributesSize,
			String attr_labels) {

		String choroplethmap = null;

		// Create a javascript array with the values of the geospatial dimension
		choroplethmap = " arr = new Array (" + geovalue + "); ";
		String date = geovalue.substring(0, geovalue.indexOf(","));
		System.out.println("date is "+date);
		// Create a javascript array with the values of the measure
		choroplethmap += "measure = new Array(" + measurevalue + ");";
//System.out.println("measurevalue is "+measurevalue);
		// Create a javasript array with the values of the dimensions (except
		// for the geospatial dimension)
		choroplethmap += "observ = new Array(" + dimvalues + ");";

		// Create one javascript array for the values of each of the attributes
		// of the cube
		for (int j = 0; j < cubeAttributesSize; j++)
			choroplethmap += "attribute" + j + " = new Array("
					+ attributeValues.get(j) + ");";

		// Create a javascript array with the labels of the attributes of the
		// cube
		choroplethmap += "attr_labels2 = new Array(" + attr_labels + ");";

		// The geoJSON data
		
		BufferedReader br = null;
		try {
			if(mapzoom.equals("BE")){
			br = new BufferedReader(new FileReader("choropleth_json_be.txt"));
			}
			else if(mapzoom.equals("EU")){
				br = new BufferedReader(new FileReader("choropleth_json_eu.txt"));
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String line;
		String polygons = "";
		String lines2 = null;
		int number = 0;
		try {
			while ((line = br.readLine()) != null) {
			   polygons+=line+"\r\n ";
	
			   // process the line.
			   number++;
			}
			
		}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			String polygon = polygons;
		//choroplethmap +="var statesData ={\"type\": \"FeatureCollection\", \"features\": ["
//System.out.println("polygons "+polygon);
//System.out.println("lines "+number);
	choroplethmap += "var statesData = "+ polygon +";";
		//choroplethmap+="var polyg = "+ polygon +";";
	//	choroplethmap +="alert(statesData);";
	choroplethmap +="var coding = "+date +";";
	//choroplethmap +="alert(coding);";
		choroplethmap += "MQ.geocode().search([" + date + "]) .on('success', ";
		choroplethmap += "function(e) {var results = e.result, html = '', group = [], features, marker, result, latlng, prop, best, val, map, r, i;  ";
	choroplethmap += "var tiles = L.tileLayer('https://{s}.tiles.mapbox.com/v3/{id}/{z}/{x}/{y}.png', {maxZoom:18,attribution:'Map data &copy; <a href=\"http://openstreetmap.org\">OpenStreetMap</a> contributors, ' + '<a href=\"http://creativecommons.org/licenses/by-sa/2.0/\">CC-BY-SA</a>, ' +'Imagery &copy; <a href=\"http://mapbox.com\">Mapbox</a>', id:'examples.map-20v6611k'});";
		if(mapzoom.equals("BE")){
		choroplethmap += "map = L.map('map').setView([50.8333, 4], 8);";
		}
		else if(mapzoom.equals("EU")) {
			choroplethmap += "map = L.map('map').setView([51.505, -0.09], 3);";
		}
	//	choroplethmap += "map = L.map('map').setView([50.8333, 4], 8);";
		//choroplethmap +="alert(results[0].best.latlng);";
	//	choroplethmap +="map = L.map('map', {layers: MQ.mapLayer(), zoom: 8 });";
		choroplethmap += "map.addLayer(tiles);";
// choroplethmap +="map = L.map('map', { layers: MQ.mapLayer()});";
		// Update geoJSON the density of the data with the corresponding measure values
		choroplethmap += "for (i = 0; i < arr.length; i++)";
		choroplethmap += " for(j=0; j<statesData.features.length; j++){";
		choroplethmap += " 		if(arr[i] == statesData.features[j].properties.name) {";
		choroplethmap += "			statesData.features[j].properties.density = measure[i];";
		choroplethmap +="var temp = statesData.features[j].properties.density;}"
				+ "else {statesData.features[j].properties.geometry=\" \";}};";
		//choroplethmap +="alert(temp);";
		//choroplethmap +="if(feature.properties.density!=\"No value\"){";
		choroplethmap += "function getColor(d, serie1) {"
				+ "var class1 = serie1.getRangeNum(d);"
				+ "if(class1>=0) return serie1.colors[class1];"
				+ "else return '#F0EEFA';" + "}";
		choroplethmap += "var geojson;";
		choroplethmap += "var colors = ['#d5cfdb', '#c2abdd', '#9d87b6', '#735a8f', '#3d2e4e'];";

		choroplethmap += "var dataJson = new Array();";
		choroplethmap += "for(k=0;k<measure.length;k++)"
				+ "dataJson.push(parseFloat(measure[k]));";
		choroplethmap += "serie1 = new geostats(dataJson);"
				+ "serie1.getClassJenks(5);" + "ranges = serie1.ranges;"
				+ "serie1.setPrecision(1);" + "serie1.setColors(colors);";
		
		
		
		choroplethmap += "function style(feature) {" 
		+ "return {"
				+ "fillColor:getColor(feature.properties.density, serie1),"
				+ "weight:2," + "opacity:1," + "color:'white',"
				+ "dashArray:'3'," + "fillOpacity:0.7" + "};" + "}";

		choroplethmap += "function highlightFeature(e) {"
				+ "var layer = e.target;" + "layer.setStyle({" + "weight:4,"
				+ "color:'#ffffff'," + "dashArray:''," + "fillOpacity:0.7"
				+ "});";

		choroplethmap += "if (!L.Browser.ie && !L.Browser.opera) {"
				//+ "layer.bringToFront();" 
				+ "}"
				+ "info.update(layer.feature.properties);" + "}";
		choroplethmap += "function resetHighlight(e) {"
				+ "geojson.resetStyle(e.target);" + "layer.bringToBack();"+ "info.update();" + "}";
		choroplethmap += "function zoomToFeature(e) {"
				+ "map.fitBounds(e.target.getBounds());" + "}";
		choroplethmap += "function onEachFeature(feature, layer) {"
				+ "layer.on({" + "mouseover:highlightFeature,"
				+ "mouseout:resetHighlight," + "click:zoomToFeature" + "});"
				+ "}";
		
		choroplethmap += "var info = L.control();"
				+ "info.onAdd = function (map) {"
				+ "this._div = L.DomUtil.create('div', 'info'); "
				+ "this._div.setAttribute('style', 'box-shadow:0 0 15px rgba(0,0,0,1)');"
				+ "this._div.style.cssText+='border-radius:5px';"
				+ "this._div.style.cssText+='background-color:white';"
				+ "this._div.style.cssText+='background:rgba(255,255,255,0.5)';"
				+ "this._div.style.cssText+='font:12px/14px Arial, Helvetica, sans-serif';"
				+ "this._div.style.cssText+='padding:6px 8px';"
				+ "this._div.style.cssText+='text-align:left';"
				+ "this.update();" + "return this._div;" + "};";

		choroplethmap += "info.update = function (props) { "
				+ "for(var k=0;k<arr.length;k++)"
				+ "if(props && arr[k] == props.name){"
				+ "var observations= observ[k]; ";
		
		for (int j = 0; j < cubeAttributesSize; j++) {
			choroplethmap += "observations += '<b>'+attr_labels2[" + j + "];"
					+ "observations +=':</b>';" + "observations+= attribute"
					+ j + "[k];";
		}
		choroplethmap += "}if(!observations)"
				+ "observations=\" \";"
				+

				"this._div.innerHTML = (props ?"
				+ "'<h5 style =\"text-align:center\">' + props.name + '</h5><b>Measure:</b>' + props.density + '</b><br />' + observations + '</b>' :'Hover over a state');"
				+ "};";

		choroplethmap += "info.addTo(map);";
		choroplethmap += "geojson = L.geoJson(statesData, {" + "style:style,"
				+ "onEachFeature:onEachFeature" + "}).addTo(map);";
		
		//choroplethmap +="var group = new L.featureGroup(geojson);";
		//choroplethmap +="map.fitBounds(group.getBounds());";
		
		choroplethmap += "var legend = L.control({position:'bottomright'});";

		choroplethmap += "legend.onAdd = function (map) {"
				+ "var div = L.DomUtil.create('div', 'info legend'),"
				+ "labels = [];"
				+ "div.setAttribute('style', 'box-shadow:0 0 15px rgba(0,0,0,1)');"
				+ "div.style.cssText+='border-radius:5px';"
				+ "div.style.cssText+='background-color:white';"
				+ "div.style.cssText+='background:rgba(255,255,255,0.5)';"
				+ "div.style.cssText+='font:14px/16px Arial, Helvetica, sans-serif';"
				+ "div.style.cssText+='padding:6px 8px';"
				+ "div.style.cssText+='color:#555';"
				+ "div.style.cssText+='line-height:20px';"
				+ "div.style.cssText+='text-align:left';"
				+ "div.innerHTML +='<i style=\"background:#F0EEFA; width:18px; height:18px; float:left;   margin-right:8px; opacity:0.7;\"></i> No value <br>';";
		choroplethmap += "for (var i = 0; i < ranges.length; i++) {"
				+ "div.innerHTML += '<i style=\"background:' + serie1.colors[i] + '; width:18px; height:18px; float:left; margin-right:8px;opacity:0.7;\"></i> '+ranges[i] + '<br>';"
				+ "}" +

				"return div;" + "};";

		choroplethmap += "legend.addTo(map);";
		//choroplethmap +="};";
		
		choroplethmap += "L.DomUtil.get('info').innerHTML = html;})";
		
		
		return choroplethmap;
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
}