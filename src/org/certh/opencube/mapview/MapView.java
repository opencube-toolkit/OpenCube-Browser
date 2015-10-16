package org.certh.opencube.mapview;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.certh.opencube.SPARQL.AggregationSPARQL;
import org.certh.opencube.SPARQL.CubeSPARQL;
import org.certh.opencube.SPARQL.MapViewSPARQL;
import org.certh.opencube.SPARQL.SelectionSPARQL;
import org.certh.opencube.SPARQL.SliceSPARQL;
import org.certh.opencube.cubebrowser.DataCubeBrowserPlusPlus;
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
import com.fluidops.ajax.components.FRadioButtonGroup;
import com.fluidops.iwb.api.EndpointImpl;
import com.fluidops.iwb.model.ParameterConfigDoc;
import com.fluidops.iwb.model.TypeConfigDoc;
import com.fluidops.iwb.widget.AbstractWidget;
import com.fluidops.iwb.widget.config.WidgetBaseConfig;
import com.google.common.collect.Lists;
import com.sun.jdi.connect.Connector.SelectedArgument;

/**
 * On some wiki page add
 * 
 * <code>
 *  ==OpenCube Map View==
 * 
 * <br/>
 * {{#widget:org.certh.opencube.mapview.MapView
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

	// The selected cube dimensions to use for visualization (visual dims +
	// fixed dims)
	private List<LDResource> selectedDimenisons = new ArrayList<LDResource>();

	// The geospatial dimension of the cube
	private LDResource geodimension = null;

	// All the cube attributes
	private List<LDResource> cubeAttributes = new ArrayList<LDResource>();

	// The measure of the cube
	private List<LDResource> cubeMeasures = new ArrayList<LDResource>();

	// The values of all the dimensions of the cube
	private HashMap<LDResource, List<LDResource>> allDimensionsValues = new HashMap<LDResource, List<LDResource>>();

	// A map (cube - level URI - dimension values) with all cube dimension
	// values
	private HashMap<LDResource, List<LDResource>> allLevelsValues = new HashMap<LDResource, List<LDResource>>();

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

	// The grid that organizes the top menu
	private FGrid selectCubeGrid = null;

	// The grid that organizes the top menu
	private FGrid measureDimGrid = null;

	// The FCombobox that contains the types of visualizations
	private FComboBox vistypes = null;

	// The FCombobox that contains the values of the dimensions of the cube
	private FComboBox fDimCombo = null;
	
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

	// A map with the Cube Measure URIs and the corresponding Check boxes
	private HashMap<LDResource, FCheckBox> mapMeasureURIcheckBox = new HashMap<LDResource, FCheckBox>();

	// The top container to show the check boxes with the available aggregation
	// set dimensions
	private FContainer topcontainer = new FContainer("topcontainer");

	// The top 1 container
	private FContainer top1 = null;
	private FContainer top2 = null;

	// The measures container to show the combo box with the cube measures
	private FContainer measurescontainer = new FContainer("measurescontainer");

	// The visualization type container
	private FContainer visTypeContainer = new FContainer("visTypeContainer");

	// The top container to show the check boxes with the available aggregation
	// set dimensions
	private FContainer selectCubeContainer = new FContainer("selectCubeContainer");

	// The fixed dimensions container
	private FContainer fixedDimContainer = new FContainer("fixedDimContainer");

	// The right container to show the combo boxes with the fixed dimensions values
	private FContainer languagecontainer = new FContainer("languagecontainer");

	// The levels for each dimension
	private HashMap<LDResource, List<LDResource>> allDimensionsLevels = new HashMap<LDResource, List<LDResource>>();

	private LDResource selectedGeoLevel = null;
	// The SELECTED levels of the cube. Map dim-> levels[]
	private HashMap<LDResource, List<LDResource>> selectedDimLevels = new HashMap<LDResource, List<LDResource>>();

	// A map with the Aggregation Set Dimension URIs and the corresponding
	// DIMENSION LEVELS Check boxes
	private HashMap<LDResource, List<FCheckBox>> mapDimLevelscheckBoxes = new HashMap<LDResource, List<FCheckBox>>();

	// The available languages of the cube
	private List<String> availableLanguages;

	// The selected language
	private String selectedLanguage;

	private String englishLabel;
	private List<LDResource> allCubes = new ArrayList<LDResource>();

	private LDResource selectedCube;

	// The default language
	private String defaultLang;

	private static String mapzoom;

	// Ignore multiple languages
	private boolean ignoreLang;

	private boolean languangeChange = false;

	private String selectedOperation;

	private String[] measureColors = { "black", "CornflowerBlue", "LimeGreen",
			"Tomato", "Orchid", "MediumVioletRed", "Gold", "DarkGoldenRod",
			"DarkGray", "DarkRed" };

	// The selected cube measure to visualize
	private List<LDResource> selectedMeasures = new ArrayList<LDResource>();

	private boolean firstCubeLoad = true;

	final FClientUpdate update = new FClientUpdate("javascript");

	public static class Config extends WidgetBaseConfig {
		@ParameterConfigDoc(desc = "The data cube URI to visualise", required = false)
		public String dataCubeURI;

		@ParameterConfigDoc(desc = "Use code lists to get dimension values", required = false)
		public boolean useCodeLists;

		@ParameterConfigDoc(desc = "SPARQL service to forward queries", required = false)
		public String sparqlService;

		@ParameterConfigDoc(desc = "The default language", required = false)
		public String defaultLang;

		@ParameterConfigDoc(desc = "Use multiple languages", required = false)
		public boolean ignoreLang;

		@ParameterConfigDoc(desc = "Set map zoom. Accepted values: BE, EU", required = true)
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
		}

	@Override
	protected FComponent getComponent(String id) {

		final Config config = get();

		// Central container
		cnt = new FContainer(id);

		// Get the cube URI from the widget configuration
		cubeSliceURI = config.dataCubeURI;

		if (cubeSliceURI != null && !cubeSliceURI.contains("<")) {
			cubeSliceURI = "<" + cubeSliceURI + ">";
		}

		// Get the SPARQL service (if any) from the cube configuration
		SPARQL_service = config.sparqlService;

		// The use code list parameter from the widget config
		useCodeLists = config.useCodeLists;

		mapzoom = config.mapzoom;

		if (mapzoom == null || mapzoom.equals("")) {
			mapzoom = "EU";
		}

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

		selectedOperation = "Choropleth map";
		if (cubeSliceURI == null || cubeSliceURI.equals("")) {
			addCubeSelectionCombo();
			cnt.add(selectCubeContainer);
		} else {
			String selectedCubeURI = cubeSliceURI; // remove < and > from the
													// URI. The cubes combobox
													// URI are without < and >
			selectedCubeURI = selectedCubeURI.replaceAll("<", "");
			selectedCubeURI = selectedCubeURI.replaceAll(">", "");
			selectedCube = new LDResource(selectedCubeURI);
			// show the cube
			populateMapViewContainer();
		}
		return cnt;
	}

	// Populate the Map View container
	private void populateMapViewContainer() {
		
		// Get Cube/Slice Graph
		String cubeSliceGraph = CubeSPARQL.getCubeSliceGraph(cubeSliceURI,SPARQL_service);

		// Get the type of the URI i.e. cube / slice
		List<String> cubeSliceTypes = CubeSPARQL.getType(cubeSliceURI,cubeSliceGraph, SPARQL_service);

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

				if (!ignoreLang) {
					// Get the available languages of labels
					availableLanguages = CubeSPARQL.getAvailableCubeLanguages(
							cubeDSDGraph, SPARQL_service);

					// get the selected language to use
					selectedLanguage = CubeHandlingUtils.getSelectedLanguage(
							availableLanguages, selectedLanguage);
				}

				// Get the geospatial dimension of the cube
				geodimension = CubeSPARQL.getGeoDimension(cubeSliceURI,
						cubeGraph, cubeDSDGraph, selectedLanguage, defaultLang,
						ignoreLang, SPARQL_service);

				if (geodimension != null) {

					// the first time selected dimension is empty - add the geodimension
					if (selectedDimenisons.size() == 0) {
						selectedDimenisons.add(geodimension);
					}

					hasGeoDimension = true;

					// Get all Cube dimensions
					cubeDimensions = CubeSPARQL.getDataCubeDimensions(
							cubeSliceURI, cubeGraph, cubeDSDGraph,
							selectedLanguage, defaultLang, ignoreLang,
							SPARQL_service);

					// for each cube dim get the levels (if exist)
					for (LDResource ldr : cubeDimensions) {
						// get levels from schema
						List<LDResource> currentDimLevelsWithLables = CubeSPARQL
								.getDimensionLevelsFromSchema(ldr.getURI(),
										cubeDSDGraph, selectedLanguage,
										defaultLang, ignoreLang, SPARQL_service);

						// The ordered levels do not have labels
						List<LDResource> orderedCurrentDimLevels = CubeSPARQL
								.getOrderedDimensionLevelsFromSchema(
										ldr.getURI(), cubeDSDGraph,
										SPARQL_service);

						// Ordered levels with labels
						List<LDResource> orderedDimLevelsWithLabel = new ArrayList<LDResource>();
						for (LDResource orderedLDR : orderedCurrentDimLevels) {
							int levelIndex = currentDimLevelsWithLables
									.indexOf(orderedLDR);
							LDResource levelWithLabel = currentDimLevelsWithLables
									.get(levelIndex);
							orderedDimLevelsWithLabel.add(levelWithLabel);
						}
						
						List<LDResource> finalAllDimLevels = new ArrayList<LDResource>();
						for(LDResource level:orderedDimLevelsWithLabel){
							//if the Schema level exists at the data
							if(CubeSPARQL.askDimensionLevelInDataCube(
									cubeSliceURI,level, cubeGraph,
									cubeDSDGraph,SPARQL_service)){
								finalAllDimLevels.add(level);
							}
						}
						

						allDimensionsLevels.put(ldr, finalAllDimLevels);
					}

					// Get the Cube measure
					cubeMeasures = CubeSPARQL.getDataCubeMeasure(cubeSliceURI,
							cubeGraph, cubeDSDGraph, selectedLanguage,
							defaultLang, ignoreLang, SPARQL_service);

					// Get all the dimensions of the aggregation set the cube belongs
					aggregationSetDims = AggregationSPARQL
							.getAggegationSetDimsFromCube(cubeSliceURI,
									cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQL_service);

					// Get all the dimensions per cube of the aggregations set
					cubeDimsOfAggregationSet = AggregationSPARQL
							.getCubeAndDimensionsOfAggregateSet(cubeSliceURI,
									cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQL_service);

					// Get the attributes of the cube
					cubeAttributes = CubeSPARQL.getDataCubeAttributes(
							cubeSliceURI, cubeGraph, cubeDSDGraph,
							selectedLanguage, defaultLang, ignoreLang,
							SPARQL_service);

					// Get values for each cube dimension
					allDimensionsValues = CubeHandlingUtils.getDimsValues(
							cubeDimensions, cubeSliceURI, cubeGraph,
							cubeDSDGraph, useCodeLists, selectedLanguage,
							defaultLang, ignoreLang, SPARQL_service);

					allLevelsValues = new HashMap<LDResource, List<LDResource>>();

					// Get all the values for each level (level -> values[])
					// To get the values use the allDimensionsValues
					for (LDResource dim : allDimensionsValues.keySet()) {
						for (LDResource value : allDimensionsValues.get(dim)) {
							// if there is a level at the value put the value at  the list
							if (value.getLevel() != null) {
								LDResource thisLevel = new LDResource(
										value.getLevel());
								List<LDResource> currentLevelValues = allLevelsValues.get(thisLevel);
								// If there are no values for the level yet
								if (currentLevelValues == null) {
									currentLevelValues = new ArrayList<LDResource>();
								}
								currentLevelValues.add(value);
								Collections.sort(currentLevelValues);
								allLevelsValues.put(thisLevel,currentLevelValues);
							}
						}
					}
				}

			} else if (isSlice) {

				// The slice graph is the graph of the URI computed above
				sliceGraph = cubeSliceGraph;

				// Get the cube graph from the slice
				cubeGraph = SliceSPARQL.getCubeGraphFromSlice(cubeSliceURI,
						sliceGraph, SPARQL_service);

				// Get Cube Structure graph from slice
				cubeDSDGraph = SliceSPARQL.getCubeStructureGraphFromSlice(
						cubeSliceURI, sliceGraph, SPARQL_service);

				if (!ignoreLang) {
					// Get the available languages of labels
					availableLanguages = CubeSPARQL.getAvailableCubeLanguages(
							cubeDSDGraph, SPARQL_service);

					// get the selected language to use
					selectedLanguage = CubeHandlingUtils.getSelectedLanguage(
							availableLanguages, selectedLanguage);
				}

				// Get the geospatial dimension of the cube
				geodimension = SliceSPARQL.getGeoDimension(cubeSliceURI,
						sliceGraph, cubeDSDGraph, selectedLanguage,
						SPARQL_service);

				// Get slice fixed dimensions
				sliceFixedDimensions = SliceSPARQL.getSliceFixedDimensions(
						cubeSliceURI, sliceGraph, cubeDSDGraph,
						selectedLanguage, defaultLang, ignoreLang,
						SPARQL_service);

				// if the slice has a geo dimensions
				// and it is not defined as a fixed value
				if (geodimension != null&& !sliceFixedDimensions.contains(geodimension)) {
					hasGeoDimension = true;
					sliceFixedDimensionsValues = SliceSPARQL
							.getSliceFixedDimensionsValues(
									sliceFixedDimensions, cubeSliceURI,
									sliceGraph, cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQL_service);

					// Get all cube dimensions
					List<LDResource> cubeDimsFromSlice = SliceSPARQL
							.getDataCubeDimensionsFromSlice(cubeSliceURI,
									sliceGraph, cubeDSDGraph, selectedLanguage,
									defaultLang, ignoreLang, SPARQL_service);

					// The slice visual dimensions are:(all cube dims) - (slice
					// fixed dims)
					cubeDimensions = cubeDimsFromSlice;
					cubeDimensions.removeAll(sliceFixedDimensions);

					// Get the Cube measure
					cubeMeasures = SliceSPARQL.getSliceMeasure(cubeSliceURI,
							sliceGraph, cubeDSDGraph, selectedLanguage,
							defaultLang, ignoreLang, SPARQL_service);

					// Get the attributes of the cube
					cubeAttributes = SliceSPARQL
							.getDataCubeAttributesFromSlice(cubeSliceURI,
									sliceGraph, cubeDSDGraph, selectedLanguage,
									SPARQL_service);

					// Get values for each slice dimension
					allDimensionsValues = CubeHandlingUtils
							.getDimsValuesFromSlice(cubeDimensions,
									cubeSliceURI, cubeGraph, cubeDSDGraph,
									sliceGraph, useCodeLists, selectedLanguage,
									defaultLang, ignoreLang, SPARQL_service);
				}
			}

			if (languangeChange) {
				selectedMeasures = new ArrayList<LDResource>();
				for (LDResource measureURI : mapMeasureURIcheckBox.keySet()) {
					FCheckBox check = mapMeasureURIcheckBox.get(measureURI);

					// Get selected dimensions
					if (check.checked) {
						selectedMeasures.add(measureURI);
					}
				}
				// The cubeMeasures have the correct language while the selected not
				List<LDResource> tmplist = new ArrayList<LDResource>(
						cubeMeasures);
				tmplist.retainAll(selectedMeasures);
				selectedMeasures = tmplist;

				languangeChange = false;
			}

			// In case there is no geo-dimension
			if (hasGeoDimension) {

				addVisualizationTypes();
				addCubeSelectionCombo();

				if (!ignoreLang) {
					languagecontainer = new FContainer("languagecontainer");
					// language container styling
					languagecontainer.addStyle("border-style", "solid");
					languagecontainer.addStyle("border-width", "1px");
					languagecontainer.addStyle("padding", "5px");
					languagecontainer.addStyle("border-radius", "5px");
					languagecontainer.addStyle("border-color", "#C8C8C8 ");
					languagecontainer.addStyle("display", "table-cell ");
					languagecontainer.addStyle("margin-left", "auto");
					languagecontainer.addStyle("margin-right", "auto");
					languagecontainer.addStyle("text-align", "left");
					languagecontainer.addStyle("width", "100px ");

					if (selectedLanguage.equals("en")) {
						FLabel datalang_label = new FLabel("datalang",
								"<b>Language:</b><br>");
						languagecontainer.add(datalang_label);
					} else if (selectedLanguage.equals("nl")) {
						FLabel datalang_label = new FLabel("datalang",
								"<b>Taal:</b><br>");
						languagecontainer.add(datalang_label);
					} else if (selectedLanguage.equals("fr")) {
						FLabel datalang_label = new FLabel("datalang",
								"<b>Langue:</b><br>");
						languagecontainer.add(datalang_label);
					} else if (selectedLanguage.equals("de")) {
						FLabel datalang_label = new FLabel("datalang",
								"<b>Sprache:</b><br>");
						languagecontainer.add(datalang_label);
					}

					// Add Combo box for language selection
					FComboBox datalang_combo = new FComboBox("datalang_combo") {
						@Override
						public void onChange() {
							selectedLanguage = this.getSelected().get(0).toString();
							
							allCubes = SelectionSPARQL.getMaximalCubesAndSlices(selectedLanguage,
									selectedLanguage,ignoreLang,SPARQL_service);
							languagecontainer.removeAll();
							fixedDimContainer.removeAll();
							visTypeContainer.removeAll();
							selectCubeContainer.removeAll();
							topgrid.removeAll();
							topcontainer.removeAll();
							dimensionURIfcomponents.clear();
							grid.removeAll();
							visualizations.clear();
							cnt.removeAll();
							label = 0;
							combo = 0;
							mapShown = false;
							languangeChange = true;
						
							// show the cube
							populateMapViewContainer();
							createMap(this);
							cnt.populateView();
						}
					};

					// populate language combo box
					for (String lang : availableLanguages) {
						datalang_combo.addChoice(lang, lang);
					}
					datalang_combo.setPreSelected(selectedLanguage);
					languagecontainer.add(datalang_combo);

				}

				int containerHeight;
				if (cubeMeasures.size() > aggregationSetDims.size()) {
					containerHeight = (1 + cubeMeasures.size()) * 18;
				} else {
					containerHeight = (1 + aggregationSetDims.size()) * 18;
				}
				// minimum height needed for language select
				if (containerHeight < 60) {
					containerHeight = 60;
				}

				// Add top container if it is a cube not slice
				if (isCube) {
					topcontainer = new FContainer("topcontainer");
					// top container styling
					topcontainer.addStyle("border-style", "solid");
					topcontainer.addStyle("border-width", "1px");
					topcontainer.addStyle("padding", "5px");
					topcontainer.addStyle("border-radius", "5px");
					topcontainer.addStyle("border-color", "#C8C8C8 ");
					topcontainer.addStyle("display", "table-cell ");
					topcontainer.addStyle("margin-left", "auto");
					topcontainer.addStyle("margin-right", "auto");
					topcontainer.addStyle("height", containerHeight + "px ");
					topcontainer.addStyle("width", "300px ");
					topcontainer.addStyle("text-align", "left");

					// If an aggregation set has already been created
					if (aggregationSetDims.size() > 0) {

						if (selectedLanguage.equals("en")) {
							FLabel OLAPbrowsing_label = new FLabel(
									"OLAPbrowsing_label",
									"<b>Dimensions:</b></br>");
							topcontainer.add(OLAPbrowsing_label);
						} else if (selectedLanguage.equals("nl")) {
							FLabel OLAPbrowsing_label = new FLabel(
									"OLAPbrowsing_label",
									"<b>Dimensies:</b></br>");
							topcontainer.add(OLAPbrowsing_label);
						} else if (selectedLanguage.equals("fr")) {
							FLabel OLAPbrowsing_label = new FLabel(
									"OLAPbrowsing_label",
									"<b>Dimensions:</b></br>");
							topcontainer.add(OLAPbrowsing_label);
						} else if (selectedLanguage.equals("de")) {
							FLabel OLAPbrowsing_label = new FLabel(
									"OLAPbrowsing_label",
									"<b>Dimensionen:</b></br>");
							topcontainer.add(OLAPbrowsing_label);
						}

						int aggregationDim = 1;
						// Show Aggregation set dimensions
						for (LDResource aggdim : aggregationSetDims) {

							if (!aggdim.getURI().equals(geodimension.getURI())) {

								// show one check box for each aggregation set dimension
								FCheckBox aggrDimCheckBox = new FCheckBox(
										"aggregation_" + aggregationDim,aggdim.getURIorLabel()) {

									public void onClick() {
										// Get all selected aggregation set dimensions for browsing
										List<LDResource> aggregationSetSelectedDims = new ArrayList<LDResource>();
										selectedDimenisons = new ArrayList<LDResource>();
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

										// Add the geodimension at the selected dimensions
										aggregationSetSelectedDims.add(geodimension);
										selectedDimenisons.add(geodimension);

										// Identify the cube of the aggregation
										// set that contains exactly the dimension
										// selected to be browsed
										for (LDResource cube : cubeDimsOfAggregationSet.keySet()) {
											List<LDResource> cubeDims = cubeDimsOfAggregationSet.get(cube);
											if ((cubeDims.size() == aggregationSetSelectedDims.size())
													&& cubeDims.containsAll(aggregationSetSelectedDims)) {
												System.out.println("NEW CUBE URI:"+ cube.getURI());
												// The new cube to visualize
												cubeSliceURI = "<"+ cube.getURI() + ">";
												break;
											}
										}

										// clear the previous visualization and
										// create a new one for the new cube
										languagecontainer.removeAll();
										fixedDimContainer.removeAll();
										visTypeContainer.removeAll();
										selectCubeContainer.removeAll();
										topgrid.removeAll();
										topcontainer.removeAll();
										dimensionURIfcomponents.clear();
										grid.removeAll();
										visualizations.clear();
										cnt.removeAll();
										label = 0;
										combo = 0;
										mapShown = false;
										firstCubeLoad = false;

										// show the cube
										populateMapViewContainer();
										createMap(this);
										cnt.populateView();
									}

								};

								// set as checked if the dimension is contained at the selected cube
								aggrDimCheckBox.setChecked(selectedDimenisons.contains(aggdim));
								mapDimURIcheckBox.put(aggdim, aggrDimCheckBox);
								topcontainer.add(aggrDimCheckBox);

								aggregationDim++;

								if (selectedDimenisons.contains(aggdim)) {
									List<FCheckBox> dimLevels_checkBoxes = new ArrayList<FCheckBox>();

									FContainer thisDimLevelsContainer = new FContainer(	"thisDimLevelsContainer_"+ aggregationDim);
									thisDimLevelsContainer.addStyle("border-style", "solid");
									thisDimLevelsContainer.addStyle("border-width", "1px");
									thisDimLevelsContainer.addStyle("padding",	"10px");									
									thisDimLevelsContainer.addStyle("border-radius", "10px");
									thisDimLevelsContainer.addStyle("border-color", "#C8C8C8 ");
									thisDimLevelsContainer.addStyle("display","table-cell ");
									thisDimLevelsContainer.addStyle("vertical-align", "middle ");
									thisDimLevelsContainer.addStyle("margin-left", "auto");
									thisDimLevelsContainer.addStyle("margin-right", "auto");
									thisDimLevelsContainer.addStyle("text-align", "left");

									if (selectedLanguage.equals("en")) {
										FLabel selectLevels_label = new FLabel(
												"selectLevels_label_"+ aggregationDim,
												"<p><i>(Select at most 1 levels)</i></p>");
										thisDimLevelsContainer.add(selectLevels_label);
									} else if (selectedLanguage.equals("nl")) {
										FLabel selectLevels_label = new FLabel(
												"selectLevels_label_"+ aggregationDim,
												"<p><i>(Selecteer maximaal 1 niveaus)</i></p>");
										thisDimLevelsContainer.add(selectLevels_label);
									} else if (selectedLanguage.equals("fr")) {
										FLabel selectLevels_label = new FLabel(
												"selectLevels_label_"+ aggregationDim,
												"<p><i>(Sélectionnez au maximum 1 niveaux)</i></p>");
										thisDimLevelsContainer.add(selectLevels_label);
									} else if (selectedLanguage.equals("de")) {
										FLabel selectLevels_label = new FLabel(
												"selectLevels_label_"+ aggregationDim,
												"<p><i>(Bitte wählen Sie höchstens 1 Ebenen)</i></p>");
										thisDimLevelsContainer.add(selectLevels_label);
									}

									for (LDResource level : allDimensionsLevels.get(aggdim)) {
										FCheckBox dimLevelCheckBox = new FCheckBox(
												"level_" + aggregationDim,level.getURIorLabel()) {

											public void onClick() {

												selectedDimLevels = new HashMap<LDResource, List<LDResource>>();

												// Get the selected levels for each dimension
												for (LDResource aggSetDimURI : mapDimLevelscheckBoxes.keySet()) {
													List<FCheckBox> dimLevelCheckList = mapDimLevelscheckBoxes.get(aggSetDimURI);

													// If there are levels at the Dim
													if (dimLevelCheckList != null&& dimLevelCheckList.size() > 0) {
														// The Selected Dimension Levels as is now
														List<LDResource> thisDimSelectedLevels = new ArrayList<LDResource>();

														// All the existing Dimension Levels
														List<LDResource> thisDimAllLevels = allDimensionsLevels.get(aggSetDimURI);
														if (thisDimAllLevels != null) {
															// Check all checkboxes oflevels for a dim if are checked
															for (FCheckBox check : dimLevelCheckList) {
																// If is checked add the level to the selected
																if (check.checked) {
																	// search the Level LDResource at the existing levels
																	for (LDResource l : thisDimAllLevels) {
																		// The checkbox has the same  value (URI) with the level
																		if (l.getURIorLabel().equals(check.getLabel())) {
																			// Add the LDResource level at the selected and break
																			thisDimSelectedLevels.add(l);
																			break;
																		}
																	}
																}
															}
														}
														selectedDimLevels.put(aggSetDimURI,	thisDimSelectedLevels);
													}
												}

												boolean correctNumOfLevels = true;

												for (LDResource d : selectedDimLevels.keySet()) {
													List<LDResource> thisDimSelectedLeves = selectedDimLevels.get(d);
													if (thisDimSelectedLeves.size() > 1) {
														correctNumOfLevels = false;

														// remove the added levels from the selected dim levels
														for (LDResource l : thisDimSelectedLeves) {
															// The checkbox has the same value (URI) with the level
															if (l.getURIorLabel().equals(this.getLabel())) {
																// Add the LDResource level at the selected and break
																thisDimSelectedLeves.remove(l);
																selectedDimLevels.put(d,thisDimSelectedLeves);
																break;
															}
														}
														break;
													}
												}

												if (correctNumOfLevels) {
													// clear the previous visualization and
													// create a new one for the new cube
													languagecontainer.removeAll();
													fixedDimContainer.removeAll();
													visTypeContainer.removeAll();
													selectCubeContainer.removeAll();
													topgrid.removeAll();
													topcontainer.removeAll();
													dimensionURIfcomponents	.clear();
													grid.removeAll();
													visualizations.clear();
													cnt.removeAll();
													label = 0;
													combo = 0;
													mapShown = false;
													firstCubeLoad = false;

													// show the cube
													populateMapViewContainer();
													createMap(this);
													cnt.populateView();
												} else if (!correctNumOfLevels) {

													if (selectedLanguage.equals("en")) {
														FDialog.showMessage(this.getPage(),
																"Please select at most 1 levels",
																"Please select at most 1 levels","ok");
													} else if (selectedLanguage.equals("nl")) {
														FDialog.showMessage(this.getPage(),
																"Selecteer maximaal 1 niveaus",
																"Selecteer maximaal 1 niveaus",	"ok");
													} else if (selectedLanguage.equals("fr")) {
														FDialog.showMessage(this.getPage(),
																"Sélectionnez au maximum 1 niveaux ",
																"Sélectionnez au maximum 1 niveaux ","ok");
													} else if (selectedLanguage.equals("de")) {
														FDialog.showMessage(this.getPage(),
																"Bitte wählen Sie höchstens 1 Ebenen",
																"Bitte wählen Sie höchstens 1 Ebenen","ok");
													}
													this.setChecked(false);

												}
											}
										};

										if (selectedDimLevels.get(aggdim) != null) {
											dimLevelCheckBox.setChecked(selectedDimLevels.get(aggdim).contains(level));
										}

										dimLevels_checkBoxes.add(dimLevelCheckBox);
										thisDimLevelsContainer.add(dimLevelCheckBox);
										aggregationDim++;
									}

									// if there are levels added (there is one FLabel added by default)
									if (thisDimLevelsContainer.getAllComponents().size() > 1) {
										mapDimLevelscheckBoxes.put(aggdim,	dimLevels_checkBoxes);
										topcontainer.add(thisDimLevelsContainer);
									}
								}

							}
						}
					} else {
						FLabel notOLAP = new FLabel("notOLAP", "<b>OLAP-like browsing is not supported for this cube<b>");
						topcontainer.add(notOLAP);
					}
				}

				// //////////Measures container//////////////////
				measurescontainer = new FContainer("measurescontainer");
				// measure container styling
				measurescontainer.addStyle("border-style", "solid");
				measurescontainer.addStyle("border-width", "1px");
				measurescontainer.addStyle("padding", "5px");
				measurescontainer.addStyle("border-radius", "5px");
				measurescontainer.addStyle("border-color", "#C8C8C8 ");
				measurescontainer.addStyle("height", containerHeight + "px ");
				measurescontainer.addStyle("width", "300px ");
				measurescontainer.addStyle("margin-left", "auto");
				measurescontainer.addStyle("margin-right", "auto");
				measurescontainer.addStyle("text-align", "left");

				if (selectedLanguage.equals("en")) {
					FLabel measure_label = new FLabel("measure_lb",	"<b>Measures:</b></br>");
					measurescontainer.add(measure_label);
				} else if (selectedLanguage.equals("nl")) {
					FLabel measure_label = new FLabel("measure_lb", "<b>Metingen:</b></br>");
					measurescontainer.add(measure_label);
				} else if (selectedLanguage.equals("fr")) {
					FLabel measure_label = new FLabel("measure_lb",	"<b>Mesures:</b></br>");
					measurescontainer.add(measure_label);
				} else if (selectedLanguage.equals("de")) {
					FLabel measure_label = new FLabel("measure_lb",	"<b>Messungen:</b></br>");
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

							// The cubeMeasures have the correct language while
							// the selected not
							List<LDResource> tmplist = new ArrayList<LDResource>(cubeMeasures);
							tmplist.retainAll(selectedMeasures);
							selectedMeasures = tmplist;

							// Calculate the selected dims and the NEW cube.
							// This is needed the first time.
							// Because the selected cube is the original with
							// all the dims. But we need to
							// Visualize the cube with only the geodimension
							if (firstCubeLoad) {
								// Get all selected aggregation set dimensions
								// for browsing
								List<LDResource> aggregationSetSelectedDims = new ArrayList<LDResource>();
								selectedDimenisons = new ArrayList<LDResource>();
								for (LDResource aggSetDimURI : mapDimURIcheckBox
										.keySet()) {
									FCheckBox check = mapDimURIcheckBox.get(aggSetDimURI);

									// Get selected dimensions
									if (check.checked) {
										aggregationSetSelectedDims.add(aggSetDimURI);
										selectedDimenisons.add(aggSetDimURI);
									}
								}

								// Add the geodimension at the selected
								aggregationSetSelectedDims.add(geodimension);
								selectedDimenisons.add(geodimension);

								// Identify the cube of the aggregation set that
								// contains exactly the dimension selected to be browsed
								for (LDResource cube : cubeDimsOfAggregationSet.keySet()) {
									List<LDResource> cubeDims = cubeDimsOfAggregationSet.get(cube);
									if ((cubeDims.size() == aggregationSetSelectedDims.size())
											&& cubeDims.containsAll(aggregationSetSelectedDims)) {

										System.out.println("NEW CUBE URI:"+ cube.getURI());
										// The new cube to visualize
										cubeSliceURI = "<" + cube.getURI()+ ">";
										break;
									}
								}
							}

							languagecontainer.removeAll();
							fixedDimContainer.removeAll();
							visTypeContainer.removeAll();
							selectCubeContainer.removeAll();
							topgrid.removeAll();
							topcontainer.removeAll();
							dimensionURIfcomponents.clear();
							grid.removeAll();
							visualizations.clear();
							cnt.removeAll();
							label = 0;
							combo = 0;
							mapShown = false;
							firstCubeLoad = false;

							populateMapViewContainer();
							if ((selectedMeasures.size() > 1 && selectedOperation.contains("Markers"))
									|| (selectedMeasures.size() == 1)) {
								createMap(this);
							} else if (selectedMeasures.size() > 0) {
								FDialog.showMessage(
										this.getPage(),
										"Measures",
										"Multiple measures are only supportted for Markers Map",
										"ok");
							}
							cnt.populateView();
						}
					};

					colorIndex++;

					// set as checked if the dimension is contained at the selected cube
					measureCheckBox.setChecked(selectedMeasures.contains(measure));
					mapMeasureURIcheckBox.put(measure, measureCheckBox);
					measurescontainer.add(measureCheckBox);
					measureCount++;
				}

				grid = new FGrid("grid");

				fixedDimContainer = new FContainer("fixedDimContainer");
				// Visualization Type Container styling
				fixedDimContainer.addStyle("border-style", "solid");
				fixedDimContainer.addStyle("border-width", "1px");
				fixedDimContainer.addStyle("padding", "5px");
				fixedDimContainer.addStyle("border-radius", "5px");
				fixedDimContainer.addStyle("border-color", "#C8C8C8 ");
				fixedDimContainer.addStyle("display", "table-cell ");
				fixedDimContainer.addStyle("margin-left", "auto");
				fixedDimContainer.addStyle("margin-right", "auto");
				fixedDimContainer.addStyle("height", containerHeight + "px ");
				fixedDimContainer.addStyle("width", "300px ");
				fixedDimContainer.addStyle("text-align", "left");

				FHTML text = new FHTML("text");

				if (selectedLanguage.equals("en")) {
					text.setValue("<b>Filter:</b></br>");
				} else if (selectedLanguage.equals("nl")) {
					text.setValue("<b>Filter:</b></br>");
				} else if (selectedLanguage.equals("fr")) {
					text.setValue("<b>Filtre:</b></br>");
				} else if (selectedLanguage.equals("de")) {
					text.setValue("<b>Filter:</b></br>");
				}

				text.setValue("<b>Filter:</b></br>");
				fixedDimContainer.add(text);

				if (selectedDimenisons.size() > 1) {
					// If it is a slice and there are fixed dimension values
					if (!sliceFixedDimensionsValues.isEmpty()) {

						for (LDResource sliceFixedDim : sliceFixedDimensionsValues.keySet()) {

							ArrayList<FComponent> farray = new ArrayList<FComponent>();
							Random rnd = new Random();
							Long rndLong = Math.abs(rnd.nextLong());

							String id = sliceFixedDim.getURI();

							// remove characters that are supported by the
							// FComponent ID
							id = id.replaceAll("//", "");
							id = id.replaceAll("#", "");
							id = id.replaceAll(":", "");

							// IWB does not support too long IDs
							if (id.length() > 10) {
								// use as ID the last part of the URI (the first
								// part is common to all URIs)
								id = id.substring(id.length() - 11, id.length());
							}

							// Add the label for the fixed cube dimension
							FLabel fDimLabel = new FLabel(id + "_" + rndLong
									+ "_name", sliceFixedDim.getURIorLabel()+ ":");

							farray.add(fDimLabel);
							LDResource fDimValue = sliceFixedDimensionsValues.get(sliceFixedDim);
							FLabel fDimValueLabel = new FLabel(id + "_"	+ rndLong + "_value",	fDimValue.getURIorLabel());
							farray.add(fDimValueLabel);
							grid.addRow(farray);
							farray = new ArrayList<FComponent>();
							grid.addRow(farray);
						}
					}				

					// Selected values for the fixed dimensions
					fixedDimensionsSelectedValues = CubeHandlingUtils
							.getFixedDimensionsRandomSelectedValues(
									allDimensionsValues, cubeDimensions,
									fixedDimensionsSelectedValues);

					// Dynamically create as comboboxes as the number of the
					// dimensions of the cube (except for the geospatial dimension)
					for (LDResource fDim : cubeDimensions) {

						// Remove the geo dimension from the combo boxes
						if (!fDim.getURI().equals(geodimension.getURI())) {							
							List<FComponent> dimComponents = new ArrayList<FComponent>();
							// Add label
							FLabel fDimLabel = null;
							if (fDim.getURIorLabel() != null) {
								fDimLabel = new FLabel(	(new Integer(label++).toString())
												+ "_label",	fDim.getURIorLabel().replace(".","-")+ ": ");
							} else
								fDimLabel = new FLabel((new Integer(label++).toString())
												+ "_label", "<b></b> <br>");

							dimComponents.add(fDimLabel);
							fDimLabel.addStyle("float", "left");

							// Add Combobox
							fDimCombo = new FComboBox((new Integer(combo++).toString())+ "_combo") {
								@Override
								public void onChange() {
									createMap(this);
								}
							};

							List<LDResource> dimLevels = allDimensionsLevels.get(fDim);

							// The dimension has levels
							// Show the values of the selected levels
							if (dimLevels != null && dimLevels.size() > 0) {
								List<LDResource> thisDimSelectedLevels = selectedDimLevels.get(fDim);
								if (thisDimSelectedLevels != null) {
									for (LDResource level : thisDimSelectedLevels) {
										List<LDResource> levelValues = allLevelsValues.get(level);
										for (LDResource levelVal : levelValues) {
											// Show the first 30 chars if label too long
											if (levelVal.getURIorLabel().length() > 30) {
												fDimCombo.addChoice(
														levelVal.getURIorLabel().substring(0, 30)
														+ "...", levelVal.getURI());
											} else {
												fDimCombo.addChoice(
														levelVal.getURIorLabel(),
														levelVal.getURI());
											}
										}
									}
								}
							} else {
								for (LDResource ldr : allDimensionsValues.get(fDim)) {
									// Show the first 25 chars if label too long
									if (ldr.getURIorLabel().length() > 30) {
										fDimCombo.addChoice(
												ldr.getURIorLabel().substring(0, 30) + "...",
												ldr.getURI());
									} else {
										fDimCombo.addChoice(ldr.getURIorLabel(),ldr.getURI());
									}
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
				} else {

					FHTML selectADimToFilter = new FHTML("selectADimToFilter");

					if (selectedLanguage.equals("en")) {
						selectADimToFilter.setValue("</br><i>Select a dimension to filter</i></br>");
					} else if (selectedLanguage.equals("nl")) {
						selectADimToFilter.setValue("</br><i>Selecteer een dimensie om te filteren </i></br>");
					} else if (selectedLanguage.equals("fr")) {
						selectADimToFilter.setValue("</br><i>Sélectionnez une dimension pour filtrer</i></br>");
					} else if (selectedLanguage.equals("de")) {
						selectADimToFilter.setValue("</br><i>Wählen Sie eine Dimension zu filtern</i></br>");
					}

					fixedDimContainer.add(selectADimToFilter);
				}

				FContainer thisDimLevelsContainer = new FContainer(
						"geoDimLevelsContainer");
				if (allDimensionsLevels.get(geodimension) != null
						&& allDimensionsLevels.get(geodimension).size() > 0) {

					List<FCheckBox> dimLevels_checkBoxes = new ArrayList<FCheckBox>();

					thisDimLevelsContainer.addStyle("border-style", "solid");
					thisDimLevelsContainer.addStyle("border-width", "1px");
					thisDimLevelsContainer.addStyle("padding", "5px");
					thisDimLevelsContainer.addStyle("border-radius", "5px");
					thisDimLevelsContainer.addStyle("width", "890px ");
					thisDimLevelsContainer.addStyle("border-color", "#C8C8C8 ");
					thisDimLevelsContainer.addStyle("display", "table-cell ");
					thisDimLevelsContainer.addStyle("vertical-align", "middle ");
					thisDimLevelsContainer.addStyle("margin-left", "auto");
					thisDimLevelsContainer.addStyle("margin-right", "auto");
					thisDimLevelsContainer.addStyle("text-align", "left");

					if (selectedLanguage.equals("en")) {
						FLabel geotLevels_label = new FLabel("geoLevels_label",
								"<b>Geography granularity</b>");
						thisDimLevelsContainer.add(geotLevels_label);
					} else if (selectedLanguage.equals("nl")) {
						FLabel geotLevels_label = new FLabel("geoLevels_label",
								"<b>Geografische granulariteit</b>");
						thisDimLevelsContainer.add(geotLevels_label);
					} else if (selectedLanguage.equals("fr")) {
						FLabel geotLevels_label = new FLabel("geoLevels_label",
								"<b>Granularité géographique</b>");
						thisDimLevelsContainer.add(geotLevels_label);
					} else if (selectedLanguage.equals("de")) {
						FLabel geotLevels_label = new FLabel("geoLevels_label",
								"<b>Geographischen Granularitat</b>");
						thisDimLevelsContainer.add(geotLevels_label);
					}

					int aggregationDim = 1;
					ArrayList<FComponent> geoDimLevelsComponentArray = new ArrayList<FComponent>();
					for (LDResource level : allDimensionsLevels.get(geodimension)) {

						FCheckBox dimLevelCheckBox = new FCheckBox("level_"
								+ aggregationDim, level.getURIorLabel()) {
							public void onClick() {
								List<FCheckBox> geoDimLevelCheckList = mapDimLevelscheckBoxes.get(geodimension);
								if (this.checked == true) {
									for (FCheckBox check : geoDimLevelCheckList) {
										if (!check.equals(this)) {
											check.setChecked(false);
										}
									}
								} else {
									this.setChecked(true);
								}

								// All the existing Dimension Levels
								List<LDResource> geoLevels = allDimensionsLevels.get(geodimension);

								for (LDResource l : geoLevels) {
									// The checkbox has the same value (URI)
									// with the level
									if (l.getURIorLabel().equals(this.getLabel())) {
										// Add the LDResource level at the selected and break
										selectedGeoLevel = l;
										break;
									}
								}

								languagecontainer.removeAll();
								fixedDimContainer.removeAll();
								visTypeContainer.removeAll();
								selectCubeContainer.removeAll();
								topgrid.removeAll();
								topcontainer.removeAll();
								dimensionURIfcomponents.clear();
								grid.removeAll();
								visualizations.clear();
								cnt.removeAll();
								label = 0;
								combo = 0;
								mapShown = false;

								populateMapViewContainer();

								if ((selectedMeasures.size() > 1 && selectedOperation.contains("Markers"))
										|| (selectedMeasures.size() == 1)) {
									createMap(this);
								} else if (selectedMeasures.size() > 0) {
									FDialog.showMessage(
											this.getPage(),
											"Measures",
											"Multiple measures are only supportted for Markers Map",
											"ok");
								}
								cnt.populateView();
							}
						};

						if (selectedGeoLevel != null) {
							dimLevelCheckBox.setChecked(selectedGeoLevel.equals(level));
						} else {
							selectedGeoLevel = level;
							dimLevelCheckBox.setChecked(true);
						}

						dimLevels_checkBoxes.add(dimLevelCheckBox);

						geoDimLevelsComponentArray.add(dimLevelCheckBox);
						aggregationDim++;
					}

					mapDimLevelscheckBoxes.put(geodimension,
							dimLevels_checkBoxes);
					FGrid geoDimeLevels_grid = new FGrid("geoDimeLevels_grid");
					geoDimeLevels_grid.addRow(geoDimLevelsComponentArray);
					thisDimLevelsContainer.add(geoDimeLevels_grid);
				}

				// ------------------ UI --------------------------

				// Create the FHTML object that will contain the maps
				map = new FHTML("mapping");

				ArrayList<FComponent> fcomponentArray = new ArrayList<FComponent>();
				fcomponentArray.add(selectCubeContainer);
				fcomponentArray.add(visTypeContainer);
				fcomponentArray.add(languagecontainer);

				selectCubeGrid = new FGrid("selectCubeGrid");
				selectCubeGrid.addStyle("width", "900px ");
				selectCubeGrid.addRow(fcomponentArray);

				top1 = new FContainer("top1");
				top1.addStyle("padding", "0px");
				top1.addStyle("border-color", "#C8C8C8 ");
				top1.addStyle("display", "table-cell ");
				top1.addStyle("vertical-align", "middle ");
				top1.addStyle("margin-left", "auto");
				top1.addStyle("margin-right", "auto");
				top1.addStyle("width", "900px ");
				top1.addStyle("text-align", "left");

				top1.add(selectCubeGrid);

				fcomponentArray = new ArrayList<FComponent>();

				if (isCube) {
					fcomponentArray.add(topcontainer);
				}
				fcomponentArray.add(measurescontainer);				
				fcomponentArray.add(fixedDimContainer);		

				measureDimGrid = new FGrid("measureDimGrid");
				measureDimGrid.addStyle("width", "900px ");
				measureDimGrid.addRow(fcomponentArray);

				top2 = new FContainer("top2");				
				top2.addStyle("padding", "0px");				
				top2.addStyle("border-color", "#C8C8C8 ");
				top2.addStyle("display", "table-cell ");
				top2.addStyle("vertical-align", "middle ");
				top2.addStyle("margin-left", "auto");
				top2.addStyle("margin-right", "auto");
				top2.addStyle("width", "900px ");
				top2.addStyle("text-align", "left");
				top2.add(measureDimGrid);

				topgrid = new FGrid("topgrid");
				topgrid.addStyle("padding", "0px");
				
				fcomponentArray = new ArrayList<FComponent>();
				fcomponentArray.add(selectCubeGrid);
				topgrid.addRow(fcomponentArray);

				fcomponentArray = new ArrayList<FComponent>();
				fcomponentArray.add(measureDimGrid);
				topgrid.addRow(fcomponentArray);

				fcomponentArray = new ArrayList<FComponent>();
				fcomponentArray.add(thisDimLevelsContainer);
				topgrid.addRow(fcomponentArray);

				cnt.add(topgrid);

				boolean correctNumOfLevels = true;
				List<LDResource> selectedDimsWithNoGeoDim = new ArrayList<LDResource>(selectedDimenisons);
				selectedDimsWithNoGeoDim.remove(geodimension);
				for (LDResource d : selectedDimsWithNoGeoDim) {
					List<LDResource> thisDimSelectedLeves = selectedDimLevels.get(d);
					List<LDResource> thisDimAllLevels = allDimensionsLevels	.get(d);

					if (thisDimAllLevels.size() > 0
							&& (thisDimSelectedLeves == null || thisDimSelectedLeves.size() == 0)) {
						correctNumOfLevels = false;
						break;
					}
				}

				if ((selectedMeasures.size() > 0
						&& selectedDimenisons.size() > 0 && isCube && correctNumOfLevels)
						|| (isSlice && selectedMeasures.size() > 0)) {
					cnt.add(map);

				} else {
					
					FContainer placeholder = new FContainer("placeholder");
					placeholder.addStyle("border-style", "dashed");
					placeholder.addStyle("border-width", "4px");
					placeholder.addStyle("padding", "5px");
					placeholder.addStyle("border-radius", "5px");
					placeholder.addStyle("border-color", "#C8C8C8");
					placeholder.addStyle("background-color", "#F1F1F1");
					placeholder.addStyle("height", "400px ");
					placeholder.addStyle("display", "table-cell ");
					placeholder.addStyle("align", "center");
					placeholder.addStyle("width", "900px ");

					if (selectedLanguage.equals("en")) {
						FLabel placeholderlabel = new FLabel(
								"placeholderlabel", "<b>Select at least one "
										+ "Measure to visualize on map.</b>");
						placeholder.add(placeholderlabel);
					} else if (selectedLanguage.equals("nl")) {
						FLabel placeholderlabel = new FLabel(
								"placeholderlabel",
								"<b>Selecteer minstens één meting om op de map te visualiseren.</b>");
						placeholder.add(placeholderlabel);
					} else if (selectedLanguage.equals("fr")) {
						FLabel placeholderlabel = new FLabel(
								"placeholderlabel",
								"<b>Sélectionnez au moins une mesure pour visualiser sur la carte.</b>");
						placeholder.add(placeholderlabel);
					} else if (selectedLanguage.equals("de")) {
						FLabel placeholderlabel = new FLabel(
								"placeholderlabel",
								"<b>Wählen Sie mindestens eine Maßnahme , um auf der Karte zu visualisieren.</b>");
						placeholder.add(placeholderlabel);
					}
					cnt.add(placeholder);
				}
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
			String message = "The URI <b>" + uri
					+ "</b> is not a valid cube or slice URI.";
			FLabel invalidURI_label = new FLabel("invalidURI", message);
			cnt.add(invalidURI_label);
		}
	}

	private void addVisualizationTypes() {
		visTypeContainer = new FContainer("visTypeContainer");

		// Visualization Type Container styling
		visTypeContainer.addStyle("border-style", "solid");
		visTypeContainer.addStyle("border-width", "1px");
		visTypeContainer.addStyle("padding", "5px");
		visTypeContainer.addStyle("border-radius", "5px");
		visTypeContainer.addStyle("border-color", "#C8C8C8 ");
		visTypeContainer.addStyle("display", "table-cell ");		
		visTypeContainer.addStyle("width", "100px ");
		visTypeContainer.addStyle("margin-left", "auto");
		visTypeContainer.addStyle("margin-right", "auto");
		visTypeContainer.addStyle("text-align", "left");

		// Add label for the dropdown that selects the type of visualization
		if (selectedLanguage.equals("en")) {
			visTypeLabel = new FLabel("dim1Label", "<b>Map Type:</b><br>");
			visTypeContainer.add(visTypeLabel);
		} else if (selectedLanguage.equals("nl")) {
			visTypeLabel = new FLabel("dim1Label", "<b>Type kaart:</b><br>");
			visTypeContainer.add(visTypeLabel);
		} else if (selectedLanguage.equals("fr")) {
			visTypeLabel = new FLabel("dim1Label", "<b>Type de carte:</b><br>");
			visTypeContainer.add(visTypeLabel);
		}else if (selectedLanguage.equals("de")) {
			visTypeLabel = new FLabel("dim1Label", "<b>Kartentyp:</b><br>");
			visTypeContainer.add(visTypeLabel);
		}

		// Add the types of the supported visualizations
		visualizations.add(0, "Markers map");
		visualizations.add(1, "Choropleth map");
		visualizations.add(2, "Bubble map");

		// Create the FComboBox with the supported types of visualizations.
		String selected = "";

		if (vistypes != null) {
			selected = (String) vistypes.getSelected().get(0);
		}

		vistypes = new FComboBox("vistypes") {
			@Override
			public void onChange() {
				if (isFirstLoad) {
					visTypeContainer.removeAll();
					visualizations.clear();
					cnt.removeAll();
					populateMapViewContainer();
					isFirstLoad = false;
				}
				selectedOperation = (String) vistypes.getSelected().get(0);
				vistypes.setPreSelected(selectedOperation);

				if ((selectedMeasures.size() > 1 && selectedOperation
						.contains("Markers")) || (selectedMeasures.size() == 1)) {
					createMap(this);
				} else if (selectedMeasures.size() > 0) {
					FDialog.showMessage(
							this.getPage(),
							"Measures",
							"Multiple measures are only supportted for Markers Map",
							"ok");
				}
				cnt.populateView();

			}
		};

		vistypes.addChoices(visualizations);

		if (vistypes != null) {
			vistypes.setPreSelected(selectedOperation);
		}

		visTypeContainer.add(vistypes);
	}

	private void addCubeSelectionCombo() {

		// top container styling
		selectCubeContainer = new FContainer("selectCubeContainer");

		selectCubeContainer.addStyle("border-style", "solid");
		selectCubeContainer.addStyle("border-width", "1px");
		selectCubeContainer.addStyle("padding", "5px");
		selectCubeContainer.addStyle("border-radius", "5px");
		selectCubeContainer.addStyle("width", "700px ");
		selectCubeContainer.addStyle("border-color", "#C8C8C8 ");
		selectCubeContainer.addStyle("display", "table-cell ");
		selectCubeContainer.addStyle("vertical-align", "middle ");
		selectCubeContainer.addStyle("align", "center");
		selectCubeContainer.addStyle("text-align", "left");

		if (selectedLanguage.equals("en")) {
			FLabel allcubes_label = new FLabel("allcubes_label",
					"<b>Please select a cube to visualize on map:<b>");
			selectCubeContainer.add(allcubes_label);
		} else if (selectedLanguage.equals("nl")) {
			FLabel allcubes_label = new FLabel("allcubes_label",
					"<b>Selecteer een kubus te visualiseren op de kaart:<b>");
			selectCubeContainer.add(allcubes_label);
		} else if (selectedLanguage.equals("fr")) {
			FLabel allcubes_label = new FLabel("allcubes_label",
					"<b>Veuillez sélectionner un cube de visualiser sur la carte:<b>");
			selectCubeContainer.add(allcubes_label);
		} else if (selectedLanguage.equals("de")) {
			FLabel allcubes_label = new FLabel("allcubes_label",
					"<b>Bitte wählen Sie einen Würfel, um auf der Karte zu visualisieren:<b>");
			selectCubeContainer.add(allcubes_label);
		}

		// Add Combo box with cube URIs
		FComboBox cubesCombo = new FComboBox("cubesCombo") {

			@Override
			public void onChange() {
				cubeSliceURI = "<"	+ ((LDResource) this.getSelected().get(0)).getURI()	+ ">";
				selectedCube = (LDResource) this.getSelected().get(0);
				if (isFirstLoad) {
					selectedOperation = "Choropleth map";
					isFirstLoad = false;
				} else {
					topgrid.removeAll();
					grid.removeAll();
				}

				selectedMeasures = new ArrayList<LDResource>();
				selectedDimenisons = new ArrayList<LDResource>();
				allDimensionsLevels=new HashMap<LDResource, List<LDResource>>();
				selectedGeoLevel=null;
				
				// clear the previous visualization and
				// create a new one for the new cube
				languagecontainer.removeAll();
				fixedDimContainer.removeAll();
				visTypeContainer.removeAll();
				selectCubeContainer.removeAll();
				measurescontainer.removeAll();
				topcontainer.removeAll();
				dimensionURIfcomponents.clear();
				mapMeasureURIcheckBox.clear();
				mapDimURIcheckBox.clear();

				visualizations.clear();
				cnt.removeAll();
				label = 0;
				combo = 0;
				mapShown = false;
				firstCubeLoad = true;

				// show the cube
				populateMapViewContainer();
				createMap(this);
				cnt.populateView();				
			}
		};

		// populate cubes combo box
		for (LDResource cube : allCubes) {
			if (cube.getURIorLabel() != null) {
					cubesCombo.addChoice(cube.getURIorLabel(), cube);
				}
		}

		if (selectedCube != null) {
			cubesCombo.setPreSelected(selectedCube);
		}
		selectCubeContainer.add(cubesCombo);
	}

	private void addSliceCreate() {
		// Show the create slice button if there are more that one dimension
		// i.e. one more dimension than the geodimension
		if (cubeDimensions.size() > 1 && !mapShown && isCube) {
			mapShown = true;
			FContainer bottomcontainer = new FContainer("bottomcontainer");

			// Bottom container styling
			bottomcontainer.addStyle("border-style", "solid");
			bottomcontainer.addStyle("border-width", "1px");
			bottomcontainer.addStyle("padding", "5px");
			bottomcontainer.addStyle("border-radius", "5px");
			bottomcontainer.addStyle("width", "900px ");
			bottomcontainer.addStyle("border-color", "#C8C8C8 ");
			bottomcontainer.addStyle("display", "table-cell ");
			bottomcontainer.addStyle("vertical-align", "middle ");
			bottomcontainer.addStyle("align", "center");
			bottomcontainer.addStyle("text-align", "left");

			FLabel bottomLabel = new FLabel(
					"bottomlabel","<b>Slice</b></br>"
							+ "In case you want to create and store a slice of the cube as it is presented "
							+ "in the MapView click the button:</br>");
			bottomcontainer.add(bottomLabel);

			// Button to create slice
			FButton createSlice = new FButton("createSlice", "createSlice") {
				@Override
				public void onClick() {
					String sliceURI = SliceSPARQL.createCubeSlice(cubeSliceURI,
							cubeGraph, fixedDimensionsSelectedValues,
							sliceObservations, SPARQL_service);
					String message = "A new slice with the following URI has been created:"
							+ sliceURI;
					FDialog.showMessage(this.getPage(), "New Slice created",message, "ok");
				}
			};

			bottomcontainer.add(createSlice);
			cnt.add(getNewLineComponent(false));
			cnt.add(bottomcontainer);			
		}
	}

	private void createMap(FComponent comp) {
		
		boolean correctNumOfLevels = true;
		List<LDResource> selectedDimsWithNoGeoDim = new ArrayList<LDResource>(selectedDimenisons);
		selectedDimsWithNoGeoDim.remove(geodimension);
		for (LDResource d : selectedDimsWithNoGeoDim) {
			List<LDResource> thisDimSelectedLeves = selectedDimLevels.get(d);
			List<LDResource> thisDimAllLevels = allDimensionsLevels	.get(d);

			if (thisDimAllLevels.size() > 0
					&& (thisDimSelectedLeves == null || thisDimSelectedLeves.size() == 0)) {
				correctNumOfLevels = false;
				break;
			}
		}
		
		if ((selectedMeasures.size() > 0 && selectedDimenisons.size() > 0 && isCube &&correctNumOfLevels)
				|| (isSlice && selectedMeasures.size() > 0)) {

			String maptext = "<div id=\"map\" style=\"width:920px; height:400px;\"></div>";
			map.setValue(maptext);

			String attr_labels = "";
			// Create the label of the geo-dimension that will be showed in the markers
			String geo_label = "'"	+ geodimension.getURIorLabel().replaceAll("'", "\'") + "'";
			
			// Get all the attribute labels to show in markers
			for (LDResource attres : cubeAttributes) {
				if (attres.getURIorLabel() != null)
					attr_labels += "'"+ attres.getURIorLabel().replaceAll("'", "\'")+ "',";
				else
					attr_labels += "' '";
			}

			if (attr_labels.length() > 0) {
				attr_labels = attr_labels
						.substring(0, attr_labels.length() - 1);
			}

			String measureLabels = "";

			// Get all the selected measures labels to show in markers
			for (LDResource meas : selectedMeasures) {
				if (meas.getURIorLabel() != null)
					measureLabels += "'"
							+ meas.getURIorLabel().replaceAll("'", "\'") + "',";
				else
					measureLabels += "' '";
			}

			if (measureLabels.length() > 0) {
				measureLabels = measureLabels.substring(0,measureLabels.length() - 1);
			}

			String temp = "";
			HashMap<LDResource, LDResource> tmpFixedDimensionsSelectedValues = new HashMap<LDResource, LDResource>();
			for (LDResource dimres : dimensionURIfcomponents.keySet()) {
				List<FComponent> dimComponents = dimensionURIfcomponents
						.get(dimres);
				String selectedValue = ((FComboBox) dimComponents.get(1)).getSelectedAsString().get(0);
				List<LDResource> selectedDimValues = allDimensionsValues.get(dimres);
				for (LDResource dimValue : selectedDimValues) {
					if (dimValue.getURI().equals(selectedValue)) {
						tmpFixedDimensionsSelectedValues.put(dimres, dimValue);
					}
				}
			}

			fixedDimensionsSelectedValues = tmpFixedDimensionsSelectedValues;

			TupleQueryResult res_values = null;
			if (isCube) {
				// Get query tuples for visualization
				res_values = MapViewSPARQL
						.getDVisualsiationValuesMultipleMeasures(
								cubeDimensions, geodimension,
								fixedDimensionsSelectedValues,
								selectedMeasures, cubeAttributes, cubeSliceURI,
								cubeGraph, cubeDSDGraph, selectedGeoLevel,
								selectedLanguage, englishLabel, SPARQL_service);
			} else if (isSlice) {
				// Get query tuples for visualization
				res_values = MapViewSPARQL.getDVisualsiationValuesFromSlice(
						cubeDimensions, geodimension,
						fixedDimensionsSelectedValues, selectedMeasures,
						cubeAttributes, cubeSliceURI, sliceGraph, cubeGraph,
						cubeDSDGraph, selectedLanguage, englishLabel,
						SPARQL_service);
			}

			// The result contains the selectedMeasures[], geolabel
			int results_size = selectedMeasures.size() + cubeAttributes.size()+ 2;			

			Vector<Vector<String>> obsdata = getResults(res_values,results_size);
			
			int all = obsdata.get(0).size();

			// dynamically gather selected values from each dimension combobox
			// dynamically create strings with the selected values for input in
			// the map's bubbles
			for (LDResource fDim : fixedDimensionsSelectedValues.keySet()) {
				if (fDim.getURIorLabel() != null)
					temp += "<b>" + fDim.getURIorLabel() + ":</b>";
				else
					temp += "";
				temp += fixedDimensionsSelectedValues.get(fDim).getURIorLabel()	+ "<br />";
			}

			String values = "";
			for (int i = 0; i < obsdata.get(0).size(); i++) {
				values += "'" + temp.replaceAll("'", " ") + "',";
			}

			String newvalues = "";
			if (values.length() > 2) {
				newvalues = values.substring(0, values.length() - 1);
			}			

			String englishvalues = obsdata.get(results_size - 1).toString().replaceAll("'", " ");
			String geovalues = obsdata.get(results_size - 2).toString().replaceAll("'", " ");
			
			if (mapzoom.equals("BE")&& !selectedOperation.contains("Choropleth")) {
				englishvalues = "'"	+ obsdata.get(results_size - 1).get(0).replaceAll("'", " ") + ", BE'";
				geovalues = "'"	+ obsdata.get(results_size - 2).get(0).replaceAll("'", " ") + ", BE'";				
			} else {
				englishvalues = "'"	+ obsdata.get(results_size - 1).get(0).replaceAll("'", " ") + "'";
				geovalues = "'"	+ obsdata.get(results_size - 2).get(0).replaceAll("'", " ") + "'";				
			}

			List<String> measureValues = new ArrayList<String>();
			for (int i = 0; i < selectedMeasures.size(); i++){
				String tmp2 = obsdata.get(i).toString().replaceAll("'", " ");
				tmp2 = "'" + obsdata.get(i).get(0).replaceAll("'", " ") + "'";
				measureValues.add(tmp2);
			}

			List<String> attributeValues = new ArrayList<String>();

			for (int i = 0; i < cubeAttributes.size(); i++) {
				String tmp2 = obsdata.get(results_size - 3 - i).toString().replaceAll("'", " ");
				tmp2 = "'"+ obsdata.get(results_size - 3 - i).get(0).replaceAll("'", " ") + "'";
				attributeValues.add(tmp2);
			}

			for (int i = 1; i < obsdata.get(results_size - 3).size(); i++) {
				if (mapzoom.equals("BE")&& !selectedOperation.contains("Choropleth")) {
					englishvalues += ",'"+ obsdata.get(results_size - 1).get(i).replaceAll("'", " ") + ", BE'";
					geovalues += ",'"+ obsdata.get(results_size - 2).get(i).replaceAll("'", " ") + ", BE'";					
				} else {
					englishvalues += ",'"+ obsdata.get(results_size - 1).get(i).replaceAll("'", " ") + "'";
					geovalues += ",'"+ obsdata.get(results_size - 2).get(i).replaceAll("'", " ") + "'";					
				}

				for (int j = 0; j < measureValues.size(); j++) {
					String tmp = measureValues.get(j);
					tmp += ",'" + obsdata.get(j).get(i).replaceAll("'", " ")+ "'";
					measureValues.remove(j);
					measureValues.add(j, tmp);
				}

				for (int j = 0; j < attributeValues.size(); j++) {
					String tmp = attributeValues.get(j);
					tmp += ",'"	+ obsdata.get(results_size - 3 - j).get(i).replaceAll("'", " ") + "'";
					attributeValues.remove(j);
					attributeValues.add(j, tmp);
				}
			}

			String javascript = "";
			// According to the selected value of visualization create the
			// corresponding map
			if (selectedOperation.contains("Markers")) {

				javascript = getMarkerJavascript(geovalues, measureValues,
						newvalues, attributeValues, cubeAttributes.size(),
						geo_label, attr_labels, measureLabels, all);

				update.clientCode = javascript;

				comp.addClientUpdate(update);
				javascript = "";
			} else if (selectedOperation.contains("Bubble")) {
				javascript = getBubbleJavascript(geovalues,
						measureValues.get(0), newvalues, attributeValues,
						cubeAttributes.size(), geo_label, attr_labels,
						measureLabels);
				update.clientCode = javascript;
				comp.addClientUpdate(update);
				javascript = "";

			} else if (selectedOperation.contains("Heat")) {

				javascript = getHeatMapJavascript(geovalues,
						measureValues.get(0), newvalues);
				update.clientCode = javascript;
				comp.addClientUpdate(update);
				javascript = "";
			} else if (selectedOperation.contains("Choropleth")) {
				javascript = getChoroplethMapJavascript(geovalues,
						englishvalues, measureValues.get(0), newvalues,
						attributeValues, cubeAttributes.size(), attr_labels,
						measureLabels);
				update.clientCode = javascript;
				comp.addClientUpdate(update);
				javascript = "";
			}
			// If this is not the first load of the widget
			if (!isFirstLoad) {
				cnt.populateView();
			}

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

			// for all the results of the query get the returned values and put them into the @D Vector
			while (res.hasNext()) {

				BindingSet bindingSet = res.next();

				int i = 0;
				for (String bindingName : bindingNames) {
					// Store slice observation for future use i.e. to create slice if needed
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
	 * Input:Strings with the i) values of the geospatial dimension of the cube,
	 * ii) values of the measure of the cube and iii) values of the rest of the
	 * dimensions of the cube, a list of strings with the values of the
	 * attributes of the cube, the number of the attributes of the cube, a
	 * String with the labels of the geospatial values of the cube and a String
	 * with the labels of the attribute values of the cube Output:A String with
	 * the javascript to create the marker map
	 */

	public static String getMarkerJavascript(String geovalue,
			List<String> measurevalues, String dimvalues,
			List<String> attributeValues, int cubeAttributesSize,
			String geo_label, String attr_labels, String measureLabels,
			int iterations) {
	
		String markermap = null;
	
		// Create a javascript array with the values of the measure
		markermap = "var measure = new Array();";

		// Create a javascript array with the values of the geospatial dimension
		markermap += "var arr = new Array (" + geovalue + "); ";

		// Create a javasript array with the values of the dimensions (except for the geospatial dimension)
		markermap += "observ = new Array(" + dimvalues + ");";

		// Create one javascript array for the values of each of the attributes of the cube
		for (int j = 0; j < cubeAttributesSize; j++)
			markermap += "attribute" + j + " = new Array("
					+ attributeValues.get(j) + ");";

		for (int j = 0; j < measurevalues.size(); j++)
			markermap += "measure" + j + " = new Array(" + measurevalues.get(j)	+ ");";

		// Create a javascript array with the labels of the attributes of the cube
		markermap += "attr_labels = new Array(" + attr_labels + ");";
		markermap += "meas_labels = new Array(" + measureLabels + ");";

		// Perform the geocoding
		markermap += "var counter = 0;";

		markermap += "var i,j,temparray, tempmeasure, chunk = 100;";

		markermap += "var geova = new Array(" + geovalue + ");";
		markermap += "group = [];";

		markermap += "var results, k, m=0, all=[], news, features, marker, result, latlng, prop, best, val, map, r, i;";
		markermap += "map = L.map('map', {layers: MQ.mapLayer() });";
		markermap += "var count=0;";
		markermap += "var  o = 0;";
		markermap += "news =[];";
		markermap += "for (k=0,j=geova.length; k<j; k+=chunk) {";
		markermap += "temparray = geova.slice(k,k+chunk); ";
		markermap += "tempmeasure = measure.slice(k,k+chunk); ";
		markermap += "";
		markermap += "var temp = temparray;";

		markermap += " (function (n, meas) { ";
		markermap += "setTimeout(function(){";
		markermap += "MQ.geocode().search(n) .on('success', ";
		markermap += "function(e) " + "{ results =e.result;";
		markermap += "var counter = 0;";
		markermap += "for (i = 0; i < results.length; i++) { ";
		markermap += "result = results[i].best;  ";
		markermap += "latlng = result.latlng; ";
		markermap += "}";

		markermap += "for (i = 0; i < results.length; i++) { ";
		markermap += "result = results[i].best;  ";
		markermap += "latlng = result.latlng; ";

		// Create the markers
		markermap += "if(latlng.lat!= 39.78373 && latlng.lng!=100.445882){" +
				"count++;";
		markermap += "marker = L.marker();";
		markermap += "marker.setLatLng([ latlng.lat, latlng.lng]); ";


		// Create the content of the markers' popup
		markermap += "marker.bindPopup( \"<b>\" + " + geo_label
				+ "+ \":\"+ \"</b>\" + n[i]+  \"<br />\" + observ[o]";

		for (int j = 0; j < cubeAttributesSize; j++)
			markermap += "+ \"<br />\"+ \"<b>\" +attr_labels[" + j
					+ "] +\":\"+ \"</b>\" +attribute" + j + "[o]";

		for (int j = 0; j < measurevalues.size(); j++)
			markermap += "+ \"<b>\" +meas_labels[" + j
					+ "] +\":\"+ \"</b>\" +measure" + j + "[o] + \"<br />\"";

		markermap += ");";
		markermap += "o++;";		
		markermap += "var popup = marker._popup;";
		markermap += "group.push(marker); }} " +
				"if(count==0||results==null||results.length==0){" +
				"alert('Geography values cannot be detected on map. Trying another language can fix this issue.');} ";
		markermap += "features = L.featureGroup(group).addTo(map); ";		
		markermap += "map.fitBounds(features.getBounds()); ";
		markermap += "popup.update();";
		markermap += "L.DomUtil.get('info').innerHTML = html;";
		markermap += "});";
		markermap += "}, 100);";
		markermap += " })(geova.slice(k,k+chunk), measure.slice(k,k+chunk));";
		markermap += "}";

		return markermap;

	}

	/*
	 * Input:Strings with the i) values of the geospatial dimension of the cube,
	 * ii) values of the measure of the cube and iii) values of the rest of the
	 * dimensions of the cube, a list of strings with the values of the
	 * attributes of the cube, the number of the attributes of the cube, a
	 * String with the labels of the geospatial values of the cube and a String
	 * with the labels of the attribute values of the cube Output:A String with
	 * the javascript to create the bubble map
	 */
	public static String getBubbleJavascript(String geovalue,
			String measurevalue, String dimvalues,
			List<String> attributeValues, int cubeAttributesSize,
			String geo_label, String attr_labels, String measureLabels) {

		// The javascript code to create the bubble map
		String bubblemap = null;

		// Create a javascript array with the values of the geospatial dimension
		bubblemap = " arr = new Array (" + geovalue + "); ";

		// Create a javascript array with the values of the measure
		bubblemap += "measure = new Array(" + measurevalue + ");";

		bubblemap += "meas_labels = new Array(" + measureLabels + ");";

		// Create a javasript array with the values of the dimensions (except for the geospatial dimension)
		bubblemap += "observ = new Array(" + dimvalues + ");";

		// Create one javascript array for the values of each of the attributes of the cube
		for (int j = 0; j < cubeAttributesSize; j++)
			bubblemap += "attribute" + j + " = new Array("
					+ attributeValues.get(j) + ");";

		// Create a javascript array with the labels of the attributes of the cube
		bubblemap += "attr_labels = new Array(" + attr_labels + ");";
		bubblemap += "var counter = 0;";
		bubblemap += "var i,j,temparray, chunk = 100;";
		bubblemap += "var geova = new Array(" + geovalue + ");";
		bubblemap += "group = [];";
		bubblemap += "var results, b = 0, features, marker, result, latlng, prop, best, val, map, r, i;";		
		bubblemap += "map = L.map('map', {layers: MQ.mapLayer() });";
		bubblemap += "for (k=0,j=geova.length; k<j; k+=chunk) {";		
		bubblemap += "counter++;";
		bubblemap += "temparray = geova.slice(k,k+chunk); ";
		bubblemap += "var something = \"new\"+counter;";
		bubblemap += " (function (n, meas) { ";
		bubblemap += "setTimeout(function(){";
		
		// Perform the geocoding
		bubblemap += "MQ.geocode().search(n) .on('success', ";
		bubblemap += "function(e) {results = e.result; ";		

		// Get the lat and long
		bubblemap += "count=0;" +
				"for (i = 0; i < results.length; i++) { ";
		bubblemap += "result = results[i].best; ";
		bubblemap += "latlng = result.latlng; ";
		bubblemap += "if(latlng.lat!= 39.78373 && latlng.lng!=100.445882){";

		bubblemap += "count++; ";
		// Create the bubble markers
		bubblemap += "marker = L.circleMarker();";
		bubblemap += "marker.setLatLng([ latlng.lat, latlng.lng]) ";
		bubblemap += ".bindPopup( \"<b>\" + "
				+ geo_label
				+ "+ \":\"+ \"</b>\" + n[i] + \"<br />\" + observ[b]+\"<b>\"+ meas_labels[0]+\": </b>\" +meas[i]";
		
		for (int j = 0; j < cubeAttributesSize; j++)
			bubblemap += "+ \"<br />\"+ \"<b>\" +attr_labels[" + j
					+ "] +\":\"+ \"</b>\" +attribute" + j + "[b]";
		bubblemap += ");";
		bubblemap += "b++;";
		bubblemap += "var radius = [];";
		bubblemap += "var min = Math.min.apply(Math, measure);";
		bubblemap += "var max = Math.max.apply(Math, measure);";
		bubblemap += "radius[i] = 100*((meas[i]-min)/(max-min));";
		bubblemap += "marker.setRadius(radius[i]);";
		bubblemap += "group.push(marker); }}" +
					"if(count==0||results==null||results.length==0){" +
					"alert('Geography values cannot be detected on map. Trying another language can fix this issue');} ";
		bubblemap += "features = L.featureGroup(group).addTo(map); ";
		bubblemap += "map.fitBounds(features.getBounds()); ";
		bubblemap += "L.DomUtil.get('info').innerHTML = html; });";
		bubblemap += "}, 100);";
		bubblemap += " })(geova.slice(k,k+chunk), measure.slice(k,k+chunk));";
		bubblemap += "}";

		return bubblemap;
	}

	/*
	 * Input:Strings with the i) values of the geospatial dimension of the cube,
	 * ii) values of the measure of the cube and iii) values of the rest of the
	 * dimensions of the cube, Output:A String with the javascript to create the
	 * heat map
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
	 * Input:Strings with the i) values of the geospatial dimension of the cube,
	 * ii) values of the measure of the cube and iii) values of the rest of the
	 * dimensions of the cube, Output:A String with the javascript to create the
	 * choropleth map
	 */
	public String getChoroplethMapJavascript(String geovalue,
			String englishvalue, String measurevalue, String dimvalues,
			List<String> attributeValues, int cubeAttributesSize,
			String attr_labels, String measureLabels) {

		String choroplethmap = null;
		String choroplethenglish = null;
		// Create a javascript array with the values of the geospatial dimension
		choroplethmap = " arr = new Array (" + geovalue + "); ";
		String date;
		String endate;
		choroplethmap += "enarr = new Array (" + englishvalue + "); ";

		// if there is only one geovalue no "," exists
		if (geovalue.contains(",")) {
			date = geovalue.substring(0, geovalue.indexOf(","));
		} else {
			date = geovalue;
		}

		if (englishvalue.contains(",")) {
			endate = englishvalue.substring(0, englishvalue.indexOf(","));
		} else {
			endate = englishvalue;
		}
				
		// Create a javascript array with the values of the measure
		choroplethmap += "measure = new Array(" + measurevalue + ");";
		
		// Create a javasript array with the values of the dimensions (except for the geospatial dimension)
		choroplethmap += "observ = new Array(" + dimvalues + ");";

		choroplethmap += "meas_labels = new Array(" + measureLabels + ");";

		// Create one javascript array for the values of each of the attributes of the cube
		for (int j = 0; j < cubeAttributesSize; j++)
			choroplethmap += "attribute" + j + " = new Array("+ attributeValues.get(j) + ");";

		// Create a javascript array with the labels of the attributes of the cube
		choroplethmap += "attr_labels2 = new Array(" + attr_labels + ");";

		// The geoJSON data
		BufferedReader br = null;
		try {
			if (mapzoom.equals("BE")) {
				if (selectedGeoLevel.getURIorLabel().equals("Region")
						|| selectedGeoLevel.getURIorLabel().equals("Gewest")) {
					br = new BufferedReader(new FileReader(
							"choropleth_json_be_region.txt"));
				} else if (selectedGeoLevel.getURIorLabel().equals("Province")
						|| selectedGeoLevel.getURIorLabel().equals("Provincie")) {
					br = new BufferedReader(new FileReader(
							"choropleth_json_be_province.txt"));
				} else if (selectedGeoLevel.getURIorLabel().equals("District")
						|| selectedGeoLevel.getURIorLabel().equals(
								"Arrondissement")) {
					br = new BufferedReader(new FileReader(
							"choropleth_json_be_district.txt"));
				} else if (selectedGeoLevel.getURIorLabel().equals(
						"Municipality")
						|| selectedGeoLevel.getURIorLabel().equals("Gemeente")) {
					br = new BufferedReader(new FileReader(
							"choropleth_json_be_municipality.txt"));
				}
			} else if (mapzoom.equals("EU")) {
				br = new BufferedReader(
						new FileReader("choropleth_json_eu.txt"));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String line;
		String polygons = "";
		String lines2 = null;
		int number = 0;
		try {
			while ((line = br.readLine()) != null) {
				polygons += line + "\r\n ";
				// process the line.
				number++;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String polygon = polygons;
		choroplethmap += "var statesData = " + polygon + ";";
		choroplethmap += "var coding = " + date + ";";
		choroplethmap += "MQ.geocode().search([" + date + "]) .on('success', ";
		choroplethmap += "function(e) {var results = e.result, html = '', group = [], features, marker, result, latlng, prop, best, val, map, r, i;  ";
		choroplethmap +=
		"var tiles = L.tileLayer(" +
		"'https://api.mapbox.com/v4/mapbox.light/{z}/{x}/{y}.png?" +
		"access_token=pk.eyJ1IjoiemVnaW5pcyIsImEiOiI2YzNmMzBlNTRjMjNjMDZmYzQxNjgwMjU4NGZjYTMwOCJ9.gRBAKyusxJqCPrhUP6c9Kw'," +
		"{maxZoom:18,attribution:'Map data &copy; <a href=\"http://openstreetmap.org\">OpenStreetMap</a> contributors," +
		" ' + '<a href=\"http://creativecommons.org/licenses/by-sa/2.0/\">CC-BY-SA</a>, ' +" +
		"'Imagery &copy; <a href=\"http://mapbox.com\">Mapbox</a>', id:'examples.map-20v6611k'});" ;
					
		if (mapzoom.equals("BE")) {
			choroplethmap += "map = L.map('map').setView([50.8333, 4], 8);";
		} else if (mapzoom.equals("EU")) {
			choroplethmap += "map = L.map('map').setView([51.505, -0.09], 3);";
		}		
		
		choroplethmap += "map.addLayer(tiles);";
		
		// Update geoJSON the density of the data with the corresponding measure values
		choroplethmap += "for (i = 0; i < arr.length; i++)";

		choroplethmap += " for(j=0; j<statesData.features.length; j++){";
		choroplethmap += " 		if(enarr[i] == statesData.features[j].properties.name) {";
		choroplethmap += "			statesData.features[j].properties.density = measure[i];";
		choroplethmap += "var temp = statesData.features[j].properties.density;}"
				+ "else {statesData.features[j].properties.geometry=\" \";}};";
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
				+ "if(measure.length<10){serie1.getClassJenks(Math.round(measure.length/2));}"
				+ "else{serie1.getClassJenks(5);}" + "ranges = serie1.ranges;"
				+ "serie1.setPrecision(1);" + "serie1.setColors(colors);";

		choroplethmap += "function style(feature) {" + "return {"
				+ "fillColor:getColor(feature.properties.density, serie1),"
				+ "weight:2," + "opacity:1," + "color:'white',"
				+ "dashArray:'3'," + "fillOpacity:0.7" + "};" + "}";

		choroplethmap += "function highlightFeature(e) {"
				+ "var layer = e.target;" + "layer.setStyle({" + "weight:4,"
				+ "color:'#ffffff'," + "dashArray:''," + "fillOpacity:0.7"
				+ "});";

		choroplethmap += "if (!L.Browser.ie && !L.Browser.opera) {"
					+ "}" + "info.update(layer.feature.properties);" + "}";
		choroplethmap += "function resetHighlight(e) {"
				+ "geojson.resetStyle(e.target);" + "layer.bringToBack();"
				+ "info.update();" + "}";
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
		choroplethmap += "}if(!observations)" + "observations=\" \";";

		choroplethmap += " var name;" + "for(var pi=0; pi<enarr.length; pi++){";
		choroplethmap += " 	if(props&&enarr[pi] == props.name) {name=arr[pi];}}";

		choroplethmap += "this._div.innerHTML = (props ?"
				+ "'<h5 style =\"text-align:center\">' + name + '</h5><b>'+meas_labels[0]+':</b>' + props.density + '</b><br />' + observations + '</b>' :'Hover over a state');"
				+ "};";

		choroplethmap += "info.addTo(map);";
		choroplethmap += "geojson = L.geoJson(statesData, {" + "style:style,"
				+ "onEachFeature:onEachFeature" + "}).addTo(map);";

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