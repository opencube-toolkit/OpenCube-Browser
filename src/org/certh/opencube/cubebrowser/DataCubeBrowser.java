package org.certh.opencube.cubebrowser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

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
public class DataCubeBrowser extends AbstractWidget<DataCubeBrowser.Config> {

	// The top container to show the check boxes with the available aggregation
	// set dimensions
	private FContainer selectCubeContainer = new FContainer("selectCubeContainer");

	// The top container to show the check boxes with the available aggregation
	// set dimensions
	private FContainer topcontainer = new FContainer("topcontainer");

	// The left container to show the combo boxes with the visual dimensions
	private FContainer leftcontainer = new FContainer("leftcontainer");

	// The right container to show the combo boxes with the fixed dimensions
	// values
	private FContainer rightcontainer = new FContainer("rightcontainer");

	// The right container to show the combo boxes with the fixed dimensions
	// values
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

	// The fixed dimensions
	private List<LDResource> fixedDimensions = new ArrayList<LDResource>();

	// The slice fixed dimensions
	private List<LDResource> sliceFixedDimensions = new ArrayList<LDResource>();

	// All the cube observations - to be used to create a slice
	private List<LDResource> sliceObservations = new ArrayList<LDResource>();

	// A map (dimension URI - dimension values) with all the cube dimension
	// values
	private HashMap<LDResource, List<LDResource>> allDimensionsValues = new HashMap<LDResource, List<LDResource>>();

	// The selected value for each fixed dimension
	private HashMap<LDResource, LDResource> fixedDimensionsSelectedValues = new HashMap<LDResource, LDResource>();

	// The selected value for each fixed dimension
	private HashMap<LDResource, LDResource> sliceFixedDimensionsValues = new HashMap<LDResource, LDResource>();

	// A map with the corresponding components for each fixed cube dimension
	private HashMap<LDResource, List<FComponent>> dimensionURIfcomponents = new HashMap<LDResource, List<FComponent>>();

	// A map with the Aggregation Set Dimension URIs and the corresponding Check
	// boxes
	private HashMap<LDResource, FCheckBox> mapDimURIcheckBox = new HashMap<LDResource, FCheckBox>();

	// The cube measures
	private List<LDResource> cubeMeasures = new ArrayList<LDResource>();

	// A map with the Cube Measure URIs and the corresponding Check boxes
	private HashMap<LDResource, FCheckBox> mapMeasureURIcheckBox = new HashMap<LDResource, FCheckBox>();

	// The cube URI to visualize (required)
	private String cubeSliceURI = "";

	// The SPARQL service to get data (not required)
	private String SPARQL_service = "";

	// The SPARQL service to get data (not required)
	private String defaultLang;

	// The graph of the cube
	private String cubeGraph = null;

	// The graph of the cube structure
	private String cubeDSDGraph = null;

	// The graph of the slice
	private String sliceGraph = null;

	// The table model for visualization of the cube
	private FTable ftable = new FTable("ftable");

	// True if URI is type qb:DataSet
	private boolean isCube;

	// True if URI is type qb:Slice
	private boolean isSlice;

	// True if code list will be used to get the cube dimension values
	private boolean useCodeLists;

	// True if widget is loaded for the first time
	private boolean isFirstLoad = true;

	// The central container
	private FContainer cnt = null;

	// The available languages of the cube
	private List<String> availableLanguages;

	// The selected language
	private String selectedLanguage;

	// Ignore multiple languages
	private boolean ignoreLang;

	private List<LDResource> cubeDimsFromSlice = null;

	private List<LDResource> allCubes = new ArrayList<LDResource>();

	private String[] measureColors = { "black", "CornflowerBlue", "LimeGreen",
			"Tomato" };

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

		allCubes = SelectionSPARQL.getAllAvailableCubes(SPARQL_service);
		// Prepare and show the widget
		populateCentralContainer();

		isFirstLoad = false;

		return cnt;
	}

	private void populateCentralContainer() {

		long startTime = System.currentTimeMillis();
		if (cubeSliceURI != null) {
			// Get Cube/Slice Graph
			String cubeSliceGraph = CubeSPARQL.getCubeSliceGraph(cubeSliceURI,
					SPARQL_service);

			// Get the type of the URI i.e. cube / slice
			List<String> cubeSliceTypes = CubeSPARQL.getType(cubeSliceURI,
					cubeSliceGraph, SPARQL_service);

			if (cubeSliceTypes != null) {
				// The URI corresponds to a data cube
				isCube = cubeSliceTypes
						.contains("http://purl.org/linked-data/cube#DataSet");

				// The URI corresponds to a cube Slice
				isSlice = cubeSliceTypes
						.contains("http://purl.org/linked-data/cube#Slice");
			} else {
				isCube = false;
				isSlice = false;
			}

			// If the URI is a valid cube or slice URI
			if (isCube || isSlice) {

				if (isCube) {

					// The cube graph is the graph of the URI computed above
					cubeGraph = cubeSliceGraph;

					// Get Cube Structure graph
					cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
							cubeSliceURI, cubeGraph, SPARQL_service);

					if (!ignoreLang) {
						// Get the available languages of labels
						availableLanguages = CubeSPARQL
								.getAvailableCubeLanguages(cubeDSDGraph,
										SPARQL_service);

						// get the selected language to use
						selectedLanguage = CubeHandlingUtils
								.getSelectedLanguage(availableLanguages,
										selectedLanguage);
					}

					// Get all Cube dimensions
					cubeDimensions = CubeSPARQL.getDataCubeDimensions(
							cubeSliceURI, cubeGraph, cubeDSDGraph,
							selectedLanguage, defaultLang, ignoreLang,
							SPARQL_service);

					// Thread to get cube dimensions
					Thread measuresThread = new Thread(new Runnable() {
						public void run() {
							// Get the Cube measure
							cubeMeasures = CubeSPARQL.getDataCubeMeasure(
									cubeSliceURI, cubeGraph, cubeDSDGraph,
									selectedLanguage, defaultLang, ignoreLang,
									SPARQL_service);

							// Get the selected measure to use
							selectedMeasures = CubeHandlingUtils
									.getSelectedMeasure(cubeMeasures,
											selectedMeasures);
						}
					});

					// Thread to get cube dimensions
					Thread aggregationSetDimsThread = new Thread(
							new Runnable() {
								public void run() {
									// Get all the dimensions of the aggregation
									// set the cube belongs
									aggregationSetDims = AggregationSPARQL
											.getAggegationSetDimsFromCube(
													cubeSliceURI, cubeDSDGraph,
													selectedLanguage,
													defaultLang, ignoreLang,
													SPARQL_service);
								}
							});

					// Thread to get cube dimensions
					Thread cubeDimsOfAggregationSetThread = new Thread(
							new Runnable() {
								public void run() {
									// Get all the dimensions per cube of the
									// aggregations set
									cubeDimsOfAggregationSet = AggregationSPARQL
											.getCubeAndDimensionsOfAggregateSet(
													cubeSliceURI, cubeDSDGraph,
													selectedLanguage,
													defaultLang, ignoreLang,
													SPARQL_service);
								}
							});

					// Thread to get cube dimensions
					Thread dimensionsValuesThread = new Thread(new Runnable() {
						public void run() {
							// Get values for each cube dimension
							allDimensionsValues = CubeHandlingUtils
									.getDimsValues(cubeDimensions,
											cubeSliceURI, cubeGraph,
											cubeDSDGraph, useCodeLists,
											selectedLanguage, defaultLang,
											ignoreLang, SPARQL_service);
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
						availableLanguages = CubeSPARQL
								.getAvailableCubeLanguages(cubeDSDGraph,
										SPARQL_service);

						// get the selected language to use
						selectedLanguage = CubeHandlingUtils
								.getSelectedLanguage(availableLanguages,
										selectedLanguage);
					}
				
					Thread sliceFixedDimensionsThread = new Thread(
							new Runnable() {
								public void run() {
									// Get slice fixed dimensions
									sliceFixedDimensions = SliceSPARQL
											.getSliceFixedDimensions(
													cubeSliceURI, sliceGraph,
													cubeDSDGraph,
													selectedLanguage,
													defaultLang, ignoreLang,
													SPARQL_service);
								}
							});

					Thread cubeDimsFromSliceThread = new Thread(new Runnable() {
						public void run() {
							// Get all cube dimensions
							cubeDimsFromSlice = SliceSPARQL
									.getDataCubeDimensionsFromSlice(
											cubeSliceURI, sliceGraph,
											cubeDSDGraph, selectedLanguage,
											defaultLang, ignoreLang,
											SPARQL_service);
						}
					});

					Thread cubeMeasuresThread = new Thread(new Runnable() {
						public void run() {
							// Get the Cube measure
							cubeMeasures = SliceSPARQL.getSliceMeasure(
									cubeSliceURI, sliceGraph, cubeDSDGraph,
									selectedLanguage, defaultLang, ignoreLang,
									SPARQL_service);

							// Get the selected measure to use
							selectedMeasures = CubeHandlingUtils
									.getSelectedMeasure(cubeMeasures,
											selectedMeasures);
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

					// The slice visual dimensions are: (all cube dims) - (slice
					// fixed dims)
					cubeDimensions = cubeDimsFromSlice;
					cubeDimensions.removeAll(sliceFixedDimensions);

					Thread sliceFixedDimensionsValuesThread = new Thread(
							new Runnable() {
								public void run() {
									sliceFixedDimensionsValues = SliceSPARQL
											.getSliceFixedDimensionsValues(
													sliceFixedDimensions,
													cubeSliceURI, sliceGraph,
													cubeDSDGraph,
													selectedLanguage,
													defaultLang, ignoreLang,
													SPARQL_service);
								}
							});

					Thread allDimensionsValuesThread = new Thread(
							new Runnable() {
								public void run() {
									// Get values for each slice dimension
									allDimensionsValues = CubeHandlingUtils
											.getDimsValuesFromSlice(
													cubeDimensions,
													cubeSliceURI, cubeGraph,
													cubeDSDGraph, sliceGraph,
													useCodeLists,
													selectedLanguage,
													defaultLang, ignoreLang,
													SPARQL_service);
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

				
					// top container styling
					selectCubeContainer.addStyle("border-style", "solid");
					selectCubeContainer.addStyle("border-width", "1px");
					selectCubeContainer.addStyle("padding", "10px");
					selectCubeContainer.addStyle("border-radius", "5px");
					selectCubeContainer.addStyle("width", "990px ");
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
							cubeSliceURI = "<"
									+ ((LDResource) this.getSelected().get(0))
											.getURI() + ">";

							cnt.removeAll();
							topcontainer.removeAll();
							leftcontainer.removeAll();
							rightcontainer.removeAll();
							dimensionURIfcomponents.clear();
							languagecontainer.removeAll();
							measurescontainer.removeAll();
							selectCubeContainer.removeAll();

							// Initialize everything for the new cube
							mapDimURIcheckBox = new HashMap<LDResource, FCheckBox>();
							selectedMeasures = new ArrayList<LDResource>();
							aggregationSetDims = new ArrayList<LDResource>();
							cubeDimsOfAggregationSet = new HashMap<LDResource, List<LDResource>>();
							visualDimensions = new ArrayList<LDResource>();
							fixedDimensions = new ArrayList<LDResource>();
							sliceFixedDimensions = new ArrayList<LDResource>();
							fixedDimensionsSelectedValues = new HashMap<LDResource, LDResource>();
							sliceFixedDimensionsValues = new HashMap<LDResource, LDResource>();
							mapDimURIcheckBox = new HashMap<LDResource, FCheckBox>();
							mapMeasureURIcheckBox = new HashMap<LDResource, FCheckBox>();

							// show the cube
							populateCentralContainer();

						}
					};

					// populate cubes combo box
					for (LDResource cube : allCubes) {
						if (cube.getLabel() != null) {
							cubesCombo.addChoice(cube.getLabel(), cube);
						} else {
							cubesCombo.addChoice(cube.getURI(), cube);
						}
					}

					if (cubeSliceURI != null) {
						// Remove the "<" and ">" from the cube URI
						cubesCombo.setPreSelected(new LDResource(cubeSliceURI
								.substring(1, cubeSliceURI.length() - 1)));
					}
					selectCubeContainer.add(cubesCombo);
				
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
					topcontainer.addStyle("width", "400px ");
					topcontainer.addStyle("margin-left", "auto");
					topcontainer.addStyle("margin-right", "auto");
					topcontainer.addStyle("text-align", "left");

					// If an aggregation set has already been created
					if (aggregationSetDims.size() > 0) {

						FLabel OLAPbrowsing_label = new FLabel(
								"OLAPbrowsing_label",
								"<b>Dimensions</b></br>"
										+ "Summarize observations by adding/removing dimensions: </br>");
						topcontainer.add(OLAPbrowsing_label);

						int aggregationDim = 1;

						// Show Aggregation set dimensions
						for (LDResource aggdim : aggregationSetDims) {

							// show one check box for each aggregation set
							// dimension
							FCheckBox aggrDimCheckBox = new FCheckBox(
									"aggregation_" + aggregationDim,
									aggdim.getURIorLabel()) {

								public void onClick() {

									// Get all selected aggregation set
									// dimensions for browsing
									List<LDResource> aggregationSetSelectedDims = new ArrayList<LDResource>();
									for (LDResource aggSetDimURI : mapDimURIcheckBox
											.keySet()) {
										FCheckBox check = mapDimURIcheckBox
												.get(aggSetDimURI);

										// Get selected dimensions
										if (check.checked) {
											aggregationSetSelectedDims
													.add(aggSetDimURI);
										}
									}

									// Identify the cube of the aggregation set
									// that
									// contains exactly the dimension selected
									// to be
									// browsed
									for (LDResource cube : cubeDimsOfAggregationSet
											.keySet()) {
										List<LDResource> cubeDims = cubeDimsOfAggregationSet
												.get(cube);
										if ((cubeDims.size() == aggregationSetSelectedDims
												.size())
												&& cubeDims
														.containsAll(aggregationSetSelectedDims)) {
											System.out.println("NEW CUBE URI: "
													+ cube.getURI());

											// The new cube to visualize
											cubeSliceURI = "<" + cube.getURI()
													+ ">";

											// clear the previous visualization
											// and
											// create a new one for the new cube
											cnt.removeAll();
											topcontainer.removeAll();
											leftcontainer.removeAll();
											rightcontainer.removeAll();
											dimensionURIfcomponents.clear();
											languagecontainer.removeAll();
											measurescontainer.removeAll();
											selectCubeContainer.removeAll();

											// show the cube
											populateCentralContainer();
											break;
										}
									}
								}
							};

							// set as checked if the dimension is contained at
							// the
							// selected cube
							aggrDimCheckBox.setChecked(cubeDimensions
									.contains(aggdim));
							mapDimURIcheckBox.put(aggdim, aggrDimCheckBox);
							topcontainer.add(aggrDimCheckBox);

							aggregationDim++;
						}
					} else {

						FLabel notOLAP = new FLabel("notOLAP",
								"<b>OLAP-like browsing is not "
										+ "supported for this cube<b>");
						topcontainer.add(notOLAP);
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
				measurescontainer.addStyle("height", "120px ");
				measurescontainer.addStyle("width", "330px ");
				measurescontainer.addStyle("margin-left", "auto");
				measurescontainer.addStyle("margin-right", "auto");
				measurescontainer.addStyle("text-align", "left");

				FLabel measure_label = new FLabel("measure_lb",
						"<b>Measures</b></br>"
								+ "Select the measures to visualize:");
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
							topcontainer.removeAll();
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
					languagecontainer.addStyle("width", "200px ");
					languagecontainer.addStyle("height", "120px ");

					FLabel datalang_label = new FLabel(
							"datalang",
							"<b>Language</b></br>"
									+ "Select the language of the visualized data:");
					languagecontainer.add(datalang_label);

					// Add Combo box for language selection
					FComboBox datalang_combo = new FComboBox("datalang_combo") {
						@Override
						public void onChange() {
							selectedLanguage = this.getSelected().get(0)
									.toString();
							cnt.removeAll();
							topcontainer.removeAll();
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

				// Top FGrid
				FGrid topfg = new FGrid("mytopgrid");
				ArrayList<FComponent> topfarray = new ArrayList<FComponent>();

				// Add top container with available dimensions to browse only if
				// a cube URi is provided
				if (isCube) {
					topfarray.add(topcontainer);
				}

				// Show measures panel if there are more than 1 cube measures
				if (cubeMeasures.size() > 1) {
					topfarray.add(measurescontainer);
				}

				// add language container if there are more than 1 language
				if (!ignoreLang && availableLanguages.size() > 1) {
					topfarray.add(languagecontainer);
				}

				topfg.addRow(topfarray);

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

				TupleQueryResult res = null;
				// Get query tuples for visualization
				if (isCube) {
					res = CubeBrowserSPARQL.get2DVisualsiationValues(
							visualDimensions, fixedDimensionsSelectedValues,
							selectedMeasures, allDimensionsValues,
							cubeSliceURI, cubeGraph, SPARQL_service);
				} else if (isSlice) {
					res = CubeBrowserSPARQL
							.get2DVisualsiationValuesFromSlice(
									visualDimensions,
									fixedDimensionsSelectedValues,
									selectedMeasures, allDimensionsValues,
									cubeSliceURI, sliceGraph, cubeGraph,
									SPARQL_service);
				}

				// Create an FTable model based on the query tuples
				FTableModel tm = create2DCubeTableModel(res,
						allDimensionsValues, visualDimensions);

				// Initialize FTable
				ftable.setShowCSVExport(true);
				ftable.setNumberOfRows(20);
				ftable.setEnableFilter(true);
				ftable.setOverFlowContainer(true);
				ftable.setFilterPos(FilterPos.TOP);
				ftable.setSortable(false);
				ftable.setModel(tm);

				// //////////Left container//////////////////

				// Show left container if there are 2 visual dimensions
				if (visualDimensions.size() == 2) {

					// left container styling
					leftcontainer.addStyle("border-style", "solid");
					leftcontainer.addStyle("border-width", "1px");
					leftcontainer.addStyle("padding", "10px");
					leftcontainer.addStyle("border-radius", "5px");
					leftcontainer.addStyle("border-color", "#C8C8C8 ");
					leftcontainer.addStyle("height", "130px ");
					leftcontainer.addStyle("width", "480px ");
					leftcontainer.addStyle("display", "table-cell ");
					// leftcontainer.addStyle("vertical-align", "middle ");
					leftcontainer.addStyle("align", "center");
					leftcontainer.addStyle("text-align", "left");

					FLabel fixeddimlabel = new FLabel(
							"fixeddimlabel",
							"<b>Visual dimensions</b></br> Select the two "
									+ "dimensions that define the table of the browser:");

					leftcontainer.add(fixeddimlabel);

					FGrid visualDimGrid = new FGrid("visualDimGrid");

					ArrayList<FComponent> dim1Array = new ArrayList<FComponent>();

					// Add label for Dim1 (column headings)
					FLabel dim1Label = new FLabel("dim1Label",
							"<u>Column Headings:<u>");
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
							showCube();
						}
					};

					// populate Dim1 combo box
					for (LDResource ldr : cubeDimensions) {
						dim1Combo.addChoice(ldr.getURIorLabel(), ldr.getURI());
					}

					dim1Combo.setPreSelected(visualDimensions.get(0).getURI());
					dim1Array.add(dim1Combo);

					ArrayList<FComponent> dim2Array = new ArrayList<FComponent>();

					// Add label for Dim2 (column headings)
					FLabel dim2Label = new FLabel("dim2Label",
							"<u>Rows (values in first column):</u>");
					dim2Array.add(dim2Label);

					// Add Combo box for Dim2
					final FComboBox dim2Combo = new FComboBox("dim2Combo") {
						@Override
						public void onChange() {

							// Get the URI of the 2nd selected dimension
							List<String> d2Selected = this
									.getSelectedAsString();

							// Get the URI of the 1st selected dimension
							List<String> d1Selected = null;
							for (FComponent fc : leftcontainer
									.getAllComponents()) {
								if (fc.getId().contains("_dim1Combo")) {
									d1Selected = ((FComboBox) fc)
											.getSelectedAsString();

									// Both combo boxes have the same selected
									// value
									// Select randomly another value for d2
									if (d1Selected.get(0).equals(
											d2Selected.get(0))) {
										List<Pair<String, Object>> d1choices = ((FComboBox) fc)
												.getChoices();
										for (Pair<String, Object> pair : d1choices) {
											if (!pair.snd.toString().equals(
													d1Selected.get(0))) {
												d1Selected.clear();
												d1Selected.add(pair.snd
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
							showCube();
						}
					};

					// populate Dim2 combo box
					for (LDResource ldr : cubeDimensions) {
						dim2Combo.addChoice(ldr.getURIorLabel(), ldr.getURI());
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
				rightcontainer.addStyle("height", "130px ");
				rightcontainer.addStyle("width", "480px ");
				rightcontainer.addStyle("display", "table-cell ");
				rightcontainer.addStyle("vertical-align", "middle ");
				rightcontainer.addStyle("align", "center");
				rightcontainer.addStyle("text-align", "left");

				if (!sliceFixedDimensionsValues.isEmpty()
						|| fixedDimensions.size() > 0) {
					FLabel otheropts = new FLabel("Options",
							"<b>Fixed dimensions</b></br> Change the values of the fixed dimensions:");
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
									+ sliceFixedValues,
									fDimValue.getURIorLabel());
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

				// An FGrid to show visual and fixed cube dimensions
				FGrid fg = new FGrid("mygrid");

				// Styling of FGrid
				fg.addStyle("align", "center");
				fg.addStyle("margin", "auto");

				ArrayList<FComponent> farray = new ArrayList<FComponent>();

				// Add components to FGrid

				// Show visual dimensions panel if there are 2 visual dimension
				if (visualDimensions.size() == 2) {
					farray.add(leftcontainer);
				}

				// Show fixed dimensions panel if there are any
				if ((fixedDimensions.size() > 0)
						|| (!sliceFixedDimensionsValues.isEmpty())) {
					farray.add(rightcontainer);
				}

				fg.addRow(farray);

				// add components to central container
				cnt.add(selectCubeContainer);
				cnt.add(topfg);
				cnt.add(ftable);
				cnt.add(fg);

				// Show the create slice button if there are fixed dimensions
				// i.e. a slice is not already visualized
				if (fixedDimensions.size() > 0 && isCube) {
					FContainer bottomcontainer = new FContainer(
							"bottomcontainer");

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
									+ "In case you want to create and store a two-dimensional "
									+ "slice of the cube as it is presented "
									+ "in the browser click the button: </br>");

					bottomcontainer.add(bottomLabel);

					// Button to create slice
					FButton createSlice = new FButton("createSlice",
							"createSlice") {
						@Override
						public void onClick() {
							String sliceURI = SliceSPARQL.createCubeSlice(
									cubeSliceURI, cubeGraph,
									fixedDimensionsSelectedValues,
									sliceObservations);
							String message = "A new slice with the following URI has been created: "
									+ sliceURI;

							FDialog.showMessage(this.getPage(),
									"New Slice created", message, "ok");
						}
					};

					bottomcontainer.add(createSlice);
					cnt.add(bottomcontainer);
				}

				// //////// Not a valid cube or Slice URI /////////////
			} else {

				String uri = cubeSliceURI.replaceAll("<", "");
				uri = uri.replaceAll(">", "");
				String message = "The URI <b>" + uri
						+ "</b> is not a valid cube or slice URI.";
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

		//No cube has been yet selected
		} else {
			
				// top container styling
				selectCubeContainer.addStyle("border-style", "solid");
				selectCubeContainer.addStyle("border-width", "1px");
				selectCubeContainer.addStyle("padding", "10px");
				selectCubeContainer.addStyle("border-radius", "5px");
				selectCubeContainer.addStyle("width", "990px ");
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
						cubeSliceURI = "<"+ ((LDResource) this.getSelected().get(0)).getURI() + ">";

						cnt.removeAll();
						topcontainer.removeAll();
						leftcontainer.removeAll();
						rightcontainer.removeAll();
						dimensionURIfcomponents.clear();
						languagecontainer.removeAll();
						measurescontainer.removeAll();
						selectCubeContainer.removeAll();

						// Initialize everything for the new cube
						mapDimURIcheckBox = new HashMap<LDResource, FCheckBox>();
						selectedMeasures = new ArrayList<LDResource>();
						aggregationSetDims = new ArrayList<LDResource>();
						cubeDimsOfAggregationSet = new HashMap<LDResource, List<LDResource>>();
						visualDimensions = new ArrayList<LDResource>();
						fixedDimensions = new ArrayList<LDResource>();
						sliceFixedDimensions = new ArrayList<LDResource>();
						fixedDimensionsSelectedValues = new HashMap<LDResource, LDResource>();
						sliceFixedDimensionsValues = new HashMap<LDResource, LDResource>();
						mapDimURIcheckBox = new HashMap<LDResource, FCheckBox>();
						mapMeasureURIcheckBox = new HashMap<LDResource, FCheckBox>();

						// show the cube
						populateCentralContainer();

					}
				};

				// populate cubes combo box
				for (LDResource cube : allCubes) {
					if (cube.getLabel() != null) {
						cubesCombo.addChoice(cube.getLabel(), cube);
					} else {
						cubesCombo.addChoice(cube.getURI(), cube);
					}
				}

				if (cubeSliceURI != null) {
					// Remove the "<" and ">" from the cube URI
					cubesCombo.setPreSelected(new LDResource(cubeSliceURI
							.substring(1, cubeSliceURI.length() - 1)));
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
			FLabel fDimLabel = new FLabel("fixedDimLabel_" + fixedDims, "<u>"
					+ fDim.getURIorLabel() + ":</u>");
			dimComponents.add(fDimLabel);

			farray.add(fDimLabel);

			// Add the combo box for the fixed cube dimension
			FComboBox fDimCombo = new FComboBox("fixedDimCombo_" + fixedDims) {
				@Override
				public void onChange() {
					showCube();
				}
			};

			// Populate the combo box with the values of the fixed cube
			// dimension
			for (LDResource ldr : allDimensionsValues.get(fDim)) {
				// Show the first 60 chars if label too long
				if (ldr.getURIorLabel().length() > 60) {
					fDimCombo.addChoice(ldr.getURIorLabel().substring(0, 60)
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

		TupleQueryResult res = null;
		if (isCube) {
			// Get query tuples for visualization
			res = CubeBrowserSPARQL.get2DVisualsiationValues(visualDimensions,
					fixedDimensionsSelectedValues, selectedMeasures,
					allDimensionsValues, cubeSliceURI, cubeGraph,
					SPARQL_service);
		} else if (isSlice) {
			res = CubeBrowserSPARQL.get2DVisualsiationValuesFromSlice(
					visualDimensions, fixedDimensionsSelectedValues,
					selectedMeasures, allDimensionsValues, cubeSliceURI,
					sliceGraph, cubeGraph, SPARQL_service);
		}

		// create table model for visualization
		FTableModel newTableModel = create2DCubeTableModel(res,
				allDimensionsValues, visualDimensions);
		ftable.setModel(newTableModel);
		ftable.populateView();

	}

	// Create a table model from the Tuple query result in order to visualize
	private FTableModel create2DCubeTableModel(
			TupleQueryResult res,
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

		try {

			// /// Set the first row of FTableModel////

			// If there are 2 visual dimensions
			if (visualDimensions.size() == 2) {

				// The first column of 1st row - the name of the 2nd dimension
				tm.addColumn(visualDimensions.get(1).getURIorLabel());

				// The rest columns of the first row - the values of the 1st
				// dimension
				for (LDResource dim1Val : dim1) {
					tm.addColumn(dim1Val.getURIorLabel());
				}

				// If there is 1 visual dimensions
			} else {
				tm.addColumn(visualDimensions.get(0).getURIorLabel());
				tm.addColumn("Measure");
			}

			// for each cube observation
			while (res.hasNext()) {

				BindingSet bindingSet = res.next();

				// get Dim1 value
				String dim1Val = bindingSet.getValue("dim1").stringValue();
				LDResource r1 = new LDResource(dim1Val);

				String measure = "";
				// get measures
				int i = 1;
				for (LDResource meas : selectedMeasures) {
					measure += "<font color=\""
							+ measureColors[cubeMeasures.indexOf(meas)] + "\">"
							+ "<p align=\"right\">"
							+ bindingSet.getValue("measure" + i).stringValue()
							+ " </p></font> ";
					i++;
				}

				// get observation URI
				String obsURI = bindingSet.getValue("obs").stringValue();
				LDResource obs = new LDResource(obsURI);
				obs.setLabel(measure);

				// If there are 2 visual dimensions
				if (visualDimensions.size() == 2) {
					// get Dim2 value
					String dim2Val = bindingSet.getValue("dim2").stringValue();
					LDResource r2 = new LDResource(dim2Val);

					// Add the observation to the corresponding (row, column)
					// position of the table
					v2DCube[dim2.indexOf(r2)][dim1.indexOf(r1)] = obs;
				} else {

					v2DCube[dim1.indexOf(r1)][0] = obs;
				}

				// add observation to potential slice
				sliceObservations.add(obs);
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
					Object[] data = new Object[dim1.size()];
					// Add row header (values in first column)
					data[0] = getHTMLStringFromLDResource(dim1.get(j));

					data[1] = getHTMLStringFromLDResource(v2DCube[j][0]);
					tm.addRow(data);
				}

			}

		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return tm;
	}

	// get an HTML representation of an LDResource
	private HtmlString getHTMLStringFromLDResource(LDResource ldr) {
		if (ldr == null) {
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
