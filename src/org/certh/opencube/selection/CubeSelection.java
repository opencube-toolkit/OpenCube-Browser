package org.certh.opencube.selection;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.certh.opencube.SPARQL.CubeSPARQL;
import org.certh.opencube.SPARQL.SelectionSPARQL;
import org.certh.opencube.cubebrowser.DataCubeBrowserPlus;
import org.certh.opencube.cubebrowser.DataCubeBrowserPlusPlus;
import org.certh.opencube.utils.CubeHandlingUtils;
import org.certh.opencube.utils.LDResource;

import com.fluidops.ajax.components.FButton;
import com.fluidops.ajax.components.FComboBox;
import com.fluidops.ajax.components.FComponent;
import com.fluidops.ajax.components.FContainer;
import com.fluidops.ajax.components.FDialog;
import com.fluidops.ajax.components.FGrid;
import com.fluidops.ajax.components.FHTML;
import com.fluidops.ajax.components.FLabel;
import com.fluidops.ajax.components.FRadioButtonGroup;
import com.fluidops.iwb.datacatalog.widget.DataCubeTreeResultWidget;
import com.fluidops.iwb.model.ParameterConfigDoc;
import com.fluidops.iwb.model.TypeConfigDoc;
import com.fluidops.iwb.widget.AbstractWidget;
import com.fluidops.iwb.widget.config.WidgetBaseConfig;

/**
 * On some wiki page add
 * 
 * <code>
 * = OpenCube Expander test page =
 * 
 * <br/>
 * {{#widget: org.certh.opencube.selection.CubeSelection
 * |asynch='true' 
 *  }}
 * </code>
 * 
 */

@TypeConfigDoc("The OpenCube Expander searches for compatible cubes and" +
		"  creats a new expanded cube by merging two compatible cubes.")
public class CubeSelection extends AbstractWidget<CubeSelection.Config> {

	// The container to show the available data cube
	private FContainer cubeContainer = new FContainer("cubeContainer");

	// The container to show the available cube dimensions
	private FContainer dimensionsContainer = new FContainer("dimensionsContainer");

	// The container to show the available cube measures
	private FContainer measuresContainer = new FContainer("measuresContainer");

	// The container to show the available operations
	private FContainer operationsContainer = new FContainer("operationsContainer");

	// The left container
	private FContainer leftContainer = new FContainer("leftContainer");

	// The right container
	private FContainer rightContainer = new FContainer("rightContainer");

	// A combo box to select the cube to browse
	private FComboBox cubesCombo;

	// The SPARQL service to get data (not required)
	private String SPARQL_Service;

	// The central container
	private FContainer cnt;

	private List<LDResource> cubeDimensions = new ArrayList<LDResource>();

	// The cube measures
	private HashMap<LDResource, List<LDResource>> cubeMeasures = new HashMap<LDResource, List<LDResource>>();

	// The graph of the cube
	private String cubeGraph = null;

	// The graph of the cube structure
	private String cubeDSDGraph = null;

	HashMap<LDResource, List<LDResource>> dimensionsLevels = new HashMap<LDResource, List<LDResource>>();

	HashMap<LDResource, List<LDResource>> dimensionsConceptSchemes = new HashMap<LDResource, List<LDResource>>();

	private String selectedLanguage = "";

	private String defaultLang = "";

	// Ignore multiple languages
	private boolean ignoreLang;

	private String selectedCubeURI = null;

	// True if widget is loaded for the first time
	private boolean isFirstLoad = true;

	private HashMap<LDResource, List<LDResource>> compatibleAddValue2Level = new HashMap<LDResource, List<LDResource>>();

	private Map<LDResource, Integer> allCubesAndDimCount = new HashMap<LDResource, Integer>();

	private LDResource selectedCube;

	// The dimension to use to add attribute value
	private LDResource selectedDimension;

	private FRadioButtonGroup addMeasuresAttributeValues_radioButtonGroup;

	private boolean addAttributeValue = false;

	private FComboBox operations_combo;

	private String selectedOperation = null;
	
	private boolean searchButtonPressed=false;

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

		final Config config = get();

		SPARQL_Service = config.sparqlService;

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

		// Central container
		cnt = new FContainer(id);

		// Get all cubes from the store and the number of dimensions they have
		allCubesAndDimCount = SelectionSPARQL
				.getAllAvailableCubesAndDimCountWithLinks(SPARQL_Service);

		// Prepare and show the widget
		populateCentralContainer();

		isFirstLoad = false;

		return cnt;
	}

	private void populateCentralContainer() {

		// cube container styling
		cubeContainer.addStyle("border-style", "solid");
		cubeContainer.addStyle("border-width", "1px");
		cubeContainer.addStyle("padding", "10px");
		cubeContainer.addStyle("border-radius", "5px");
		cubeContainer.addStyle("border-color", "#C8C8C8 ");
		cubeContainer.addStyle("display", "table-cell ");
		cubeContainer.addStyle("vertical-align", "middle ");
		cubeContainer.addStyle("width", "500px ");
		cubeContainer.addStyle("margin-left", "auto");
		cubeContainer.addStyle("margin-right", "auto");
		cubeContainer.addStyle("text-align", "left");

		// If there are cubes
		if (allCubesAndDimCount.keySet().size() > 0) {

			FLabel allcubes_label = new FLabel("allcubes_label",
					"<b>Please select a cube:<b>");

			cubeContainer.add(allcubes_label);

			// Add Combo box with cube URIs
			cubesCombo = new FComboBox("cubesCombo") {
				@Override
				public void onChange() {
					long totalstartTime = System.currentTimeMillis();

					selectedCube = (LDResource) this.getSelected().get(0);
					selectedCubeURI = "<" + selectedCube.getURI() + ">";
					// Get Cube/Slice Graph
					cubeGraph = CubeSPARQL.getCubeSliceGraph(selectedCubeURI,
							SPARQL_Service);

					// Get Cube Structure graph
					cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
							selectedCubeURI, cubeGraph, SPARQL_Service);

					// Thread to get Measures
					Thread dimsThread = new Thread(new Runnable() {
						public void run() {
							// Get all Cube dimensions
							cubeDimensions = CubeSPARQL.getDataCubeDimensions(
									selectedCubeURI, cubeGraph, cubeDSDGraph,
									selectedLanguage, defaultLang, ignoreLang,
									SPARQL_Service);
						}
					});

					// Thread to get Measures
					Thread measuresThread = new Thread(new Runnable() {
						public void run() {
							// Get the Cube measure
							List<LDResource> selectedCubeMeasures = CubeSPARQL
									.getDataCubeMeasure(selectedCubeURI,
											cubeGraph, cubeDSDGraph,
											selectedLanguage, defaultLang,
											ignoreLang, SPARQL_Service);

							cubeMeasures.clear();
							cubeMeasures.put(selectedCube, selectedCubeMeasures);
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
						e1.printStackTrace();
					}

					// Thread to get dimension levels
					Thread dimLevelsThread = new Thread(new Runnable() {
						public void run() {
							dimensionsLevels = CubeHandlingUtils
									.getDimensionsLevels(selectedCubeURI,
											cubeDimensions, cubeDSDGraph,
											cubeGraph, selectedLanguage,
											defaultLang, ignoreLang,
											SPARQL_Service);
						}
					});

					// Thread to get dimension concept schemes
					Thread dimConceptSchemesThread = new Thread(new Runnable() {
						public void run() {
							dimensionsConceptSchemes = CubeHandlingUtils
									.getDimensionsConceptSchemes(
											cubeDimensions, cubeDSDGraph,
											selectedLanguage, defaultLang,
											ignoreLang, SPARQL_Service);
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

					cubeContainer.removeAll();
					dimensionsContainer.removeAll();
					measuresContainer.removeAll();
					operationsContainer.removeAll();
					rightContainer.removeAll();
					leftContainer.removeAll();
					cnt.removeAll();

					compatibleAddValue2Level = new HashMap<LDResource, List<LDResource>>();
					addAttributeValue = false;
					
					//DEFAUL PRESELECTED OPERATION
					selectedOperation = null;

					populateCentralContainer();

					long totalstopTime = System.currentTimeMillis();
					long totalelapsedTime = totalstopTime - totalstartTime;
					System.out.println("Load selection time: "+ totalelapsedTime);
				}
			};

		
			
			
			// populate cubes combo box
			for (LDResource cube : allCubesAndDimCount.keySet()) {
				  if (cube.getLabel() != null) {
					   cubesCombo.addChoice(cube.getLabel(), cube);
				  } else {
					  cubesCombo.addChoice(cube.getURI(), cube); 
				  }
			}

			if (selectedCubeURI != null) {
				// Remove the "<" and ">" from the cube URI
				cubesCombo.setPreSelected(new LDResource(selectedCubeURI
						.substring(1, selectedCubeURI.length() - 1)));
			}

			cubeContainer.add(cubesCombo);
		} else {
			FLabel noAvailableCubes_label = new FLabel(
					"noAvailableCubes_label",
					"<b>There are not any available data cubes<b>");
			cubeContainer.add(noAvailableCubes_label);
		}

		if (cubeDimensions.size() > 0) {

			// cube container styling
			dimensionsContainer.addStyle("border-style", "solid");
			dimensionsContainer.addStyle("border-width", "1px");
			dimensionsContainer.addStyle("padding", "10px");
			dimensionsContainer.addStyle("border-radius", "5px");
			dimensionsContainer.addStyle("border-color", "#C8C8C8 ");
			dimensionsContainer.addStyle("display", "table-cell ");
			dimensionsContainer.addStyle("vertical-align", "middle ");
			dimensionsContainer.addStyle("width", "500px ");
			dimensionsContainer.addStyle("margin-left", "auto");
			dimensionsContainer.addStyle("margin-right", "auto");
			dimensionsContainer.addStyle("text-align", "left");

			//ADD TREE RESULT WIDGTET 
			
			DataCubeTreeResultWidget treeWidget = new DataCubeTreeResultWidget();
			treeWidget.setPageContext(pc);
			DataCubeTreeResultWidget.Config treeWidgetConfig = treeWidget.get();
			treeWidgetConfig.icon = "/favicon.ico";
			treeWidgetConfig.title = "Cube Dimensions";
			treeWidgetConfig.query = "PREFIX  qb: <http://purl.org/linked-data/cube#> "
					+ "SELECT DISTINCT ?parent ?child "
					+ "WHERE {"
					+ "GRAPH <"	+ cubeGraph	+ ">{"
					+ selectedCubeURI
					+ " qb:structure ?structure}"
					+ "GRAPH <"	+ cubeDSDGraph+ ">{ "
					+ "?structure qb:component ?component ."
					+ "?component qb:dimension ?parent." 
					+ "}"
					+ "GRAPH <"	+ cubeGraph	+ ">{"
					+ "?x qb:dataSet "+ selectedCubeURI+ ". " 
					+ "?x ?parent ?child. }}";
			treeWidgetConfig.asynch = true;
			treeWidget.setConfig(treeWidgetConfig);
			dimensionsContainer.add(treeWidget.getComponentUAE("myid"));
		}


				
		if (cubeMeasures.size() > 0) {

			// cube container styling
			measuresContainer.addStyle("border-style", "solid");
			measuresContainer.addStyle("border-width", "1px");
			measuresContainer.addStyle("padding", "10px");
			measuresContainer.addStyle("border-radius", "5px");
			measuresContainer.addStyle("border-color", "#C8C8C8 ");
			measuresContainer.addStyle("display", "table-cell ");
			measuresContainer.addStyle("vertical-align", "middle ");
			measuresContainer.addStyle("width", "500px ");
			measuresContainer.addStyle("margin-left", "auto");
			measuresContainer.addStyle("margin-right", "auto");
			measuresContainer.addStyle("text-align", "left");

			String originalCubeMeasure_label = "<b>Cube measures:</b><br><ol>";

			// show measures of the selected cube
			for (LDResource measure : cubeMeasures.get(selectedCube)) {
				originalCubeMeasure_label += "<li>" + measure.getURIorLabel()
						+ "</li>";
			}

			originalCubeMeasure_label += "</ol>";
			FLabel measures_label = new FLabel("measures_label",
					originalCubeMeasure_label);
			measuresContainer.add(measures_label);
		}

		if (!isFirstLoad) {
			// cube container styling
			operationsContainer.addStyle("border-style", "solid");
			operationsContainer.addStyle("border-width", "1px");
			operationsContainer.addStyle("padding", "10px");
			operationsContainer.addStyle("border-radius", "5px");
			operationsContainer.addStyle("border-color", "#C8C8C8 ");
			operationsContainer.addStyle("display", "table-cell ");
			operationsContainer.addStyle("vertical-align", "middle ");
			operationsContainer.addStyle("width", "500px ");
			operationsContainer.addStyle("margin-left", "auto");
			operationsContainer.addStyle("margin-right", "auto");
			operationsContainer.addStyle("text-align", "left");

			FLabel operations_label = new FLabel("operations_label",
					"<b>Select an operation:<b>");

			operationsContainer.add(operations_label);
			operationsContainer.add(getNewLineComponent());

			// Add Combo box for language selection
			operations_combo = new FComboBox("operations_combo") {
				@Override
				public void onChange() {
					selectedOperation = this.getSelected().get(0).toString();
					if (selectedOperation.equals("Add value to dimension")) {
						addAttributeValue = true;
						cubeContainer.removeAll();
						dimensionsContainer.removeAll();
						measuresContainer.removeAll();
						operationsContainer.removeAll();
						rightContainer.removeAll();
						leftContainer.removeAll();
						cnt.removeAll();

						populateCentralContainer();
					} else {
						addAttributeValue = false;
						cubeContainer.removeAll();
						dimensionsContainer.removeAll();
						measuresContainer.removeAll();
						operationsContainer.removeAll();
						rightContainer.removeAll();
						leftContainer.removeAll();
						cnt.removeAll();

						populateCentralContainer();
					}
				}
			};
			
			operations_combo.addChoice("Add measure");			
			operations_combo.addChoice("Add value to dimension");

			if (selectedOperation != null) {
				operations_combo.setPreSelected(selectedOperation);
				if (selectedOperation.equals("Add value to dimension")) {
					addAttributeValue = true;
				}
			}else{
				operations_combo.setPreSelected("Add measure");
			}
			
			operationsContainer.add(operations_combo);

			if (addAttributeValue) {
				// Add Combo box for language selection
				FComboBox expansionDimension_combo = new FComboBox(
						"expansionDimension_combo") {
					@Override
					public void onChange() {
						selectedDimension = (LDResource) this.getSelected().get(0);
					}
				};

				for (LDResource ldr : cubeDimensions) {
					expansionDimension_combo.addChoice(ldr.getURIorLabel(), ldr);
				}
				
				if(selectedDimension==null){
					selectedDimension=cubeDimensions.get(0);
				}
				expansionDimension_combo.setPreSelected(selectedDimension);				
				
				operationsContainer.add(getNewLineComponent());
				operationsContainer.add(getNewLineComponent());
				operationsContainer.add(expansionDimension_combo);
			}

			FButton executeOperation_button = new FButton("executeOperation","Search...") {
				@Override
				public void onClick() {
					searchButtonPressed=true;
					String selectedOperation = operations_combo.getSelected().get(0).toString();
					compatibleAddValue2Level = new HashMap<LDResource, List<LDResource>>();
					addAttributeValue = false;
					if (selectedOperation.equals("Add measure")) {

						long startTime = System.currentTimeMillis();

						// get measure compatible cubes and measures - without the selected cube						
						HashMap<LDResource, List<LDResource>> compatible=
								SelectionSPARQL.getLinkAddMeasureCompatibleCubes(
										selectedCube, cubeGraph, cubeDSDGraph,
										 selectedLanguage,
										defaultLang, ignoreLang, SPARQL_Service);
						List<LDResource> selectedCubeMeasures = cubeMeasures
								.get(selectedCube);

						// clean cube measures and set new values
						cubeMeasures = new HashMap<LDResource, List<LDResource>>();
						for (LDResource cube : compatible.keySet()) {
							cubeMeasures.put(cube, compatible.get(cube));
						}

						cubeMeasures.put(selectedCube, selectedCubeMeasures);

						cubeContainer.removeAll();
						dimensionsContainer.removeAll();
						measuresContainer.removeAll();
						operationsContainer.removeAll();
						rightContainer.removeAll();
						leftContainer.removeAll();
						cnt.removeAll();

						populateCentralContainer();					

					} else if (selectedOperation.equals("Add value to dimension")) {
						long startTime = System.currentTimeMillis();

						compatibleAddValue2Level = SelectionSPARQL
								.getLinkAddValueToLevelCompatibleCubes(
										selectedCube, cubeGraph, cubeDSDGraph,
										selectedDimension, selectedLanguage,
										defaultLang, ignoreLang, SPARQL_Service);

						cubeContainer.removeAll();
						dimensionsContainer.removeAll();
						measuresContainer.removeAll();
						operationsContainer.removeAll();
						rightContainer.removeAll();
						leftContainer.removeAll();
						cnt.removeAll();

						populateCentralContainer();

					} else if (selectedOperation.equals("Run cluster experiment")) {
						long startTime = System.currentTimeMillis();

						HashMap<LDResource, HashMap<LDResource, List<LDResource>>> clusterCubesAndDims = SelectionSPARQL
								.getClusterCubesAndDimValues(selectedCube,
										cubeGraph, cubeDSDGraph,
										allCubesAndDimCount, cubeDimensions,
										dimensionsLevels,
										cubeMeasures.get(selectedCube),
										dimensionsConceptSchemes,
										selectedLanguage, defaultLang,
										ignoreLang, SPARQL_Service);

						PrintWriter writer = null;
						try {
							writer = new PrintWriter(
									"cluster_threashold_compatibility.txt");
						} catch (FileNotFoundException e2) {
							e2.printStackTrace();
						}

						for (LDResource cubeToCheck : clusterCubesAndDims.keySet()) {

							selectedCube = cubeToCheck;
							selectedCubeURI = "<" + cubeToCheck.getURI() + ">";
							// Get Cube/Slice Graph
							cubeGraph = CubeSPARQL.getCubeSliceGraph(
									selectedCubeURI, SPARQL_Service);

							// Get Cube Structure graph
							cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
									selectedCubeURI, cubeGraph, SPARQL_Service);

							// Thread to get Measures
							Thread measuresThread = new Thread(new Runnable() {
								public void run() {
									// Get the Cube measure
									List<LDResource> selectedCubeMeasures = CubeSPARQL
											.getDataCubeMeasure(
													selectedCubeURI, cubeGraph,
													cubeDSDGraph,
													selectedLanguage,
													defaultLang, ignoreLang,
													SPARQL_Service);
									cubeMeasures.clear();
									cubeMeasures.put(selectedCube,
											selectedCubeMeasures);
								}
							});

							// start thread
							measuresThread.start();

							// wait until thread finish
							try {
								measuresThread.join();
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}

							// Thread to get dimension levels
							Thread dimLevelsThread = new Thread(new Runnable() {
								public void run() {
									dimensionsLevels = CubeHandlingUtils
											.getDimensionsLevels(
													selectedCubeURI,
													cubeDimensions,
													cubeDSDGraph, cubeGraph,
													selectedLanguage,
													defaultLang, ignoreLang,
													SPARQL_Service);
								}
							});

							// Thread to get dimension concept schemes
							Thread dimConceptSchemesThread = new Thread(
									new Runnable() {
										public void run() {
											dimensionsConceptSchemes = CubeHandlingUtils
													.getDimensionsConceptSchemes(
															cubeDimensions,
															cubeDSDGraph,
															selectedLanguage,
															defaultLang,
															ignoreLang,
															SPARQL_Service);
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

							HashMap<LDResource, HashMap<LDResource, List<LDResource>>> tmpcompatibleAddValue2Level = SelectionSPARQL
									.getStatisticsForAddValueToLevelCompatibleCubes(
											selectedCube, cubeGraph,
											cubeDSDGraph, allCubesAndDimCount,
											cubeDimensions, dimensionsLevels,
											cubeMeasures.get(selectedCube),
											dimensionsConceptSchemes,
											selectedLanguage, defaultLang,
											ignoreLang, SPARQL_Service);
						}
						writer.close();

						PrintWriter writer2 = null;
						try {
							writer2 = new PrintWriter("cluster_analysis.txt");
						} catch (FileNotFoundException e2) {
							e2.printStackTrace();
						}
						
						List<LDResource> correctDims = new ArrayList<LDResource>(cubeDimensions);
						
						for (LDResource cubeToCheck : compatibleAddValue2Level.keySet()) {
							HashMap<LDResource, List<LDResource>> cubeToCheckDimsAndValues = 
									clusterCubesAndDims.get(cubeToCheck);
							for (LDResource currentCube : compatibleAddValue2Level.keySet()) {
								// Do not check the cube with itself
								if (!currentCube.equals(cubeToCheck)) {
									HashMap<LDResource, List<LDResource>> currentCubeDimsAndValues = 
											clusterCubesAndDims.get(currentCube);
									HashMap<LDResource, List<Integer>> dimIntersections = new HashMap<LDResource, List<Integer>>();
									for (LDResource dim : correctDims) {
										List<LDResource> cubeToCheckDimValues = cubeToCheckDimsAndValues.get(dim);
										List<LDResource> currentCubeDimValues = currentCubeDimsAndValues.get(dim);
										List<LDResource> intersection = new ArrayList<LDResource>(cubeToCheckDimValues);
										intersection.retainAll(currentCubeDimValues);
										List<Integer> intersectionCounts = new ArrayList<Integer>();
										intersectionCounts.add(cubeToCheckDimValues.size());
										intersectionCounts.add(currentCubeDimValues.size());
										intersectionCounts.add(intersection.size());
										dimIntersections.put(dim,intersectionCounts);

										// Comment out to get detailed N=1 overlap per dimension
										writer2.println(cubeToCheck.getURI()
												+ "," + currentCube.getURI()
												+ "," + dim.getURI() + ","
												+ cubeToCheckDimValues.size()
												+ ","
												+ currentCubeDimValues.size()
												+ "," + intersection.size());
									}									
								}
							}
						}
						writer2.close();

						cubeContainer.removeAll();
						dimensionsContainer.removeAll();
						measuresContainer.removeAll();
						operationsContainer.removeAll();
						rightContainer.removeAll();
						leftContainer.removeAll();
						cnt.removeAll();

						populateCentralContainer();
						
					}
				}
			};

			operationsContainer.add(getNewLineComponent());
			operationsContainer.add(getNewLineComponent());
			operationsContainer.add(executeOperation_button);
		}

		// Add attribute value
		// Show new attribute values per dimension
		if (compatibleAddValue2Level.keySet().size() > 0 &&
				operations_combo.getSelected().get(0).toString().
				equals("Add value to dimension") ) {

			rightContainer.addStyle("border-style", "solid");
			rightContainer.addStyle("border-width", "1px");
			rightContainer.addStyle("padding", "10px");
			rightContainer.addStyle("border-radius", "5px");
			rightContainer.addStyle("border-color", "#C8C8C8 ");
			rightContainer.addStyle("display", "table-cell ");
			rightContainer.addStyle("vertical-align", "middle ");
			rightContainer.addStyle("margin-left", "auto");
			rightContainer.addStyle("margin-right", "auto");
			rightContainer.addStyle("text-align", "left");

			FLabel addAttributeValue_label = new FLabel(
					"addAttributeValueMeasure_label",
					"<b>Available values to add to dimension: "
							+ selectedDimension.getURIorLabel() + ":<b>");
			rightContainer.add(addAttributeValue_label);
			rightContainer.add(getNewLineComponent());

			addMeasuresAttributeValues_radioButtonGroup = new FRadioButtonGroup(
					"compAttributeValuesRadio");
			int i=1;
			for (LDResource compCube : compatibleAddValue2Level.keySet()) {
				// show the dimension values only of compatible cubes
				if (!compCube.equals(selectedCube)) {
					String compCubeAttributeValues_str = "<b>"
							+ compCube.getURIorLabel() + "</b><br>";

					compCubeAttributeValues_str+="<a id=\"show_id"+i+"\" " +
							"onclick=\"document.getElementById('spoiler_id"+i+"').style.display=''; " +
							"document.getElementById('show_id"+i+"').style.display='none';\" " +
							"class=\"link\">[Show dimension values]</a>" +
							"<span id=\"spoiler_id"+i+"\" style=\"display: none\">" +
							"<a onclick=\"document.getElementById('spoiler_id"+i+"').style.display='none';" +
							" document.getElementById('show_id"+i+"').style.display='';\"" +
							" class=\"link\">[Hide dimension values]</a><br>";
					
					// show the measures of the compatible cubes
					compCubeAttributeValues_str += "<ol>";
					for (LDResource compAttributeValue : compatibleAddValue2Level
							.get(compCube)) {
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

			//By default checked the 1st button
			rightContainer.add(addMeasuresAttributeValues_radioButtonGroup);			
			rightContainer.add(getNewLineComponent());
			
			FButton mergeAttributeValuesCubes_button = new FButton(
					"mergeAttributeValueCubes", "Merge cubes (Add values)...") {
				@Override
				public void onClick() {
					//a cube is selected for expansion
					if(addMeasuresAttributeValues_radioButtonGroup.checked!=null){
						LDResource expanderCube = new LDResource(
								addMeasuresAttributeValues_radioButtonGroup.checked.value);
						
						String newcube=SelectionSPARQL.mergeCubesAddAttributeValueMinimal(selectedCube,
								expanderCube,  selectedDimension,
								 selectedLanguage,  defaultLang,  ignoreLang,
								 SPARQL_Service);
						
						FDialog.showMessage(this.getPage(),	
								"Merged cube created","A new merged cube with URI:"+newcube+" have been created! ","ok");
					}					
				}
			};
			
			
			

			rightContainer.add(mergeAttributeValuesCubes_button);
			
		// Add measure operator
		// There exist new cube - measures to add
		} else if (cubeMeasures.keySet().size() > 1 &&
				operations_combo.getSelected().get(0).toString().equals("Add measure")) {
			rightContainer.addStyle("border-style", "solid");
			rightContainer.addStyle("border-width", "1px");
			rightContainer.addStyle("padding", "10px");
			rightContainer.addStyle("border-radius", "5px");
			rightContainer.addStyle("border-color", "#C8C8C8 ");
		
			FLabel addMeasure_label = new FLabel("addMeasure_label",
					"<b>Available measures to add:<b>");
			rightContainer.add(addMeasure_label);
			rightContainer.add(getNewLineComponent());
			addMeasuresAttributeValues_radioButtonGroup = new FRadioButtonGroup(
					"compMeasuresRadio");
			for (LDResource compCube : cubeMeasures.keySet()) {
				// show the measures only of compatible cubes
				if (!compCube.equals(selectedCube)) {
					String compCubeMeasures = "<b>" + compCube.getURIorLabel()
							+ "</b>";

					// show the measures of the compatible cubes
					compCubeMeasures += "<ol>";
					for (LDResource compCubeMeasure : cubeMeasures
							.get(compCube)) {
						compCubeMeasures += "<li>"
								+ compCubeMeasure.getURIorLabel() + "</li>";
					}
					compCubeMeasures += "</ol><br>";
					addMeasuresAttributeValues_radioButtonGroup.addRadioButton(
							compCubeMeasures, compCube.getURI());
				}
			}
			rightContainer.add(addMeasuresAttributeValues_radioButtonGroup);

			FButton mergeMeasureCubes_button = new FButton("mergeMeasureCubes","Merge cubes (Add measures)...") {
				@Override
				public void onClick() {
					if(addMeasuresAttributeValues_radioButtonGroup.checked!=null){
						LDResource expanderCube = new LDResource(
								addMeasuresAttributeValues_radioButtonGroup.checked.value);
												
						String newCube=SelectionSPARQL.mergeCubesAddMeasureSingleObservation(selectedCube,
								expanderCube, selectedLanguage,  defaultLang,  ignoreLang,
								 SPARQL_Service);
						
						FDialog.showMessage(this.getPage(),	
								"Merged cube created","A new merged cube with URI: "+newCube+" have been created! ","ok");
					}				
				}
			};
			rightContainer.add(mergeMeasureCubes_button);
		}else if(searchButtonPressed){

			rightContainer = new FContainer("rightContainer");
			rightContainer.addStyle("padding", "10px");
			rightContainer.addStyle("display", "table-cell ");
			rightContainer.addStyle("vertical-align", "middle ");
			rightContainer.addStyle("margin-left", "auto");
			rightContainer.addStyle("margin-right", "auto");
			rightContainer.addStyle("text-align", "left");

			FLabel noResultsFound = new FLabel(
					"noResultsFound_label",
					"<b>No results found <b>");
			rightContainer.add(noResultsFound);
						
		}

		// Left FGrid
		FGrid selectionGrid = new FGrid("selectionGrid");
		ArrayList<FComponent> selectionFarray = new ArrayList<FComponent>();

		selectionFarray.add(cubeContainer);
		selectionGrid.addRow(selectionFarray);

		selectionFarray = new ArrayList<FComponent>();
		selectionFarray.add(dimensionsContainer);
		selectionGrid.addRow(selectionFarray);

		selectionFarray = new ArrayList<FComponent>();
		selectionFarray.add(measuresContainer);
		selectionGrid.addRow(selectionFarray);

		selectionFarray = new ArrayList<FComponent>();
		selectionFarray.add(operationsContainer);
		selectionGrid.addRow(selectionFarray);

		// Add grid to left container
		leftContainer.add(selectionGrid);
		
		FContainer leftContainerOut=new FContainer("leftContainerOut");
		leftContainerOut.addStyle("height", "10000px");
		leftContainerOut.add(leftContainer);

		// Total FGrid
		FGrid totalGrid = new FGrid("totalGrid");
		ArrayList<FComponent> totalFarray = new ArrayList<FComponent>();
		totalFarray.add(leftContainerOut);

		if (compatibleAddValue2Level.keySet().size() > 0|| cubeMeasures.keySet().size() > 1||
				searchButtonPressed) {
			FContainer rightContainerOut=new FContainer("rightContainerOut");
			rightContainerOut.addStyle("height", "10000px");
			rightContainerOut.add(rightContainer);
			
			totalFarray.add(rightContainerOut);
			if(searchButtonPressed){
				searchButtonPressed=false;
			}
		}

		totalGrid.addRow(totalFarray);

		cnt.add(totalGrid);
		
		// If this is not the first load of the widget
		if (!isFirstLoad) {
			cnt.populateView();
		}
	}

	// Adds a new line to UI
	private FHTML getNewLineComponent() {
		Random rand = new Random();
		FHTML fhtml = new FHTML("fhtmlnewline_" + Math.abs(rand.nextLong()));
		fhtml.setValue("<br>");
		return fhtml;
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