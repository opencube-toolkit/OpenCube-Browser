package org.certh.opencube.selection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.certh.opencube.SPARQL.CubeSPARQL;
import org.certh.opencube.SPARQL.SelectionSPARQL;
import org.certh.opencube.utils.CubeHandlingUtils;
import org.certh.opencube.utils.LDResource;

import com.fluidops.ajax.components.FButton;
import com.fluidops.ajax.components.FComboBox;
import com.fluidops.ajax.components.FComponent;
import com.fluidops.ajax.components.FContainer;
import com.fluidops.ajax.components.FDialog;
import com.fluidops.ajax.components.FHTML;
import com.fluidops.ajax.components.FLabel;
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
 * {{#widget: org.certh.opencube.selection.LinkCreator |asynch='true' }}
 * </code>
 * 
 */

@TypeConfigDoc("The OpenCube Cube Selection enables the selection of cube dimensions and"
		+ "measures to visualize.")
public class LinkCreator extends AbstractWidget<LinkCreator.Config> {

	// The SPARQL service to get data (not required)
	private String SPARQL_Service;

	// The central container
	private FContainer cnt;

	private List<LDResource> cubeDimensions = new ArrayList<LDResource>();

	// The cube measures
	private List<LDResource> cubeMeasures = new ArrayList<LDResource>();

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

	private Map<LDResource, Integer> allCubesAndDimCount = new HashMap<LDResource, Integer>();

	private LDResource selectedCube;

	private FComboBox cubesCombo;

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
				.getAllAvailableCubesAndDimCount(SPARQL_Service);

		// Prepare and show the widget
		populateCentralContainer();

		return cnt;
	}

	private void populateCentralContainer() {

		// cube container styling
		cnt.addStyle("border-style", "solid");
		cnt.addStyle("border-width", "1px");
		cnt.addStyle("padding", "10px");
		cnt.addStyle("border-radius", "5px");
		cnt.addStyle("border-color", "#C8C8C8 ");
		cnt.addStyle("display", "table-cell ");
		cnt.addStyle("vertical-align", "middle ");
		cnt.addStyle("width", "400px ");
		cnt.addStyle("margin-left", "auto");
		cnt.addStyle("margin-right", "auto");
		cnt.addStyle("text-align", "left");

		// If there are cubes
		if (allCubesAndDimCount.keySet().size() > 0) {

			FLabel allcubes_label = new FLabel("allcubes_label",
					"<b>Please select a cube to create links:<b>");

			cnt.add(allcubes_label);

			// Add Combo box with cube URIs
			cubesCombo = new FComboBox("cubesCombo") {
				@Override
				public void onChange() {
					selectedCube = (LDResource) this.getSelected().get(0);
					selectedCubeURI = "<" + selectedCube.getURI() + ">";
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

			cnt.add(cubesCombo);
			cnt.add(getNewLineComponent());
			cnt.add(getNewLineComponent());

			FButton createLinks_button = new FButton("createLinks_button",
					"Create links") {
				@Override
				public void onClick() {

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
							cubeMeasures = CubeSPARQL.getDataCubeMeasure(
									selectedCubeURI, cubeGraph, cubeDSDGraph,
									selectedLanguage, defaultLang, ignoreLang,
									SPARQL_Service);
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
					int numberOfLinksAddValueToLevel = 0;

					// Create add value to level links
					for (LDResource selectedDim : cubeDimensions) {
						numberOfLinksAddValueToLevel += SelectionSPARQL
								.linkAddValueToLevelCompatibleCubes(
										selectedCube, cubeGraph, cubeDSDGraph,
										allCubesAndDimCount, cubeDimensions,
										dimensionsLevels, cubeMeasures,
										dimensionsConceptSchemes, selectedDim,
										1.0, selectedLanguage, defaultLang,
										ignoreLang, SPARQL_Service);
					}

					// Create add measure links
					int numberOfLinksAddMeasure = SelectionSPARQL
							.linkMeasureCompatibleCubes(selectedCube,
									cubeGraph, cubeDSDGraph,
									allCubesAndDimCount, cubeDimensions,
									dimensionsLevels, dimensionsConceptSchemes,
									cubeMeasures, 1.0, selectedLanguage,
									defaultLang, ignoreLang, SPARQL_Service);

					FDialog.showMessage(
							this.getPage(),
							"Links created",
							"Links have been created for cube "
									+ selectedCube.getURI()
									+ ". Add value to level links: "
									+ numberOfLinksAddValueToLevel
									+ ". Add measure links: "
									+ numberOfLinksAddMeasure + ".", "ok");
				}

			};

			cnt.add(createLinks_button);

		} else {
			FLabel noAvailableCubes_label = new FLabel(
					"noAvailableCubes_label",
					"<b>There are not any available data cubes<b>");
			cnt.add(noAvailableCubes_label);
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
		return "Link creation widget";
	}

	@Override
	public Class<?> getConfigClass() {
		return Config.class;
	}
}