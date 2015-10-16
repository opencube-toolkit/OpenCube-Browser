package org.certh.opencube.slicing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.certh.opencube.SPARQL.CubeSPARQL;
import org.certh.opencube.SPARQL.SliceSPARQL;
import org.certh.opencube.utils.CubeHandlingUtils;
import org.certh.opencube.utils.LDResource;

import com.fluidops.ajax.components.FButton;
import com.fluidops.ajax.components.FCheckBox;
import com.fluidops.ajax.components.FComboBox;
import com.fluidops.ajax.components.FComponent;
import com.fluidops.ajax.components.FContainer;
import com.fluidops.ajax.components.FDialog;
import com.fluidops.ajax.components.FGrid;
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
 * {{#widget: org.certh.opencube.slicing.SliceCreator| 
 *  dataCubeURI=<http://eurostat.linked-statistics.org/data/cens_hnctz> 
 *  |asynch='true'}}
 * </code>
 * 
 */

@TypeConfigDoc("The OpenCube Slice creator enables the creation of slices of data cubes" +
		"by selecting the fixed cube dimensions." )
public class SliceCreator extends AbstractWidget<SliceCreator.Config> {

	// The left container to show the combo boxes with the visual dimensions
	private FContainer leftcontainer = new FContainer("leftcontainer");

	// The left container to show the combo boxes with the visual dimensions
	private FContainer rightcontainer = new FContainer("rightcontainer");

	// The SPARQL service to get data (not required)
	private String SPARQL_service;

	// The cube URI to visualize (required)
	private String cubeURI;

	// True if code list will be used to get the cube dimension values
	private boolean useCodeLists;

	// The central container
	private FContainer cnt;

	// All the cube dimensions
	private List<LDResource> cubeDimensions = new ArrayList<LDResource>();

	// The graph of the cube
	private String cubeGraph = null;

	// The graph of the cube structure
	private String cubeDSDGraph = null;

	// A map (dimension URI - dimension values) with all the cube dimension
	// values
	private HashMap<LDResource, List<LDResource>> allDimensionsValues = new HashMap<LDResource, List<LDResource>>();

	// A map with the Aggregation Set Dimension URi and the corresponding Check
	// box
	private HashMap<LDResource, FCheckBox> mapDimURIcheckBox = new HashMap<LDResource, FCheckBox>();

	// The fixed dimensions
	private List<LDResource> fixedDimensions = new ArrayList<LDResource>();

	// The selected value for each fixed dimension
	private HashMap<LDResource, LDResource> fixedDimensionsSelectedValues = new HashMap<LDResource, LDResource>();

	// A map with the corresponding components for each fixed cube dimension
	private HashMap<LDResource, List<FComponent>> dimensionURIfcomponents = new HashMap<LDResource, List<FComponent>>();
	
	private List<String> availableLanguages;
	
	private String selectedLanguage="";
	
	private String defaultLang="";
	
	//Ignore multiple languages
	private boolean ignoreLang;

	public static class Config extends WidgetBaseConfig {
		@ParameterConfigDoc(desc = "The data cube URI to visualise", required = true)
		public String dataCubeURI;

		@ParameterConfigDoc(desc = "Use code lists to get dimension values", required = false)
		public boolean useCodeLists;

		@ParameterConfigDoc(desc = "SPARQL service to forward queries", required = false)
		public String sparqlService;
		
		@ParameterConfigDoc(desc = "Use multiple languages", required = false)
		public boolean ignoreLang;

	}

	@Override
	protected FComponent getComponent(String id) {

		final Config config = get();

		// Central container
		cnt = new FContainer(id);

		// Get the cube URI from the widget configuration
		cubeURI = config.dataCubeURI;

		// Get the SPARQL service (if any) from the cube configuration
		SPARQL_service = config.sparqlService;

		// The the use code list parameter from the widget config
		useCodeLists = config.useCodeLists;
		
		//Use multiple languages, get parameter from widget configuration
		ignoreLang=config.ignoreLang;

		if(!ignoreLang){
			defaultLang=com.fluidops.iwb.util.Config.getConfig().getPreferredLanguage();
			
			selectedLanguage=defaultLang;
		}


		// Central container
		cnt = new FContainer(id);

		boolean isCube = false;
		// Get Cube/Slice Graph
		cubeGraph = CubeSPARQL.getCubeSliceGraph(cubeURI, SPARQL_service);

		// Get the type of the URI i.e. cube / slice
		List<String> cubeSliceTypes = CubeSPARQL.getType(cubeURI, cubeGraph,
				SPARQL_service);

		if (cubeSliceTypes != null) {
			// The URI corresponds to a data cube
			isCube = cubeSliceTypes.contains("http://purl.org/linked-data/cube#DataSet");

		}

		// If the URI is a valid cube URI
		if (isCube) {

			// Get Cube Structure graph
			cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(cubeURI, cubeGraph,
					SPARQL_service);
			
			//Get the available languages of labels
			availableLanguages=CubeSPARQL.getAvailableCubeLanguages(
					cubeDSDGraph,SPARQL_service);

			//get the selected language to use
			selectedLanguage=CubeHandlingUtils.getSelectedLanguage(
					availableLanguages, selectedLanguage);

			// Get all Cube dimensions
			cubeDimensions = CubeSPARQL.getDataCubeDimensions(cubeURI,
					cubeGraph, cubeDSDGraph,selectedLanguage, defaultLang,ignoreLang,SPARQL_service);

			// Get values for each cube dimension
			allDimensionsValues = CubeHandlingUtils.getDimsValues(
					cubeDimensions, cubeURI, cubeGraph, cubeDSDGraph,
					useCodeLists, selectedLanguage,defaultLang,ignoreLang,SPARQL_service);

			// top container styling
			leftcontainer.addStyle("border-style", "solid");
			leftcontainer.addStyle("border-width", "1px");
			leftcontainer.addStyle("padding", "20px");
			leftcontainer.addStyle("border-radius", "5px");
			leftcontainer.addStyle("border-color", "#C8C8C8 ");
			leftcontainer.addStyle("height", "250px ");
			leftcontainer.addStyle("width", "500px ");
			leftcontainer.addStyle("display", "table-cell ");
			leftcontainer.addStyle("vertical-align", "middle ");
			leftcontainer.addStyle("align", "center");

			FLabel fixedDimensions_label = new FLabel("fixedDimensions_label",
					"<b>Please select the dimensions to fix for the slice:<b>");

			leftcontainer.add(fixedDimensions_label);

			// Show cube dimensions
			for (LDResource dim : cubeDimensions) {

				// IWB does not support too long IDs
				String checkBoxID = dim.getURIorLabel();
				if (checkBoxID.length() > 10) {
					checkBoxID = dim.getURIorLabel().substring(0, 10);
				}

				// show one check box for each cube dimension
				FCheckBox dimCheckBox = new FCheckBox("dim_" + checkBoxID,
						dim.getURIorLabel()) {

					public void onClick() {
						fixedDimensions.clear();
						// Detect user selected fixed dimensions
						for (LDResource dim : mapDimURIcheckBox.keySet()) {
							FCheckBox check = mapDimURIcheckBox.get(dim);
							// Get selected fixed dimensions
							if (check.checked) {
								fixedDimensions.add(dim);
							}
						}

						// remove previous combo boxes, labels and newlines from
						// right container
						Collection<FComponent> allcomp = rightcontainer
								.getAllComponents();
						for (FComponent comp : allcomp) {
							rightcontainer.removeAndRefresh(comp);
						}						

						FLabel fixedDimValues_label = new FLabel("fixedDimValues_label",
								"<b>Please select the values of the fixed dimensions for the slice:<b>");
						
						rightcontainer.addAndRefresh(fixedDimValues_label);

						// Selected values for the fixed dimensions
						fixedDimensionsSelectedValues = CubeHandlingUtils
								.getFixedDimensionsRandomSelectedValues(
										allDimensionsValues, fixedDimensions,fixedDimensionsSelectedValues);

						// Add labels and combo boxed for the fixed cube dimensions
						addFixedDimensions();
					}
				};

				//All check boxes are not checked by default
				dimCheckBox.setChecked(false);
				mapDimURIcheckBox.put(dim, dimCheckBox);

				leftcontainer.add(dimCheckBox);
			}
			
			// right container styling
			rightcontainer.addStyle("border-style", "solid");
			rightcontainer.addStyle("border-width", "1px");
			rightcontainer.addStyle("padding", "20px");
			rightcontainer.addStyle("border-radius", "5px");
			rightcontainer.addStyle("border-color", "#C8C8C8 ");
			rightcontainer.addStyle("height", "250px ");
			rightcontainer.addStyle("width", "500px ");
			rightcontainer.addStyle("display", "table-cell ");
			rightcontainer.addStyle("vertical-align", "middle ");
			rightcontainer.addStyle("align", "center");
			
			FLabel fixedDimValues_label = new FLabel("fixedDimValues_label",
					"<b>Please select the values of the fixed dimensions for the slice:<b>");
			
			rightcontainer.add(fixedDimValues_label);
			
			// An FGrid to show visual and fixed cube dimensions
			FGrid fg = new FGrid("mygrid");

			// Styling of FGrid
			fg.addStyle("align", "center");
			fg.addStyle("margin", "auto");

			ArrayList<FComponent> farray = new ArrayList<FComponent>();

			// Add components to FGrid
			farray.add(leftcontainer);
			farray.add(rightcontainer);
			
			fg.addRow(farray);
				
			// Button to create slice
			FButton createSlice = new FButton("createSlice", "createSlice") {
				@Override
				public void onClick() {
					
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
					
					//Get observations belonging to the slice
					List<LDResource> sliceObservations=SliceSPARQL.getSliceObservations(
							fixedDimensionsSelectedValues, cubeURI,cubeGraph, SPARQL_service);
					
					//create new slice
					String sliceURI = SliceSPARQL.createCubeSlice(cubeURI, cubeGraph,
							fixedDimensionsSelectedValues,	sliceObservations,SPARQL_service);
					
					String message = "A new slice with the following URI has been created: "+ sliceURI;

					FDialog.showMessage(this.getPage(),	"New Slice created", message, "ok");
				}
			};
				
			cnt.add(fg);
			cnt.add(createSlice);

		//Not a valid cube URI
		} else {

			String uri = cubeURI.replaceAll("<", "");
			uri = uri.replaceAll(">", "");
			String message = "The URI <b>" + uri
					+ "</b> is not a valid cube +URI.";
			FLabel invalidURI_label = new FLabel("invalidURI", message);
			cnt.add(invalidURI_label);
		}				
		return cnt;
	}

	// Add labels and combo boxed for the fixed cube dimensions
	private void addFixedDimensions() {
		rightcontainer.addAndRefresh(getNewLineComponent(true));
		dimensionURIfcomponents.clear();
		// Add a Label - Combo box for each fixed cube dimension
		for (LDResource fDim : fixedDimensions) {
			List<FComponent> dimComponents = new ArrayList<FComponent>();
			// Add the label for the fixed cube dimension
			FLabel fDimLabel = new FLabel(fDim.getURIorLabel() + "_label",
					"<b>" + fDim.getURIorLabel() + "</b>");
			dimComponents.add(fDimLabel);
			rightcontainer.addAndRefresh(fDimLabel);

			// IWB does not support too long IDs
			String comboID = fDim.getURIorLabel();
			if (comboID.length() > 10) {
				comboID = fDim.getURIorLabel().substring(0, 10);
			}

			// Add the combo box for the fixed cube dimension
			FComboBox fDimCombo = new FComboBox(comboID + "_combo");

			// Populate the combo box with the values of the fixed cube
			// dimension
			for (LDResource ldr : allDimensionsValues.get(fDim)) {
				fDimCombo.addChoice(ldr.getURIorLabel(), ldr.getURI());
			}

			// Combo box pre-selected value
			fDimCombo.setPreSelected(fixedDimensionsSelectedValues.get(fDim).getURI());
			dimComponents.add(fDimCombo);
			rightcontainer.addAndRefresh(fDimCombo);
			rightcontainer.addAndRefresh(getNewLineComponent(true));
			dimensionURIfcomponents.put(fDim, dimComponents);
		}

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