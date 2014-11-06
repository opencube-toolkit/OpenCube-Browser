package org.certh.opencube.selection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.certh.opencube.SPARQL.AggregationSPARQL;
import org.certh.opencube.SPARQL.CubeSPARQL;
import org.certh.opencube.SPARQL.SelectionSPARQL;
import org.certh.opencube.aggregation.AggregationSetCreator.Config;
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
import com.fluidops.ajax.components.FRadioButtonGroup;
import com.fluidops.ajax.components.FRadioButtonGroup.FRadioButton;
import com.fluidops.iwb.model.ParameterConfigDoc;
import com.fluidops.iwb.model.TypeConfigDoc;
import com.fluidops.iwb.util.IWBConfigClassLocator;
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
 * |asynch='true' 
 *  }}
 * </code>
 * 
 */

@TypeConfigDoc("The OpenCube Cube Selection enables the selection of cube dimensions and"
		+ "measures to visualize.")
public class CubeSelection extends AbstractWidget<CubeSelection.Config> {

	// The container to show the available data cube
	private FContainer cubeContainer = new FContainer("cubeContainer");

	// The container to show the available cube dimensions
	private FContainer dimensionsContainer = new FContainer("dimensionsContainer");

	// The container to show the available cube measures
	private FContainer measuresContainer = new FContainer("measuresContainer");
	
	// The container to show the available operations 
	private FContainer operationsContainer = new FContainer("operationsContainer");
	
	// The container to show the available dimensions/values 
	private FContainer valuesOfLevelContainer = new FContainer("valuesOfLevelContainer");
	
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
	private HashMap<LDResource,List<LDResource>> cubeMeasures = new HashMap<LDResource, List<LDResource>>();

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

	private List<String> availableLanguages;

	private String selectedLanguage = "";

	private String defaultLang = "";
	
	//Ignore multiple languages
	private boolean ignoreLang;

	private String selectedCubeURI = null;

	// True if widget is loaded for the first time
	private boolean isFirstLoad = true;
	
	// A map the Dimension values URIs and the corresponding Check boxes
	private HashMap<LDResource, FCheckBox> mapValueURIcheckBox = new HashMap<LDResource, FCheckBox>();
	
	//private HashMap<LDResource, List<LDResource>> allDimensionsValues = new HashMap<LDResource, List<LDResource>>();
	
	private HashMap<LDResource,HashMap<LDResource, List<LDResource>>> compatibleAddValue2Level=new 
			HashMap<LDResource, HashMap<LDResource,List<LDResource>>>();
	
	private Map<LDResource, Integer> allCubesAndDimCount= new HashMap<LDResource, Integer>();
	
	private LDResource selectedCube;
	
	

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
		
		//Use multiple languages, get parameter from widget configuration
		ignoreLang=config.ignoreLang;

		if(!ignoreLang){
			defaultLang = config.defaultLang;
						
			if(defaultLang==null || defaultLang.equals("")){
				defaultLang=com.fluidops.iwb.util.Config.getConfig().getPreferredLanguage();
			}
			selectedLanguage=defaultLang;
		}

		// Central container
		cnt = new FContainer(id);
		
		// Get all cubes from the store and the number of dimensions they have 
		allCubesAndDimCount=SelectionSPARQL.getAllAvailableCubesAndDimCount(SPARQL_Service); 

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
		cubeContainer.addStyle("width", "400px ");
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
					
					selectedCube=(LDResource)this.getSelected().get(0);
					selectedCubeURI = "<"+ selectedCube.getURI() + ">";
					// Get Cube/Slice Graph
					cubeGraph = CubeSPARQL.getCubeSliceGraph(
							selectedCubeURI, SPARQL_Service);

					// Get Cube Structure graph
					cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
							selectedCubeURI, cubeGraph, SPARQL_Service);

				
					//Thread to get Measures
					Thread dimsThread= new Thread(new  Runnable() {
						public void run() {
							// Get all Cube dimensions
							cubeDimensions = CubeSPARQL.getDataCubeDimensions(
									selectedCubeURI, cubeGraph, cubeDSDGraph,
									selectedLanguage, defaultLang,ignoreLang, SPARQL_Service);
						}
					});
					
					
					//Thread to get Measures
					Thread measuresThread= new Thread(new  Runnable() {
						public void run() {
							// Get the Cube measure
							List<LDResource> selectedCubeMeasures = CubeSPARQL.getDataCubeMeasure(
									selectedCubeURI, cubeGraph, cubeDSDGraph,
									selectedLanguage, defaultLang,ignoreLang, SPARQL_Service);
							
							cubeMeasures.clear();
							cubeMeasures.put(selectedCube,selectedCubeMeasures);
						}
					});
					
					//start thread
					dimsThread.start();
					measuresThread.start();
					
					//wait until thread finish
					try {
						dimsThread.join();
						measuresThread.join();
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				

					//Thread to get dimension levels
					Thread dimLevelsThread= new Thread(new  Runnable() {
						public void run() {
							dimensionsLevels = CubeHandlingUtils.getDimensionsLevels(selectedCubeURI,
									cubeDimensions, cubeDSDGraph,cubeGraph, selectedLanguage,
									defaultLang,ignoreLang, SPARQL_Service);
						}
					});
					
					//Thread to get dimension concept schemes
					Thread dimConceptSchemesThread= new Thread(new  Runnable() {
						public void run() {
							dimensionsConceptSchemes=CubeHandlingUtils.getDimensionsConceptSchemes(
									cubeDimensions, cubeDSDGraph, selectedLanguage,
									defaultLang,ignoreLang, SPARQL_Service);
						}
					});
																
					//Start threads
					dimLevelsThread.start();
					dimConceptSchemesThread.start();
										
					//wait until thread finished
					try {
						dimLevelsThread.join();
						dimConceptSchemesThread.join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
									
					cubeContainer.removeAll();
					dimensionsContainer.removeAll();
					measuresContainer.removeAll();
					operationsContainer.removeAll();
					valuesOfLevelContainer.removeAll();
					leftContainer.removeAll();
					cnt.removeAll();
					
					selectedDimensions = new ArrayList<LDResource>();
					compatibleAddValue2Level=new HashMap<LDResource, HashMap<LDResource,List<LDResource>>>();

					populateCentralContainer();
					
					long totalstopTime = System.currentTimeMillis();
					long totalelapsedTime = totalstopTime - totalstartTime;
					System.out.println("Load selection time: " + totalelapsedTime);
				}
			};

			// populate cubes combo box
			for (LDResource cube : allCubesAndDimCount.keySet()) {
				if(cube.getURI().contains("cube_")){
						if(cube.getLabel()!=null){
							cubesCombo.addChoice(cube.getLabel(), cube);
						}else{
							cubesCombo.addChoice(cube.getURI(), cube);
						}
					
				}else{
					if(cube.getLabel()!=null){
						cubesCombo.addChoice(cube.getLabel(), cube);
					}else{
						cubesCombo.addChoice(cube.getURI(), cube);
					}
				}
				
			}
			
			if(selectedCubeURI!=null){
				//Remove the "<" and ">" from the cube URI
				cubesCombo.setPreSelected(new LDResource(selectedCubeURI.substring(
						1, selectedCubeURI.length()-1)));
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
			dimensionsContainer.addStyle("width", "400px ");
			dimensionsContainer.addStyle("margin-left", "auto");
			dimensionsContainer.addStyle("margin-right", "auto");
			dimensionsContainer.addStyle("text-align", "left");
			
			FLabel dimensions_label = new FLabel("dimensions_label",
					"<b>Available dimensions:<b>");

			dimensionsContainer.add(dimensions_label);

			int dimcount=1;
			for (LDResource dim : cubeDimensions) {
				FLabel dim_label = new FLabel("dim_label"+dimcount,dim.getURIorLabel());
				dimensionsContainer.add(dim_label);
				dimcount++;
				if(dimensionsLevels.get(dim).size()>0){
					for (LDResource level : dimensionsLevels.get(dim)) {
						FLabel level_label = new FLabel("level_label"+dimcount,"&nbsp&nbsp&nbsp-"+level.getURIorLabel());
						dimensionsContainer.add(level_label);
						dimcount++;
					}
				}else{
					FLabel level_label = new FLabel("dim_label"+dimcount,"&nbsp&nbsp&nbsp-"+dim.getURIorLabel());
					dimensionsContainer.add(level_label);
					dimcount++;
				}
			}			
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
			measuresContainer.addStyle("width", "400px ");
			measuresContainer.addStyle("margin-left", "auto");
			measuresContainer.addStyle("margin-right", "auto");
			measuresContainer.addStyle("text-align", "left");
			
			FLabel measures_label = new FLabel("measures_label",
					"<b>Available measures:<b>");

			measuresContainer.add(measures_label);
			int i=1;
			for (LDResource mCube : cubeMeasures.keySet()) {
		
				for(LDResource measure:cubeMeasures.get(mCube)){
					// show one check box for each cube dimension
					FCheckBox measureCheckBox = new FCheckBox("measure_"+i,
							measure.getURIorLabel()+" ("+mCube.getURIorLabel()+")");
				
					measureCheckBox.setEnabled(false);
					measuresContainer.add(measureCheckBox);
					i++;
				}
			}
		}
		
		if(!isFirstLoad){
			// cube container styling
			operationsContainer.addStyle("border-style", "solid");
			operationsContainer.addStyle("border-width", "1px");
			operationsContainer.addStyle("padding", "10px");
			operationsContainer.addStyle("border-radius", "5px");
			operationsContainer.addStyle("border-color", "#C8C8C8 ");
			operationsContainer.addStyle("display", "table-cell ");
			operationsContainer.addStyle("vertical-align", "middle ");
			operationsContainer.addStyle("width", "400px ");
			operationsContainer.addStyle("margin-left", "auto");
			operationsContainer.addStyle("margin-right", "auto");
			operationsContainer.addStyle("text-align", "left");
			
			FLabel operations_label = new FLabel("operations_label",
					"<b>Please select an operation:<b>");
	
			operationsContainer.add(operations_label);

			// Add Combo box for language selection
			FComboBox operations_combo = new FComboBox("operations_combo") {
					@Override
					public void onChange() {
						String selectedOperation=this.getSelected().get(0).toString();
						compatibleAddValue2Level=new HashMap<LDResource, HashMap<LDResource,List<LDResource>>>();
						if(selectedOperation.equals("Add measure")){
							
							long startTime = System.currentTimeMillis();
											
							HashMap<LDResource,List<LDResource>> compatible=
									SelectionSPARQL.getMeasureCompatibleCubes(
											selectedCube,cubeGraph,cubeDSDGraph,
											allCubesAndDimCount, cubeDimensions, dimensionsLevels, 
											dimensionsConceptSchemes,
											selectedLanguage,defaultLang,ignoreLang,SPARQL_Service);
						
							List<LDResource> selectedCubeMeasures=cubeMeasures.get(selectedCube);
							
							cubeMeasures=new HashMap<LDResource, List<LDResource>>();
							for(LDResource cube:compatible.keySet()){
								cubeMeasures.put(cube,compatible.get(cube));
							}
							cubeMeasures.put(selectedCube, selectedCubeMeasures);
							
							cubeContainer.removeAll();
							dimensionsContainer.removeAll();
							measuresContainer.removeAll();
							operationsContainer.removeAll();
							valuesOfLevelContainer.removeAll();
							leftContainer.removeAll();
							cnt.removeAll();
							
							selectedDimensions = new ArrayList<LDResource>();

							populateCentralContainer();
							
							long stopTime = System.currentTimeMillis();
							long elapsedTime = stopTime - startTime;
							System.out.println("Add measure time: " + elapsedTime);
						
						}else if(selectedOperation.equals("Add value to level")){
							long startTime = System.currentTimeMillis();
																			
							List<LDResource> selectedCubeMeasures=cubeMeasures.get(
									new LDResource(selectedCubeURI.substring(1, selectedCubeURI.length()-1)));
							
							compatibleAddValue2Level=SelectionSPARQL.geAddValueToLevelCompatibleCubes(
									cubeDimensions, dimensionsLevels,selectedCubeMeasures,
									dimensionsConceptSchemes, selectedLanguage,defaultLang,
									ignoreLang,SPARQL_Service);
							
							cubeContainer.removeAll();
							dimensionsContainer.removeAll();
							measuresContainer.removeAll();
							operationsContainer.removeAll();
							valuesOfLevelContainer.removeAll();
							leftContainer.removeAll();
							cnt.removeAll();
							
							selectedDimensions = new ArrayList<LDResource>();

							populateCentralContainer();
							
							long stopTime = System.currentTimeMillis();
							long elapsedTime = stopTime - startTime;
							System.out.println("Add value to level time: " + elapsedTime);
						}
					}
			};

			operations_combo.addChoice("Add dimension");
			operations_combo.addChoice("Add measure");
			operations_combo.addChoice("Add level to dimension");
			operations_combo.addChoice("Change hierarchy to level");
			operations_combo.addChoice("Add value to level");
			
			operationsContainer.add(operations_combo);
		}
		
		if(compatibleAddValue2Level.keySet().size()>0){

			// cube container styling
			valuesOfLevelContainer.addStyle("border-style", "solid");
			valuesOfLevelContainer.addStyle("border-width", "1px");
			valuesOfLevelContainer.addStyle("padding", "10px");
			valuesOfLevelContainer.addStyle("border-radius", "5px");
			valuesOfLevelContainer.addStyle("border-color", "#C8C8C8 ");
			valuesOfLevelContainer.addStyle("display", "table-cell ");
			valuesOfLevelContainer.addStyle("vertical-align", "middle ");
		//	valuesOfLevelContainer.addStyle("width", "400px ");
			valuesOfLevelContainer.addStyle("margin-left", "auto");
			valuesOfLevelContainer.addStyle("margin-right", "auto");
			valuesOfLevelContainer.addStyle("text-align", "left");
			
			FLabel addValue2Level_label = new FLabel("addValue2Level_label","<b>Available dimensions' values:<b>");
			
			valuesOfLevelContainer.add(addValue2Level_label);
			
			HashMap<LDResource,List<LDResource>> compatibleDimensionValues=
							new HashMap<LDResource, List<LDResource>>();
			
			for(LDResource cube:compatibleAddValue2Level.keySet()){
				
				HashMap<LDResource, List<LDResource>> cubeDimValue=compatibleAddValue2Level.get(cube);
				
				for(LDResource dim:cubeDimValue.keySet()){
					List<LDResource> dimValues=cubeDimValue.get(dim);
					
					List<LDResource> existingDimValues=compatibleDimensionValues.get(dim);
					
					//No existing dim values
					if(existingDimValues==null){
						compatibleDimensionValues.put(dim, dimValues);
					}else{
						HashSet<LDResource> valuesSet=new HashSet<LDResource>(dimValues);
						valuesSet.addAll(existingDimValues);
						List<LDResource> mergedValues=new ArrayList<LDResource>(valuesSet);
						compatibleDimensionValues.put(dim, mergedValues);
					}
				}
			}
			int compatibleDimCount=1;
			for(LDResource dim:compatibleDimensionValues.keySet() ){
				FLabel dim_Label = new FLabel("compatibleDim_label"+compatibleDimCount,
						"<b>"+dim.getURIorLabel()+"</b>");
				valuesOfLevelContainer.add(dim_Label);
				compatibleDimCount++;
				
				List<LDResource> values=compatibleDimensionValues.get(dim);
				for(LDResource v:values){
					String value_label_str=v.getURIorLabel();
					if(value_label_str.length()>40){
						value_label_str=value_label_str.substring(0,40)+"...";
					}
					value_label_str+=" (<i>";
					
					for(LDResource cube:compatibleAddValue2Level.keySet()){
						
						HashMap<LDResource, List<LDResource>> cubeDimValue=compatibleAddValue2Level.get(cube);
						
						for(LDResource cubeDim:cubeDimValue.keySet()){
							List<LDResource> dimValues=cubeDimValue.get(cubeDim);
							if(dimValues.contains(v)){
								value_label_str+=cube.getURIorLabel()+", ";
							}
						}
					}
					
					value_label_str=value_label_str.substring(0,value_label_str.length()-2);
					value_label_str+="</i>)";
					
					// show one check box for each cube dimension
					FCheckBox valueCheckBox = new FCheckBox("valueCheckBox"+compatibleDimCount,	value_label_str);
					
					LDResource tmp=new LDResource((selectedCubeURI.substring(1, selectedCubeURI.length()-1)));
					
					if(value_label_str.contains(tmp.getURIorLabel())){
						valueCheckBox.setChecked(true);
						valueCheckBox.setEnabled(false);
					}
					
					//Add Dimension value -> checkBox at map
					mapValueURIcheckBox.put(v, valueCheckBox);
					valuesOfLevelContainer.add(valueCheckBox);
					compatibleDimCount++;
				}								
			}				
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
		
		//Add grid to left container
		leftContainer.add(selectionGrid);
		
		// Total FGrid
		FGrid totalGrid = new FGrid("totalGrid");
		ArrayList<FComponent> totalFarray = new ArrayList<FComponent>();
		totalFarray.add(leftContainer);
		
		if(compatibleAddValue2Level.keySet().size()>0){
			totalFarray.add(valuesOfLevelContainer);
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
		fhtml.setValue("<br><br>");
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