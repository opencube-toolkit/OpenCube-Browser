package org.certh.opencube.aggregation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.certh.opencube.SPARQL.AggregationSPARQL;
import org.certh.opencube.SPARQL.CubeSPARQL;
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
 * {{#widget: org.certh.opencube.aggregation.AggregationSetCreator}}
 * 
 * </code>
 * 
 */

@TypeConfigDoc("The OpenCube Aggregation Set creator activates the OLAP-like browsing of " +
		"an existing data cube by creating the corresponding aggregated data cubes.")
public class AggregationSetCreator extends
		AbstractWidget<AggregationSetCreator.Config> {

	// A combo box to select the cube to enable OLAP-like browsing
	private FComboBox cubesCombo;

	// The SPARQL service to get data (not required)
	private String SPARQL_Service;

	// The central container
	private FContainer cnt;
	
	List<LDResource> cubesWithNoAggregationSet=new ArrayList<LDResource>();
	private List<String> availableLanguages;
	
	private String selectedLanguage="";
	
	private String defaultLang="";
	
	//Ignore multiple languages
	private boolean ignoreLang;

	public static class Config extends WidgetBaseConfig {
		@ParameterConfigDoc(desc = "SPARQL service to forward queries", required = false)
		public String sparqlService;
		
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
			defaultLang=com.fluidops.iwb.util.Config.getConfig().getPreferredLanguage();
						
			selectedLanguage=defaultLang;
		}

		// Central container
		cnt = new FContainer(id);

		// central container styling
		cnt.addStyle("border-style", "solid");
		cnt.addStyle("border-width", "1px");
		cnt.addStyle("padding", "20px");
		cnt.addStyle("border-radius", "5px");
		cnt.addStyle("border-color", "#C8C8C8 ");
		cnt.addStyle("display", "table-cell ");
		cnt.addStyle("vertical-align", "middle ");
		cnt.addStyle("height", "180px ");
		cnt.addStyle("margin-left", "auto");
		cnt.addStyle("margin-right", "auto");

		// Get all cubes from the store that do not have Aggregation sets
		cubesWithNoAggregationSet = AggregationSPARQL.getCubesWithNoAggregationSet();

		//If there are cubes out of  Aggregation Set
		if (cubesWithNoAggregationSet.size() > 0) {

			FLabel aggregation_label = new FLabel("aggregation_label",
					"<b>Please select cube for which you want to enable OLAP-like browsing:<b>");

			// Add Combo box with cube URIs
			cubesCombo = new FComboBox("cubesCombo");

			// populate cubes combo box
			for (LDResource cube : cubesWithNoAggregationSet) {
				cubesCombo.addChoice(cube.getURI(), cube.getURI());
			}

			// Button to create aggregation set
			FButton createAggregationSet = new FButton("createAggregationSet",
					"enable OLAP-like browsing for all") {
				@Override
				public void onClick() {
					for (LDResource cube : cubesWithNoAggregationSet) {
						// Cube URI to enable OLAP-like browsing
						String cubeURI = "<"+cube.getURI()+">";
								//"<"+ cubesCombo.getSelectedAsString().get(0) + ">";

						// Get cube graph
						String cubeGraph = CubeSPARQL.getCubeSliceGraph(
								cubeURI, SPARQL_Service);

						// Get Cube Structure graph
						String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
								cubeURI, cubeGraph, SPARQL_Service);

						// Get the available languages of labels
						availableLanguages = CubeSPARQL
								.getAvailableCubeLanguages(

								cubeDSDGraph, SPARQL_Service);

						// get the selected language to use
						selectedLanguage = CubeHandlingUtils
								.getSelectedLanguage(availableLanguages,
										selectedLanguage);

						// Get all Cube dimensions
						List<LDResource> cubeDimensions = CubeSPARQL
								.getDataCubeDimensions(cubeURI, cubeGraph,
										cubeDSDGraph, selectedLanguage,
										defaultLang, ignoreLang, SPARQL_Service);

						// Get the Cube measure
						List<LDResource> cubeMeasure = CubeSPARQL
								.getDataCubeMeasure(cubeURI, cubeGraph,
										cubeDSDGraph, selectedLanguage,
										defaultLang, ignoreLang, SPARQL_Service);

						// Create new aggregation set
						String aggregationSetURI = AggregationSPARQL
								.createNewAggregationSet(cubeDSDGraph,
										SPARQL_Service);

						// Attach original cube to aggregation set
						AggregationSPARQL.attachCube2AggregationSet(
								aggregationSetURI, cubeDSDGraph, cubeURI,
								SPARQL_Service);

						OrderedPowerSet<LDResource> ops = new OrderedPowerSet<LDResource>(
								(ArrayList<LDResource>) cubeDimensions);

						// calculate all dimension combinations
						for (int j = 1; j < cubeDimensions.size(); j++) {
							System.out.println("SIZE = " + j);

							List<LinkedHashSet<LDResource>> perms = ops
									.getPermutationsList(j);
							for (Set<LDResource> myset : perms) {
								String st = "";
								for (LDResource l : myset) {
									st += l.getURI() + " ";
								}
								System.out.println(st);

								// create new cube of aggregation set
						/*		String newCubeURI = AggregationSPARQL
										.createCubeForAggregationSet(myset,
												cubeMeasure, cubeURI,
												cubeGraph, cubeDSDGraph,
												aggregationSetURI,
												SPARQL_Service);

								System.out.println("NEW CUBE: " + newCubeURI);*/
							}
							System.out.println("----------");
						}
					}
					FDialog.showMessage(this.getPage(),
							"OLAP-like browsing enabled",
							"OLAP-Like browsing has been enabled for all cubes: "
							, "ok");

				}
			};
			
			// Button to create aggregation set
			FButton createSingleAggregationSet = new FButton("createSingleAggregationSet",
								"enable OLAP-like browsing for selected cube") {
							@Override
							public void onClick() {
									// Cube URI to enable OLAP-like browsing
									String cubeURI ="<"+ cubesCombo.getSelectedAsString().get(0) + ">";

									// Get cube graph
									String cubeGraph = CubeSPARQL.getCubeSliceGraph(
											cubeURI, SPARQL_Service);

									// Get Cube Structure graph
									String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
											cubeURI, cubeGraph, SPARQL_Service);

									// Get the available languages of labels
									availableLanguages = CubeSPARQL
											.getAvailableCubeLanguages(

											cubeDSDGraph, SPARQL_Service);

									// get the selected language to use
									selectedLanguage = CubeHandlingUtils
											.getSelectedLanguage(availableLanguages,
													selectedLanguage);

									// Get all Cube dimensions
									List<LDResource> cubeDimensions = CubeSPARQL
											.getDataCubeDimensions(cubeURI, cubeGraph,
													cubeDSDGraph, selectedLanguage,
													defaultLang, ignoreLang, SPARQL_Service);

									// Get the Cube measure
									List<LDResource> cubeMeasure = CubeSPARQL
											.getDataCubeMeasure(cubeURI, cubeGraph,
													cubeDSDGraph, selectedLanguage,
													defaultLang, ignoreLang, SPARQL_Service);

									// Create new aggregation set
									String aggregationSetURI = AggregationSPARQL
											.createNewAggregationSet(cubeDSDGraph,
													SPARQL_Service);

									// Attach original cube to aggregation set
									AggregationSPARQL.attachCube2AggregationSet(
											aggregationSetURI, cubeDSDGraph, cubeURI,
											SPARQL_Service);

									OrderedPowerSet<LDResource> ops = new OrderedPowerSet<LDResource>(
											(ArrayList<LDResource>) cubeDimensions);

									// calculate all dimension combinations
									for (int j = 1; j < cubeDimensions.size(); j++) {
										System.out.println("SIZE = " + j);

										List<LinkedHashSet<LDResource>> perms = ops
												.getPermutationsList(j);
										for (Set<LDResource> myset : perms) {
											String st = "";
											for (LDResource l : myset) {
												st += l.getURI() + " ";
											}
											System.out.println(st);
/*
											// create new cube of aggregation set
											String newCubeURI = AggregationSPARQL
													.createCubeForAggregationSet(myset,
															cubeMeasure, cubeURI,
															cubeGraph, cubeDSDGraph,
															aggregationSetURI,
															SPARQL_Service);

											System.out.println("NEW CUBE: " + newCubeURI);*/
										}
										System.out.println("----------");
									}
								
								FDialog.showMessage(this.getPage(),
										"OLAP-like browsing enabled",
										"OLAP-Like browsing has been enabled for cube: "+
										 cubesCombo.getSelectedAsString().get(0)
										, "ok");

							}
						};
						
						// Button to create aggregation set
						FButton createRollUpAggregation = new FButton("createRollUpAggregation",
											"enable Roll-up browsing for selected cube") {
										@Override
										public void onClick() {
											// Cube URI to enable OLAP-like browsing
											String cubeURI ="<"+ cubesCombo.getSelectedAsString().get(0) + ">";

											// Get cube graph
											String cubeGraph = CubeSPARQL.getCubeSliceGraph(
														cubeURI, SPARQL_Service);

											// Get Cube Structure graph
											String cubeDSDGraph = CubeSPARQL.getCubeStructureGraph(
														cubeURI, cubeGraph, SPARQL_Service);

											// Get the available languages of labels
											availableLanguages = CubeSPARQL.getAvailableCubeLanguages(
																	cubeDSDGraph, SPARQL_Service);

											// get the selected language to use
											selectedLanguage = CubeHandlingUtils.getSelectedLanguage(
														availableLanguages,	selectedLanguage);

											// Get all Cube dimensions
											List<LDResource> cubeDimensions = CubeSPARQL
														.getDataCubeDimensions(cubeURI, cubeGraph,
																cubeDSDGraph, selectedLanguage,
																defaultLang, ignoreLang, SPARQL_Service);

											// Get the Cube measure
											List<LDResource> cubeMeasure = CubeSPARQL
													.getDataCubeMeasure(cubeURI, cubeGraph,
															cubeDSDGraph, selectedLanguage,
															defaultLang, ignoreLang, SPARQL_Service);
											
											//dimension levels from data
											HashMap<LDResource, List<LDResource>> dimensionsLevels = 
													CubeHandlingUtils.getDimensionsLevels(
															cubeURI,cubeDimensions,
															cubeDSDGraph, cubeGraph,
															selectedLanguage,
															defaultLang, ignoreLang,
															SPARQL_Service);
											
											//dimension levels from schema
											HashMap<LDResource, List<LDResource>> dimensionsLevelsFromSchema =
													new HashMap<LDResource, List<LDResource>>();
											for(LDResource dim:cubeDimensions){
												List<LDResource> dimLevelsFromSchema=
														CubeSPARQL.getDimensionLevelsFromSchema(
																dim.getURI(), cubeDSDGraph, selectedLanguage, 
																defaultLang, ignoreLang, SPARQL_Service);
												dimensionsLevelsFromSchema.put(dim, dimLevelsFromSchema);
												
											}
											
											
											//compute rollup aggregations if they are not computed yet
											// i.e. the schema and data levels are different 
											for(LDResource dim:dimensionsLevels.keySet()){
												List<LDResource> dimLevels=dimensionsLevels.get(dim);
												List<LDResource> dimSchemaLevels=dimensionsLevelsFromSchema.get(dim);

												//If the schema has more levels than the data
												if(dimSchemaLevels.size()>dimLevels.size()){
													// create rollup aggregations for dimension with hierarchy
													AggregationSPARQL.createRollUpAggregation(dim,
															cubeDimensions, cubeMeasure, cubeURI,cubeGraph,
															cubeDSDGraph,SPARQL_Service);
												}												
											}
											
											
											
											// Create new aggregation set
											String aggregationSetURI = AggregationSPARQL
													.createNewAggregationSet(cubeDSDGraph,
															SPARQL_Service);

											// Attach original cube to aggregation set
											AggregationSPARQL.attachCube2AggregationSet(
													aggregationSetURI, cubeDSDGraph, cubeURI,
													SPARQL_Service);

											OrderedPowerSet<LDResource> ops = new OrderedPowerSet<LDResource>(
													(ArrayList<LDResource>) cubeDimensions);

											// calculate all dimension combinations
											for (int j = 1; j < cubeDimensions.size(); j++) {
												System.out.println("SIZE = " + j);

												List<LinkedHashSet<LDResource>> perms = ops
														.getPermutationsList(j);
												for (Set<LDResource> myset : perms) {
													String st = "";
													for (LDResource l : myset) {
														st += l.getURI() + " ";
													}
													System.out.println(st);

													// create new cube of aggregation set
													String newCubeURI = AggregationSPARQL
															.createCubeForAggregationSet(myset,
																	cubeMeasure, cubeURI,
																	cubeGraph, cubeDSDGraph,
																	aggregationSetURI,dimensionsLevelsFromSchema,
																	SPARQL_Service);

													System.out.println("NEW CUBE: " + newCubeURI);
												}
												System.out.println("----------");
											}
										
											
											
											
											
																	
											FDialog.showMessage(this.getPage(),
													"RollUp operation for browsing enabled",
													"RollUp operation for browsing has been enabled for cube: "+
													 cubesCombo.getSelectedAsString().get(0), "ok");
										}
									};

			FLabel much_time = new FLabel("much_time",
					"The process may take long depending on the cube's size");

			cnt.add(aggregation_label);
			cnt.add(cubesCombo);
			cnt.add(getNewLineComponent());
			cnt.add(createSingleAggregationSet);
			cnt.add(getNewLineComponent());
			cnt.add(createAggregationSet);
			cnt.add(getNewLineComponent());
			cnt.add(createRollUpAggregation);
			cnt.add(getNewLineComponent());
			cnt.add(much_time);
		} else {
			FLabel noCubes4Aggregation = new FLabel("noCubes4Aggregation",
					"<b>There are no available cubes to enable OLAP-like browsing<b>");
			cnt.add(noCubes4Aggregation);
		}
		return cnt;

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
		return "Data Cube Browser widget";
	}

	@Override
	public Class<?> getConfigClass() {
		return Config.class;
	}
}