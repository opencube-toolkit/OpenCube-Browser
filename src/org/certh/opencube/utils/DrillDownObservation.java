package org.certh.opencube.utils;

import java.util.HashMap;
import java.util.Map;

public class DrillDownObservation {
	
	//multiple measures may exist
	
	//measure --> measure value
	public Map<LDResource,String> Level1measureValueMap=new HashMap<LDResource, String>();
	
	//dimension value --> measure --> measure value
	public Map<LDResource,Map<LDResource,String>> Level2MeasureDimensionValueMap=
			new HashMap<LDResource, Map<LDResource,String>>();
	

}
