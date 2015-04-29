package com.fluidops.iwb.datacatalog.widget;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;


import com.fluidops.ajax.components.FComponent;
import com.fluidops.ajax.components.FContainer;
import com.fluidops.ajax.components.FLabel;
import com.fluidops.ajax.components.FTree;
import com.fluidops.ajax.models.ExtendedTreeNode;
import com.fluidops.ajax.models.FTreeModel;
import com.fluidops.iwb.api.EndpointImpl;
import com.fluidops.iwb.api.ReadDataManager;
import com.fluidops.iwb.api.ReadDataManagerImpl;
import com.fluidops.iwb.model.ParameterConfigDoc;
import com.fluidops.iwb.model.ParameterConfigDoc.Type;
import com.fluidops.iwb.widget.AbstractWidget;
import com.fluidops.iwb.widget.config.WidgetBaseConfig;
import com.fluidops.util.StringUtil;

public class DataCubeTreeResultWidget extends AbstractWidget<DataCubeTreeResultWidget.Config>{

	
	private static Logger logger = Logger.getLogger(DataCubeTreeResultWidget.class.getName());

	public static final String PARENT_QUERY_VARIABLE = "parent";

	public static final String CHILD_QUERY_VARIABLE = "child";

	public static final String NO_DATA_MESSAGE = "No data";

	public static class Config extends WidgetBaseConfig{
		@ParameterConfigDoc(
				desc = "Query to denote parent - child relationsship. Parent nodes are marked as '?" + PARENT_QUERY_VARIABLE + "' children nodes as '?" + CHILD_QUERY_VARIABLE + "' .",
				type=Type.TEXTAREA)
		public String query;	

		@ParameterConfigDoc(
				desc = "icon string, e.g. /images/name.png")
		public String icon;

		@ParameterConfigDoc(
				desc = "Name of the Widget")
		public String title;
	}

	static Value root = ValueFactoryImpl.getInstance().createURI("http://www.fluidops.com/Root");

	@Override
	public String getTitle() {
		return get().title == null ? "Unknown title" : get().title;
	}

	@Override
	public Class<?> getConfigClass() {
		return DataCubeTreeResultWidget.Config.class;
	}

	@Override
	protected FComponent getComponent(String id) {
		FContainer fcont = new FContainer(id);
		// Create tree model
		CubeTreeNode rootTreeNode = new CubeTreeNode(new CubeTreeNodeElement(root, get().icon));
	//	rootTreeNode.initRoot();
	
		FTreeModel<CubeTreeNodeElement> tm = new FTreeModel<CubeTreeNodeElement>(rootTreeNode);

		if(tm.isLeaf(tm.getRoot())){
			fcont.add(new FLabel("nodata", NO_DATA_MESSAGE));
		}else{
			// Put the FTreeModel into an FTree and return the tree
			FTree treeResult = new FTree(id, tm);
			fcont.add(treeResult);
		}

		return fcont;
	}

	/*
	 * Stores the data for CubeTreeNode. 
	 */
	public static class CubeTreeNodeElement{
		private Value val;
		private String icon;
		private boolean topLevel = true;

		private List<CubeTreeNode> children = new LinkedList<CubeTreeNode>();

		public CubeTreeNodeElement(Value val, String icon){
			this.val = val;
			this.icon = icon;
		}

		public Value getValue(){
			return this.val;
		}

		public String getIcon(){
			return this.icon;
		}

		public void setLowerLevel(){
			this.topLevel = false;
		}

		public boolean isTopLevel(){
			return this.topLevel;
		}

		public void addChild(CubeTreeNode child){
			this.children.add(child);
		}

		public List<CubeTreeNode> getChildren(){
			return this.children;
		}
	}

	public class CubeTreeNode extends ExtendedTreeNode<CubeTreeNodeElement>{

		private static final long serialVersionUID = -5753580026877699589L;


		public CubeTreeNode(CubeTreeNodeElement obj) {
			super(obj);
			if(this.getObj().getValue().equals(root)){
				this.initRoot();
			}
		}

		@Override
		public List<? extends ExtendedTreeNode<CubeTreeNodeElement>> getChildren() {
			return getObj().getChildren();
		}

		/*
		 * Initialising all parent - child Relationships
		 * Only called one time at the beginning from root(in the constructor of CubeTreeNode). 
		 */
		private void initRoot(){
		//	List<CubeTreeNode> children = new LinkedList<CubeTreeNode>();
			ReadDataManager dm = ReadDataManagerImpl.getDataManager(pc.repository);
			String query = get().query;

			if(checkQueryParams(query)){

				Map<Value, CubeTreeNode> map = new HashMap<Value, CubeTreeNode>();
				TupleQueryResult result = null;
				try {
					result = dm.sparqlSelect(query, true, pc.value, false);
					BindingSet bs; 
					Value rootVal, childVal;
					CubeTreeNode root, child;
					try{
						while(result.hasNext()){
							bs = result.next();
							rootVal = bs.getValue(PARENT_QUERY_VARIABLE);
							childVal = bs.getValue(CHILD_QUERY_VARIABLE);
							if(rootVal instanceof URI && childVal instanceof URI){
								if(!map.containsKey(rootVal)){
									map.put(rootVal, new CubeTreeNode(new CubeTreeNodeElement(rootVal, get().icon)));
								}
								root = map.get(rootVal);

								if(!map.containsKey(childVal)){
									map.put(childVal, new CubeTreeNode(new CubeTreeNodeElement(childVal, get().icon)));
								}
								child = map.get(childVal);
								child.setAsChild();
								//setting the relationship
								root.addChild(child);
							}
						}
						/*
						 * now find first level nodes that get appended to the virtual root
						 */
						for(Value key : map.keySet()){
							if(map.get(key).isTopLevel()){
						//		children.add(map.get(key));
								this.addChild(map.get(key));
							}
						}
					} finally{
						if(result != null){
							result.close();
						}
					}
				} catch (MalformedQueryException e) {
					logger.error("Failed", e);
				} catch (QueryEvaluationException e) {
					logger.error("Failed", e);
				}

			}else{
				throw new RuntimeException("Query does not contain the required variables");
			}

		}

		private void setAsChild(){
			this.getObj().setLowerLevel();
		}

		private boolean isTopLevel(){
			return this.getObj().isTopLevel();
		}

		private void addChild(CubeTreeNode child){
			this.getObj().addChild(child);
		}

		public int getChildCount(){
			return getObj().getChildren().size();
		}

		@Override
		public boolean isRoot(){
			return getObj().getValue().equals(root);
		}

		private boolean checkQueryParams(String query){
			//check if the required query parameters are set.
			int start , end;
			String lowerCaseQuery = query.toLowerCase();
			start = lowerCaseQuery.indexOf("select");
			end = lowerCaseQuery.indexOf("{", start);

			String select = lowerCaseQuery.substring(start, end);
			if(!select.contains("?" + PARENT_QUERY_VARIABLE)){
				return false;
			}else{
				return select.contains("?" + CHILD_QUERY_VARIABLE);
			}

		}

		@Override
		public List<String> setValues(CubeTreeNodeElement obj) {
			// Distinguish between root and nodes
        	String link;
        	if (isRoot()){
        		link = "<b>" + getTitle() + "</b>";
        	}
        	else{
        		link = EndpointImpl.api().getRequestMapper().getAHrefFromValue(obj.getValue(), false, true, null);
        	}
            
        	List<String> res = new ArrayList<String>();
        	//icon
        	String iconImg = getIcon(obj);
        	//add icon in here in front of link
            res.add(iconImg + link);
            return res;
		}
		
		private String getIcon(CubeTreeNodeElement obj){
			String iconImgStr = "";
			if(StringUtil.isNotNullNorEmpty(obj.getIcon())){
        		iconImgStr = obj.getIcon();
			}
        	
        	if(StringUtil.isNullOrEmpty(iconImgStr)){
        		return "";
        	}else{
        		return "<img src=\""+iconImgStr+"\" style=\"max-height: 24px;\" align=\"absmiddle\" title=\""+obj.getValue().stringValue().replace("\"", "")+"\"/>&nbsp;";
        	}
		}

	}

}
