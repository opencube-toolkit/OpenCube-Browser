package org.certh.opencube.utils;
import org.openrdf.model.Literal;
import org.openrdf.model.impl.LiteralImpl;


public class LDResource implements Comparable {
	
	private String URI;
//	private String label;
	private Literal labelLiteral;
	private String level;
	
	public LDResource() {
		super();
	}

	public LDResource(String uRI) {
		URI = uRI;
	}
	
	public LDResource(String uRI,String label) {
		URI = uRI;
		if(label!=null){
			this.labelLiteral=new LiteralImpl(label);			
		}
	}
	
	public LDResource(String uRI,Literal label) {
		URI = uRI;
		if(label!=null){
			this.labelLiteral=label;			
		}
	}

	public String getURI() {
		return URI;
	}

	public void setURI(String uRI) {
		URI = uRI;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		 this.level= level;
	}
	
	public String getLabel() {
		if(labelLiteral!=null){
			return labelLiteral.getLabel();
		}else{
			return null;
		}
	}
	
	public Literal getLabelLiteral(){
		return labelLiteral;
	}

	public void setLabel(String label) {
		this.labelLiteral = new LiteralImpl(label);
	}
	
	public void setLabelLiteral(Literal labelLiteral) {
		this.labelLiteral = labelLiteral;
	}
	
	public String getLanguage(){
		if(labelLiteral!=null){
			return labelLiteral.getLanguage();
		}else{
			return "";
		}
	}

	// If label exists return the label
	// else return the last part of the URI (either after last '#' or after last '/')
	public String getURIorLabel()  {

		if (labelLiteral != null && labelLiteral.getLabel()!=null &&
				!labelLiteral.getLabel().equals("")) {
			return labelLiteral.getLabel();			
		} else if (URI.contains("#")) {
			return URI.substring(URI.lastIndexOf("#") + 1, URI.length());
		} else {
			return URI.substring(URI.lastIndexOf("/") + 1, URI.length());
		}

	}
	
	// If label exists return the label
	// else return the last part of the URI (either after last '#' or after last '/')
	public String getLastPartOfURI()  {
		if (URI.contains("#")) {
			return URI.substring(URI.lastIndexOf("#") + 1, URI.length());
		} else {
			return URI.substring(URI.lastIndexOf("/") + 1, URI.length());
		}

	}

	@Override
	public boolean equals(Object obj) {
		// if the two objects are equal in reference, they are equal
		if (this == obj) {
			return true;
		} else if (obj == null) {
			return false;
		} else if (obj instanceof LDResource) {
			LDResource cust = (LDResource) obj;
			if (cust.getURI() != null && cust.getURI().equals(URI)) {
				return true;
			}
		}
	
		return false;
	}
	
	public int hashCode(){
		return URI.hashCode();
		
	}	

	@Override
	public int compareTo(Object otherResource) {
		
		return  this.getURIorLabel().compareTo(((LDResource)otherResource).getURIorLabel());
	}	
	
}
