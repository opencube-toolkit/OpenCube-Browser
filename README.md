OpenCube OLAP Browser
===============

The OpenCube browser enables the exploration of an RDF Data Cube by presenting each time a two-dimensional slice of the cube as a table.

### How it works

The OpenCube Browser is developed as a separate component of the OpenCube toolkit and supports browsing of cubes that are stored either at the native triple store 
or at remote SPARQL end-points. The browser can be initialized by creating a widget giving as input
the URI of the cube to be browsed and the the URI of the remote SPARQL end-point (if needed).

Widget configuration for use with the native triple store:
```
{{#widget: DataCubeBrowser|
   dataCubeURI= '<http://eurostat.linked-statistics.org/data/cens_hnctz>'|
   asynch='true'
}}
```    
Widget configuration for use with the remote SPARQL end-point containing data for the 2011 Irish Census:
```
{{#widget: DataCubeBrowser| 
     dataCubeURI=<http://data.cso.ie/census-2011/dataset/families-by-children-age-and-type-of-family/lt>|
	 sparqlService='<http://data.cso.ie/sparql>'|
     asynch='true' 
}}
``` 

###Functionality

Currently the OpenCube Browser supports the following functionalities:
+ The OpenCube Browser presents in a table the values of a two-dimensional slice of an RDF data cube. The user can change the number of rows of the table (by default the browser presents 20 rows per page).
+ The user can change the 2 dimensions that define the table of the browser.
+ The user can change the values of the fixed dimensions and thus select a different slice to be presented.
+ The user can perform roll-up and drill-down OLAP operations through dimensions reduction and insertion respectively. The user can add or remove the dimensions of the cube to browse. This functionality is supported only for cubes having at least one ''countable'' measure.
+ The user can create and store a two-dimensional slice of the cube based on the data presented in the browser. 