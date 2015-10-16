OpenCube Browser & OLAP Browser
===============

The OpenCube Browser enables the table-based visualizations of RDF data cubes. 
While the OpenCube OLAP Browser enables performing OLAP operations (e.g. pivot, drill-down, and roll-up) on top of multiple linked data cubes.

### How it works

The OpenCube Browser and OLAP Browser are developed as a separate components of the OpenCube toolkit and is part of the “Data Exploring” lifecycle step. They supports browsing of cubes that are stored either at the native triple store or at remote SPARQL end-points. They can be initialized by creating a widget. 

The OpenCube Browser is developed as a separate component of the OpenCube toolkit and supports browsing of cubes that are stored either at the native triple store 
or at remote SPARQL end-points. The browser can be initialized by creating a widget giving as input
the URI of the cube to be browsed and the the URI of the remote SPARQL end-point (if needed).

Widget configuration for use with the native triple store:
```
{{#widget: DataCubeBrowserPlusPlus| asynch='true' }}
```    

Widget configuration for use with the remote SPARQL end-point containing data for the 2011 Irish Census:
```
{{#widget: DataCubeBrowser| 
     sparqlService='<http://data.cso.ie/sparql>'|
     asynch='true' 
}}
``` 

###Functionality

Currently the OpenCube (OLAP) Browser supports the following functionalities:
+ The OpenCube OLAP Browser presents in a table the values of a two-dimensional slice of an RDF data cube. 
+ The user can change the number of rows of the table (by default the browser presents 10 rows per page). 
+ The user can change the 2 dimensions that define the table of the browser. 
+ The user can change the values of the fixed dimensions (i.e. the dimensions of the cube that are not shown in the table) and thus select a different slice to be presented. 
+ The user can perform roll-up and drill-down OLAP operations by selecting the corresponding levels of the hierarchy (Only at OLAP Browser). 
+ The user can add or remove the dimensions of the cube to browse. This functionality is supported only for cubes having at least one aggregatable measure. 
+ The user can create and store a two-dimensional slice of the cube based on the data presented in the browser. 
+ The user can see available expansion based on the current presented cube (Only at OLAP Browser).
+ The user can see an Integrated view of multiple compatible cubes (Only at OLAP Browser).