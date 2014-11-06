package org.certh.opencube.SPARQL;

import org.openrdf.query.BooleanQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLConnection;
import org.openrdf.repository.sparql.SPARQLRepository;

import com.fluidops.iwb.api.EndpointImpl;
import com.fluidops.iwb.api.ReadDataManager;
import com.fluidops.iwb.api.query.QueryBuilder;

public class QueryExecutor {
	
	// Execute a SPARQL select using the native IWB triple store
		// Input: the query to execute
		public static TupleQueryResult executeSelect(String query) {
			ReadDataManager dm = EndpointImpl.api().getDataManager();
			QueryBuilder<TupleQuery> queryBuilder = QueryBuilder
					.createTupleQuery(query);

			TupleQueryResult res = null;
		//	System.out.println(query);
			long startTime = System.currentTimeMillis();
			try {
				TupleQuery tulpequery = queryBuilder.build(dm);
				res = tulpequery.evaluate();
			} catch (MalformedQueryException e) {
				e.printStackTrace();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}

			long stopTime = System.currentTimeMillis();
			long elapsedTime = stopTime - startTime;
		//	System.out.println("Query Time: " + elapsedTime);

			return res;
		}

		// Execute a SPARQL ASK using the native IWB triple store
		// Input the query to execute
		public static boolean executeASK(String query) {
			ReadDataManager dm = EndpointImpl.api().getDataManager();
			QueryBuilder<BooleanQuery> queryBuilder = QueryBuilder
					.createBooleanQuery(query);

			System.out.println(query);
			long startTime = System.currentTimeMillis();

			boolean result = false;
			try {
				BooleanQuery tulpequery = queryBuilder.build(dm);
				result = tulpequery.evaluate();
			} catch (MalformedQueryException e) {

				e.printStackTrace();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}

			long stopTime = System.currentTimeMillis();
			long elapsedTime = stopTime - startTime;
			System.out.println("Query Time: " + elapsedTime);

			return result;
		}

		// Execute a SPARQL UPDATE using the native IWB triple store
		// Input the query to execute
		public static void executeUPDATE(String query) {
			ReadDataManager dm = EndpointImpl.api().getDataManager();

			QueryBuilder<Update> queryBuilder = QueryBuilder.createUpdate(query);

			System.out.println(query);
			long startTime = System.currentTimeMillis();

			try {
				Update updatequery = queryBuilder.build(dm);
				updatequery.execute();
			} catch (MalformedQueryException e) {
				e.printStackTrace();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (UpdateExecutionException e) {
				e.printStackTrace();
			}

			long stopTime = System.currentTimeMillis();
			long elapsedTime = stopTime - startTime;
			System.out.println("Query Time: " + elapsedTime);

		}

		// Execute a SPARQL ASK using an external triple store
		// Input the query to execute and the triple store URI
		public static boolean executeASK_direct(String query, String endpointUrl) {
			SPARQLRepository repo = new SPARQLRepository(endpointUrl);
			RepositoryConnection con = new SPARQLConnection(repo);

			boolean result = false;
			BooleanQuery booleanQuery;
			try {
				booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL, query);
				result = booleanQuery.evaluate();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (MalformedQueryException e) {
				e.printStackTrace();
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}

			return result;

		}

		// Execute a SPARQL Select using an external triple store
		// Input the query to execute and the triple store URI
		public static TupleQueryResult executeSelect_direct(String query,
				String endpointUrl) {
			SPARQLRepository repo = new SPARQLRepository(endpointUrl);
			RepositoryConnection con = new SPARQLConnection(repo);

			TupleQueryResult res = null;
			//System.out.println(query);
			try {
				TupleQuery tulpequery = con.prepareTupleQuery(QueryLanguage.SPARQL,	query);
				res = tulpequery.evaluate();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (MalformedQueryException e) {
				e.printStackTrace();
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			}

			return res;

		}

}
