package com.github.cneftali.neo4j.data.cypher;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class CypherTest {

	private static final String DB_PATH = "target/graph.db";

	private static final Map<String, String> config = new HashMap<String, String>();
	
	static {
		config.put("neostore.nodestore.db.mapped_memory", "161M");
		config.put("neostore.relationshipstore.db.mapped_memory", "714M");
		config.put("neostore.propertystore.db.mapped_memory", "90M");
		config.put("neostore.propertystore.db.strings.mapped_memory", "130M");
		config.put("neostore.propertystore.db.arrays.mapped_memory", "130M");
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DB_PATH).setConfig(config).newGraphDatabase();
		registerShutdownHook(graphDb);
		final ExecutionEngine engine = new ExecutionEngine(graphDb);
		getByMemberId(engine);
		
		

	}

	private static void getByMemberId(final ExecutionEngine engine) {
		final ExecutionResult result = engine.execute("CYPHER 1.9 START person=node:Members(id = \"31023\") RETURN person.id");
		
		for (final Map<String, Object> row : result) {
			for (final Entry<String, Object> column : row.entrySet()) {
				 System.out.println(column.getKey() + ": " + column.getValue() + "; ");
			}
		}
	}
	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		} );
	}
}
