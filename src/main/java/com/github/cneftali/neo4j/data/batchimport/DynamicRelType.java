package com.github.cneftali.neo4j.data.batchimport;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.graphdb.RelationshipType;

public class DynamicRelType implements RelationshipType {

	private static ConcurrentMap<String, DynamicRelType> instances = new ConcurrentHashMap<String, DynamicRelType>();


	private String name;

	public static DynamicRelType getType(String name) {
		DynamicRelType temp = instances.get(name);
		if (temp == null) {
			temp = new DynamicRelType(name);
			instances.put(name, temp);
		}
		return temp;
	}

	public static DynamicRelType getType(Class<?> clazz) {
		return getType(clazz.getName());
	}

	private DynamicRelType(String name) {
		this.name = name;
	}

	public String name() {
		return name;
	}

}
