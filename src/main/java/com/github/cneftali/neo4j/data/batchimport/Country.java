package com.github.cneftali.neo4j.data.batchimport;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public enum Country {
	FRA, USA, ROU, ESP, DEU,
	JAP, AUS, BRA, CAN, TON,
	MAR, MEX, PER, SMR, THA,
	IRL, JAM, KAZ, ARG, KHM;

	private static final List<Country> VALUES = Collections.unmodifiableList(Arrays.asList(values()));
	private static final int SIZE = VALUES.size();
	
	private static final Random RANDOM = new Random();
	
	public static Country randomCountry() {
		return VALUES.get(RANDOM.nextInt(SIZE));
	}
}