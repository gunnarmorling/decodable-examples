package dev.morling.demos.txbuffering;

import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Counts(Map<String, Integer> counts) {

	public void increment(String collection) {
		if (counts.containsKey(collection)) {
			counts.put(collection, counts.get(collection) + 1);
		}
		else {
			counts.put(collection, 1);
		}
	}

	public int getCount(String collection) {
		return counts.get(collection);
	}
}
