package dev.morling.demos.txbuffering;

import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DataChangeEvent(
		Map<String, Object> before,
		Map<String, Object> after,
		Map<String, Object> source,
		String op) {

	public int txId() {
		return (int)source().get("txId");
	}

	public String qualifiedTable() {
		return (String)source().get("schema") + "." + (String)source().get("table");
	}
}
