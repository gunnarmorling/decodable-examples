package dev.morling.demos.txbuffering;

public record DataChangeEventPair(DataChangeEvent left, DataChangeEvent right) {

	public long txId() {
		return Math.max((int) left.source().get("txId"), (int) right.source().get("txId"));
	}
}
