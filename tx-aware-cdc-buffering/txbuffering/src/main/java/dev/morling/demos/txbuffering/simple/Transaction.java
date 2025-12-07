package dev.morling.demos.txbuffering.simple;

import java.util.List;

public record Transaction(long txId, List<CollectionCount> counts) {

	public int countFor(String collection) {
		for (CollectionCount count : counts) {
			if (count.collection.equals(collection)) {
				return count.count;
			}
		}

		return 0;
	}

	public static record CollectionCount(String collection, int count) {}
}
