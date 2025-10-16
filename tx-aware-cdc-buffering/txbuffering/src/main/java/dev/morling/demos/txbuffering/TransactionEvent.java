package dev.morling.demos.txbuffering;

import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * {
  "status": "END",
  "id": "775:34365024",
  "event_count": 1,
  "data_collections": [
    {
      "data_collection": "inventory.customers",
      "event_count": 1
    }
  ],
  "ts_ms": 1760426071299
}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TransactionEvent(Status status, String id, int event_count, List<DataCollection> data_collections, long ts_ms) {
	public static enum Status { BEGIN, END };

	public int txId() {
		return Integer.valueOf(id.split(":")[0]);
	}

	public int countFor(String collection) {
		for (DataCollection dataCollection : data_collections) {
			if (dataCollection.data_collection().equals(collection)) {
				return dataCollection.event_count();
			}
		}

		return 0;
	}
}
