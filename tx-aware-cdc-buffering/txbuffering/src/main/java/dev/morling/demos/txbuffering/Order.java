package dev.morling.demos.txbuffering;

import java.util.Map;

public record Order(int id, int quantity, long productId) {

	public static Order fromDataChangeEvent(DataChangeEvent dataChangeEvent) {
		if (!(dataChangeEvent.op().equals("c") || dataChangeEvent.op().equals("r") || dataChangeEvent.op().equals("u"))) {
			throw new IllegalStateException("Expecting INSERT or UPDATE event");
		}

		Map<String, Object> order = dataChangeEvent.after();

		return new Order((int)order.get("id"), (int)order.get("quantity"), (int)order.get("product_id"));
	}
}
