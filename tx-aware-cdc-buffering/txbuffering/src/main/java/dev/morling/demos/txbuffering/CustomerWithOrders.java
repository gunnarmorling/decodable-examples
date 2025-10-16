package dev.morling.demos.txbuffering;

import java.util.Map;

public record CustomerWithOrders(int id, String firstName, String lastName, String email, Map<Integer, Order> ordersById, long txId) {

	public static CustomerWithOrders fromDataChangeEventPair(DataChangeEventPair changeEventPair) {
		Map<String, Object> customer = changeEventPair.left().after();

		if (!(changeEventPair.left().op().equals("c") || changeEventPair.left().op().equals("r"))) {
			throw new IllegalStateException("Expecting INSERT event");
		}

		Order order = Order.fromDataChangeEvent(changeEventPair.right());
		return new CustomerWithOrders(
				(int)customer.get("id"),
				(String)customer.get("first_name"),
				(String)customer.get("last_name"),
				(String)customer.get("email"),
				Map.of(order.id(), order),
				changeEventPair.txId()
		);
	}

//	public List<Order> orders() {
//		return ordersById.values().stream()
//			.sorted((o1, o2) -> o1.id() > o2.id() ? 1 : -1)
//			.collect(Collectors.toList());
//	}

	public CustomerWithOrders updateFromDataChangeEventPair(DataChangeEventPair changeEventPair) {
		if (changeEventPair.left().op().equals("c") || changeEventPair.left().op().equals("r") || changeEventPair.left().op().equals("u")) {
			Map<String, Object> customer = changeEventPair.left().after();

			if (changeEventPair.right().op().equals("c") || changeEventPair.right().op().equals("r") || changeEventPair.right().op().equals("u")) {
				Order order = Order.fromDataChangeEvent(changeEventPair.right());
				ordersById.put(order.id(), order);
			}
			else if (changeEventPair.right().op().equals("d")) {
				int id = (int) changeEventPair.right().before().get("id");
				ordersById.remove(id);
			}

			return new CustomerWithOrders(
					(int)customer.get("id"),
					(String)customer.get("first_name"),
					(String)customer.get("last_name"),
					(String)customer.get("email"),
					ordersById,
					changeEventPair.txId());
		}
		else {
			Map<String, Object> customer = changeEventPair.left().before();
			return new CustomerWithOrders(
					(int)customer.get("id"),
					null,
					null,
					null,
					null,
					changeEventPair.txId());
		}
	}
}
