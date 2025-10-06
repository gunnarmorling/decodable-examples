package dev.morling.demos.txbuffering;

import java.util.List;
import java.util.Map;

public record CustomerWithOrders(int id, String firstName, String lastName, String email, List<Order> orders) {

	public static CustomerWithOrders fromDataChangeEventPair(DataChangeEventPair changeEventPair) {
		Map<String, Object> customer = changeEventPair.left().after();

		if (!(changeEventPair.left().op().equals("c") || changeEventPair.left().op().equals("r"))) {
			throw new IllegalStateException("Expecting INSERT event");
		}

		return new CustomerWithOrders(
				(int)customer.get("id"),
				(String)customer.get("first_name"),
				(String)customer.get("last_name"),
				(String)customer.get("email"),
				List.of(Order.fromDataChangeEvent(changeEventPair.right()))
		);
	}

	public CustomerWithOrders updateFromDataChangeEventPair(DataChangeEventPair changeEventPair) {
		if (changeEventPair.left().op().equals("c") || changeEventPair.left().op().equals("r") || changeEventPair.left().op().equals("u")) {
			Map<String, Object> customer = changeEventPair.left().after();

			if (changeEventPair.right().op().equals("c") || changeEventPair.right().op().equals("r")) {
				orders.add(Order.fromDataChangeEvent(changeEventPair.right()));
			}
			else if (changeEventPair.right().op().equals("u")) {
				Order order = Order.fromDataChangeEvent(changeEventPair.right());
				int idx = 0;
				for (int i = 0; i < orders.size(); i++) {
					if (orders.get(i).id() == order.id()) {
						idx = i;
						break;
					}
				}

				orders.set(idx, order);
			}
			else if (changeEventPair.right().op().equals("d")) {
				int id = (int) changeEventPair.right().before().get("id");
				int idx = 0;
				for (int i = 0; i < orders.size(); i++) {
					if (orders.get(i).id() == id) {
						idx = i;
						break;
					}
				}

				orders.remove(idx);
			}

			return new CustomerWithOrders(
					(int)customer.get("id"),
					(String)customer.get("first_name"),
					(String)customer.get("last_name"),
					(String)customer.get("email"),
					orders);
		}
		else {
			Map<String, Object> customer = changeEventPair.left().before();
			return new CustomerWithOrders(
					(int)customer.get("id"),
					null,
					null,
					null,
					null);
		}
	}
}
