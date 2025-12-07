package dev.morling.demos.txbuffering.simple;

public record OrderLine(long txId, long id, long orderId, String description) implements TransactionAware {

}
