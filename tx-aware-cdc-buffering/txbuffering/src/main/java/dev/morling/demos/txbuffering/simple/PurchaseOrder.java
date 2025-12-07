package dev.morling.demos.txbuffering.simple;

public record PurchaseOrder(long txId, long id, String description) implements TransactionAware {

}
