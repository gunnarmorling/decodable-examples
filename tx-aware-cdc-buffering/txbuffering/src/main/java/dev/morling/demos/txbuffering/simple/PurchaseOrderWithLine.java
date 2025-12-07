package dev.morling.demos.txbuffering.simple;

public record PurchaseOrderWithLine(long txId, long id, String description, long lineId, String lineDescription) implements TransactionAware {

}
