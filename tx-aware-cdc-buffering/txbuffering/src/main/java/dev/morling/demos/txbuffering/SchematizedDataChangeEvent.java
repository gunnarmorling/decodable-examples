package dev.morling.demos.txbuffering;

import java.util.Map;

public record SchematizedDataChangeEvent(Map<String, Object> schema, DataChangeEvent payload) {
}
