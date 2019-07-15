package fi.vm.yti.codelist.api.domain;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

final class ElasticRequestUtils {

    private ElasticRequestUtils() {
        // prevent construction
    }

    static Map<String, String> labelFromKeyValueNode(final JsonNode labelNode) {
        final Map<String, String> ret = new HashMap<>();
        if (labelNode != null) {
            final Iterator<Map.Entry<String, JsonNode>> labelIter = labelNode.fields();
            while (labelIter.hasNext()) {
                final Map.Entry<String, JsonNode> entry = labelIter.next();
                final JsonNode value = entry.getValue();
                if (value.isTextual()) {
                    ret.put(entry.getKey(), value.textValue());
                } else if (value.isArray() && value.has(0)) {
                    ret.put(entry.getKey(), value.get(0).textValue());
                }
            }
        }
        return !ret.isEmpty() ? ret : null;
    }

    static String getTextValueOrNull(final JsonNode node,
                                     final String fieldName) {
        if (node != null) {
            final JsonNode field = node.get(fieldName);
            if (field != null) {
                return field.textValue();
            }
        }
        return null;
    }
}
