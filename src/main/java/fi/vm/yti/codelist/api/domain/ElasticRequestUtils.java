package fi.vm.yti.codelist.api.domain;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

final class ElasticRequestUtils {

    private ElasticRequestUtils() {
        // prevent construction
    }

    static Map<String, String> labelFromKeyValueNode(JsonNode labelNode) {
        Map<String, String> ret = new HashMap<>();
        if (labelNode != null) {
            Iterator<Map.Entry<String, JsonNode>> labelIter = labelNode.fields();
            while (labelIter.hasNext()) {
                Map.Entry<String, JsonNode> entry = labelIter.next();
                JsonNode value = entry.getValue();
                if (value.isTextual()) {
                    ret.put(entry.getKey(), value.textValue());
                } else if (value.isArray() && value.has(0)) {
                    ret.put(entry.getKey(), value.get(0).textValue());
                }
            }
        }
        return !ret.isEmpty() ? ret : null;
    }

    static String getTextValueOrNull(JsonNode node,
                                     String fieldName) {
        if (node != null) {
            JsonNode field = node.get(fieldName);
            if (field != null) {
                return field.textValue();
            }
        }
        return null;
    }
}
