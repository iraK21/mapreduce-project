package utils;

import java.util.HashMap;
import java.util.Map;

/**
 * This class serves as a parser for the XML input files.
 */
public class XMLParser {

    /**
     * This method creates a key-value pair map out of the given string.
     * @param xml String to be parsed
     * @return Map of XMLgit
     */
    public static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
                    .split("\"");

            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];

                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }

        return map;
    }

}
