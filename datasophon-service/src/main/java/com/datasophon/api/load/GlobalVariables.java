package com.datasophon.api.load;

import java.util.HashMap;
import java.util.Map;

public class GlobalVariables {
    
    // cluster variable
    private static Map<Integer, Map<String, String>> map = new HashMap<>();
    
    public static void put(Integer key, Map<String, String> value) {
        map.put(key, value);
    }
    
    public static Map<String, String> get(Integer key) {
        return map.get(key);
    }
    
    public static boolean exists(Integer key) {
        return map.containsKey(key);
    }
}
