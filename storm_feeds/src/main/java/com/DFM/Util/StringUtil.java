package com.DFM.Util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

public class StringUtil {
    public static String removeUnicode(String inputString) {
        String cleanString = StringUtil.filter(inputString);

        cleanString = cleanString.replaceAll("\u00BB", ">>");
        cleanString = cleanString.replaceAll("\u00BC", " 1/4");
        cleanString = cleanString.replaceAll("\u00BD", " 1/2");
        cleanString = cleanString.replaceAll("\u00BE", " 3/4");
        cleanString = cleanString.replaceAll("\u2013", "-");
        cleanString = cleanString.replaceAll("\u2014", "-");
        cleanString = cleanString.replaceAll("\u2018", "'");
        cleanString = cleanString.replaceAll("\u2019", "'");
        cleanString = cleanString.replaceAll("\u201A", "'");
        cleanString = cleanString.replaceAll("\u201B", "'");
        cleanString = cleanString.replaceAll("\u201C", "\"");
        cleanString = cleanString.replaceAll("\u201D", "\"");
        cleanString = cleanString.replaceAll("\u201E", "\"");
        cleanString = cleanString.replaceAll("\u201F", "\"");
        cleanString = cleanString.replaceAll("\u2026", "...");
        cleanString = cleanString.replaceAll("\u0027", "'");
        cleanString = cleanString.replaceAll("\\\\u00BB", ">>");
        cleanString = cleanString.replaceAll("\\\\u00BC", " 1/4");
        cleanString = cleanString.replaceAll("\\\\u00BD", " 1/2");
        cleanString = cleanString.replaceAll("\\\\u00BE", " 3/4");
        cleanString = cleanString.replaceAll("\\\\u2013", "-");
        cleanString = cleanString.replaceAll("\\\\u2014", "-");
        cleanString = cleanString.replaceAll("\\\\u2018", "'");
        cleanString = cleanString.replaceAll("\\\\u2019", "'");
        cleanString = cleanString.replaceAll("\\\\u201A", "'");
        cleanString = cleanString.replaceAll("\\\\u201B", "'");
        cleanString = cleanString.replaceAll("\\\\u201C", "\"");
        cleanString = cleanString.replaceAll("\\\\u201D", "\"");
        cleanString = cleanString.replaceAll("\\\\u201E", "\"");
        cleanString = cleanString.replaceAll("\\\\u201F", "\"");
        cleanString = cleanString.replaceAll("\\\\u2026", "...");
        cleanString = cleanString.replaceAll("\\\\u0027", "'");
        cleanString = cleanString.replaceAll("¶", "");
        return cleanString;
    }

    public static String hyphenateString(String input) {
        // Replace invalid characters with empty strings.
        String hyphenated = input;
        hyphenated = hyphenated.replaceAll("[\\s]", "-");
        hyphenated = hyphenated.replaceAll("[^a-zA-Z0-9-\\._]", "");
        hyphenated = hyphenated.replaceAll("\\-{2,}", "-");
        Pattern pattern = Pattern.compile("\\.([a-zA-Z]{2,5}[0-9]*)\\.");
        while (pattern.matcher(hyphenated).matches()) {
            hyphenated = hyphenated.replaceAll("\\.([a-zA-Z]{2,5}[0-9]*)\\.", ".$1_.");
        }
        return hyphenated;
    }

    public static String filter(String str) {
        char previous = 0;
        StringBuilder filtered = new StringBuilder(str.length());
        for (int i = 0; i < str.length(); i++) {
            char current = str.charAt(i);
            if (current >= 32 && current <= 126) {
                filtered.append(current);
            } else if (current == 65533 && current != previous) {
                filtered.append("&#8212;");
            } else {
                if (current != previous) {
                    filtered.append("&#").append(Integer.toString(current)).append(";");
                }
            }
            previous = current;
        }

        return filtered.toString();
    }

    public static String getHostname() {
        try {
            InetAddress address;
            address = InetAddress.getLocalHost();
            return address.getHostName();
        } catch (UnknownHostException ex) {
            return "unknown";
        }
    }

}
