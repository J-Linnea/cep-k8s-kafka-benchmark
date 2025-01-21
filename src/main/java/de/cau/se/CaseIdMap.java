package de.cau.se;

import java.util.HashMap;

/**
 * This map keeps track of the last activity observed for a trace id.
 */
public class CaseIdMap<T> extends HashMap<T, String> {
    public String accept(final T caseId, final String activity) {
        String lastActivity = get(caseId);

        if (lastActivity == null) {
            put(caseId, activity);
        } else {
            replace(caseId, lastActivity, activity);
        }

        return lastActivity;
    }
}
