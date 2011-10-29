/**
 * Copyright 2011 Lennart Koopmann <lennart@socketfeed.com>
 *
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package org.graylog2.streams;

import com.mongodb.DBObject;
import org.bson.types.ObjectId;

/**
 * StreamRule.java: Mar 17, 2011 10:27:48 PM
 *
 * Representing the rules of a single stream.
 *
 * @author Lennart Koopmann <lennart@socketfeed.com>
 */
public class StreamRule {

    public final static int TYPE_MESSAGE = 1;
    public final static int TYPE_HOST = 2;
    public final static int TYPE_SEVERITY = 3;
    public final static int TYPE_FACILITY = 4;
    // Type 5 is reserved for frontend usage (timeframe filter)
    public final static int TYPE_ADDITIONAL = 6;
    // Type 7 used to be for the removed hostgroup feature.
    public final static int TYPE_SEVERITY_OR_HIGHER = 8;
    public final static int TYPE_HOST_REGEX = 9;
    public final static int TYPE_FULL_MESSAGE = 10;
    public final static int TYPE_FILENAME_LINE = 11;

    private ObjectId objectId = null;
    private int ruleType = 0;
    private String value = null;

    public StreamRule(DBObject rule) {
        this.objectId = (ObjectId) rule.get("_id");
        this.ruleType = (Integer) rule.get("rule_type");
        this.value = (String) rule.get("value");
    }

    /**
     * @return the objectId
     */
    public ObjectId getObjectId() {
        return objectId;
    }

    /**
     * @return the ruleType
     */
    public int getRuleType() {
        return ruleType;
    }

    /**
     * @return the value
     */
    public String getValue() {
        return value;
    }



}
