/*
 * Copyright 2014 TORCH GmbH
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
 */
package org.graylog2.indexer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Id;

/**
 * An index group config is the set of message indices Graylog2 uses to store data.
 * This is essentially the naming scheme component of the index storage model.
 */
@JsonAutoDetect
public class IndexGroupConfig {
    private static final Logger log = LoggerFactory.getLogger(IndexGroupConfig.class);

    @Id
    @org.mongojack.ObjectId
    public ObjectId _id;

    private final String globalPrefix;
    private final String indexName;


    public IndexGroupConfig(String globalPrefix, String indexName) {
        this.globalPrefix = globalPrefix;
        this.indexName = indexName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexGroupConfig)) return false;

        IndexGroupConfig that = (IndexGroupConfig) o;

        if (!indexName.equals(that.indexName)) return false;
        if (!globalPrefix.equals(that.globalPrefix)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = globalPrefix.hashCode();
        result = 31 * result + indexName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "IndexGroupConfig{" +
                "globalPrefix='" + globalPrefix + '\'' +
                ", indexName='" + indexName + '\'' +
                '}';
    }

    public String getIndexName() {
        return indexName;
    }

    public String getName() {
        return globalPrefix + "_" + indexName;
    }
}
