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

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.bson.types.ObjectId;
import org.graylog2.Configuration;
import org.graylog2.bindings.providers.MongoJackObjectMapperProvider;
import org.graylog2.database.MongoConnection;
import org.mongojack.DBCursor;
import org.mongojack.JacksonDBCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Safe to use from multiple threads.
 */
@Singleton
public class IndexGroupConfigService {
    private static final Logger log = LoggerFactory.getLogger(IndexGroupConfigService.class);

    private final String defaultIndexName;

    private final AtomicReference<Map<String, IndexGroupConfig>> indexGroups = new AtomicReference<>();

    private final static String COLLECTION_NAME = "index_families";

    private final JacksonDBCollection<IndexGroupConfig, ObjectId> dbCollection;

    @Inject
    public IndexGroupConfigService(Configuration configuration,
                                   MongoConnection mongoConnection,
                                   MongoJackObjectMapperProvider mapper) {
        defaultIndexName = configuration.getElasticsearchDefaultIndexName();
        indexGroups.set(Maps.<String, IndexGroupConfig>newHashMap());

        dbCollection = JacksonDBCollection.wrap(
                mongoConnection.getDatabase().getCollection(COLLECTION_NAME),
                IndexGroupConfig.class,
                ObjectId.class,
                mapper.get());

    }

    public IndexGroupConfig getDefault() {
        return indexGroups.get().get(defaultIndexName);
    }

    public IndexGroupConfig getGroup(String indexName) {
        return indexGroups.get().get(indexName);
    }

    public void setIndexGroups(Set<IndexGroupConfig> families) {
        // ensure that the indices all exist
        // TODO this needs to be integrate with Deflector somehow

        final Map<String, IndexGroupConfig> newMap = Maps.uniqueIndex(families, new Function<IndexGroupConfig, String>() {
            @Override
            public String apply(IndexGroupConfig family) {
                return family.getIndexName();
            }
        });
        log.debug("Updating index group configs to {}", newMap);
        indexGroups.set(newMap);
    }

    public Set<IndexGroupConfig> getIndexGroups() {
        return Sets.newHashSet(indexGroups.get().values());
    }

    public Set<IndexGroupConfig> loadAll() {
        final DBCursor<IndexGroupConfig> families = dbCollection.find();
        return Sets.newHashSet(families.iterator());
    }
}
