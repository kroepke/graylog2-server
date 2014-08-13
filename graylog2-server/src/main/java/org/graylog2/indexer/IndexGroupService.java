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
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.graylog2.indexer.indices.Indices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * An index group is a set of {@link org.graylog2.indexer.Index indices} which is a snapshot of actually existing indices
 * in Elasticsearch.
 *
 * Each index group has a deflector associated with it, which is then used in index retention and rotation to manage the
 * individual indices and the deflector (= write) alias.
 *
 * The recognized index groups are being configured via the {@link org.graylog2.indexer.IndexGroupConfig index group config}
 * which is persisted in MongoDB.
 */
public class IndexGroupService {
    private static final Logger log = LoggerFactory.getLogger(IndexGroupService.class);

    private final Indices indicesService;
    private final IndexGroup.Factory indexGroupFactory;

    public IndexGroupService(Indices indicesService, IndexGroup.Factory indexGroupFactory) {
        this.indicesService = indicesService;
        this.indexGroupFactory = indexGroupFactory;
    }

    /**
     * Updates the set of indices by reading from ElasticSearch.
     */
    public IndexGroup getCurrentIndexGroup(IndexGroupConfig groupConfig) {
        final Map<String, IndexStats> rawIndexNames = indicesService.getAll();
        if (rawIndexNames == null) {
            log.error("Unable to read elasticsearch index names");
            return null;
        }

        final CreateIndexFunction createIndexFromName = new CreateIndexFunction(groupConfig);
        final IsPartOfGroup isPartOfGroup = new IsPartOfGroup(groupConfig);

        Set<Index> indices = Sets.newHashSet();
        // not using the Collections2.filter and transform methods so we don't have to iterate twice
        for (String rawIndexName : rawIndexNames.keySet()) {
            if (!isPartOfGroup.apply(rawIndexName)) {
                log.debug("Skipping {}", rawIndexName);
                continue;
            }
            final Index index = createIndexFromName.apply(rawIndexName);
            if (index == null) {
                continue;
            }
            if (index.isDeflector()) {
                index.setAlias(indicesService.aliasExists(index.getName()));
            }
            indices.add(index);
        }

        return indexGroupFactory.createWithIndices(groupConfig, indices);
    }


    public static class IsPartOfGroup implements Predicate<String> {
        private final IndexGroupConfig groupConfig;

        public IsPartOfGroup(IndexGroupConfig groupConfig) {
            this.groupConfig = groupConfig;
        }
        @Override
        public boolean apply(String indexName) {
            return indexName.startsWith(groupConfig.getName());
        }
    }

    public static class CreateIndexFunction implements Function<String, Index> {
        private final IndexGroupConfig groupConfig;
        private final IsPartOfGroup isPartOfGroup;

        public CreateIndexFunction(IndexGroupConfig groupConfig) {
            this.groupConfig = groupConfig;
            isPartOfGroup = new IsPartOfGroup(groupConfig);
        }

        @Override
        public Index apply(String input) {
            if (!isPartOfGroup.apply(input)) {
                throw new IllegalStateException("Index name is not part of this family");
            }

            final String baseName = groupConfig.getName();
            final String numberPart = input.substring(baseName.length() + 1);
            if (Deflector.DEFLECTOR_SUFFIX.equals(numberPart)) {
                return new Index(baseName);
            }

            try {
                final Long idxNumber = Long.valueOf(numberPart);
                return new Index(baseName, idxNumber);
            } catch (NumberFormatException e) {
                log.warn("Invalid index number in index name " + input, e);
                throw new IllegalStateException("Invalid index name", e);
            }
        }
    }
}
