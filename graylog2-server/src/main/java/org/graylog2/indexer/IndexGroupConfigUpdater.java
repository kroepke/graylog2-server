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

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.graylog2.Configuration;
import org.graylog2.plugin.periodical.Periodical;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

@Singleton
public class IndexGroupConfigUpdater extends Periodical {
    private static final Logger log = LoggerFactory.getLogger(IndexGroupConfigUpdater.class);
    private final IndexGroupConfigService indexGroupConfigService;
    private String defaultGlobalPrefix;
    private String defaultIndexName;

    @Inject
    public IndexGroupConfigUpdater(Configuration configuration,
                                   IndexGroupConfigService indexGroupConfigService) {
        this.indexGroupConfigService = indexGroupConfigService;
        defaultGlobalPrefix = configuration.getElasticSearchIndexPrefix();
        defaultIndexName = configuration.getElasticsearchDefaultIndexName();
    }

    @Override
    public boolean runsForever() {
        return false;
    }

    @Override
    public boolean stopOnGracefulShutdown() {
        return true;
    }

    @Override
    public boolean masterOnly() {
        return false;
    }

    @Override
    public boolean startOnThisNode() {
        return true;
    }

    @Override
    public boolean isDaemon() {
        return true;
    }

    @Override
    public int getInitialDelaySeconds() {
        return 0;
    }

    @Override
    public int getPeriodSeconds() {
        return 1;
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public void doRun() {
        final Set<IndexGroupConfig> newSet = indexGroupConfigService.loadAll();
        // the default family is always in the set.
        // TODO this shouln't be hardcoded
        newSet.add(new IndexGroupConfig(defaultGlobalPrefix, defaultIndexName));

        final Set<IndexGroupConfig> oldSet = indexGroupConfigService.getIndexGroups();
        if (Sets.symmetricDifference(oldSet, newSet).isEmpty()) {
            log.debug("Index families are unchanged, not updating");
        } else {
            log.debug("Updating index families. old {} new {}", oldSet, newSet);
            indexGroupConfigService.setIndexGroups(newSet);
        }
    }
}
