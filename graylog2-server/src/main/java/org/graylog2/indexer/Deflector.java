/*
 * Copyright 2012-2014 TORCH GmbH
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

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.graylog2.Configuration;
import org.graylog2.indexer.indices.Indices;
import org.graylog2.indexer.indices.jobs.OptimizeIndexJob;
import org.graylog2.indexer.ranges.RebuildIndexRangesJob;
import org.graylog2.system.activities.Activity;
import org.graylog2.system.activities.ActivityWriter;
import org.graylog2.system.jobs.SystemJobConcurrencyException;
import org.graylog2.system.jobs.SystemJobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 *
 * Format of actual indexes behind the Deflector:
 *   [configured_prefix]_1
 *   [configured_prefix]_2
 *   [configured_prefix]_3
 *   ...
 * 
 * @author Lennart Koopmann <lennart@socketfeed.com>
 */
public class Deflector { // extends Ablenkblech
    
    private static final Logger LOG = LoggerFactory.getLogger(Deflector.class);
    
    public static final String DEFLECTOR_SUFFIX = "deflector";
    private final SystemJobManager systemJobManager;
    private final ActivityWriter activityWriter;
    private final RebuildIndexRangesJob.Factory rebuildIndexRangesJobFactory;
    private final OptimizeIndexJob.Factory optimizeIndexJobFactory;
    private final String indexPrefix;
    private final Indices indices;
    private final IndexGroup indexGroup;

    @AssistedInject
    public Deflector(SystemJobManager systemJobManager,
                     Configuration configuration,
                     ActivityWriter activityWriter,
                     RebuildIndexRangesJob.Factory rebuildIndexRangesJobFactory,
                     OptimizeIndexJob.Factory optimizeIndexJobFactory,
                     Indices indices,
                     @Assisted IndexGroup indexGroup) {
        indexPrefix = configuration.getElasticSearchIndexPrefix();

        this.systemJobManager = systemJobManager;
        this.activityWriter = activityWriter;
        this.rebuildIndexRangesJobFactory = rebuildIndexRangesJobFactory;
        this.optimizeIndexJobFactory = optimizeIndexJobFactory;
        this.indices = indices;
        this.indexGroup = indexGroup;
    }
    
    public boolean isUp() {
        final Index writeIndex = indexGroup.getWriteIndex();
        return writeIndex != null && writeIndex.isAlias();
    }
    
    public void setUp() {
        // Check if there already is an deflector index pointing somewhere.
        if (isUp()) {
            LOG.info("Found deflector alias <{}>. Using it.", getName());
        } else {
            LOG.info("Did not find an deflector alias. Setting one up now.");

            try {
            // Do we have a target index to point to?
                try {
                    final Index latestIndex = indexGroup.getLatestIndex();
                    LOG.info("Pointing to already existing index target <{}>", latestIndex);

                    pointTo(latestIndex);
                } catch(NoSuchElementException ex) {
                    final String msg = "There is no index target to point to. Creating one now.";
                    LOG.info(msg);
                    activityWriter.write(new Activity(msg, Deflector.class));

                    cycle(); // No index, so automatically cycling to a new one.
                }
            } catch (InvalidAliasNameException e) {
                LOG.error("Seems like there already is an index called [{}]", getName());
            }
        }
    }
    
    public void cycle() {
        LOG.info("Cycling deflector to next index now.");

        final Index nextIndex = indexGroup.createNextIndex();
        final Index latestIndex = indexGroup.getLatestIndex();

        if (latestIndex == null) {
            LOG.info("Cycling from <none> to <{}>", nextIndex);
        } else {
            LOG.info("Cycling from <{}> to <{}>", latestIndex, nextIndex);
        }
        
        // Create new index.
        LOG.info("Creating index target <{}>...", nextIndex);
        if (!indexGroup.createIndex(nextIndex)) {
            LOG.error("Could not properly create new target <{}>", nextIndex);
        }
        updateIndexRanges();

        LOG.info("Done!");
        
        // Point deflector to new index.
        LOG.info("Pointing deflector to new target index....");

        Activity activity = new Activity(Deflector.class);
        if (latestIndex == null) {
            // Only pointing, not cycling.
            pointTo(nextIndex);
            activity.setMessage("Cycled deflector from <none> to <" + nextIndex + ">");
        } else {
            // Re-pointing from existing old index to the new one.
            pointTo(nextIndex, latestIndex);
            LOG.info("Flushing old index <{}>.", latestIndex);
            indices.flush(latestIndex.getName());

            LOG.info("Setting old index <{}> to read-only.", latestIndex);
            indices.setReadOnly(latestIndex.getName());
            activity.setMessage("Cycled deflector from <" + latestIndex + "> to <" + nextIndex + ">");

            try {
                systemJobManager.submit(optimizeIndexJobFactory.create(latestIndex.getName()));
            } catch (SystemJobConcurrencyException e) {
                // The concurrency limit is very high. This should never happen.
                LOG.error("Cannot optimize index <" + latestIndex + ">.", e);
            }
        }

        LOG.info("Done!");

        activityWriter.write(activity);
    }

    public String[] getAllDeflectorIndexNames() {
        final Set<Index> allIndices = indexGroup.getAllIndices();
        final Collection<String> names = Collections2.transform(allIndices, Index.TO_NAME);
        return names.toArray(new String[names.size()]);
    }
    
    public Map<String, IndexStats> getAllDeflectorIndices() {
        Map<String, IndexStats> result = Maps.newHashMap();
        Indices indices = this.indices;
        if (indices != null) {
            for (Map.Entry<String, IndexStats> e : indices.getAll().entrySet()) {
                String name = e.getKey();

                if (ourIndex(name)) {
                    result.put(name, e.getValue());
                }
            }
        }
        return result;
    }
    
    public String getNewestTargetName() throws NoTargetIndexException {
        return indexGroup.getLatestIndex().getName();
    }

    public String buildIndexName(String prefix, long number) {
        return prefix + "_" + number;
    }

    public static int extractIndexNumber(String indexName) throws NumberFormatException {
        String[] parts = indexName.split("_");
        
        try {
            return Integer.parseInt(parts[parts.length-1]);
        } catch(Exception e) {
            LOG.debug("Could not extract index number from index <" + indexName + ">.", e);
            throw new NumberFormatException();
        }
    }
    
    private boolean ourIndex(String indexName) {
        return !indexName.equals(getName()) && indexName.startsWith(indexPrefix + "_");
    }
    
    public void pointTo(Index newIndex, Index oldIndex) {
        indices.cycleAlias(getName(), newIndex.getName(), oldIndex.getName());
    }
    
    public void pointTo(Index newIndex) {
        indices.cycleAlias(getName(), newIndex.getName());
    }

    private void updateIndexRanges() {
        // Re-calculate index ranges.
        try {
            systemJobManager.submit(rebuildIndexRangesJobFactory.create(this));
        } catch (SystemJobConcurrencyException e) {
            String msg = "Could not re-calculate index ranges after cycling deflector: Maximum concurrency of job is reached.";
            activityWriter.write(new Activity(msg, Deflector.class));
            LOG.error(msg, e);
        }
    }

    public String getCurrentActualTargetIndex() {
        return indices.aliasTarget(getName());
    }

    public String getName() {
        return indexGroup.getName() + "_" + DEFLECTOR_SUFFIX;
    }

    public interface Factory {
        Deflector create(IndexGroup indexGroup);
    }

}