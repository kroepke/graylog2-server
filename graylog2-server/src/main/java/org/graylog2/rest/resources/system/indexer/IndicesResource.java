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
package org.graylog2.rest.resources.system.indexer;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.graylog2.indexer.Deflector;
import org.graylog2.indexer.cluster.Cluster;
import org.graylog2.indexer.indices.IndexStatistics;
import org.graylog2.indexer.indices.Indices;
import org.graylog2.indexer.ranges.RebuildIndexRangesJob;
import org.graylog2.rest.documentation.annotations.*;
import org.graylog2.rest.resources.RestResource;
import org.graylog2.security.RestPermissions;
import org.graylog2.system.jobs.SystemJob;
import org.graylog2.system.jobs.SystemJobConcurrencyException;
import org.graylog2.system.jobs.SystemJobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
@RequiresAuthentication
@Api(value = "Indexer/Indices", description = "Index informations")
@Path("/system/indexer/indices")
public class IndicesResource extends RestResource {

    private static final Logger LOG = LoggerFactory.getLogger(IndicesResource.class);

    @Inject
    private RebuildIndexRangesJob.Factory rebuildIndexRangesJobFactory;
    @Inject
    private Indices indices;
    @Inject
    private Cluster cluster;
    @Inject
    private Deflector deflector;
    @Inject
    private SystemJobManager systemJobManager;

    @GET @Timed
    @Path("/{index}")
    @ApiOperation(value = "Get information of an index and its shards.")
    @Produces(MediaType.APPLICATION_JSON)
    public Response single(@ApiParam(title = "index") @PathParam("index") String index) {
        checkPermission(RestPermissions.INDICES_READ, index);

        Map<String, Object> result = Maps.newHashMap();

        try {
            final IndexStatistics stats = indices.getIndexStats(index);
            if (stats == null) {
                LOG.error("Index [{}] not found.", index);
                return Response.status(404).build();

            }
            List<Map<String, Object>> routing = Lists.newArrayList();
            for (ShardRouting shardRouting : stats.getShardRoutings()) {
                routing.add(shardRouting(shardRouting));
            }

            result.put("primary_shards", indexStats(stats.getPrimaries()));
            result.put("all_shards", indexStats(stats.getTotal()));
            result.put("routing", routing);
            result.put("is_reopened", indices.isReopened(index));
        } catch (Exception e) {
            LOG.error("Could not get indices information.", e);
            return Response.status(500).build();
        }

        return Response.ok().entity(json(result)).build();
    }

    @GET @Timed
    @Path("/closed")
    @ApiOperation(value = "Get a list of closed indices that can be reopened.")
    @Produces(MediaType.APPLICATION_JSON)
    public Response closed() {
        Map<String, Object> result = Maps.newHashMap();

        Set<String> closedIndices;
        try {

            closedIndices = Sets.filter(indices.getClosedIndices(), new Predicate<String>() {
                @Override
                public boolean apply(String indexName) {
                    return isPermitted(RestPermissions.INDICES_READ, indexName);
                }
            });

        } catch (Exception e) {
            LOG.error("Could not get closed indices.", e);
            return Response.status(500).build();
        }

        result.put("indices", closedIndices);
        result.put("total", closedIndices.size());

        return Response.ok().entity(json(result)).build();
    }

    @POST @Timed
    @Path("/{index}/reopen")
    @ApiOperation(value = "Reopen a closed index. This will also trigger an index ranges rebuild job.")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reopen(@ApiParam(title = "index") @PathParam("index") String index) {
        checkPermission(RestPermissions.INDICES_CHANGESTATE, index);

        indices.reopenIndex(index);

        // Trigger index ranges rebuild job.
        SystemJob rebuildJob = rebuildIndexRangesJobFactory.create(deflector);
        try {
            systemJobManager.submit(rebuildJob);
        } catch (SystemJobConcurrencyException e) {
            LOG.error("Concurrency level of this job reached: " + e.getMessage());
            throw new WebApplicationException(403);
        }

        return Response.noContent().build();
    }

    @POST @Timed
    @Path("/{index}/close")
    @ApiOperation(value = "Close an index. This will also trigger an index ranges rebuild job.")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "You cannot close the current deflector target index.")
    })
    public Response close(@ApiParam(title = "index") @PathParam("index") String index) {
        checkPermission(RestPermissions.INDICES_CHANGESTATE, index);

        if (deflector.getCurrentActualTargetIndex().equals(index)) {
            return Response.status(403).build();
        }

        // Close index.
        indices.close(index);

        // Trigger index ranges rebuild job.
        SystemJob rebuildJob = rebuildIndexRangesJobFactory.create(deflector);
        try {
            systemJobManager.submit(rebuildJob);
        } catch (SystemJobConcurrencyException e) {
            LOG.error("Concurrency level of this job reached: " + e.getMessage());
            throw new WebApplicationException(403);
        }

        return Response.noContent().build();
    }

    @DELETE @Timed
    @Path("/{index}")
    @ApiOperation(value = "Delete an index. This will also trigger an index ranges rebuild job.")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "You cannot delete the current deflector target index.")
    })
    public Response delete(@ApiParam(title = "index") @PathParam("index") String index) {
        checkPermission(RestPermissions.INDICES_DELETE, index);

        if (deflector.getCurrentActualTargetIndex().equals(index)) {
            return Response.status(403).build();
        }

        // Delete index.
        indices.delete(index);

        // Trigger index ranges rebuild job.
        SystemJob rebuildJob = rebuildIndexRangesJobFactory.create(deflector);
        try {
            systemJobManager.submit(rebuildJob);
        } catch (SystemJobConcurrencyException e) {
            LOG.error("Concurrency level of this job reached: " + e.getMessage());
            throw new WebApplicationException(403);
        }

        return Response.noContent().build();
    }

    private Map<String, Object> shardRouting(ShardRouting route) {
        Map<String, Object> result = Maps.newHashMap();

        result.put("id", route.shardId().getId());
        result.put("state", route.state().name().toLowerCase());
        result.put("active", route.active());
        result.put("primary", route.primary());
        result.put("node_id", route.currentNodeId());
        result.put("node_name", cluster.nodeIdToName(route.currentNodeId()));
        result.put("node_hostname", cluster.nodeIdToHostName(route.currentNodeId()));
        result.put("relocating_to", route.relocatingNodeId());

        return result;
    }

    private Map<String, Object> indexStats(final CommonStats stats) {
        Map<String, Object> result = Maps.newHashMap();

        result.put("flush", new HashMap<String, Object>() {{
            put("total", stats.getFlush().getTotal());
            put("time_seconds", stats.getFlush().getTotalTime().getSeconds());
        }});

        result.put("get", new HashMap<String, Object>() {{
            put("total", stats.getGet().getCount());
            put("time_seconds", stats.getGet().getTime().getSeconds());
        }});

        result.put("index", new HashMap<String, Object>() {{
            put("total", stats.getIndexing().getTotal().getIndexCount());
            put("time_seconds", stats.getIndexing().getTotal().getIndexTime().getSeconds());
        }});

        result.put("merge", new HashMap<String, Object>() {{
            put("total", stats.getMerge().getTotal());
            put("time_seconds", stats.getMerge().getTotalTime().getSeconds());
        }});

        result.put("refresh", new HashMap<String, Object>() {{
            put("total", stats.getRefresh().getTotal());
            put("time_seconds", stats.getRefresh().getTotalTime().getSeconds());
        }});

        result.put("search_query", new HashMap<String, Object>() {{
            put("total", stats.getSearch().getTotal().getQueryCount());
            put("time_seconds", stats.getSearch().getTotal().getQueryTime().getSeconds());
        }});

        result.put("search_fetch", new HashMap<String, Object>() {{
            put("total", stats.getSearch().getTotal().getFetchCount());
            put("time_seconds", stats.getSearch().getTotal().getFetchTime().getSeconds());
        }});

        result.put("open_search_contexts", stats.getSearch().getOpenContexts());
        result.put("store_size_bytes", stats.getStore().getSize().getBytes());
        result.put("segments", stats.getSegments().getCount());

        result.put("documents", new HashMap<String, Object>() {{
            put("count", stats.getDocs().getCount());
            put("deleted", stats.getDocs().getDeleted());
        }});

        return result;
    }

}
