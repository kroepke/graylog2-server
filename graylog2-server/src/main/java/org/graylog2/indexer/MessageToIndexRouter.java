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

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.graylog2.plugin.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Routes individual messages to indices, based on the result of some rule evaluation.
 *
 */
@Singleton
public class MessageToIndexRouter {
    private static final Logger log = LoggerFactory.getLogger(MessageToIndexRouter.class);

    private final IndexGroupConfigService indexGroupConfigService;

    @Inject
    public MessageToIndexRouter(IndexGroupConfigService indexGroupConfigService) {
        this.indexGroupConfigService = indexGroupConfigService;
    }

    public Multimap<Index, Message> route(Collection<Message> messages) {
        final SetMultimap<Index, Message> routes = MultimapBuilder.hashKeys().hashSetValues().build();

        // map each message to each of its streams (abbrev to 6 chars)
        for (Message message : messages) {
//            final IndexGroupConfig defaultFamily = indexGroupConfigService.getDefault();
//            if (message.getStreamIds().size() == 0) {
//                routes.put(defaultFamily.getWriteIndex(), message);
//            } else {
//                for (String streamId : message.getStreamIds()) {
//                    final String shortStreamName = streamId.substring(0, 6);
//
//                    final IndexGroupConfig indexGroupConfig = indexGroupConfigService.getGroup(shortStreamName);
//                    final Index index;
//                    if (indexGroupConfig != null) {
//                        index = indexGroupConfig.getWriteIndex();
//                    } else {
//                        log.error("Unknown index for name {}", shortStreamName);
//                        index = defaultFamily.getWriteIndex();
//                    }
//                    routes.put(index, message);
//                }
//            }
        }

        return routes;
    }
}
