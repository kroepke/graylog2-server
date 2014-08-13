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

import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog2.indexer.indices.Indices;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.base.Predicates.not;
import static org.graylog2.indexer.Index.IS_DEFLECTOR;

public class IndexGroup {

    private final Indices indicesService;
    private final Deflector.Factory deflectorFactory;
    private final IndexGroupConfig groupConfig;
    private final IndexGroupService.IsPartOfGroup partOfGroupPredicate;

    private final TreeSet<Index> indices;
    private final Index deflectorIndex;

    @AssistedInject
    public IndexGroup(Indices indicesService,
                      Deflector.Factory deflectorFactory,
                      @Assisted IndexGroupConfig groupConfig,
                      @Assisted Collection<Index> indices) {
        this.indicesService = indicesService;
        this.deflectorFactory = deflectorFactory;
        this.groupConfig = groupConfig;
        this.partOfGroupPredicate = new IndexGroupService.IsPartOfGroup(groupConfig);

        this.indices = Sets.newTreeSet(Index.AGE_COMPARATOR);
        this.indices.addAll(Collections2.filter(indices, not(IS_DEFLECTOR)));
        deflectorIndex = Iterables.find(indices, IS_DEFLECTOR);
    }

    public Deflector getDeflector() {
        return deflectorFactory.create(this);
    }

    public Index getWriteIndex() {
        return deflectorIndex;
    }

    public Index getLatestIndex() {
        return indices.last();
    }

    public String getName() {
        return groupConfig.getName();
    }

    /**
     * Creates an Index object for the next index in this family.
     * This index does not yet exist, the result of this method should be passed to {@link #createIndex(Index)} to actually
     * create the corresponding elasticsearch index.
     * @return
     */
    public Index createNextIndex() {
        final Index latestIndex = getLatestIndex();
        final long number = latestIndex.getNumber();
        return new Index(latestIndex.getName(), number + 1);
    }

    public Set<Index> getAllIndices() {
        return Sets.newHashSet(indices);
    }

    public boolean createIndex(Index index) {
        if (! partOfGroupPredicate.apply(index.getName())) {
            return false;
        }
        final boolean success = indicesService.create(index.getName());
        if (success) {
            indices.add(index);
        }
        return success;
    }

    public boolean removeIndex(Index index) {
        if (! partOfGroupPredicate.apply(index.getName())) {
            return false;
        }
        final boolean success = indicesService.delete(index.getName());
        if (success) {
            indices.remove(index);
        }
        return success;
    }

    public interface Factory {
        IndexGroup createWithIndices(IndexGroupConfig groupConfig,
                                     Collection<Index> indices);
    }

}
