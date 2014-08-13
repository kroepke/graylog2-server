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
import com.google.common.collect.ComparisonChain;

import java.util.Comparator;

/**
 * The description of an elasticsearch index we deal with. Usually part of an {@link IndexGroupConfig index group}.
 *
 */
public class Index {
    private final String name;
    private final long number;
    private boolean alias;

    public Index(String name, long number) {
        this.name = name;
        this.number = number;
        this.alias = false;
    }

    public Index(String baseName) {
        this.name = baseName;
        this.number = Long.MIN_VALUE;
    }

    public String getName() {
        if (isDeflector()) {
            return name + "_deflector";
        } else {
            return name + "_" + number;
        }
    }

    public long getNumber() {
        return number;
    }

    public boolean isDeflector() {
        return number == Long.MIN_VALUE;
    }

    public static final Comparator<Index> AGE_COMPARATOR = new Comparator<Index>() {
        @Override
        public int compare(Index o1, Index o2) {
            return ComparisonChain.start().compare(o1.getNumber(), o2.getNumber()).result();
        }
    };

    public static final Predicate<Index> IS_DEFLECTOR = new IsDeflectorIndex();

    public static final Function<Index, String> TO_NAME = new ToNameFunction();
    public boolean isAlias() {
        return alias;
    }

    public void setAlias(boolean isAlias) {
        this.alias = isAlias;
    }

    @Override
    public String toString() {
        return "Index " + getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Index)) return false;

        Index index = (Index) o;

        if (alias != index.alias) return false;
        if (number != index.number) return false;
        if (!name.equals(index.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) (number ^ (number >>> 32));
        result = 31 * result + (alias ? 1 : 0);
        return result;
    }

    private static class IsDeflectorIndex implements Predicate<Index> {
        @Override
        public boolean apply(Index index) {
            return index.isDeflector();
        }
    }

    private static class ToNameFunction implements Function<Index, String> {
        @Override
        public String apply(Index input) {
            return input.getName();
        }
    }
}
