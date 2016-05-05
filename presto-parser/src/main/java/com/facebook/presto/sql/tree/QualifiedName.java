/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class QualifiedName
{
    private final List<String> originalParts;
    private final List<String> parts;
    private final int size;

    private Optional<QualifiedName> prefix;

    public static QualifiedName of(String first, String... rest)
    {
        requireNonNull(first, "first is null");
        return new QualifiedName(Lists.asList(first, rest));
    }

    public QualifiedName(String name)
    {
        this(ImmutableList.of(name));
    }

    public QualifiedName(Iterable<String> parts)
    {
        requireNonNull(parts, "parts is null");
        checkArgument(!isEmpty(parts), "parts is empty");
        this.parts = ImmutableList.copyOf(transform(parts, part -> part.toLowerCase(ENGLISH)));
        this.originalParts = ImmutableList.copyOf(parts);
        this.size = this.parts.size();
        if (this.size == 1) {
            this.prefix = Optional.empty();
        }
        else {
            this.prefix = Optional.of(new QualifiedName(this.originalParts, this.parts, this.size - 1));
        }
    }

    private QualifiedName(List<String> originalParts, List<String> parts, int size)
    {
        this.originalParts = originalParts;
        this.parts = parts;
        this.size = size;
        if (size == 1) {
            this.prefix = Optional.empty();
        }
        else {
            this.prefix = Optional.of(new QualifiedName(originalParts, parts, size - 1));
        }
    }

    public List<String> getParts()
    {
        return parts.subList(0, size);
    }

    public List<String> getOriginalParts()
    {
        return originalParts.subList(0, size);
    }

    @Override
    public String toString()
    {
        if (prefix.isPresent()) {
            return prefix.get() + "." + getSuffix();
        }
        return getSuffix();
    }

    /**
     * For an identifier of the form "a.b.c.d", returns "a.b.c"
     * For an identifier of the form "a", returns absent
     */
    public Optional<QualifiedName> getPrefix()
    {
        return prefix;
    }

    public boolean hasSuffix(QualifiedName name)
    {
        int current = this.size - 1;
        int other = name.size - 1;
        if (current < other) {
            return false;
        }
        while (this.parts.get(current).equals(name.parts.get(other))) {
            if (other <= 0) {
                return true;
            }
            if (current <= 0) {
                return false;
            }
            current--;
            other--;
        }
        return false;
    }

    public String getSuffix()
    {
        return parts.get(size - 1);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QualifiedName obj = (QualifiedName) o;
        return Objects.equals(prefix, obj.prefix) && Objects.equals(getSuffix(), obj.getSuffix());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(prefix, getSuffix());
    }
}
