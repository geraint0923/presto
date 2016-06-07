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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class LikeClause
        extends TableElement
{
    private final QualifiedName tableName;
    private final Optional<List<LikeOption>> likeOptions;

    public static class LikeOption
    {
        public enum Type
        {
            INCLUDING,
            EXCLUDING
        }

        public enum Property
        {
            BUCKET,
            PARTITION,
            ALL
        }

        private final Type type;
        private final Property property;

        public LikeOption(Type type, Property property)
        {
            this.type = requireNonNull(type, "type is null");
            this.property = requireNonNull(property, "property is null");
        }

        public Type getType()
        {
            return type;
        }

        public Property getProperty()
        {
            return property;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            LikeOption o = (LikeOption) obj;
            return Objects.equals(this.type, o.type) &&
                    Objects.equals(this.property, o.property);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, property);
        }
    }

    public LikeClause(QualifiedName tableName, Optional<List<LikeOption>> options)
    {
        this(Optional.empty(), tableName, options);
    }

    public LikeClause(NodeLocation location, QualifiedName tableName, Optional<List<LikeOption>> options)
    {
        this(Optional.of(location), tableName, options);
    }

    private LikeClause(Optional<NodeLocation> location, QualifiedName tableName, Optional<List<LikeOption>> options)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.likeOptions = requireNonNull(options, "options is null");
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public Optional<List<LikeOption>> getLikeOptions()
    {
        return likeOptions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLikeClause(this, context);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LikeClause o = (LikeClause) obj;
        return Objects.equals(this.tableName, o.tableName) &&
                Objects.equals(this.likeOptions, o.likeOptions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, likeOptions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", tableName)
                .add("options", likeOptions)
                .toString();
    }
}
