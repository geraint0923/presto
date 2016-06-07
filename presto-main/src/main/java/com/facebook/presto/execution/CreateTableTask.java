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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.LikeClause;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.tree.LikeClause.LikeOption.Property.ALL;
import static com.facebook.presto.sql.tree.LikeClause.LikeOption.Property.BUCKET;
import static com.facebook.presto.sql.tree.LikeClause.LikeOption.Property.PARTITION;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CreateTableTask
        implements DataDefinitionTask<CreateTable>
{
    private static final Map<LikeClause.LikeOption.Property, List<String>> LIKE_INCLUDING_PROPERTIES = ImmutableMap.of(
            PARTITION, ImmutableList.of("partitioned_by"),
            BUCKET, ImmutableList.of("bucketed_by", "bucket_count"));

    @Override
    public String getName()
    {
        return "CREATE TABLE";
    }

    @Override
    public String explain(CreateTable statement)
    {
        return "CREATE TABLE " + statement.getName();
    }

    @Override
    public CompletableFuture<?> execute(CreateTable statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine)
    {
        checkArgument(!statement.getElements().isEmpty(), "no columns for table");

        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (tableHandle.isPresent()) {
            if (!statement.isNotExists()) {
                throw new SemanticException(TABLE_ALREADY_EXISTS, statement, "Table '%s' already exists", tableName);
            }
            return completedFuture(null);
        }

        List<ColumnMetadata> columns = new ArrayList<>();
        Map<String, Object> finalProperties = new HashMap<>();
        for (TableElement element : statement.getElements()) {
            if (element instanceof ColumnDefinition) {
                ColumnDefinition column = (ColumnDefinition) element;
                Type type = metadata.getType(parseTypeSignature(column.getType()));
                if ((type == null) || type.equals(UNKNOWN)) {
                    throw new SemanticException(TYPE_MISMATCH, column, "Unknown type for column '%s' ", column.getName());
                }
                columns.add(new ColumnMetadata(column.getName(), type));
            }
            else if (element instanceof LikeClause) {
                LikeClause likeClause = (LikeClause) element;
                QualifiedObjectName likeTableName = createQualifiedObjectName(session, statement, likeClause.getTableName());
                Optional<TableHandle> likeTable = metadata.getTableHandle(session, likeTableName);
                if (!likeTable.isPresent()) {
                    throw new SemanticException(MISSING_TABLE, statement, "Like table '%s' doesn't exist", likeTableName);
                }

                TableMetadata likeTableMetadata = metadata.getTableMetadata(session, likeTable.get());

                Map<String, Object> currentProperties = likeTableMetadata.getMetadata().getProperties();
                mergeTableProperties(currentProperties, finalProperties, likeClause.getLikeOptions());

                List<ColumnMetadata> likeColumns = likeTableMetadata.getVisibleColumns();
                for (ColumnMetadata columnMetadata : likeColumns) {
                    columns.add(new ColumnMetadata(columnMetadata.getName(), columnMetadata.getType()));
                }
            }
            else {
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Invalid TableElement: " + element.getClass().getName());
            }
        }

        accessControl.checkCanCreateTable(session.getRequiredTransactionId(), session.getIdentity(), tableName);

        Map<String, Object> properties = metadata.getTablePropertyManager().getTableProperties(
                tableName.getCatalogName(),
                statement.getProperties(),
                session,
                metadata);

        combineProperties(finalProperties, statement.getProperties().keySet(), properties);

        TableMetadata tableMetadata = new TableMetadata(
                tableName.getCatalogName(),
                new ConnectorTableMetadata(tableName.asSchemaTableName(), columns, finalProperties, session.getUser(), false));

        metadata.createTable(session, tableName.getCatalogName(), tableMetadata);

        return completedFuture(null);
    }

    private void mergeTableProperties(Map<String, Object> source, Map<String, Object> destination, Optional<List<LikeClause.LikeOption>> likeOptions)
    {
        if (likeOptions.isPresent()) {
            List<LikeClause.LikeOption> options = likeOptions.get();
            Set<LikeClause.LikeOption.Property> optionProperties = new HashSet<>();
            boolean includingAll = false;
            for (LikeClause.LikeOption option : options) {
                LikeClause.LikeOption.Property likeProperty = option.getProperty();
                if (optionProperties.contains(likeProperty)) {
                    throw new PrestoException(StandardErrorCode.SYNTAX_ERROR, "the same LIKE option should only be specified once: " + option.getType());
                }
                optionProperties.add(likeProperty);

                if (option.getType().equals(LikeClause.LikeOption.Type.INCLUDING)) {
                    if (likeProperty.equals(ALL)) {
                        includingAll = true;
                        break;
                    }
                    else if (LIKE_INCLUDING_PROPERTIES.containsKey(likeProperty)) {
                        for (String propertyKey : LIKE_INCLUDING_PROPERTIES.get(likeProperty)) {
                            if (source.containsKey(propertyKey)) {
                                destination.put(propertyKey, source.get(propertyKey));
                            }
                        }
                    }
                }
            }
            if (includingAll) {
                destination.putAll(source);
            }
        }
    }

    private void combineProperties(Map<String, Object> finalProperties, Set<String> specifiedPropertyKeys, Map<String, Object> defaultProperties)
    {
        for (Map.Entry<String, Object> entry : defaultProperties.entrySet()) {
            if (specifiedPropertyKeys.contains(entry.getKey()) || !finalProperties.containsKey(entry.getKey())) {
                finalProperties.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
