package com.palsenberg;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by igmar on 01/03/16.
 */
public class DataCopy {
    private static final Integer display_message_at = 100;

    private String source_keyspace = null;
    private String destination_keyspace = null;
    private Cluster source_cluster = null;
    private Cluster destination_cluster = null;
    private Session source_session = null;
    private Session destination_session = null;
    private final CopyMode copymode;

    public DataCopy(final InetAddress sourceIp, final String sourceKeyspace, final InetAddress destinationIp, final String destinationKeyspace, final CopyMode cm) {
        this.source_keyspace = sourceKeyspace;
        this.destination_keyspace = destinationKeyspace;
        this.copymode = cm;
        source_cluster = Cluster.builder().addContactPoint(sourceIp.getHostAddress()).build();
        if (source_cluster != null) {
            source_session = source_cluster.connect(source_keyspace);
        }
        destination_cluster = Cluster.builder().addContactPoint(destinationIp.getHostAddress()).build();
        if (destination_cluster != null) {
            destination_session = destination_cluster.connect(destination_keyspace);
        }

    }

    public void close() {
        if (source_cluster != null) {
            source_cluster.close();
        }
        if (destination_cluster != null) {
            destination_cluster.close();
        }
    }

    public void run() throws RuntimeException {
        // name, iscounter
        Map<String, TableMetadata> metadata = analyse_metadata();
        Map<String, Boolean> tables;
        if (this.copymode == CopyMode.TABLECREATE) {
            tables = create_tables(metadata);
        } else if (this.copymode == CopyMode.TABLETRUNCATE) {
            tables = truncate_tables(metadata);
        } else {
            tables = noop_tables(metadata);
        }

        System.out.println(String.format("Tables info '%s'", tables));
        copy_tables(metadata, tables);
    }

    private Map<String, Boolean> truncate_tables(final Map<String, TableMetadata> tables) throws RuntimeException {
        final Map<String, Boolean> r = noop_tables(tables);

        for (Map.Entry<String, TableMetadata> entry : tables.entrySet()) {
            final String table = entry.getKey();
            try {
                System.out.println(String.format("Truncate destination table '%s'", table));
                destination_session.execute(String.format("TRUNCATE TABLE %s", table));
            } catch (InvalidQueryException e) {
                // Ignore
            }
            String CQL = entry.getValue().asCQLQuery().replace(source_keyspace, destination_keyspace);
            CQL = StringUtils.substringBefore(CQL, " WITH ");
            System.out.println(String.format("Creating destination table '%s' (%s)", table, CQL));
            destination_session.execute(String.format("%s", CQL));
            if (entry.getValue().getIndexes().size() > 0) {
                System.out.println(String.format("Creating indexes in table '%s'", table));
                for (IndexMetadata index : entry.getValue().getIndexes()) {
                    String CQLIdx = index.asCQLQuery().replace(source_keyspace, destination_keyspace);
                    destination_session.execute(CQLIdx);
                }
            }
        }

        return r;
    }

    private Map<String, Boolean> noop_tables(final Map<String, TableMetadata> tables) throws RuntimeException {
        Map<String, Boolean> r = new HashMap<>();
        for (Map.Entry<String, TableMetadata> entry : tables.entrySet()) {
            // We check here if it's a counter table
            for (ColumnMetadata column : entry.getValue().getColumns()) {
                if (column.getType().equals(DataType.counter())) {
                    r.put(entry.getKey(), true);
                    break;
                } else {
                    r.put(entry.getKey(), false);
                }
            }
        }
        return r;
    }

    private Map<String, Boolean> create_tables(final Map<String, TableMetadata> tables) throws RuntimeException {
        final Map<String, Boolean> r = noop_tables(tables);

        for (Map.Entry<String, TableMetadata> entry : tables.entrySet()) {
            final String table = entry.getKey();
            try {
                System.out.println(String.format("Deleting destination table '%s'", table));
                destination_session.execute(String.format("DROP TABLE %s", table));
            } catch (InvalidQueryException e) {
                // Ignore
            }
            String CQL = entry.getValue().asCQLQuery().replace(source_keyspace, destination_keyspace);
            CQL = StringUtils.substringBefore(CQL, " WITH ");
            System.out.println(String.format("Creating destination table '%s' (%s)", table, CQL));
            destination_session.execute(String.format("%s", CQL));
            if (entry.getValue().getIndexes().size() > 0) {
                System.out.println(String.format("Creating indexes in table '%s'", table));
                for (IndexMetadata index : entry.getValue().getIndexes()) {
                    String CQLIdx = index.asCQLQuery().replace(source_keyspace, destination_keyspace);
                    destination_session.execute(CQLIdx);
                }
            }
        }

        return r;
    }

    private Map<String, TableMetadata> analyse_metadata() throws RuntimeException {
        System.out.println(String.format("Copying metadata for source_keyspace '%s'", source_keyspace));
        KeyspaceMetadata kmd = getKeyspaceMetaData(this.source_keyspace);
        if (kmd == null) {
            throw new RuntimeException("Failed to get source_keyspace metadata");
        }

        Collection<TableMetadata> tables = kmd.getTables();
        Map<String, TableMetadata> r = new HashMap<>();
        for (TableMetadata tmd : tables) {
            System.out.println(String.format("Analyse table '%s'", tmd.getName()));
            r.put(tmd.getName(), tmd);
        }
        return r;
    }

    private void copy_tables(final Map<String, TableMetadata> metadata, Map<String, Boolean> tables) throws RuntimeException {
        if (!CollectionUtils.isEqualCollection(metadata.keySet(), tables.keySet())) {
            System.out.println("ERROR : metadata and tables don't match up. Bad, refusing to continue");
            throw new RuntimeException("Metadata and tables don't match up");
        }

        for (Map.Entry<String, Boolean> entry : tables.entrySet()) {
            if (entry.getValue()) {
                copy_counter_table(entry.getKey(), metadata.get(entry.getKey()));
            } else {
                copy_regular_table(entry.getKey(), metadata.get(entry.getKey()));
            }
        }
    }

    private void copy_regular_table(final String table, final TableMetadata tableMetadata) throws RuntimeException {
        final Select select = QueryBuilder.select().from(table);
        select.setFetchSize(display_message_at);
        final ResultSet resultSet = source_session.execute(select);
        int cnt = 0;

        System.out.println(String.format("Copy regular table '%s'", table));

        long start = System.currentTimeMillis();
        while (resultSet.iterator().hasNext()) {
            if (cnt % display_message_at == 0) {
                System.out.println(String.format("Done %s rows (%s)", cnt, System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            }
            Row row = resultSet.iterator().next();

            Insert insert = QueryBuilder.insertInto(table);
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                final String columnName = column.getName();
                insert = insert.value(columnName, row.getObject(columnName));
            }
            destination_session.executeAsync(insert);
            cnt++;
        }
        System.out.println(String.format("Processed %d rows for table '%s'", cnt, table));
    }

    private void copy_counter_table(final String table, final TableMetadata tableMetadata) throws RuntimeException {
        final Select select = QueryBuilder.select().from(table);
        select.setFetchSize(display_message_at);
        final ResultSet resultSet = source_session.execute(select);

        System.out.println(String.format("Copy counter table '%s'", table));

        String counterColumn = null;
        ColumnMetadata firstColumn = null;
        // Figure out the counter
        List<ColumnMetadata> primaryKey = tableMetadata.getPrimaryKey();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.getType().equals(DataType.counter())) {
                counterColumn = column.getName();
                continue;
            }
            if (firstColumn == null && primaryKey.contains(column)) {
                firstColumn = column;
                continue;
            }
        }

        if (counterColumn == null) {
            throw new RuntimeException("No counter column present");
        }

        if (firstColumn == null) {
            throw new RuntimeException("No first column present");
        }

        int cnt = 0;
        while (resultSet.iterator().hasNext()) {
            if (cnt % display_message_at == 0) {
                System.out.println(String.format("Done %s rows", cnt));
            }
            Row row = resultSet.iterator().next();

            Update.Where update_cnt = QueryBuilder.update(table).with(QueryBuilder.incr(counterColumn, row.getLong(counterColumn))).where(QueryBuilder.eq(firstColumn.getName(), row.getObject(firstColumn.getName())));

            for (ColumnMetadata column : tableMetadata.getPrimaryKey()) {
                if (column.getName().equals(firstColumn.getName())) {
                    continue;
                }
                final String columnName = column.getName();
                update_cnt = update_cnt.and(QueryBuilder.eq(columnName, row.getObject(columnName)));
            }
            destination_session.executeAsync(update_cnt);
            cnt++;
        }
        System.out.println(String.format("Processed %d rows for table '%s'", cnt, table));
    }

    private KeyspaceMetadata getKeyspaceMetaData(final String keyspace) {
        Metadata md = source_cluster.getMetadata();
        List<KeyspaceMetadata> sourceMetaData = md.getKeyspaces();

        for (KeyspaceMetadata kmd : sourceMetaData) {
            if (kmd.getName().equals(keyspace)) {
                return kmd;
            }
        }
        throw new RuntimeException(String.format("Keyspace '%s' does not exist", keyspace));
    }
}
