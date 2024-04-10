/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.converters;

import java.util.Set;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.SerializerType;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * CloudEvents maker for records producer by the PostgreSQL connector.
 *
 * @author Chris Cranford
 */
public class PostgresCloudEventsMaker extends CloudEventsMaker {

    private static final Set<String> POSTGRES_DATA_FIELDS = Collect.unmodifiableSet(
            Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);

    private static final String TXID_KEY = "txId";
    private static final String XMIN_KEY = "xmin";
    private static final String LSN_KEY = "lsn";
    private static final String SEQUENCE_KEY = "sequence";

    private static final Set<String> POSTGRES_SOURCE_FIELDS = Collect.unmodifiableSet(
            TXID_KEY,
            XMIN_KEY,
            LSN_KEY,
            SEQUENCE_KEY);

    public PostgresCloudEventsMaker(RecordAndMetadata recordAndMetadata, SerializerType dataContentType, String dataSchemaUriBase,
                                    String cloudEventsSchemaName) {
        super(recordAndMetadata, dataContentType, dataSchemaUriBase, cloudEventsSchemaName);
    }

    @Override
    public String ceId() {
        return "name:" + sourceField(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";lsn:" + sourceField(LSN_KEY).toString()
                + ";txId:" + sourceField(TXID_KEY).toString()
                + ";sequence:" + sourceField(SEQUENCE_KEY).toString();
    }

    @Override
    protected Set<String> connectorSpecificDataFields() {
        return POSTGRES_DATA_FIELDS;
    }

    @Override
    public Set<String> connectorSpecificSourceFields() {
        return POSTGRES_SOURCE_FIELDS;
    }
}
