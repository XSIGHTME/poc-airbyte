/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations;

import static io.airbyte.integrations.source.vertica.VerticaSource.VERTICA_CDC_OFFSET;
import static io.airbyte.integrations.source.vertica.VerticaSource.VERTICA_DB_HISTORY;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.debezium.CdcSavedInfoFetcher;
import io.airbyte.integrations.source.relationaldb.models.CdcState;
import java.util.Optional;

public class VerticaCdcSavedInfoFetcher implements CdcSavedInfoFetcher {

  private final JsonNode savedOffset;
  private final JsonNode savedSchemaHistory;

  public VerticaCdcSavedInfoFetcher(final CdcState savedState) {
    final boolean savedStatePresent = savedState != null && savedState.getState() != null;
    this.savedOffset = savedStatePresent ? savedState.getState().get(VERTICA_CDC_OFFSET) : null;
    this.savedSchemaHistory = savedStatePresent ? savedState.getState().get(VERTICA_DB_HISTORY) : null;
  }

  @Override
  public JsonNode getSavedOffset() {
    return savedOffset;
  }

  @Override
  public Optional<JsonNode> getSavedSchemaHistory() {
    return Optional.ofNullable(savedSchemaHistory);
  }

}
