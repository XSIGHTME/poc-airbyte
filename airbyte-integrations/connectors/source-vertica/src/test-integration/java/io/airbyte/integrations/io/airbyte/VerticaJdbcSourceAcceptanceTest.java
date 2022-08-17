/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.io.airbyte.integration_tests.sources;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.integrations.source.jdbc.test.JdbcSourceAcceptanceTest;
import io.airbyte.integrations.source.vertica.VerticaSource;
import java.nio.file.Path;
import java.sql.JDBCType;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

// Run as part of integration tests, instead of unit tests, because there is no test container for
// Vertica.
class VerticaJdbcSourceAcceptanceTest extends JdbcSourceAcceptanceTest {

  private JsonNode config;

  private static JsonNode getStaticConfig() {
    return Jsons.deserialize(IOs.readFile(Path.of("secrets/config.json")));
  }

  @BeforeEach
  public void setup() throws Exception {
    config = getStaticConfig();
    super.setup();
  }

  @Override
  public boolean supportsSchemas() {
    return true;
  }

  @Override
  public AbstractJdbcSource<JDBCType> getJdbcSource() {
    return new VerticaSource();
  }

  @Override
  public JsonNode getConfig() {
    return config;
  }

  @Override
  public String getDriverClass() {
    return VerticaSource.DRIVER_CLASS;
  }

  @AfterEach
  public void tearDownVertica() throws SQLException {
    super.tearDown();
  }

}
