package org.apache.calcite.jdbc;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.util.Sources;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class HttpTest {

    static {
        // log levels
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.INFO);
        ((Logger) LoggerFactory.getLogger("org.apache.calcite")).setLevel(Level.INFO);
        LogManager.getRootLogger().setLevel(org.apache.log4j.Level.INFO);
        LogManager.getLogger("org.apache.calcite").setLevel(org.apache.log4j.Level.INFO);
    }

    HttpServer testServer;

    int port = 8080;

    private AtomicInteger count = new AtomicInteger();

    private AtomicInteger errors = new AtomicInteger();

    private AtomicInteger rowsLoaded = new AtomicInteger();

    @Before
    public void setUp() throws SQLException {

        HttpServer.Builder<?> avaticaServerBuilder = new HttpServer.Builder<>()
                .withHandler(getLocalService(), Serialization.JSON)
                .withPort(port);

        testServer = avaticaServerBuilder.build();

        testServer.start();
    }

    @After
    public void tearDown() {
        if (testServer != null) {
            testServer.stop();
        }
    }

    @Test
    public void testConcurrenClients() throws Exception {
        System.err.println("=== Test Run ===");

        AtomicBoolean logOnce = new AtomicBoolean(true);

        IntStream.range(0, 100).parallel().forEach(idx -> {
            try {
                AvaticaTestClient testClient = new AvaticaTestClient("http://localhost:" + port);
                Connection connection = testClient.getConnection();

                if (logOnce.getAndSet(false)) {
                    output(connection.getMetaData().getTables(null, null, null, null));
                }

                try (ResultSet r = connection.getMetaData().getTables(null, null, null, null)) {
                    while (r.next()) {
                        String schema = r.getString(2);
                        String table = r.getString(3);
                        if (schema.equalsIgnoreCase("sales")) {

                            Statement stmt = connection.createStatement();
                            ResultSet rs = stmt.executeQuery("select * from " + schema + "." + table);
                            ResultSetMetaData rsmd = rs.getMetaData();
                            int cols = rsmd.getColumnCount();

                            while (rs.next()) {
                                for (int c = 1; c <= cols; c++) {
                                    rs.getString(c);
                                }
                                rowsLoaded.incrementAndGet();
                            }
                            rs.close();
                            stmt.close();
                            count.incrementAndGet();

                        }
                    }
                    System.err.print("*");
                }

                connection.close();

                System.err.print("*");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        System.out.println("Tables loaded " + count + ", Errors: " + errors + ", Rows Total:" + rowsLoaded);

        System.err.println("\n-- DONE --");
    }

    private String jsonPath(String model) {
        return Sources.of(HttpTest.class.getResource("/" + model + ".json")).file().getAbsolutePath();
    }

    private Service getLocalService() throws SQLException {
        Properties info = new Properties();
        info.put("model", jsonPath("model"));

        System.err.println("=== Server Setup ===");
        return new LocalService(new JdbcMeta("jdbc:calcite:", info));
    }

    private void output(ResultSet resultSet) throws SQLException {
        PrintStream out = System.out;

        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1;; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
    }

    /**
     * Providing access to remote Avatica HTTP server using Avatica JDBC driver.
     * See https://calcite.apache.org/avatica/docs/client_reference.html
     */
    static class AvaticaTestClient {

        private String jdbcUrl;

        public AvaticaTestClient(String serverUrl) {
            // load Avatica driver
            try {
                String driverClass = org.apache.calcite.avatica.remote.Driver.class.getName();
                Class.forName(driverClass).newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            Serialization serialization = Serialization.JSON;

            jdbcUrl = "jdbc:avatica:remote:url=" + serverUrl
                    + ";serialization=" + serialization.name().toLowerCase();
        }

        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(jdbcUrl);
        }
    }
}
