package com.landawn.abacus.util.neo4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

// https://github.com/neo4j-contrib/neo4j-jdbc
public class Neo4jJdbcTest {

    @Test
    public void test_01() throws Exception {
        // Make sure Neo4j Driver is registered
        Class.forName("org.neo4j.jdbc.Driver");

        // Connect
        Connection con = DriverManager.getConnection("jdbc:neo4j://localhost:7474/", "neo4j", "admin");

        // Querying
        try (Statement stmt = con.createStatement()) {
            stmt.executeUpdate("CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})");

            ResultSet rs = stmt.executeQuery("MATCH (cloudAtlas {title: \"Cloud Atlas\"})<-[:DIRECTED]-(directors) RETURN directors.name");
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        }
    }

}
