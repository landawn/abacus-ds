/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.landawn.abacus.DataSet;
import com.landawn.abacus.util.CassandraExecutor.UDTCodec;
import com.landawn.abacus.util.SQLBuilder.E;
import com.landawn.abacus.util.SQLBuilder.NE;
import com.landawn.abacus.util.entity.Song;
import com.landawn.abacus.util.entity.Users;

/**
 *
 * @since 0.8
 * 
 * @author Haiyang Li
 */
public class CassandraExecutorTest extends AbstractNoSQLTest {
    static final String collectionName = "testData";

    static final CassandraExecutor cassandraExecutor;

    static {
        final CodecRegistry codecRegistry = new CodecRegistry();

        final Cluster cluster = Cluster.builder().withCodecRegistry(codecRegistry).addContactPoint("127.0.0.1").build();

        codecRegistry.register(new UDTCodec<Users.Name>(cluster, "simplex", "fullname", Users.Name.class) {
            @Override
            protected Users.Name deserialize(UDTValue value) {
                if (value == null) {
                    return null;
                }
                Users.Name name = new Users.Name();
                name.setFirstName(value.getString("firstName"));
                name.setLastName(value.getString("lastName"));
                return name;
            }

            @Override
            protected UDTValue serialize(Users.Name value) {
                return value == null ? null : newUDTValue().setString("firstName", value.getFirstName()).setString("lastName", value.getLastName());
            }
        });

        codecRegistry.register(new UDTCodec<Users.Address>(cluster, "simplex", "address", Users.Address.class) {
            @Override
            protected Users.Address deserialize(UDTValue value) {
                if (value == null) {
                    return null;
                }
                Users.Address address = new Users.Address();
                address.setStreet(value.getString("street"));
                address.setCity(value.getString("city"));
                address.setZipCode(value.getInt("zipCode"));
                return address;
            }

            @Override
            protected UDTValue serialize(Users.Address value) {
                return value == null ? null
                        : newUDTValue().setString("street", value.getStreet()).setString("city", value.getCity()).setInt("zipcode", value.getZipCode());
            }
        });

        cassandraExecutor = new CassandraExecutor(cluster.connect());
        //        cassandraExecutor.execute("CREATE KEYSPACE IF NOT EXISTS simplex\r\n" + "  WITH REPLICATION = { \r\n" + "   'class' : 'SimpleStrategy', \r\n"
        //                + "   'replication_factor' : 1 \r\n" + "  };");
    }

    @Test
    public void test_user_defined_type() {
        //        cassandraExecutor.execute("DROP TABLE IF EXISTS simplex.users;");
        //        cassandraExecutor.execute("DROP TYPE IF EXISTS simplex.fullname;");
        //        cassandraExecutor.execute("DROP TYPE IF EXISTS simplex.address;");
        //        cassandraExecutor.execute("CREATE TYPE IF NOT EXISTS simplex.fullname(firstName text, lastName text);");
        //        cassandraExecutor.execute("CREATE TYPE IF NOT EXISTS simplex.address(street text, city text, zipCode int);");
        //        cassandraExecutor
        //                .execute("CREATE TABLE IF NOT EXISTS simplex.users(id uuid PRIMARY KEY, name frozen <fullname>, addresses map<text, frozen <address>>);");
        //        cassandraExecutor.execute("CREATE TABLE IF NOT EXISTS simplex.users(id uuid PRIMARY KEY, name text, addresses text);");

        Users user = new Users();
        user.setId(UUID.randomUUID());
        Users.Name name = new Users.Name();
        name.setFirstName("fn");
        name.setLastName("ln");
        user.setName(name);

        Users.Address address = new Users.Address();
        address.setCity("sunnyvale");
        address.setStreet("1");
        address.setZipCode(123);
        Map<String, Users.Address> addresses = N.asMap("home", address);
        user.setAddresses(addresses);

        String sql = NE.insert("id", "name", "addresses").into("simplex.users").sql();
        cassandraExecutor.execute(sql, user);

        DataSet ds = cassandraExecutor.query("SELECT * FROM simplex.users");
        ds.println();

        ds = cassandraExecutor.query(Users.class, "SELECT * FROM simplex.users");
        ds.println();

        Users user2 = cassandraExecutor.findFirst(Users.class, "SELECT * FROM simplex.users").orElse(null);
        N.println(N.stringOf(user2));

        user2 = cassandraExecutor.findFirst(Users.class, "SELECT name.firstName FROM simplex.users").orElse(null);
        N.println(N.stringOf(user2));
        assertEquals("fn", user2.getName().getFirstName());
    }

    @Test
    public void test_hello() {
        cassandraExecutor.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':3};");

        cassandraExecutor.execute("CREATE TABLE IF NOT EXISTS simplex.songs (" + "id uuid PRIMARY KEY," + "title text," + "album text," + "artist text,"
                + "tags set<text>," + "data blob" + ");");

        cassandraExecutor.execute("CREATE TABLE IF NOT EXISTS simplex.playlists (" + "id uuid," + "title text," + "album text, " + "artist text,"
                + "song_id uuid," + "PRIMARY KEY (id, title, album, artist)" + ");");

        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) " + "VALUES (" + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'La Petite Tonkinoise'," + "'Bye Bye Blackbird'," + "'Joséphine Baker'," + "{'jazz', '2013'})" + ";");

        cassandraExecutor.execute("INSERT INTO simplex.playlists (id, song_id, title, album, artist) " + "VALUES (" + "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,"
                + "756716f7-2e54-4715-9f00-91dcbea6cf50," + "'La Petite Tonkinoise'," + "'Bye Bye Blackbird'," + "'Joséphine Baker'" + ");");

        ResultSet results = cassandraExecutor.execute("SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");

        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"), row.getString("album"), row.getString("artist")));

            N.println(CassandraExecutor.toEntity(Map.class, row));
        }

        N.println(CassandraExecutor.extractData(results));

        results = cassandraExecutor.execute("SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
        N.println(CassandraExecutor.extractData(results));

        System.out.println();
    }

    @Test
    public void test_hello_2() {
        cassandraExecutor.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':3};");

        cassandraExecutor.execute("CREATE TABLE IF NOT EXISTS simplex.songs (" + "id uuid PRIMARY KEY," + "title text," + "album text," + "artist text,"
                + "tags set<text>," + "data blob" + ");");

        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) " + "VALUES (" + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'La Petite Tonkinoise'," + "'Bye Bye Blackbird'," + "'Joséphine Baker'," + "{'jazz', '2013'})" + ";");

        ResultSet results = cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"), row.getString("album"), row.getString("artist")));

            N.println(CassandraExecutor.toEntity(Song.class, row));
            N.println(CassandraExecutor.toEntity(Map.class, row));
        }

        N.println(CassandraExecutor.extractData(results));

        DataSet dataSet = cassandraExecutor.query("SELECT * FROM simplex.songs WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

        dataSet.println();
    }

    @Test
    public void test_query() {
        cassandraExecutor.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':3};");

        cassandraExecutor.execute("CREATE TABLE IF NOT EXISTS simplex.songs (" + "id uuid PRIMARY KEY," + "title text," + "album text," + "artist text,"
                + "tags set<text>," + "data blob" + ");");

        Song song = new Song();
        song.setId(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"));
        song.setTitle("La Petite Tonkinoise");
        song.setAlbum("Bye Bye Blackbird");
        song.setArtist("Joséphine Baker");
        song.setTags(N.asSet("jazz", "2013"));

        ResultSet resultSet = cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) " + "VALUES (?, ?, ?, ?, ?)", song.getId(),
                song.getTitle(), song.getAlbum(), song.getArtist(), N.stringOf(song.getTags()));

        N.println(resultSet.one());

        assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));

        assertEquals(1, cassandraExecutor.count("SELECT count(*) FROM simplex.songs WHERE id = ?", song.getId()));

        Song dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        Map<String, Object> map = cassandraExecutor.findFirst(Map.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
        N.println(map);
        assertEquals(song.getId(), map.get("id"));

        DataSet dataSet = cassandraExecutor.query("SELECT * FROM simplex.songs WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");
        dataSet.println();
        assertEquals(song.getId(), dataSet.get("id"));

        UUID uuid = cassandraExecutor.queryForSingleResult(UUID.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
        N.println(uuid);
        assertEquals(song.getId(), uuid);

        String strUUID = cassandraExecutor.queryForSingleResult(String.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
        N.println(strUUID);
        assertEquals(song.getId().toString(), strUUID);

        assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));

        cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", song.getId());
        assertFalse(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));
    }

    @Test
    public void test_find() {
        cassandraExecutor.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':3};");

        cassandraExecutor.execute("CREATE TABLE IF NOT EXISTS simplex.songs (" + "id uuid PRIMARY KEY," + "title text," + "album text," + "artist text,"
                + "tags set<text>," + "data blob" + ");");

        Song song = new Song();
        song.setId(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"));
        song.setTitle("La Petite Tonkinoise");
        song.setAlbum("Bye Bye Blackbird");
        song.setArtist("Joséphine Baker");
        song.setTags(N.asSet("jazz", "2013"));

        ResultSet resultSet = cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) " + "VALUES (?, ?, ?, ?, ?)", song.getId(),
                song.getTitle(), song.getAlbum(), song.getArtist(), N.stringOf(song.getTags()));

        N.println(resultSet.one());

        assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));

        assertEquals(1, cassandraExecutor.count("SELECT count(*) FROM simplex.songs WHERE id = ?", song.getId()));

        List<Song> dbSongs = cassandraExecutor.list(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId());
        N.println(dbSongs);
        assertEquals(song.getId(), dbSongs.get(0).getId());

        List<Map<String, String>> maps = (List) cassandraExecutor.list(Map.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId());
        N.println(maps);
        assertEquals(song.getId(), maps.get(0).get("id"));

        List<String> strs = cassandraExecutor.list(String.class, "SELECT title FROM simplex.songs WHERE id = ?", song.getId());
        N.println(strs);
        assertEquals(song.getTitle(), strs.get(0));

        cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", song.getId());
        assertFalse(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));
    }

    @Test
    public void test_query_async() throws InterruptedException, ExecutionException {
        cassandraExecutor.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':3};");

        cassandraExecutor.execute("CREATE TABLE IF NOT EXISTS simplex.songs (" + "id uuid PRIMARY KEY," + "title text," + "album text," + "artist text,"
                + "tags set<text>," + "data blob" + ");");

        Song song = new Song();
        song.setId(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"));
        song.setTitle("La Petite Tonkinoise");
        song.setAlbum("Bye Bye Blackbird");
        song.setArtist("Joséphine Baker");
        song.setTags(N.asSet("jazz", "2013"));

        ResultSet resultSet = cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) " + "VALUES (?, ?, ?, ?, ?)", song.getId(),
                song.getTitle(), song.getAlbum(), song.getArtist(), N.stringOf(song.getTags()));

        N.println(resultSet.one());

        assertTrue(cassandraExecutor.asyncExists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get());

        assertEquals(1, cassandraExecutor.asyncCount("SELECT count(*) FROM simplex.songs WHERE id = ?", song.getId()).get().longValue());

        Song dbSong = cassandraExecutor.asyncFindFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get().orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        Map<String, Object> map = cassandraExecutor.asyncFindFirst(Map.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get().orElse(null);
        N.println(map);
        assertEquals(song.getId(), map.get("id"));

        DataSet dataSet = cassandraExecutor.asyncQuery("SELECT * FROM simplex.songs WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;").get();
        dataSet.println();
        assertEquals(song.getId(), dataSet.get("id"));

        UUID uuid = cassandraExecutor.asyncQueryForSingleResult(UUID.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId()).get().orElse(null);
        N.println(uuid);
        assertEquals(song.getId(), uuid);

        String strUUID = cassandraExecutor.asyncQueryForSingleResult(String.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId()).get()
                .orElse(null);
        N.println(strUUID);
        assertEquals(song.getId().toString(), strUUID);

        assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));

        cassandraExecutor.asyncExecute("DELETE FROM simplex.songs WHERE id = ?", song.getId()).get();
        assertFalse(cassandraExecutor.asyncExists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get());
    }

    @Test
    public void test_parameterized() {
        cassandraExecutor.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':3};");

        cassandraExecutor.execute("CREATE TABLE IF NOT EXISTS simplex.songs (" + "id uuid PRIMARY KEY," + "title text," + "album text," + "artist text,"
                + "tags set<text>," + "data blob" + ");");

        Song song = new Song();
        song.setId(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"));
        song.setTitle("La Petite Tonkinoise");
        song.setAlbum("Bye Bye Blackbird");
        song.setArtist("Joséphine Baker");
        song.setTags(N.asSet("jazz", "2013"));

        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) " + "VALUES (#{id}, #{title}, #{album}, #{artist}, #{tags});",
                song);

        Song dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", N.asList(song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", N.asProps("id", song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", song.getId()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", N.asList(song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", N.asProps("id", song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", song.getId()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", N.asList(song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", N.asProps("id", song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", song.getId()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", N.asList(song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        try {
            cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", N.asProps("id", song.getId()));
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {

        }

        try {
            cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", song);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {

        }

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId().toString()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", N.asList(song.getId().toString())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", N.asProps("id", song.getId().toString()))
                .orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", song.getId().toString()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", N.asList(song.getId().toString())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", N.asProps("id", song.getId().toString()))
                .orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", song.getId().toString()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", N.asList(song.getId().toString())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", N.asProps("id", song.getId().toString()))
                .orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", song.getId().toString()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", N.asList(song.getId().toString())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        try {
            cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", N.asProps("id", song.getId().toString()));
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {

        }

        try {
            cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", song);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {

        }

        cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = #{id}", song.getId());
        assertFalse(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = #{id}", song.getId()));
    }

    @Test
    public void test_update() {
        cassandraExecutor.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':3};");

        cassandraExecutor.execute("CREATE TABLE IF NOT EXISTS simplex.songs (" + "id uuid PRIMARY KEY," + "title text," + "album text," + "artist text,"
                + "tags set<text>," + "data blob" + ");");

        cassandraExecutor.execute("CREATE TABLE IF NOT EXISTS simplex.playlists (" + "id uuid," + "title text," + "album text, " + "artist text,"
                + "song_id uuid," + "PRIMARY KEY (id, title, album, artist)" + ");");

        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) " + "VALUES (" + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'La Petite Tonkinoise'," + "'Bye Bye Blackbird'," + "'Joséphine Baker'," + "{'jazz', '2013'})" + ";");

        cassandraExecutor.execute("INSERT INTO simplex.playlists (id, song_id, title, album, artist) " + "VALUES (" + "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,"
                + "756716f7-2e54-4715-9f00-91dcbea6cf50," + "'La Petite Tonkinoise'," + "'Bye Bye Blackbird'," + "'Joséphine Baker'" + ");");

        ResultSet results = cassandraExecutor.execute("SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");

        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"), row.getString("album"), row.getString("artist")));

            N.println(CassandraExecutor.toEntity(Map.class, row));
        }

        N.println(CassandraExecutor.extractData(results));

        results = cassandraExecutor.execute("SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
        N.println(CassandraExecutor.extractData(results));

        System.out.println();

        String sql = E.update("simplex.songs").set("title", "album").where("id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d").sql();
        ResultSet resultSet = cassandraExecutor.execute(sql, "new title", "new Album");
        N.println(resultSet);
        N.println(resultSet.one());
        N.println(resultSet.wasApplied());
        assertTrue(resultSet.wasApplied());

        sql = E.deleteFrom("simplex.songs").where("id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d").sql();
        resultSet = cassandraExecutor.execute(sql);
        N.println(resultSet);
        N.println(resultSet.one());
        N.println(resultSet.wasApplied());
        assertTrue(resultSet.wasApplied());

        sql = E.update("simplex.songs").set("title", "album").where("id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d").sql();
        resultSet = cassandraExecutor.execute(sql, "new title", "new Album");
        N.println(resultSet);
        N.println(resultSet.one());
        N.println(resultSet.wasApplied());
        assertTrue(resultSet.wasApplied());

        System.out.println();
    }
}
