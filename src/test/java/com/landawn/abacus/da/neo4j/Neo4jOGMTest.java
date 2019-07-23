package com.landawn.abacus.da.neo4j;

import java.util.Collection;
import java.util.Map;

import org.junit.Test;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;

import com.landawn.abacus.da.neo4j.model.Actor;
import com.landawn.abacus.da.neo4j.model.Address;
import com.landawn.abacus.da.neo4j.model.Customer;
import com.landawn.abacus.da.neo4j.model.Movie;
import com.landawn.abacus.da.neo4j.model.Order;
import com.landawn.abacus.da.neo4j.model.OrderItem;
import com.landawn.abacus.da.neo4j.model.Product;
import com.landawn.abacus.da.neo4j.model.Vendor;
import com.landawn.abacus.util.DateUtil;
import com.landawn.abacus.util.N;

// http://neo4j.com/docs/ogm/java/stable/
public class Neo4jOGMTest {
    private static final SessionFactory sessionFactory = new SessionFactory("com.landawn.abacus.util.neo4j.model");

    @Test
    public void test_movie() throws Exception {
        Session session = sessionFactory.openSession();

        Movie movie = new Movie("The Matrix", 1999);

        Actor keanu = new Actor("Keanu Reeves");
        keanu.actsIn(movie);

        Actor carrie = new Actor("Carrie-Ann Moss");
        carrie.actsIn(movie);

        //Persist the movie. This persists the actors as well.
        session.save(movie);

        Collection<Movie> movies = session.loadAll(Movie.class);
        N.println(movies);

        //Load a movie
        Movie matrix = session.load(Movie.class, movie.getId());

        for (Actor actor : matrix.getActors()) {
            System.out.println("Actor: " + actor.getName());
        }

        Result rs = session.query("MATCH (cloudAtlas {title: \"Cloud Atlas\"})<-[:DIRECTED]-(directors) RETURN directors.name", N.asProps());

        for (Map<String, Object> row : rs) {
            System.out.println(row);
        }
    }

    @Test
    public void test_order() throws Exception {
        final Session session = sessionFactory.openSession();

        final Address address = new Address().setStreet("1130 Kifer").setCity("Sunnyvale").setState("CA").setCountry("USA").setZipCode(94085);
        final Address address1 = new Address().setStreet("1130 Kifer").setCity("Sunnyvale").setState("CA").setCountry("USA").setZipCode(94085);

        Vendor vendor = new Vendor().setName("TeleNav").setAddress(address);
        Product product = new Product().setName("TN GPS").setCategory("software/services").setPrice(13).setVendor(vendor);

        OrderItem orderItem = new OrderItem().setProduct(product).setQuantity(1).setAmount(26);
        OrderItem orderItem1 = new OrderItem().setProduct(product).setQuantity(2).setAmount(13);
        Order order = new Order().setOrderItemList(N.asList(orderItem, orderItem1)).setAmount(39).setDate(DateUtil.currentJUDate()).setShippingAddress(address1);

        Customer customer = new Customer().setFirstName(N.uuid()).setLastName("xyz").setAddress(address1).setOrderList(N.asList(order));

        session.save(vendor);
        session.save(customer);

        vendor = session.load(Vendor.class, vendor.getId());
        N.println(vendor);

        customer = session.load(Customer.class, customer.getId());
        N.println(customer);

        Result rs = session.query(
                "MATCH (customer:Customer)-[:OWNS]->(order:Order)-[:CONTAINS]->(orderItem:OrderItem)-[:OF]->(product:Product) WHERE product.name='TN GPS' RETURN customer ORDER BY customer.name ASC LIMIT 10;",
                N.asProps());

        for (Map<String, Object> row : rs) {
            System.out.println(row);
        }

        session.deleteAll(Customer.class);
        session.deleteAll(Vendor.class);

        //        Collection<Vendor> vendors = session.loadAll(Vendor.class);
        //        N.println(vendors);
        //
        //        Collection<Customer> customers = session.loadAll(Customer.class);
        //        N.println(customers);
    }
}
