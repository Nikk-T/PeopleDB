package com.creativeinstall.peopledb.repository;

import com.creativeinstall.peopledb.model.Person;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ClassBasedNavigableIterableAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import static org.assertj.core.api.FactoryBasedNavigableListAssert.assertThat;

public class PeopleRepositoryTests {

    private Connection connection;
    private PeopleRepository repo;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:/Users/nikolaytaganov/Documents/Study/DBeaver_Database/:Users:nikolaytaganov:Documents:Study:DBeaver_Database:peopledb");
        connection.setAutoCommit(false); // This line changes the JDBC default value of TRUE for the TEST connections, so after the connection
                                        // is closed - the test lines will NOT be recoreded into actual database. However - for test everything will look as the record was made
                                            // not to be used in production code of course
        repo = new PeopleRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson() throws SQLException {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,00,00, ZoneId.of("+2")));
        Person savedPerson = repo.save(john);
        Assertions.assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,00,00, ZoneId.of("+2")));
        Person bobby = new Person("Bart", "Maniac", ZonedDateTime.of(1982,11,15,15,15,00,00, ZoneId.of("+2")));
        Person savedPerson1 = repo.save(john);
        Person savedPerson2 = repo.save(bobby);
        Assertions.assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canFindPersonById() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,00,00, ZoneId.of("+2")));
        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findByID(savedPerson.getId()).get();
        Assertions.assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void testPersonNotFound() {
        Optional<Person> foundPerson = repo.findByID(-1L);
        Assertions.assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canGetCount() {
        long startCount = repo.count();
        repo.save(new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,00,00, ZoneId.of("+2"))));
        repo.save(new Person("Bart", "Maniac", ZonedDateTime.of(1982,11,15,15,15,00,00, ZoneId.of("+2"))));
        long endCount = repo.count();
        Assertions.assertThat(endCount).isEqualTo(startCount + 2);
        System.out.println("Total amount of records is: " + endCount);
    }

    @Test
    public void canDelete() {
        Person savedPerson = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,00,00, ZoneId.of("+2"))));
        long startCount = repo.count();
        repo.delete(savedPerson);
        long endCount = repo.count();
        Assertions.assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeleteMultiplePeople() {
        Person p1 = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,00,00, ZoneId.of("+2"))));
        Person p2 = repo.save(new Person("Bart", "Maniac", ZonedDateTime.of(1982,11,15,15,15,00,00, ZoneId.of("+2"))));
        repo.delete(p1, p2);
    }


}
