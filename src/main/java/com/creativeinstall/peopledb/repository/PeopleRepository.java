package com.creativeinstall.peopledb.repository;

import com.creativeinstall.peopledb.exception.UnableToSaveException;
import com.creativeinstall.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

public class PeopleRepository {
    public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?)";
    public static final String FIND_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID=?";
    public static final String COUNT_RECORDS_SQL = "SELECT ID FROM PEOPLE";
    public static final String DELETE_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    public static final String UPDATE_PERSON_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?";
    private Connection connection;
    public PeopleRepository(Connection connection) {  // This pattern called DEPENDANCY Injection - we are opening connection outside of the class and INJECTING in construction
                                                        // And as the connection is being added in constructor - we CAN NOT make a copy of a class WITHOUT the connection
        this.connection = connection;
    }

    public Person save(Person person) throws  UnableToSaveException {

        try {
            PreparedStatement ps = connection.prepareStatement(SAVE_PERSON_SQL, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, person.getFirstName());
            ps.setString(2, person.getLastName());
            ps.setTimestamp(3, convertDobFromZoned(person.getDob())); // SEE comment to method below

            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()){
                long id = rs.getLong(1);
                person.setId(id);
                System.out.println(person);
            }
            System.out.printf("Records affected %d%n", recordsAffected);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: "+person);
        }
        return person;
    }

    public Optional<Person> findByID(Long id) {
        Person person = null;
        try {
            PreparedStatement ps = connection.prepareStatement(FIND_BY_ID_SQL);
            ps.setLong(1, id);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                Long personId = resultSet.getLong("ID");
                String firstName = resultSet.getString("FIRST_NAME");
                String lastName = resultSet.getString("LAST_NAME");
                ZonedDateTime dob = ZonedDateTime.of(resultSet.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0")); // Check the savePerson() method - we allign everyone to zone 0 there
                BigDecimal salary = resultSet.getBigDecimal("SALARY");
                person = new Person(firstName, lastName, dob);
                person.setSalary(salary);
                person.setId(personId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(person); // Rethink optionals!!!
    }

    public long count() {
      Long counter = 0L;
        try {
            PreparedStatement ps = connection.prepareStatement(COUNT_RECORDS_SQL);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                ++counter;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return counter;
    }

    public void delete(Person person) {
        try {
            PreparedStatement ps = connection.prepareStatement(DELETE_BY_ID_SQL);
            ps.setLong(1, person.getId());
            int affectedRecordsCount = ps.executeUpdate();
            System.out.println("Records affected: " + affectedRecordsCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(Person...people) {  // This is varArg (remember - its simple version of an array !! )
                                            // but varArg is actually an array, so we do next:
        String ids = Arrays.stream(people)  // make stream out of the array
                .map(p -> p.getId())        // now we made a stream out of array and converted it into stream of Longs
                .map(String::valueOf)       // now its stream of strings, that represent ID
                .collect(joining(",")); // and now we made a coma delimited string of ID's - like 10, 20, 30, 40 etc.
                                                // and we will use it as an argument for SQL statement
        System.out.println("IDs: " + ids);
        System.out.println("DELETE FROM PEOPLE WHERE ID IN (:ids)".replace(":ids", ids));
        try {
            Statement stat = connection.createStatement();
            int i = stat.executeUpdate("DELETE FROM PEOPLE WHERE ID IN (:ids)".replace(":ids", ids)); // we replace (:ids) with our actual String ids
            System.out.println("Records affected: " + i);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(Person person) {
        try {
            PreparedStatement ps = connection.prepareStatement(UPDATE_PERSON_SQL, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, person.getFirstName());
            ps.setString(2, person.getLastName());
            ps.setTimestamp(3, convertDobFromZoned(person.getDob()));
            ps.setBigDecimal(4, person.getSalary());
            ps.setLong(5, person.getId());

            int recordsAffected = ps.executeUpdate();

            System.out.printf("Records affected %d%n", recordsAffected);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: "+person);
        }
    }

    private Timestamp convertDobFromZoned(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
    // the maniac construction above is happened because in JAVA class Person() we decided to store DOB as a ZonedDateTime,
    // but in out SQL database we store a TimeStamp, so we need to convert our ZonedDateTime to time Zone of (+0) - its GMT
    // and then convert the result to LocalDate, that allows to convert to SQL timestamp
}
