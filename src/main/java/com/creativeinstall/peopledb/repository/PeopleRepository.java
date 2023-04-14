package com.creativeinstall.peopledb.repository;

import com.creativeinstall.peopledb.model.Person;

import java.sql.*;
import java.time.ZoneId;

public class PeopleRepository {
    public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?)";
    private Connection connection;
    public PeopleRepository(Connection connection) {  // This pattern called DEPENDANCY Injection - we are opening connection outside of the class and INJECTING in construction
                                                        // And as the connection is being added in constructor - we CAN NOT make a copy of a class WITHOUT the connection
        this.connection = connection;
    }

    public Person save(Person person) {

        try {
            PreparedStatement ps = connection.prepareStatement(SAVE_PERSON_SQL, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, person.getFirstName());
            ps.setString(2, person.getLastName());
            ps.setTimestamp(3, Timestamp.valueOf(person.getDob().withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime())); // SEE comment below
            // the maniac construction above is happened because in JAVA class Person() we decided to store DOB as a ZonedDateTime,
            // but in out SQL database we store a TimeStamp, so we need to convert our ZonedDateTime to time Zone of (+0) - its GMT
            // and then convert the result to LocalDate, that allows to convert to SQL timestamp

            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()){
                long id = rs.getLong(1);
                person.setId(id);
                System.out.println(person);
            }
            System.out.printf("Records affected %d%n", recordsAffected);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return person;
    }
}
