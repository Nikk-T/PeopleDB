package com.creativeinstall.peopledb.repository;

import com.creativeinstall.peopledb.annotation.SQL;
import com.creativeinstall.peopledb.exception.UnableToSaveException;
import com.creativeinstall.peopledb.model.CrudOperation;
import com.creativeinstall.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

// This class is refactored. All Database boiler plate methods are in CRUDRepository Class now
// Here we have only Person specific methods.
// So using CRUDRepository we can create multiple repositories for different objects

public class PeopleRepository extends CRUDRepository<Person> {
    public static final String SAVE_PERSON_SQL = """
                    INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL) 
                    VALUES(?, ?, ?, ?, ?)""";
    public static final String FIND_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID=?";
    public static final String FIND_ALL_SQL = "SELECT * FROM PEOPLE";
    public static final String COUNT_RECORDS_SQL = "SELECT ID FROM PEOPLE";
    public static final String DELETE_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    public static final String DELETE_BY_MULTIPLE_ID_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    public static final String UPDATE_PERSON_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?";

    public PeopleRepository(Connection connection) {  // This pattern called DEPENDANCY Injection - we are opening connection
        super(connection);                              // outside of the class and INJECTING in construction
    }                                                    // we CAN NOT make a copy of a class WITHOUT the connection
//    @Override
//    String getSaveSql() {
//        return SAVE_PERSON_SQL;
//    }

//    @Override
//    String getFindByIdSQL() {
//        return FIND_BY_ID_SQL;
//    }

//    @Override
//    String getFindAllSql() {
//        return FIND_ALL_SQL;
//    }
//    @Override
//    String getCountRecordsSql() {return COUNT_RECORDS_SQL;}
//    @Override
//    String getDeleteByIdSql(){
//        return DELETE_BY_ID_SQL;
//    }
//    @Override
//    String getDeleteByMultipleIdSql(){
//        return DELETE_BY_MULTIPLE_ID_SQL;
//    }
//    @Override
//    String getUpdateByIdSql(){
//        return UPDATE_PERSON_SQL;
//    }

    @Override
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID) // to make string shorter - we just referring to the constant above
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)      //  to the constant above
    @SQL(value = COUNT_RECORDS_SQL, operationType = CrudOperation.COUNT) // Count(), Delete(), DeleteAll() methods
    @SQL(value = DELETE_BY_ID_SQL, operationType = CrudOperation.DELETE_ONE) // does not use the method below, but we
    @SQL(value = DELETE_BY_MULTIPLE_ID_SQL, operationType = CrudOperation.DELETE_MANY) //can put its SQL annotation anywhere in the class
    Person extractEntityFromResultSet(ResultSet resultSet) throws SQLException {
        Long personId = resultSet.getLong("ID");
        String firstName = resultSet.getString("FIRST_NAME");
        String lastName = resultSet.getString("LAST_NAME");
        ZonedDateTime dob = ZonedDateTime.of(resultSet.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0")); // Check the savePerson() method - we allign everyone to zone 0 there
        BigDecimal salary = resultSet.getBigDecimal("SALARY");
        Person person = new Person(firstName, lastName, dob);
        person.setSalary(salary);
        person.setId(personId);
        return person;
    }

    @Override
    @SQL(value = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL) VALUES(?, ?, ?, ?, ?)", operationType = CrudOperation.SAVE)
    void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobFromZoned(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
        ps.setString(5, entity.getEmail());
    }

    @Override
    @SQL(value = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?", operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobFromZoned(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
        ps.setLong(5, entity.getId());
    }


    private Timestamp convertDobFromZoned(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
    // the maniac construction above is happened because in JAVA class Person() we decided to store DOB as a ZonedDateTime,
    // but in out SQL database we store a TimeStamp, so we need to convert our ZonedDateTime to time Zone of (+0) - its GMT
    // and then convert the result to LocalDate, that allows to convert to SQL timestamp

}

