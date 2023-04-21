package com.creativeinstall.peopledb.repository;

import com.creativeinstall.peopledb.annotation.SQL;
import com.creativeinstall.peopledb.exception.UnableToSaveException;
import com.creativeinstall.peopledb.model.Entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

abstract class CRUDRepository<T extends Entity> {

    protected Connection connection;

    public CRUDRepository(Connection connection) {  // This pattern called DEPENDANCY Injection - we are opening connection
                                                    // outside of the class and INJECTING in construction
        this.connection = connection;               // And as the connection is being added in constructor -
    }                                               // we CAN NOT make a copy of a class WITHOUT the connection

    private String getSqlByAnnotation(String methodName, Supplier<String> sqlGetter){
        return Arrays.stream(this.getClass().getDeclaredMethods())    // This mad construction scans the metods of this class
                .filter(m -> methodName.contentEquals(m.getName()))  // makes stream of names, looking for name that was passed in by a parameter in the stream
                .map(m -> m.getAnnotation(SQL.class))  //Now it looks for the annotation of the method mapForSave
                .map(SQL::value)   //and saves it into string
                .findFirst().orElseGet(sqlGetter); // if no annotation found - use the method that was passed as a second parameter
    }
    public T save(T entity) throws UnableToSaveException {

        try {
            PreparedStatement ps = connection.prepareStatement(
                    getSqlByAnnotation("mapForSave", this::getSaveSql), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);

            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()){
                long id = rs.getLong(1);
                entity.setId(id);
                System.out.println(entity);
            }
            System.out.printf("Records affected %d%n", recordsAffected);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: "+ entity);
        }
        return entity;
    }

    public Optional<T> findByID(Long id) {
        T entity = null;
        try {
            PreparedStatement ps = connection.prepareStatement(getFindByIdSQL());
            ps.setLong(1, id);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                entity = extractEntityFromResultSet(resultSet);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(entity); // Rethink optionals!!!
    }

    public List<T> findAll(){
        List<T> entities = new ArrayList<>();
        try {PreparedStatement ps = connection.prepareStatement(getFindAllSql());
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                entities.add(extractEntityFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return entities;
    }

    public long count() {
        Long counter = 0L;
        try {
            PreparedStatement ps = connection.prepareStatement(getCountRecordsSql());
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                ++counter;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return counter;
    }

    public void delete(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getDeleteByIdSql());
            ps.setLong(1, entity.getId());
            int affectedRecordsCount = ps.executeUpdate();
            System.out.println("Records affected: " + affectedRecordsCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(T...entities) {  // This is varArg (remember - its simple version of an array !! )
        String ids = Arrays.stream(entities)  // make stream out of the array
                .map(p -> p.getId())        // now we made a stream out of array and converted it into stream of Longs
                .map(String::valueOf)       // now its stream of strings, that represent ID
                .collect(joining(",")); // and now we made a coma delimited string of ID's - like 10, 20, 30, 40 etc.
        // and we will use it as an argument for SQL statement
        try {
            Statement stat = connection.createStatement();
            int i = stat.executeUpdate(getDeleteByMultipleIdSql().replace(":ids", ids)); // we replace (:ids) with our actual String ids
            System.out.println("Records affected: " + i);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getUpdateByIdSql(), Statement.RETURN_GENERATED_KEYS);
            MapForUpdate(entity, ps);

            int recordsAffected = ps.executeUpdate();

            System.out.printf("Records affected %d%n", recordsAffected);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person:" + entity);
        }
    }

    abstract void MapForUpdate(T entity, PreparedStatement ps) throws SQLException;

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException ; // SEE comment to method below

    protected String getSaveSql() { // it can not be abstract anymore - as its used if nothing found in annotations
        return "";
    };

    /**
     *
     * @return returns a String that represents SQL needed to retrieve an entity by ID,
     * the SQL must contain one SQL parameter i.e. "?" that will bind to the
     * entities's ID
     */
    abstract String getFindByIdSQL();
    abstract String getFindAllSql();
    abstract String getCountRecordsSql();
    /**
     *
     * @return returns a String that represents SQL needed to delete an entity by ID,
     * the SQL must contain one SQL parameter i.e. "?" that will bind to the
     * entities's ID
     */
    abstract String getDeleteByIdSql();
    abstract String getDeleteByMultipleIdSql();
    abstract String getUpdateByIdSql();

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

}
