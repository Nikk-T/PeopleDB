package com.creativeinstall.peopledb.repository;

import com.creativeinstall.peopledb.annotation.Id;
import com.creativeinstall.peopledb.annotation.MultiSQL;
import com.creativeinstall.peopledb.annotation.SQL;
import com.creativeinstall.peopledb.exception.UnableToSaveException;
import com.creativeinstall.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

abstract class CRUDRepository<T> {

    protected Connection connection;

    public CRUDRepository(Connection connection) {  // This pattern called DEPENDANCY Injection - we are opening connection
                                                    // outside of the class and INJECTING in construction
        this.connection = connection;               // And as the connection is being added in constructor -
    }                                               // we CAN NOT make a copy of a class WITHOUT the connection

    private String getSqlByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter){
        // This construction scans the methods of this class, makes stream of names
        // and looking for multiple SQL annotations, maps them and making stream of SQL annotations
        Stream<SQL> multiSqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(MultiSQL.class))
                .map(m -> m.getAnnotation(MultiSQL.class))
                .flatMap(msql -> Arrays.stream(msql.value()));

        // This construction scans the methods of this class, makes stream of names
        // and looking for single SQL annotations, maps them and making stream of SQL annotations
        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(SQL.class))
                .map(m -> m.getAnnotation(SQL.class));
        // Now we concat both streams, and looking if the annotation we found is equal
        // to the one passed in - then go saves it into string and returns
        return Stream.concat(multiSqlStream, sqlStream)
                .filter(a -> a.operationType().equals(operationType))
                .map(SQL::value)   //and
                .findFirst().orElseGet(sqlGetter); // if no annotation found - use the method that was passed as a second parameter
    }

    private Long findIdByAnnotation(T entity) {
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .map(f -> {
                    f.setAccessible(true);
                    Long id = null;
                    try {
                        id = (long)f.get(entity); // (long) - we casting to long what ever we found annotated by id
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    return id;
                })
                .findFirst().orElseThrow(() -> new RuntimeException("No ID annotation field found"));
    }

    private void setIdByAnnotation(Long id, T entity) {
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .forEach(f -> {
                    f.setAccessible(true);
                    try {
                        f.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Unable to set ID value");
                    }
                });
    }
    public T save(T entity) throws UnableToSaveException {

        try {
            PreparedStatement ps = connection.prepareStatement(
                    getSqlByAnnotation(CrudOperation.SAVE, this::getSaveSql), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);

            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()){
                long id = rs.getLong(1);
                setIdByAnnotation(id, entity);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: "+ entity);
        }
        return entity;
    }

    public Optional<T> findByID(Long id) {
        T entity = null;
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSQL));
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
        try {PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSql));
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
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.COUNT, this::getCountRecordsSql));
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
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE_ONE, this::getDeleteByIdSql));
            ps.setLong(1, findIdByAnnotation(entity));
            int affectedRecordsCount = ps.executeUpdate();
            System.out.println("Records affected: " + affectedRecordsCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    public void delete(T...entities) {  // This is varArg (remember - its simple version of an array !! )
        String ids = Arrays.stream(entities)  // make stream out of the array
                .map(p -> findIdByAnnotation(p))        // now we made a stream out of array and converted it into stream of Longs
                .map(String::valueOf)       // now its stream of strings, that represent ID
                .collect(joining(",")); // and now we made a coma delimited string of ID's - like 10, 20, 30, 40 etc.
        // and we will use it as an argument for SQL statement
        try {
            Statement stat = connection.createStatement();
            int i = stat.executeUpdate(getSqlByAnnotation(CrudOperation.DELETE_MANY, this::getDeleteByMultipleIdSql).replace(":ids", ids)); // we replace (:ids) with our actual String ids
            System.out.println("Records affected: " + i);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.UPDATE, this::getUpdateByIdSql), Statement.RETURN_GENERATED_KEYS);
            mapForUpdate(entity, ps);
            ps.setLong(5, findIdByAnnotation(entity));
            int recordsAffected = ps.executeUpdate();

            System.out.printf("Records affected %d%n", recordsAffected);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person:" + entity);
        }
    }

    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException ; // SEE comment to method below

    protected String getSaveSql() { // it can not be abstract anymore - as its used if nothing found in annotations
        throw new RuntimeException("SQL not defined..");
    };

    /**
     *
     * @return returns a String that represents SQL needed to retrieve an entity by ID,
     * the SQL must contain one SQL parameter i.e. "?" that will bind to the
     * entities's ID
     */
    protected String getFindByIdSQL(){ // it can not be abstract anymore - as its used if nothing found in annotations
        throw new RuntimeException("SQL not defined..");
    };
    protected String getFindAllSql(){ // it can not be abstract anymore - as its used if nothing found in annotations
        throw new RuntimeException("SQL not defined..");
    };
    protected String getCountRecordsSql(){ // it can not be abstract anymore - as its used if nothing found in annotations
        throw new RuntimeException("SQL not defined..");
    };
    /**
     *
     * @return returns a String that represents SQL needed to delete an entity by ID,
     * the SQL must contain one SQL parameter i.e. "?" that will bind to the
     * entities's ID
     */
    protected String getDeleteByIdSql(){ // it can not be abstract anymore - as its used if nothing found in annotations
        throw new RuntimeException("SQL not defined..");
    };
    protected String getDeleteByMultipleIdSql(){ // it can not be abstract anymore - as its used if nothing found in annotations
        throw new RuntimeException("SQL not defined..");
    };
    protected String getUpdateByIdSql(){
         throw new RuntimeException("SQL not defined..");
    }

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

}
