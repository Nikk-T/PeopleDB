package com.creativeinstall.peopledb.repository;

import com.creativeinstall.peopledb.exception.UnableToSaveException;
import com.creativeinstall.peopledb.model.Entity;
import com.creativeinstall.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

abstract class CRUDRepository<T extends Entity> {

    protected Connection connection;

    public CRUDRepository(Connection connection) {  // This pattern called DEPENDANCY Injection - we are opening connection
                                                    // outside of the class and INJECTING in construction
        this.connection = connection;               // And as the connection is being added in constructor -
    }                                               // we CAN NOT make a copy of a class WITHOUT the connection


    public T save(T entity) throws UnableToSaveException {

        try {
            PreparedStatement ps = connection.prepareStatement(getSaveSql(), Statement.RETURN_GENERATED_KEYS);
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

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException ; // SEE comment to method below

    abstract String getSaveSql();

    /**
     *
     * @return returns a String that represents SQL needed to retrieve an entity by ID,
     * the SQL must contain one SQL parameter i.e. "?" that will bind to the
     * entities's ID
     */
    abstract String getFindByIdSQL();
    abstract String getFindAllSql();

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

}
