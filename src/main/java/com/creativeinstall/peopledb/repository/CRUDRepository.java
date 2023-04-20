package com.creativeinstall.peopledb.repository;

import com.creativeinstall.peopledb.exception.UnableToSaveException;
import com.creativeinstall.peopledb.model.Entity;

import java.sql.*;

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

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException ; // SEE comment to method below

    abstract String getSaveSql();

}
