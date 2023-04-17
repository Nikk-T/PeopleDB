package com.creativeinstall.peopledb.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.ClassBasedNavigableIterableAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class PersonTest {

    @Test
    public void testForEquality(){
        Person p1 = new Person("p1", "Smith", ZonedDateTime.of(2000, 9, 30, 15, 00, 00, 00, ZoneId.of("+3") ));
        Person p2 = new Person("p1", "Smith", ZonedDateTime.of(2000, 9, 30, 15, 00, 00, 00, ZoneId.of("+3") ));
        org.assertj.core.api.Assertions.assertThat(p1).isEqualTo(p2);
    }

    @Test
    public void testForInequality(){
        Person p1 = new Person("p1", "Smith", ZonedDateTime.of(2000, 9, 30, 15, 00, 00, 00, ZoneId.of("+3") ));
        Person p2 = new Person("p2", "Smith", ZonedDateTime.of(2000, 9, 30, 15, 00, 00, 00, ZoneId.of("+3") ));
        org.assertj.core.api.Assertions.assertThat(p1).isNotEqualTo(p2);
    }

}