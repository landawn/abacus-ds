/*
 * Copyright (c) 2020, Haiyang Li.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.landawn.abacus.da.hbase.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * 
 * By default, the field name in the class is mapped to {@code Column Family} in HBase table and {@code Column} name will be empty String {@code ""} if there is no annotation {@code Column/ColumnFamily} added to the class/field and the field type is not an entity with getter/setter methods.
 * <br />
 * For example:
 * <pre> 
    public static class Account {
        {@literal @}Id
        private String id; // columnFamily/Column in HBase will be: "id:"
        private String gui; // columnFamily/Column in HBase will be: "gui:"
        private Name name;  // columnFamily/Column in HBase will be: "name:firstName" and "name:lastName" 
        private String emailAddress; // columnFamily/Column in HBase will be: "emailAddress:"
    }

    public static class Name {
        private String firstName; // columnFamily/Column in HBase will be: "name:firstName" 
        private String lastName; // columnFamily/Column in HBase will be: "name:lastName" 
    }
 * </pre>
 * 
 * But if the class is annotated by {@literal @}ColumnFamily, the field name in the class will be mapped to {@code Column} in HBase table.
 * 
 * <pre> 
    {@literal @}ColumnFamily("columnFamily2B");
    public static class Account {
        {@literal @}Id
        private String id; // columnFamily/Column in HBase will be: "columnFamily2B:id"
        {@literal @}Column("guid") 
        private String gui; // columnFamily/Column in HBase will be: "columnFamily2B:guid"
        {@literal @}ColumnFamily("fullName")
        private Name name;  // columnFamily/Column in HBase will be: "fullName:givenName" and "fullName:lastName"
        {@literal @}ColumnFamily("email")
        private String emailAddress; // columnFamily/Column in HBase will be: "email:emailAddress"
    }
    
    public static class Name {
        {@literal @}Column("givenName")
        private String firstName;
        private String lastName;
    }
 * </pre>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.TYPE })
public @interface ColumnFamily {
    String value() default "";
}
