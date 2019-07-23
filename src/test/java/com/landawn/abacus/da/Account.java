/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da;

import java.sql.Timestamp;
import java.util.List;

import javax.xml.bind.annotation.XmlTransient;

import com.landawn.abacus.core.AbstractDirtyMarker;
import com.landawn.abacus.da.entity.AccountContact;
import com.landawn.abacus.da.entity.AccountDevice;
import com.landawn.abacus.da.entity.ExtendDirtyBasicPNL;
import com.landawn.abacus.util.N;

/**
 * Generated by Abacus. DO NOT edit it!
 * 
 * @since ${version}
 * 
 * @author Haiyang Li
 */
public class Account extends AbstractDirtyMarker implements ExtendDirtyBasicPNL.AccountPNL {
    private static final long serialVersionUID = 1003890905612L;

    private String id;
    private String gui;
    private String emailAddress;
    private String firstName;
    private String middleName;
    private String lastName;
    private Timestamp birthDate;
    private int status;
    private Timestamp lastUpdateTime;
    private Timestamp createTime;
    private AccountContact contact;
    private List<AccountDevice> devices;

    public Account() {
        super(__);
    }

    public Account(String id) {
        this();

        setId(id);
    }

    public Account(String gui, String emailAddress, String firstName, String middleName, String lastName, Timestamp birthDate, int status,
            Timestamp lastUpdateTime, Timestamp createTime, AccountContact contact, List<AccountDevice> devices) {
        this();

        setGUI(gui);
        setEmailAddress(emailAddress);
        setFirstName(firstName);
        setMiddleName(middleName);
        setLastName(lastName);
        setBirthDate(birthDate);
        setStatus(status);
        setLastUpdateTime(lastUpdateTime);
        setCreateTime(createTime);
        setContact(contact);
        setDevices(devices);
    }

    public Account(String id, String gui, String emailAddress, String firstName, String middleName, String lastName, Timestamp birthDate, int status,
            Timestamp lastUpdateTime, Timestamp createTime, AccountContact contact, List<AccountDevice> devices) {
        this();

        setId(id);
        setGUI(gui);
        setEmailAddress(emailAddress);
        setFirstName(firstName);
        setMiddleName(middleName);
        setLastName(lastName);
        setBirthDate(birthDate);
        setStatus(status);
        setLastUpdateTime(lastUpdateTime);
        setCreateTime(createTime);
        setContact(contact);
        setDevices(devices);
    }

    @XmlTransient
    @Override
    public boolean isDirty() {
        return super.isDirty() || (contact == null ? false : contact.isDirty()) || (devices == null ? false : super.isEntityDirty(devices));
    }

    @Override
    public void markDirty(boolean isDirty) {
        super.markDirty(isDirty);

        if (contact != null) {
            contact.markDirty(isDirty);
        }

        if (devices != null) {
            super.markEntityDirty(devices, isDirty);
        }
    }

    public String getId() {
        return id;
    }

    public Account setId(String id) {
        super.setUpdatedPropName(ID);
        this.id = id;

        return this;
    }

    public String getGUI() {
        return gui;
    }

    public Account setGUI(String gui) {
        super.setUpdatedPropName(GUI);
        this.gui = gui;

        return this;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public Account setEmailAddress(String emailAddress) {
        super.setUpdatedPropName(EMAIL_ADDRESS);
        this.emailAddress = emailAddress;

        return this;
    }

    public String getFirstName() {
        return firstName;
    }

    public Account setFirstName(String firstName) {
        super.setUpdatedPropName(FIRST_NAME);
        this.firstName = firstName;

        return this;
    }

    public String getMiddleName() {
        return middleName;
    }

    public Account setMiddleName(String middleName) {
        super.setUpdatedPropName(MIDDLE_NAME);
        this.middleName = middleName;

        return this;
    }

    public String getLastName() {
        return lastName;
    }

    public Account setLastName(String lastName) {
        super.setUpdatedPropName(LAST_NAME);
        this.lastName = lastName;

        return this;
    }

    public Timestamp getBirthDate() {
        return birthDate;
    }

    public Account setBirthDate(Timestamp birthDate) {
        super.setUpdatedPropName(BIRTH_DATE);
        this.birthDate = birthDate;

        return this;
    }

    public int getStatus() {
        return status;
    }

    public Account setStatus(int status) {
        super.setUpdatedPropName(STATUS);
        this.status = status;

        return this;
    }

    public Timestamp getLastUpdateTime() {
        return lastUpdateTime;
    }

    public Account setLastUpdateTime(Timestamp lastUpdateTime) {
        super.setUpdatedPropName(LAST_UPDATE_TIME);
        this.lastUpdateTime = lastUpdateTime;

        return this;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public Account setCreateTime(Timestamp createTime) {
        super.setUpdatedPropName(CREATE_TIME);
        this.createTime = createTime;

        return this;
    }

    public AccountContact getContact() {
        return contact;
    }

    public Account setContact(AccountContact contact) {
        super.setUpdatedPropName(CONTACT);
        this.contact = contact;

        return this;
    }

    public List<AccountDevice> getDevices() {
        return devices;
    }

    public Account setDevices(List<AccountDevice> devices) {
        super.setUpdatedPropName(DEVICES);
        this.devices = devices;

        return this;
    }

    public Account copy() {
        Account copy = new Account();

        copy.id = this.id;
        copy.gui = this.gui;
        copy.emailAddress = this.emailAddress;
        copy.firstName = this.firstName;
        copy.middleName = this.middleName;
        copy.lastName = this.lastName;
        copy.birthDate = this.birthDate;
        copy.status = this.status;
        copy.lastUpdateTime = this.lastUpdateTime;
        copy.createTime = this.createTime;
        copy.contact = this.contact;
        copy.devices = this.devices;

        return copy;
    }

    @Override
    public int hashCode() {
        int h = 17;
        h = 31 * h + N.hashCode(id);
        h = 31 * h + N.hashCode(gui);
        h = 31 * h + N.hashCode(emailAddress);
        h = 31 * h + N.hashCode(firstName);
        h = 31 * h + N.hashCode(middleName);
        h = 31 * h + N.hashCode(lastName);
        h = 31 * h + N.hashCode(birthDate);
        h = 31 * h + N.hashCode(status);
        h = 31 * h + N.hashCode(lastUpdateTime);
        h = 31 * h + N.hashCode(createTime);
        h = 31 * h + N.hashCode(contact);
        h = 31 * h + N.hashCode(devices);

        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof Account) {
            Account other = (Account) obj;

            if (N.equals(id, other.id) && N.equals(gui, other.gui) && N.equals(emailAddress, other.emailAddress) && N.equals(firstName, other.firstName)
                    && N.equals(middleName, other.middleName) && N.equals(lastName, other.lastName) && N.equals(birthDate, other.birthDate)
                    && N.equals(status, other.status) && N.equals(lastUpdateTime, other.lastUpdateTime) && N.equals(createTime, other.createTime)
                    && N.equals(contact, other.contact) && N.equals(devices, other.devices)) {

                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return "{" + "id=" + N.toString(id) + ", " + "gui=" + N.toString(gui) + ", " + "emailAddress=" + N.toString(emailAddress) + ", " + "firstName="
                + N.toString(firstName) + ", " + "middleName=" + N.toString(middleName) + ", " + "lastName=" + N.toString(lastName) + ", " + "birthDate="
                + N.toString(birthDate) + ", " + "status=" + N.toString(status) + ", " + "lastUpdateTime=" + N.toString(lastUpdateTime) + ", " + "createTime="
                + N.toString(createTime) + ", " + "contact=" + N.toString(contact) + ", " + "devices=" + N.toString(devices) + "}";
    }
}
