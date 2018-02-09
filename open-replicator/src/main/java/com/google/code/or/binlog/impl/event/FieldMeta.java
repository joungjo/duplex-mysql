package com.google.code.or.binlog.impl.event;

import java.io.Serializable;

@SuppressWarnings("serial")
public class FieldMeta implements Serializable {

    private String columnName;
    private String columnType;
    private String isNullable;
    private String iskey;
    private String defaultValue;
    private String extra;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getIsNullable() {
        return isNullable;
    }

    public void setIsNullable(String isNullable) {
        this.isNullable = isNullable;
    }

    public String getIskey() {
        return iskey;
    }

    public void setIskey(String iskey) {
        this.iskey = iskey;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public boolean isUnsigned() {
    	return columnType.toLowerCase().contains("unsigned");
    }

    public boolean isKey() {
    	return iskey.toUpperCase().equals("PRI");
    }

    public boolean isNullable() {
    	return isNullable.toUpperCase().equals("YES");
    }

    public String toString() {
        return "FieldMeta [columnName=" + columnName + ", columnType=" + columnType + ", defaultValue="
               + defaultValue + ", extra=" + extra + ", isNullable=" + isNullable + ", iskey=" + iskey + "]";
    }

}