/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.code.or.binlog.impl.event;

import java.util.List;

import com.google.code.or.binlog.BinlogParseRecord;
import com.google.code.or.binlog.EventContext;

/**
 * 
 * @author Jingqi Xu
 */
@SuppressWarnings("serial")
public abstract class AbstractRowEvent extends AbstractBinlogEventV4 {
	//
	protected long tableId;
	protected int reserved;
	protected String databaseName;
	protected String tableName;
	protected List<FieldMeta> fieldMetas;
	protected byte[] types;
	
	@Override
	public void callback(EventContext context) {
		TableMapEvent mapEvent = (TableMapEvent)context.get();
		if (mapEvent == null) {
			mapEvent = BinlogParseRecord.readTableMapEvent();
			context.set(mapEvent);
		}
		this.databaseName = mapEvent.getDatabaseName().toString();
		this.tableName = mapEvent.getTableName().toString();
		this.types = mapEvent.getColumnTypes();
		this.fieldMetas = mapEvent.getFieldMetas();
		context.doSomething(this);
	}
	
	/**
	 * 
	 */
	public long getTableId() {
		return tableId;
	}

	public void setTableId(long tableId) {
		this.tableId = tableId;
	}

	public int getReserved() {
		return reserved;
	}

	public void setReserved(int reserved) {
		this.reserved = reserved;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<FieldMeta> getFieldMetas() {
		return fieldMetas;
	}

	public void setFieldMetas(List<FieldMeta> fieldMetas) {
		this.fieldMetas = fieldMetas;
	}

	public byte[] getTypes() {
		return types;
	}

	public void setTypes(byte[] types) {
		this.types = types;
	}
	
}
