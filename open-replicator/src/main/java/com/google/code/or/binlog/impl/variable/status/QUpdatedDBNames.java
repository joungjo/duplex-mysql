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
package com.google.code.or.binlog.impl.variable.status;

import java.io.IOException;

import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.common.util.MySQLConstants;
import com.google.code.or.common.util.ToStringBuilder;
import com.google.code.or.io.XInputStream;

/**
 * 
 * @author Jingqi Xu
 */
@SuppressWarnings("serial")
public class QUpdatedDBNames extends AbstractStatusVariable {
	//
	public static final int TYPE = MySQLConstants.Q_UPDATED_DB_NAMES;
	
	//
	private final int accessedDbCount;
	private final StringColumn[] accessedDbs;

	/**
	 * 
	 */
	public QUpdatedDBNames(int accessedDbCount, StringColumn[] accessedDbs) {
		super(TYPE);
		this.accessedDbCount = accessedDbCount;
		this.accessedDbs = accessedDbs;
	}

	/**
	 * 
	 */
	@Override
	public String toString() {
		return new ToStringBuilder(this)
		.append("accessedDbCount", accessedDbCount)
		.append("accessedDbs", accessedDbs).toString();
	}
	
	/**
	 * 
	 */
	public int getAccessedDbCount() {
		return accessedDbCount;
	}

	public StringColumn[] getAccessedDbs() {
		return accessedDbs;
	}
	
	/**
	 * 
	 */
	public static QUpdatedDBNames valueOf(XInputStream tis) throws IOException {
		int accessedDbCount= tis.readInt(1);
		StringColumn accessedDbs[] = null;
		if(accessedDbCount > MySQLConstants.MAX_DBS_IN_EVENT_MTS) {
			accessedDbCount = MySQLConstants.OVER_MAX_DBS_IN_EVENT_MTS;
		} else {
			accessedDbs = new StringColumn[accessedDbCount];
			for(int i = 0; i < accessedDbCount; i++) {
				accessedDbs[i] = tis.readNullTerminatedString();
			}
		}
		return new QUpdatedDBNames(accessedDbCount, accessedDbs);
	}
}
