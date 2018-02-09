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
package com.google.code.or.binlog;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.common.glossary.column.StringColumn;

/**
 * 
 * @author Zhang Zhuo
 *
 */
public class BinlogParseRecord {
	private static final int MAX_FILE_NAME_LENGTH = 271;
	private static MappedByteBuffer tableMapBuffer;
	private static MappedByteBuffer positionBuffer;
	static {
		try {
			tableMapBuffer = FileChannel.open(
					Paths.get(System.getProperty("user.dir"), "record", "tablemap.rec"), 
					StandardOpenOption.CREATE, 
					StandardOpenOption.WRITE, 
					StandardOpenOption.READ).
					map(MapMode.READ_WRITE, 0, MAX_FILE_NAME_LENGTH);
			positionBuffer = FileChannel.open(
					Paths.get(System.getProperty("user.dir"), "record", "position.rec"), 
					StandardOpenOption.CREATE, 
					StandardOpenOption.WRITE, 
					StandardOpenOption.READ).
					map(MapMode.READ_WRITE, 0, MAX_FILE_NAME_LENGTH);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	
	/**
	 * get byte arrary starting from index
	 * @param bs This method transfers bytes from a buffer 
	 * into the given destination array. An invocation of this
	 * method of the form src.get(a) behaves in exactly the 
	 * same way as the invocation
	 * @param index Sets the buffer's position. If the mark is 
	 * defined and larger than the new position then it is 
	 * discarded. 
	 */
	public static void positionRead(byte[] bs, int index) {
		positionBuffer.position(index);
		positionBuffer.get(bs);
	}
	
	public static byte positionReadByte() {
		return positionBuffer.get();
	}
	
	public static byte positionReadByte(int index) {
		return positionBuffer.get(index);
	}
	
	/**
	 * Absolute get method. Reads the byte at the given index. 
	 * <p>Reads four bytes at the given index, composing them 
	 * into a int value according to the current byte order. </p>
	 * @param index The index from which the bytes will be read
	 * @return The int value at the given index
	 */
	public static int positionReadInt(int index) {
		return positionBuffer.getInt(index);
	}
	
	/**
	 * Relative get method for reading an int value. 
	 * <p>Reads the next four bytes at this buffer's 
	 * current position, composing them into an int 
	 * value according to the current byte order, 
	 * and then increments the position by four. </p>
	 * @return
	 */
	public static int positionReadInt() {
		return positionBuffer.getInt();
	}
	
	public static long positionReadLong() {
		return positionBuffer.getLong();
	}
	
	public static long positionReadLong(int index) {
		return positionBuffer.getLong(index);
	}
	
	public static void writeHeader(BinlogEventV4Header header, AtomicBoolean sign, AtomicBoolean skip) {
		long nextPosition = header.getNextPosition();
		long position = header.getPosition();
		String fileName = header.getBinlogFileName();
		try {
			byte[] bytes = fileName.getBytes("utf-8");
			positionBuffer.position(0);
			positionBuffer.put((byte) -1);
			positionBuffer.put((byte) (sign.get() ? -1 : 0));
			positionBuffer.put((byte) (skip.get() ? -1 : 0));
			positionBuffer.putLong(position);
			positionBuffer.putLong(nextPosition);
			positionBuffer.putInt(bytes.length);
			positionBuffer.put(bytes);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	public static void writeHeader(BinlogEventV4Header header, AtomicBoolean sign) {
		long nextPosition = header.getNextPosition();
		long position = header.getPosition();
		String fileName = header.getBinlogFileName();
		try {
			byte[] bytes = fileName.getBytes("utf-8");
			positionBuffer.position(0);
			positionBuffer.put((byte) -1);
			positionBuffer.put((byte) (sign.get() ? -1 : 0));
			positionBuffer.put((byte) (-1));
			positionBuffer.putLong(position);
			positionBuffer.putLong(nextPosition);
			positionBuffer.putInt(bytes.length);
			positionBuffer.put(bytes);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	public static void tableMapWrite(TableMapEvent event) {
		if (event == null) {
			return;
		}
		tableMapBuffer.position(0);
		tableMapBuffer.put((byte) 0xff);
		tableMapBuffer.putLong(event.getTableId());
		
		byte[] database = event.getDatabaseName().getValue();
		int databaseLength = database.length;
		tableMapBuffer.putInt(databaseLength);
		
		byte[] table = event.getTableName().getValue();
		int tableLength = table.length;
		tableMapBuffer.putInt(tableLength);
		
		byte[] columnTypes = event.getColumnTypes();
		int typesLength = columnTypes.length;
		tableMapBuffer.putInt(typesLength);

		tableMapBuffer.put(database);
		tableMapBuffer.put(table);
		tableMapBuffer.put(columnTypes);
	}

	public static TableMapEvent readTableMapEvent() {
		tableMapBuffer.position(0);
		byte b = tableMapBuffer.get();
		if (b != -1) {
			return null;
		}
		TableMapEvent event = new TableMapEvent();
		event.setTableId(tableMapBuffer.getLong());
		
		int databaseLength = tableMapBuffer.getInt();
		int tableLength = tableMapBuffer.getInt();
		int typesLength = tableMapBuffer.getInt();
		
		byte[] bs = new byte[databaseLength];
		tableMapBuffer.get(bs);
		event.setDatabaseName(StringColumn.valueOf(bs));
		
		bs = new byte[tableLength];
		tableMapBuffer.get(bs);
		event.setTableName(StringColumn.valueOf(bs));
		
		bs = new byte[typesLength];
		tableMapBuffer.get(bs);
		event.setColumnTypes(bs);
		return event;
	}

}
