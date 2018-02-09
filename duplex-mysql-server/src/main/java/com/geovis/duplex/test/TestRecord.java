package com.geovis.duplex.test;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TestRecord {

	public static void main(String[] args) throws IOException {
		FileChannel open = FileChannel.open(
				Paths.get(System.getProperty("user.dir"), "record", "position.rec"), 
				StandardOpenOption.CREATE, 
				StandardOpenOption.WRITE, 
				StandardOpenOption.READ);
		MappedByteBuffer map = open.map(MapMode.READ_WRITE, 0, 285);
		System.out.println(map.get());
		System.out.println(map.get());
		System.out.println(map.get());
		System.out.println(map.getLong());
		System.out.println(map.getLong());
		int length = map.getInt();
		byte[] bs = new byte[length];
		map.get(bs);
		System.out.println(new String(bs));
	}

}
