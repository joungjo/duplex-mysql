package com.geovis.duplex.mysql.server;

import java.nio.file.Paths;

public class PathTest {

	public static void main(String[] args) {
		System.out.println(Paths.get("record", "position.rec").toFile().getAbsolutePath());
	}

}
