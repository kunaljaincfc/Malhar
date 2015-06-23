package com.datatorrent.lib.ml;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class LocalFileWrite {

	@Test
	public void test() throws IllegalArgumentException, IOException {
		FileSystem tempFS = FileSystem.newInstance(new Path("C:\\Users\\kunal\\Desktop\\test").toUri(), new Configuration());

		tempFS = ((LocalFileSystem) tempFS).getRaw();
		tempFS.create(new Path("C:\\Users\\kunal\\Desktop\\test"));
	}
}