package com.acadgild.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileListing {

	public static void main(String[] args) {
		if(args.length!=1){
			System.out.println("please pass argument");
			System.exit(1);
			
		}
		Path path=new Path(args[0]);
		try{
			Configuration conf=new Configuration();
			FileSystem fileSystem=FileSystem.get(path.toUri(),conf);
			FileStatus[] fileStatus=fileSystem.listStatus(path);
			for(FileStatus fstat:fileStatus){
				if(fstat.isDirectory()){
					System.out.println("Directory" + fstat.getPath());
					
				}else if(fstat.isFile()){
					System.out.println("File" + fstat.getPath());
				}else if(fstat.isSymlink()){
					System.out.println("Symlink" + fstat.getPath());
				}
				
			}
		}
		catch(IOException e){
			e.printStackTrace();
		}

	}

}
