package com.acadgild.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Task2 {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length!=1){
			System.out.println("please pass argument");
			System.exit(1);
			
		}
		Path path=new Path(args[0]);
			Configuration conf=new Configuration();
			iterateDirectory(path,conf);	
		
	}
	static void iterateDirectory(Path dirpath,Configuration config){
		try{
		FileSystem fileSystem=FileSystem.get(dirpath.toUri(),config);
		FileStatus[] fileStatus=fileSystem.listStatus(dirpath);
		for(FileStatus fstat:fileStatus){
			if(fstat.isDirectory()){
				Path dirPath=fstat.getPath();
				System.out.println("Directory" + dirPath);
				iterateDirectory(dirPath,config);				
				
			}else if(fstat.isFile()){
				System.out.println("File" + fstat.getPath());
			}else if(fstat.isSymlink()){
				System.out.println("Symlink" + fstat.getPath());
			}
		}
		}catch(IOException e){
		e.printStackTrace();
		}	
	}

}
