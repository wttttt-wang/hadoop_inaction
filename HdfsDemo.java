import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Created with Eclipse.
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang/hadoop_inaction
 * Date: 2017-03-13
 * Hadoop Version: Hadoop 2.6.2
 */

public class HdfsDemo {
	public static final String HdfsPath = "hdfs://10.3.242.98:9000/";
	public static void main(String[] args) throws Exception{
		// FileSystem: http://hadoop.apache.org/docs/r2.6.2/api/org/apache/hadoop/fs/FileSystem.html
		// FileSystem fs = FileSystem.get(new URI(HdfsPath), new Configuration());
		FileSystem fs = getFileSystem();
		// TODO: demo usage of fs
		String path = "/test";
		listFiles(fs, path);	
		String hdfsPath = "test/emp/emp";
		String localPath = "/Users/wttttt/Documents/test";
		downloadFile(fs, hdfsPath, localPath);
	}

	private static FileSystem getFileSystem() throws IOException, URISyntaxException{
		// Attention: default constructor implicitly invokes super constructor which is assumed to
		// 			  throw some exception which u need to handle in sub class's constructor.
		return FileSystem.get(new URI(HdfsPath), new Configuration());
	}
	
	private static void listFiles(FileSystem fs, String para) throws IOException{
		// List the statuses of the files/directories in the given path if the path is a directory.
		final FileStatus[] listStatus = fs.listStatus(new Path(para));
		// FileStatus: http://hadoop.apache.org/docs/r2.6.2/api/org/apache/hadoop/fs/FileStatus.html
		for (FileStatus fileStatus : listStatus) {
            String isDir = fileStatus.isDir() ? "Directory" : "File";
            String permission = fileStatus.getPermission().toString();
            short replication = fileStatus.getReplication();
            long length = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication
                    + "\t" + length + "\t" + path);
            
            if(isDir.equals("Directory")){
                listFiles(fs, path);
            }
        }
		
	}
	
	private static void downloadFile(FileSystem fs, String hdfsPath, String localPath) throws IOException{
		final FSDataInputStream in = fs.open(new Path(HdfsPath + hdfsPath));
		final FileOutputStream out = new FileOutputStream(localPath);
		// IOUtils: http://hadoop.apache.org/docs/r2.6.2/api/org/apache/hadoop/io/IOUtils.html
		IOUtils.copyBytes(in, out, 1024, true);
		System.out.println("Download File Success!");
	}
	
	private static void uploadFile(FileSystem fs) throws IOException {
        final FSDataOutputStream out = fs.create(new Path(HdfsPath
                + "check.log"));
        final FileInputStream in = new FileInputStream("C:\\CheckMemory.log");
        IOUtils.copyBytes(in, out, 1024, true);
        System.out.println("Upload File Success!");
    }

    private static void deleteFile(FileSystem fs, String hdfsPath) throws IOException {
        fs.delete(new Path(hdfsPath), true);
        System.out.println("Delete File:" + hdfsPath + " Success!");
    }

    private static void createDirectory(FileSystem fs, String hdfsPath) throws IOException {
        fs.mkdirs(new Path(hdfsPath));
        System.out.println("Create Directory:" + hdfsPath + " Success!");
    }

}
