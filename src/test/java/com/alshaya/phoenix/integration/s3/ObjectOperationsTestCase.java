package com.alshaya.phoenix.integration.s3;

import com.alshaya.phoenix.integration.s3.exception.InvalidRegionException;

import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;

import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
/**
 * Unit test for the S3 Upload Utility.
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ObjectOperationsTestCase {
	
	private static final String REGION_NAME = "ap-south-1";
	private final String BUCKET_NAME = "als-fenix-bucket";
	private static final String FILE_IN_DIR = "C:\\u04\\in";
	private static final String FILE_OUT_DIR = "C:\\u04\\out";
	private static final String OBJECT_KEY_ROOT = "UPLOAD_TEST_" + ThreadLocalRandom.current().nextInt(1000);
	
	private static List<FileKey> fileList;
	private static ObjectOperations objOps;
	
	@BeforeClass
	public static void createUploadFiles() {
		final int MAX_FILE_COUNT = 2;
		fileList = new ArrayList<FileKey>(MAX_FILE_COUNT);
		
		for (int i = 0; i < MAX_FILE_COUNT; i++) {
			String fileName = FILE_OUT_DIR + File.separator + "test_file_" + i + ".bin";
			String objKey = OBJECT_KEY_ROOT + "/test_file_" + ThreadLocalRandom.current().nextInt(1000) + ".dat";
			fileList.add(new FileKey(fileName, objKey));
			
			RandomAccessFile testFile;
			try {
				testFile = new RandomAccessFile(fileName, "rw");
				testFile.setLength(1024 * 1024); //File  size = 1MB
				testFile.close();
			} catch (IOException e) {
				e.printStackTrace();
				Assume.assumeNoException(e);
			}
		}
	}
	
	@AfterClass
	public static void cleanup() {
		Assume.assumeTrue(null != fileList  && fileList.size() != 0);
		fileList.forEach(fileKey -> { 
										try {
											(new java.io.File(fileKey.localFileName)).delete();
											(new java.io.File(FILE_IN_DIR + File.separator + Paths.get(fileKey.localFileName).getFileName())).delete();
										} catch (Exception ee) {/**Ignore Error and continue**/}
									});
	}
	
	@BeforeClass
	public static void createClient() throws InvalidRegionException {
		objOps = new ObjectOperations(REGION_NAME);
	}
	
	@Test(expected = NoSuchBucketException.class)
	public void tc1InvalidBucket() {
		String invalidBucket = "this_bucket_doesnt_exist";
		
		objOps.putObject(invalidBucket, fileList.get(0).bucketObjKey, fileList.get(0).localFileName);
	}
	
	@Test(expected = NoSuchKeyException.class)
	public void tc2InvalidObjKey() {
		String invalidKey = "this_obj_doesnt_exist";
		objOps.getObject(BUCKET_NAME, invalidKey, fileList.get(0).localFileName);
	}
	
	@Test
	public void tc3FileUpload() {
		for(FileKey uploadFile : fileList) {
			boolean status = objOps.putObject(BUCKET_NAME, uploadFile.bucketObjKey, uploadFile.localFileName);
			Assert.assertTrue(status);
		}
	}
	
	@Test
	public void tc4FileDownload() {
		fileList.forEach(downloadFile -> {
							boolean status = objOps.getObject(BUCKET_NAME, downloadFile.bucketObjKey, FILE_IN_DIR
																										+ File.separator
																										+ Paths.get(downloadFile.localFileName).getFileName());
							Assert.assertTrue(status);
						});
	}
	
	@Test
	public void tc5FileDelete() {
		boolean status = objOps.deleteObject(BUCKET_NAME, fileList.get(0).bucketObjKey);
		Assert.assertTrue(status);
	}
	
	@Test
	@Ignore
	public void tc6MultiFileDelete() {
		List<String> objKeys = new ArrayList<String>(fileList.size());
		fileList.forEach(fileEntry -> objKeys.add(fileEntry.bucketObjKey));
		
		boolean status = objOps.deleteObjects(BUCKET_NAME, objKeys);
		Assert.assertTrue(status);
	}
	
	@Test
	public void tc7ListBucketItems() {
		List<String> objKeys = objOps.listObject(BUCKET_NAME);
		Assert.assertNotNull(objKeys);
		Assert.assertNotEquals(objKeys.size(), 0);
	}
	
	private static class FileKey {
		public String localFileName;
		public String bucketObjKey;
		
		public FileKey(String localFileName, String bucketObjKey) {
			this.localFileName = localFileName;
			this.bucketObjKey = bucketObjKey;
		}
	}
}
