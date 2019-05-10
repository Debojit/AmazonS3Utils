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

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
/**
 * Unit test for the S3 Upload Utility.
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ObjectOperationsTestCase {
	
	private static final String REGION_NAME = "ap-south-1";
	private final String BUCKET_NAME = "als-fenix-bucket";
	private final static String FILE_IN_DIR = "C:\\u99\\in";
	private final static String FILE_OUT_DIR = "C:\\u99\\out";
	private final static String OBJECT_KEY_ROOT = "UPLOAD_TEST_" + ThreadLocalRandom.current().nextInt(1000);
	
	private static List<FileKey> fileList;
	private static ObjectOperations objOps;
	
	@BeforeClass
	public static void createUploadFiles() {
		final int MAX_FILE_COUNT = 1;
		fileList = new ArrayList<FileKey>(MAX_FILE_COUNT);
		
		//Create test files and object keys
		for (int i = 0; i < MAX_FILE_COUNT; i++) {
			String fileName = FILE_OUT_DIR + File.separator + "test_file_" + i + ".bin";
			String objKey = OBJECT_KEY_ROOT + "/test_file_" + ThreadLocalRandom.current().nextInt(1000) + ".dat";
			fileList.add(new FileKey(fileName, objKey));
			
			RandomAccessFile testFile;
			try {
				testFile = new RandomAccessFile(fileName, "rw");
				testFile.setLength(200 * 1024 * 1024);
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
		objOps.listObject(BUCKET_NAME).forEach(key -> objOps.deleteObject(BUCKET_NAME, key));
		objOps.listObject("als-tgt-bucket").forEach(key -> objOps.deleteObject("als-tgt-bucket", key));
	}
	
	@BeforeClass
	public static void createClient() throws InvalidRegionException {
		objOps = new ObjectOperations(REGION_NAME);
	}
	
	@Test(expected = SdkException.class)
	public void tc1InvalidCredentialsProfile() throws InvalidRegionException {
		String credentialsProfile = "invalid_credentials_profile";
		
		(new ObjectOperations(REGION_NAME, credentialsProfile)).listObject(BUCKET_NAME);
	}
	
	@Test(expected = InvalidRegionException.class)
	public void tc2InvalidRegion() throws InvalidRegionException {
		String invalidRegion = "this_is_an_invalid_region";
		new ObjectOperations(invalidRegion);
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
	public void tc5CopyFile() {
		String tgtBucket = "als-tgt-bucket";
		boolean status = objOps.copyObject(BUCKET_NAME, fileList.get(0).bucketObjKey, tgtBucket, fileList.get(0).bucketObjKey);
		Assert.assertTrue(status);
	}
	
	@Test
	public void tc6MoveFile() {
		String tgtBucket = "als-tgt-bucket";
		boolean status = objOps.moveObject(BUCKET_NAME, fileList.get(0).bucketObjKey, tgtBucket, fileList.get(0).bucketObjKey);
		Assert.assertTrue(status);
	}
	
	@Test
	public void tc7FileDelete() {
		boolean status = objOps.deleteObject(BUCKET_NAME, fileList.get(0).bucketObjKey);
		Assert.assertTrue(status);
		String tgtBucket = "als-tgt-bucket";
		objOps.deleteObject(tgtBucket, fileList.get(0).bucketObjKey);
	}
	
	@Test
	@Ignore
	public void tc8MultiFileDelete() {
		List<String> objKeys = new ArrayList<String>(fileList.size());
		fileList.forEach(fileEntry -> objKeys.add(fileEntry.bucketObjKey));
		
		boolean status = objOps.deleteObjects(BUCKET_NAME, objKeys);
		Assert.assertTrue(status);
	}
	
	@Test
	public void tc9ListBucketItems() {
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
