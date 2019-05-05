package com.alshaya.phoenix.integration.s3;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.alshaya.phoenix.integration.s3.client.Client;
import com.alshaya.phoenix.integration.s3.exception.InvalidRegionException;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class ObjectOperations {
	
	private S3Client s3;
	
	public ObjectOperations(String regionName) throws InvalidRegionException {
		s3 = Client.createClient(regionName);
	}
	
	public boolean getObject(String bucketName, String objKey, String fileName) throws NoSuchBucketException,
																					   NoSuchKeyException,
																					   S3Exception,
																					   AwsServiceException,
																					   SdkClientException {
		GetObjectRequest request = GetObjectRequest.builder()
													  .bucket(bucketName)
												      .key(objKey)
												      .build();
		GetObjectResponse response = s3.getObject(request, ResponseTransformer.toFile(Paths.get(fileName)));
		return response.sdkHttpResponse().isSuccessful();
	}

	public boolean putObject(String bucketName, String objKey, String objLocalPath) throws NoSuchBucketException,
																						   S3Exception,
																						   AwsServiceException,
																						   SdkClientException {
		boolean status;
		long fileSize = (new File(objLocalPath)).length() / (1024 * 1024);
		if(fileSize > 100) { //Multi-part upload if file size > 100MB
			int parts = (int)(new File(objLocalPath)).length() / (1024 * 1024);
			List<CompletedPart> complParts = new ArrayList<CompletedPart>(parts);
			CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
																			   .bucket(bucketName)
																			   .key(objKey)
																			   .build();
			CreateMultipartUploadResponse response = s3.createMultipartUpload(request);
			String uploadId = response.uploadId();
			for(int i = 0; i < parts; i++) {
				UploadPartRequest partRequest = UploadPartRequest.builder()
																 .bucket(bucketName)
																 .key(objKey)
																 .uploadId(uploadId)
																 .partNumber(i + 1)
																 .build();
				String eTag = s3.uploadPart(partRequest, Paths.get(objLocalPath)).eTag();
				complParts.add(CompletedPart.builder()
											.partNumber(i)
											.eTag(eTag)
											.build());
			}
			
			CompletedMultipartUpload complUpload = CompletedMultipartUpload.builder()
																		   .parts(complParts)
																		   .build();
			CompleteMultipartUploadRequest complUpRequest = CompleteMultipartUploadRequest.builder()
																						   .bucket(bucketName)
																						   .key(objKey)
																						   .uploadId(uploadId)
																						   .multipartUpload(complUpload)
																						   .build();
			status = s3.completeMultipartUpload(complUpRequest).sdkHttpResponse().isSuccessful();
		}
		else { //If file size < 100MB, upload as normal
			PutObjectRequest putRequest = PutObjectRequest.builder()
					  .bucket(bucketName)
					  .key(objKey)
					  .build();
			RequestBody payload = RequestBody.fromFile(new File(objLocalPath));
			PutObjectResponse response = s3.putObject(putRequest, payload);
			status = response.sdkHttpResponse().isSuccessful();
		}
		return status;
	}
	
	public boolean deleteObject(String bucketName, String objKey) {
		DeleteObjectRequest request = DeleteObjectRequest.builder()
														   .bucket(bucketName)
														   .key(objKey)
														   .build();
		return s3.deleteObject(request).sdkHttpResponse().isSuccessful();
	}
	
	public boolean deleteObjects(String bucketName, List<String> objKeys) {
		List<ObjectIdentifier> objects = new ArrayList<ObjectIdentifier>(objKeys.size());
		objKeys.forEach(objKey -> objects.add(ObjectIdentifier.builder()
															  .key(objKey)
															  .build()));
		
		DeleteObjectsRequest request = DeleteObjectsRequest.builder()
														   .bucket(bucketName)
														   .delete(deleteObjs -> Delete.builder()
																   					   .objects(objects))
														   .build();
		return s3.deleteObjects(request).sdkHttpResponse().isSuccessful();
	}
	
	public boolean copyObject(String srcBucket, String srcObjKey, String tgtBucket, String tgtObjKey) {
		CopyObjectRequest request = CopyObjectRequest.builder()
													 .copySource(srcBucket + "/" + srcObjKey)
													 .bucket(tgtBucket)
													 .key(tgtObjKey)
													 .build();
		return s3.copyObject(request).sdkHttpResponse().isSuccessful();
	}
	
	public boolean moveObject(String srcBucket, String srcObjKey, String tgtBucket, String tgtObjKey) {
		if(this.copyObject(srcBucket, srcObjKey, tgtBucket, tgtObjKey))
			return this.deleteObject(srcBucket, srcObjKey);
		else
			return false;
	}
	
	public List<String> listObject(String bucketName) {
		List<String> objectList = new ArrayList<String>();
		
		ListObjectsV2Request listReq = ListObjectsV2Request.builder()
											               .bucket(bucketName)
											               .maxKeys(1000)
											               .build();
		
		ListObjectsV2Iterable listRes = s3.listObjectsV2Paginator(listReq);
		listRes.stream()
			   .flatMap(r -> r.contents().stream())
			   .forEach(content -> objectList.add(content.key()));
		return objectList;
	}	
}
