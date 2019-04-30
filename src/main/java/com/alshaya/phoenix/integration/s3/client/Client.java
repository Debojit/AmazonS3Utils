package com.alshaya.phoenix.integration.s3.client;

import com.alshaya.phoenix.integration.s3.exception.InvalidRegionException;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class Client {
	private static S3Client instance;
	
	private static void createClient(String regionName) throws InvalidRegionException{
		if (Region.regions().contains(Region.of(regionName)))
			instance = S3Client.builder()
							   .region(Region.of(regionName))
							   .build();
		else
			throw new InvalidRegionException("Invalid region " + regionName + ". Process aborted.");
	}
	
	private static void createClient(String regionName, String credentialsProfile) throws InvalidRegionException{
		if (Region.regions().contains(Region.of(regionName)))
			instance = S3Client.builder()
							   .credentialsProvider(ProfileCredentialsProvider.builder().profileName(credentialsProfile).build())
							   .region(Region.of(regionName))
							   .build();
		else
			throw new InvalidRegionException("Invalid region " + regionName + ". Process aborted.");
	}
	
	public static S3Client getInstance(String regionName) throws InvalidRegionException{
		if (instance == null)
			createClient(regionName);
		return instance;
	}
	
	public static S3Client getInstance(String regionName, String credentialsProfile) throws InvalidRegionException{
		if (instance == null)
			createClient(regionName, credentialsProfile);
		else if ("".equalsIgnoreCase(credentialsProfile))
			createClient(regionName);
		return instance;
	}
	
}
