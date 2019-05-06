package com.alshaya.phoenix.integration.s3.client;

import com.alshaya.phoenix.integration.s3.exception.InvalidRegionException;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class Client {
	public static S3Client createClient(String regionName) throws InvalidRegionException{
		S3Client client = null;
		if (Region.regions().contains(Region.of(regionName)))
			client = S3Client.builder()
							   .region(Region.of(regionName))
							   .build();
		else
			throw new InvalidRegionException("Invalid region " + regionName + ". Process aborted.");
		return client;
	}
}
