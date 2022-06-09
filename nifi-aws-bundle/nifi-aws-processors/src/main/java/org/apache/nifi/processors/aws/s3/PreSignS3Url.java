/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.aws.s3;

import static java.util.Map.entry;

import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.internal.BasicProfile;
import com.amazonaws.auth.profile.internal.ProfileStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;

@SupportsBatching
@SeeAlso({ FetchS3Object.class, DeleteS3Object.class, ListS3.class })
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "Amazon", "S3", "AWS", "Archive", "PreSign" })
@CapabilityDescription("Generates a presigned URL that you can give to others so that they can retrieve an object from an S3 bucket.")
@ReadsAttributes({
                @ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the S3 object")
})
@WritesAttributes({
                @WritesAttribute(attribute = "s3.bucket", description = "The S3 bucket where the Object was put in S3"),
                @WritesAttribute(attribute = "s3.key", description = "The S3 key within where the Object was put in S3"),
                @WritesAttribute(attribute = "s3.contenttype", description = "The S3 content type of the S3 Object that put in S3"),
                @WritesAttribute(attribute = "s3.version", description = "The version of the S3 Object that was put to S3"),
                @WritesAttribute(attribute = "s3.exception", description = "The class name of the exception thrown during processor execution"),
                @WritesAttribute(attribute = "s3.additionalDetails", description = "The S3 supplied detail from the failed operation"),
                @WritesAttribute(attribute = "s3.statusCode", description = "The HTTP error code (if available) from the failed operation"),
                @WritesAttribute(attribute = "s3.errorCode", description = "The S3 moniker of the failed operation"),
                @WritesAttribute(attribute = "s3.errorMessage", description = "The S3 exception message from the failed operation"),
                @WritesAttribute(attribute = "s3.etag", description = "The ETag of the S3 Object"),
                @WritesAttribute(attribute = "s3.contentdisposition", description = "The content disposition of the S3 Object that put in S3"),
                @WritesAttribute(attribute = "s3.cachecontrol", description = "The cache-control header of the S3 Object"),
                @WritesAttribute(attribute = "s3.uploadId", description = "The uploadId used to upload the Object to S3"),
                @WritesAttribute(attribute = "s3.expiration", description = "A human-readable form of the expiration date of "
                                +
                                "the S3 object, if one is set"),
                @WritesAttribute(attribute = "s3.sseAlgorithm", description = "The server side encryption algorithm of the object"),
                @WritesAttribute(attribute = "s3.usermetadata", description = "A human-readable form of the User Metadata of "
                                +
                                "the S3 object, if any was set"),
                @WritesAttribute(attribute = "s3.encryptionStrategy", description = "The name of the encryption strategy, if any was set"), })
public class PreSignS3Url extends AbstractS3Processor {

        public static final PropertyDescriptor EXPIRATION_RULE_ID = new PropertyDescriptor.Builder()
                        .name("Expiration Time Rule")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
                        Arrays.asList(KEY, BUCKET, ACCESS_KEY, SECRET_KEY, REGION, TIMEOUT, EXPIRATION_RULE_ID));

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {

                FlowFile flowFile = session.get();
                if (flowFile == null) {
                        return;
                }

                final String bucket = context.getProperty(BUCKET).evaluateAttributeExpressions(flowFile).getValue();
                final String region = context.getProperty(REGION).evaluateAttributeExpressions(flowFile).getValue();
                final String access_key_id = context.getProperty(ACCESS_KEY).evaluateAttributeExpressions(flowFile)
                                .getValue();
                final String secret_access_key = context.getProperty(SECRET_KEY).evaluateAttributeExpressions(flowFile)
                                .getValue();                
                final String filename = flowFile.getAttributes().get(CoreAttributes.FILENAME.key());
                final long expirationTimeout = context.getProperty(EXPIRATION_RULE_ID)
                                .asTimePeriod(TimeUnit.MILLISECONDS).longValue();

                URL url = presign(bucket, region, filename, access_key_id, secret_access_key, expirationTimeout);

                Map<String, String> preSignAttributes = Map.ofEntries(
                                entry("Expiration Timeout", String.valueOf(expirationTimeout)),
                                entry("PreSignUrl", url.toString())
                );
                                                
                flowFile= session.putAllAttributes(flowFile, preSignAttributes);

                session.transfer(flowFile, REL_SUCCESS);
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                return properties;
        }

        private URL presign(String bucket, String region, String objectKey, String access_key_id,
                        String secret_access_key, long expSeconds) {

                String profileName = "nifi";
                
                Map<String, String> awsProperties = Map.ofEntries(
                                entry("aws_access_key_id", access_key_id),
                                entry("aws_secret_access_key", secret_access_key));
                BasicProfile basicProfile = new BasicProfile(profileName, awsProperties);

                AWSCredentialsProvider awsCredentialsProvider = new ProfileStaticCredentialsProvider(basicProfile);

                try {
                        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                                        .withRegion(region)
                                        .withCredentials(awsCredentialsProvider)
                                        .build();

                        // Calculate the pre-signed URL expiry  
                        java.util.Date expiration = new java.util.Date();            
                        final long expTimeMillis = (expSeconds) + Instant.now().toEpochMilli();
                        
                        expiration.setTime(expTimeMillis);
                        
                        // Generate the presigned URL.                        
                        GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(
                                        bucket, objectKey)
                                        .withMethod(HttpMethod.GET)
                                        .withExpiration(expiration);
                        return s3Client.generatePresignedUrl(generatePresignedUrlRequest);

                } catch (AmazonServiceException e) {
                        getLogger().warn(e.getErrorMessage());
                } catch (SdkClientException e) {
                        // Amazon S3 couldn't be contacted for a response, or the client
                        // couldn't parse the response from Amazon S3.
                        getLogger().warn(e.getMessage());
                }

                return null;                
        }

}