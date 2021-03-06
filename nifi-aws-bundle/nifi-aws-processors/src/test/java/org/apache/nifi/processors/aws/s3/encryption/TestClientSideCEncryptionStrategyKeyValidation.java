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
package org.apache.nifi.processors.aws.s3.encryption;

import org.apache.nifi.components.ValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.nifi.processors.aws.s3.encryption.S3EncryptionTestUtil.createKey;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestClientSideCEncryptionStrategyKeyValidation {

    private ClientSideCEncryptionStrategy strategy;

    @BeforeEach
    public void setUp() {
        strategy = new ClientSideCEncryptionStrategy();
    }

    @Test
    public void testValid256BitKey() {
        String key = createKey(256);

        ValidationResult result = strategy.validateKey(key);

        assertTrue(result.isValid());
    }

    @Test
    public void testValid192BitKey() {
        String key = createKey(192);

        ValidationResult result = strategy.validateKey(key);

        assertTrue(result.isValid());
    }

    @Test
    public void testValid128BitKey() {
        String key = createKey(128);

        ValidationResult result = strategy.validateKey(key);

        assertTrue(result.isValid());
    }

    @Test
    public void testNotSupportedKeySize() {
        String key = createKey(512);

        ValidationResult result = strategy.validateKey(key);

        assertFalse(result.isValid());
    }

    @Test
    public void testNullKey() {
        String key = null;

        ValidationResult result = strategy.validateKey(key);

        assertFalse(result.isValid());
    }

    @Test
    public void testEmptyKey() {
        String key = "";

        ValidationResult result = strategy.validateKey(key);

        assertFalse(result.isValid());
    }

    @Test
    public void testNotBase64EncodedKey() {
        String key = "NotBase64EncodedKey";

        ValidationResult result = strategy.validateKey(key);

        assertFalse(result.isValid());
    }
}
