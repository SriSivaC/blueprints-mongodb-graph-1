/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amertkara.tinkerpop.blueprints.impl.mongodb.embedded;

import org.junit.After;
import org.junit.Before;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public class InMemoryMongoDB {
    protected static String HOST = "localhost";
    protected static int PORT = 27017;
    protected static String SERVER_NAME = "mongodb_1";
    protected static String DB_NAME = "test_graph_db";
    protected static String COLLECTION_EDGES = "edgeCollection";
    protected static String COLLECTION_VERTICES = "vertexCollection";
    
    protected MongoClient mongoClient;
    protected MongoDatabase mongoDB;
    
    /**
     * @throws java.lang.Exception void
     */
    @Before
    public void setUp() throws Exception {
        mongoClient = new MongoClient(HOST, PORT);
        mongoDB = mongoClient.getDatabase(DB_NAME);
        mongoDB.createCollection(COLLECTION_EDGES);
        mongoDB.createCollection(COLLECTION_VERTICES);
    }

    /**
     * @throws java.lang.Exception void
     */
    @After
    public void tearDown() throws Exception {
        mongoClient.dropDatabase(DB_NAME);
    }
}
