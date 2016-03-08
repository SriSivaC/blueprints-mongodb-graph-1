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
package com.amertkara.tinkerpop.blueprints.impl.mongodb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public class MongoDBVertexIterable implements Iterable<Vertex> {
    private FindIterable<Document> iterable;
    private MongoDBGraph graph;
    private List<Vertex> vertices = new ArrayList<Vertex>();
    
    public MongoDBVertexIterable(final FindIterable<Document> iterable, final MongoDBGraph graph) {
        this.iterable = iterable;
        this.graph = graph;
    }
    
    @Override
    public Iterator<Vertex> iterator() {
        Iterator<Document> it = this.iterable.iterator();
        
        while (it.hasNext()) {
            Document document = (Document) it.next();
            vertices.add((Vertex)new MongoDBVertex(document, graph));
        }
        
        return vertices.iterator();
    }
}
