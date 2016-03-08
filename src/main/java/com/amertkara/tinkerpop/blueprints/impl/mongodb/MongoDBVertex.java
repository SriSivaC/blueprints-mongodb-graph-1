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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public class MongoDBVertex extends MongoDBElement implements Vertex {
    
    public MongoDBVertex(final Document vertex, final MongoDBGraph graph) {
        super(vertex, graph);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        if (direction.equals(Direction.OUT)) {
            Document query = new Document(MongoDBConstants.FIELD_OUTV, new DBRef(MongoDBConstants.DEFAULT_VERTEX_COLLECTION, getId())).append(MongoDBConstants.FIELD_LABEL, new Document("$in", Arrays.asList(labels)));
            FindIterable<Document> result = this.graph.getEdgeCollection().find(query);
            return new MongoDBEdgeIterable(result, graph);
        } else if (direction.equals(Direction.IN)) {
            Document query = new Document(MongoDBConstants.FIELD_INV, new DBRef(MongoDBConstants.DEFAULT_VERTEX_COLLECTION, getId())).append(MongoDBConstants.FIELD_LABEL, new Document("$in", Arrays.asList(labels)));
            FindIterable<Document> result = this.graph.getEdgeCollection().find(query);
            return new MongoDBEdgeIterable(result, graph);
        } else {
            Document query = new Document("$or", Arrays.asList(
                    new Document(MongoDBConstants.FIELD_OUTV, new DBRef(MongoDBConstants.DEFAULT_VERTEX_COLLECTION, getId())).append(MongoDBConstants.FIELD_LABEL, new Document("$in", Arrays.asList(labels))),
                    new Document(MongoDBConstants.FIELD_INV, new DBRef(MongoDBConstants.DEFAULT_VERTEX_COLLECTION, getId())).append(MongoDBConstants.FIELD_LABEL, new Document("$in", Arrays.asList(labels)))));
            FindIterable<Document> result = this.graph.getEdgeCollection().find(query);
            return new MongoDBEdgeIterable(result, graph);
        }
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        List<Integer> ids = new ArrayList<Integer>();
        Iterator<Document> it;
        FindIterable<Document> result;
        
        if (direction.equals(Direction.OUT)) {
            Document query = new Document(MongoDBConstants.FIELD_INV, new DBRef(MongoDBConstants.DEFAULT_VERTEX_COLLECTION, getId())).append(MongoDBConstants.FIELD_LABEL, new Document("$in", Arrays.asList(labels)));
            result = this.graph.getEdgeCollection().find(query);
            it = result.iterator();
            
            while (it.hasNext()) {
                Document document = (Document) it.next();
                DBRef inV = (DBRef) document.get(MongoDBConstants.FIELD_OUTV);
                ids.add((Integer) inV.getId());
            }
        } else if (direction.equals(Direction.IN)) {
            Document query = new Document(MongoDBConstants.FIELD_OUTV, new DBRef(MongoDBConstants.DEFAULT_VERTEX_COLLECTION, getId())).append(MongoDBConstants.FIELD_LABEL, new Document("$in", Arrays.asList(labels)));
            result = this.graph.getEdgeCollection().find(query);
            it = result.iterator();
            
            while (it.hasNext()) {
                Document document = (Document) it.next();
                DBRef outV = (DBRef) document.get(MongoDBConstants.FIELD_INV);
                ids.add((Integer) outV.getId());
            }
        } else {
            Document query = new Document("$or", Arrays.asList(
                    new Document(MongoDBConstants.FIELD_OUTV, new DBRef(MongoDBConstants.DEFAULT_VERTEX_COLLECTION, getId())).append(MongoDBConstants.FIELD_LABEL, new Document("$in", Arrays.asList(labels))),
                    new Document(MongoDBConstants.FIELD_INV, new DBRef(MongoDBConstants.DEFAULT_VERTEX_COLLECTION, getId())).append(MongoDBConstants.FIELD_LABEL, new Document("$in", Arrays.asList(labels)))));
            result = this.graph.getEdgeCollection().find(query);
            it = result.iterator();
            
            while (it.hasNext()) {
                Document document = (Document) it.next();
                DBRef inV = (DBRef) document.get(MongoDBConstants.FIELD_INV);
                DBRef outV = (DBRef) document.get(MongoDBConstants.FIELD_OUTV);
                if (inV.getId() != getId()) {
                    ids.add((Integer) inV.getId());
                } else {
                    ids.add((Integer) outV.getId());
                }
            }
        }
        
        return new MongoDBVertexIterable(this.graph.getVertexCollection().find(new Document(MongoDBConstants.FIELD_ID, new Document("$in", ids))), graph);
    }

    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        return this.graph.addEdge(null, this, inVertex, label);
    }

    @Override
    public MongoCollection<Document> getMongoCollection() {
        return this.graph.getVertexCollection();
    }
    
    /**
     * Vertex interface doesn't have a getLabel method however we will need it
     * for vertices as well.
     * 
     * @return the string value at the field {@link MongoDBConstants#FIELD_LABEL}
     */
    public String getLabel() {
        return this.rawElement.getString(MongoDBConstants.FIELD_LABEL);
    }
}
