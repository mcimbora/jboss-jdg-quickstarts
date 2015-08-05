/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.as.quickstarts.datagrid.hotrod.query;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Memo;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person;
import org.jboss.as.quickstarts.datagrid.hotrod.query.marshallers.PersonMarshaller;
import org.jboss.as.quickstarts.datagrid.hotrod.query.marshallers.PhoneNumberMarshaller;
import org.jboss.as.quickstarts.datagrid.hotrod.query.marshallers.PhoneTypeMarshaller;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;
import java.util.Properties;

/**
 * @author Adrian Nistor
 */
public class ReproducerManager {

   private static final String SERVER_HOST = "jdg.host";
   private static final String HOTROD_PORT = "jdg.hotrod.port";
   private static final String CACHE_NAME = "jdg.cache";
   private static final String PROPERTIES_FILE = "jdg.properties";

   private static final String PROTOBUF_DEFINITION_RESOURCE = "/quickstart/addressbook.proto";

   private RemoteCacheManager cacheManager;

   /**
    * A cache that hold both Person and Memo objects.
    */
   private RemoteCache<Integer, Object> addressbookCache;

   public ReproducerManager() throws Exception {
      final String host = jdgProperty(SERVER_HOST);
      final int hotrodPort = Integer.parseInt(jdgProperty(HOTROD_PORT));
      final String cacheName = jdgProperty(CACHE_NAME);  // The name of the address book  cache, as defined in your server config.

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer()
            .host(host)
            .port(hotrodPort)
            .marshaller(new ProtoStreamMarshaller());  // The Protobuf based marshaller is required for query capabilities
      cacheManager = new RemoteCacheManager(builder.build());

      addressbookCache = cacheManager.getCache(cacheName);
      if (addressbookCache == null) {
         throw new RuntimeException("Cache '" + cacheName + "' not found. Please make sure the server is properly configured");
      }

      registerSchemasAndMarshallers();
   }

   /**
    * Register the Protobuf schemas and marshallers with the client and then register the schemas with the server too.
    */
   private void registerSchemasAndMarshallers() throws IOException {
      // Register entity marshallers on the client side ProtoStreamMarshaller instance associated with the remote cache manager.
      SerializationContext ctx = ProtoStreamMarshaller.getSerializationContext(cacheManager);
      ctx.registerProtoFiles(FileDescriptorSource.fromResources(PROTOBUF_DEFINITION_RESOURCE));
      ctx.registerMarshaller(new PersonMarshaller());
      ctx.registerMarshaller(new PhoneNumberMarshaller());
      ctx.registerMarshaller(new PhoneTypeMarshaller());

      // generate the 'memo.proto' schema file based on the annotations on Memo class and register it with the SerializationContext of the client
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String memoSchemaFile = protoSchemaBuilder
            .fileName("memo.proto")
            .packageName("quickstart")
            .addClass(Memo.class)
            .build(ctx);

      // register the schemas with the server too
      RemoteCache<String, String> metadataCache = cacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put(PROTOBUF_DEFINITION_RESOURCE, readResource(PROTOBUF_DEFINITION_RESOURCE));
      metadataCache.put("memo.proto", memoSchemaFile);
      String errors = metadataCache.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
      if (errors != null) {
         throw new IllegalStateException("Some Protobuf schema files contain errors:\n" + errors);
      }
   }

   private void stop() {
      cacheManager.stop();
   }

   public static void main(String[] args) throws Exception {
      ReproducerManager manager = new ReproducerManager();

      final int numEntries = 500;
      for (int i = 0; i < numEntries; i++) {
         Person p = new Person();
         p.setId(i);
         p.setName(String.valueOf(i));
         manager.addressbookCache.put(i, p);
      }

      System.out.println("Loading done");

      QueryFactory qf = Search.getQueryFactory(manager.addressbookCache);

      final int threadCount = 5;
      Thread[] threads = new Thread[threadCount];
      for (int i = 0; i < threadCount; i++) {
         threads[i] = new QueryThread(qf);
         threads[i].start();
      }
      for (int i = 0; i < threadCount; i++) {
         threads[i].join();
      }

      System.out.println("All threads have joined");

      manager.stop();
   }

   private static class QueryThread extends Thread {

      private final QueryFactory factory;

      public QueryThread(QueryFactory factory) {
         this.factory = factory;
      }

      @Override
      public void run() {
         for (int i = 0; i < 1000; i++) {
            Query query = factory.from(Person.class)
                  .having("name").like("500").toBuilder()
                  .build();
            List<Person> results = query.list();
         }
      }
   }

   private String jdgProperty(String name) {
      InputStream res = null;
      try {
         res = getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE);
         Properties props = new Properties();
         props.load(res);
         return props.getProperty(name);
      } catch (IOException ioe) {
         throw new RuntimeException(ioe);
      } finally {
         if (res != null) {
            try {
               res.close();
            } catch (IOException e) {
               // ignore
            }
         }
      }
   }

   private String readResource(String resourcePath) throws IOException {
      InputStream is = getClass().getResourceAsStream(resourcePath);
      try {
         final Reader reader = new InputStreamReader(is, "UTF-8");
         StringWriter writer = new StringWriter();
         char[] buf = new char[1024];
         int len;
         while ((len = reader.read(buf)) != -1) {
            writer.write(buf, 0, len);
         }
         return writer.toString();
      } finally {
         is.close();
      }
   }
}
