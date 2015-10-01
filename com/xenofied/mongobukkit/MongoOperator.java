package com.xenofied.mongobukkit;

import com.mongodb.Block;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;


abstract class MongoOperator extends JavaPlugin {
    private static MongoClient client;
    private static boolean isStrict = false;
    private static ArrayList<String> dbNames;
    private static final HashMap<String, Document> queries = new HashMap<>();
    private static final HashMap<String, ArrayList<Document>> queryBlocks = new HashMap<>();
    public static final ArrayList<UUID> userCache = new ArrayList<>();
    static long nextContext = 0;

    public static void setClient(MongoClient c) {
        if (client == null)
            client = c;
    }

    public static void setDBNames(ArrayList<String> a){
        if (dbNames == null)
            dbNames = a;
    }

    static void setIsStrict(boolean b){
        isStrict = b;
    }

    public static void addDatabase(String n){

    }

    public static SingleResultCallback<Void> getInsertUserCallback(final MongoCaller caller, final UUID playerId) {
        return new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                caller.onInsertUser(playerId);
            }
        };
    }

    public static SingleResultCallback<Void> getInsertCallback(final MongoCaller caller, final Document d, final String context) {
        return new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                caller.onInsert(context, d);
            }
        };
    }

    public static SingleResultCallback<Document> getHasUserCallback(final MongoCaller caller, final UUID playerId) {
        return new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                if (result != null)
                    userCache.add(playerId);
                caller.onHasUser(playerId, result != null);
            }
        };
    }

    public static SingleResultCallback<UpdateResult> getUpdateCallback(final MongoCaller caller, final String context) {
        return new SingleResultCallback<UpdateResult>() {
            @Override
            public void onResult(final UpdateResult result, final Throwable t) {
                caller.onUpdate(context, result);
            }
        };
    }

    public static SingleResultCallback<Document> getQueryCallback(final MongoCaller caller, final String context) {
        return new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                queries.put(context, result);
                caller.onQuery(context, result);
            }
        };
    }

    public static SingleResultCallback<Void> getBlockQueryCallback(final MongoCaller caller, final String context) {
        return new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                caller.onQuery(context, result);
            }
        };
    }

    public static String getCallbackContext(JavaPlugin plugin) {
        nextContext += 1;
        return Integer.toString(plugin.hashCode()) + "." + Long.toString(nextContext);
    }

    public static Document getQuery(String context) {
        if (queries.containsKey(context))
            return queries.get(context);
        return null;
    }

    public static ArrayList<Document> getQueryBlock(String context){
        if(queryBlocks.containsKey(context))
            return queryBlocks.get(context);
        return null;
    }

    public static void insertIntoCollection(final MongoOperator operator, final MongoCaller caller, final String context,
                                            final Document d, final String databaseName, final String collectionName) {
        Bukkit.getScheduler().runTaskAsynchronously(operator, new Runnable() {
            @Override
            public void run() {
                MongoCollection<Document> collection = getCollection(databaseName, collectionName);
                if(collection == null)
                    return;
                collection.insertOne(d, getInsertCallback(caller, d, context));
            }
        });
    }

    public static void updateDocument(final MongoOperator operator, final MongoCaller caller, final String context, final Document d1,
                                      final Document d2, final String databaseName, final String collectionName) {
        Bukkit.getScheduler().runTaskAsynchronously(operator, new Runnable() {
            @Override
            public void run() {
                MongoCollection<Document> collection = getCollection(databaseName, collectionName);
                if(collection == null)
                    return;
                collection.updateOne(
                        d1,
                        new Document("$set", d2).append("$currentDate", new Document("lastModified", true)),
                        getUpdateCallback(caller, context)
                );
            }
        });
    }

    public static void queryFirstDocument(final MongoOperator operator, final MongoCaller caller, final String context,
                                          final Bson b, final String databaseName, final String collectionName) {
        Bukkit.getScheduler().runTaskAsynchronously(operator, new Runnable() {
            @Override
            public void run() {
                MongoCollection<Document> collection = getCollection(databaseName, collectionName);
                if(collection == null)
                    return;

                collection.find(b).first(getQueryCallback(caller, context));
            }
        });
    }

    public static void queryAllDocument(final MongoOperator operator, final MongoCaller caller, final String context,
                                        final Bson b, final String databaseName, final String collectionName) {
        Bukkit.getScheduler().runTaskAsynchronously(operator, new Runnable() {
            @Override
            public void run() {
                MongoCollection<Document> collection = getCollection(databaseName, collectionName);
                if(collection == null)
                    return;
                collection.find().forEach(new Block<Document>() {
                    @Override
                    public void apply(final Document document) {
                        if (!queryBlocks.containsKey(context))
                            queryBlocks.put(context, new ArrayList<Document>());
                        queryBlocks.get(context).add(document);
                    }
                }, getBlockQueryCallback(caller, context));
            }
        });
    }

    public static MongoDatabase getMongoDB(String name){
        if(isStrict && !dbNames.contains(name))
                return null;
        return client.getDatabase(name);
    }

    public static MongoCollection<Document> getCollection(String databaseName, String collectionName){
        MongoDatabase database = getMongoDB(databaseName);
        if(database == null)
            return null;
        return database.getCollection(collectionName);
    }

    public void closeClient(){
        client.close();
    }
}
