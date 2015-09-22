package com.xenofied.mongobukkit;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;


abstract class MongoOperator extends JavaPlugin{
    static long nextContext = 0;
    private static final HashMap<String, Document> queries = new HashMap<>();
    public static final ArrayList<UUID> userCache = new ArrayList<>();

    public static SingleResultCallback<Void> getInsertCallback(final MongoCaller caller, final UUID playerId) {
        return new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                caller.onInsert(playerId);
            }
        };
    }

    public static SingleResultCallback<Document> getHasUserCallback(final MongoCaller caller, final UUID playerId) {
        return new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                if(result != null)
                    userCache.add(playerId);
                caller.onHasUser(playerId, result != null);
            }
        };
    }

    public static SingleResultCallback<UpdateResult> getUpdateCallback(final MongoCaller caller, final String context){
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

    public static String getCallbackContext(JavaPlugin plugin, UUID playerId){
        nextContext += 1;
        return Integer.toString(plugin.hashCode()) + "." + playerId.toString() + "."+ Long.toString(nextContext);
    }

    public static Document getQuery(String context){
        if(!queries.containsKey(context))
            return null;
        return queries.get(context);
    }
}
