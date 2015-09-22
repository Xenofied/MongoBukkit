package com.xenofied.mongobukkit;

import static com.mongodb.client.model.Filters.*;

import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;


import java.util.ArrayList;
import java.util.UUID;

/**
 * MongoDB backend storage for Bukkit plugins.
 * @author Alexander C.
 *
 */

public final class MongoBukkit extends MongoOperator implements MongoCaller {
    private static MongoBukkit plugin;
    private static MongoClient client;
    private static ArrayList<MongoDatabase> databases;
    private static MongoCollection<Document> users;


    @Override
    public void onEnable() {
        plugin = this;


        Config.createConfig();

        String addressString = getConfig().getString("host");
        if(addressString == null){
            warn("Unable to find the address string!");
            return;
        }

        log("Connecting to: " + addressString);

        client = MongoClients.create(addressString);

        String userPath = getConfig().getString("user-db");
        if(userPath == null){
            warn("No user database name defined!");
            warn("Falling back to default: MongoBukkit.users");
            userPath = "MongoBukkit.users";
        }
        String[] path = userPath.split(".");
        if(path.length != 2){
            warn("Invalid path specified!");
            warn("Falling back to default: MongoBukkit.users");
            path = new String[]{"MongoBukkit", "users"};
        }
        users = client.getDatabase(path[0]).getCollection(path[1]);

        ArrayList<String> dbNames = (ArrayList<String>) getConfig().getStringList("databases");

        databases = new ArrayList<>();
        for(String s: dbNames){
            addDatabase(s);
        }

        new LoginListener();
    }

    public static MongoClient getClient(){
        return client;
    }

    public static ArrayList<MongoDatabase> getDatabases(){
        return databases;
    }

    public static MongoDatabase getDatabase(String name){
        for(MongoDatabase db: databases){
            if(name.equalsIgnoreCase(db.getName()))
                return db;
        }
        return null;
    }

    public static void addDatabase(String name){
        databases.add(client.getDatabase(name));
    }

    public static MongoCollection<Document> getUserCollection(){
        return users;
    }

    public static void insertUser(final MongoCaller caller, Player player){
        final Document doc = new Document("_id", player.getUniqueId())
                .append("registerDate", System.currentTimeMillis() / 1000L)
                .append("username", player.getName());

        final UUID playerId = player.getUniqueId();

        Bukkit.getScheduler().runTask(plugin, new Runnable(){
            @Override
            public void run(){
                getUserCollection().insertOne(doc, getInsertCallback(caller, playerId));
            }
        });
    }


    public static void updatePlayer(MongoCaller caller, String context, Player player, Document doc){
        updatePlayer(caller, context, player.getUniqueId(), doc);
    }

    public static void updatePlayer(final MongoCaller caller, final String context, final UUID playerId,
                                    final Document doc){

        Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable(){
            @Override
            public void run(){
                getUserCollection().updateOne(new Document("_id", playerId), new Document("$set", doc)
                                .append("$currentDate", new Document("lastModified", true)),
                        getUpdateCallback(caller, context));
            }
        });

    }

    public static void queryPlayer(MongoCaller caller, String context, Player player){
        queryPlayer(caller, context, player.getUniqueId());
    }

    public static void queryPlayer(final MongoCaller caller, final String context, final UUID playerId){
        Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable(){
            @Override
            public void run(){
                getUserCollection().find(eq("_id", playerId)).first(getQueryCallback(caller, context));
            }
        });
    }

    public static void hasPlayer(final MongoCaller caller, Player player){
        hasPlayer(caller, player.getUniqueId());
    }

    public static void hasPlayer(final MongoCaller caller, final UUID playerId){
        if(userCache.contains(playerId)) {
            caller.onHasUser(playerId, true);
            return;
        }
        Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable(){
            @Override
            public void run(){
                getUserCollection().find(eq("_id", playerId)).first(getHasUserCallback(caller, playerId));
            }
        });
    }

    @Override
    public void onDisable() {
        client.close();
        userCache.clear();
        databases.clear();
    }

    public static void log(String s){
        plugin.getLogger().info(s);
    }

    public static void warn(String s){
        plugin.getLogger().warning(s);
    }

    public static MongoBukkit getPlugin(){
        return plugin;
    }

    public void onInsert(UUID playerId){
        log("Player " + playerId.toString() + " was added to the user database.");
    }

    public void onHasUser(UUID playerId, boolean result){
        if(!result){
            Player player = Bukkit.getPlayer(playerId);
            if(player == null)
                return;
            insertUser(plugin, player);
        }
    }

    public void onUpdate(String context, UpdateResult result){}

    public void onQuery(String context, Document d){}

}
