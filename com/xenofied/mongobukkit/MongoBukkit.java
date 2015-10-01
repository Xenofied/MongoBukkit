package com.xenofied.mongobukkit;

import static com.mongodb.client.model.Filters.*;

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
    private static ArrayList<MongoDatabase> databases;
    private static String[] userDbPath;


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

        setClient(MongoClients.create(addressString));

        String userPath = getConfig().getString("user-db");
        log("User DB: " + userPath);
        if(userPath == null){
            warn("No user database name defined!");
            warn("Falling back to default: MongoBukkit.users");
            userPath = "MongoBukkit.users";
        }
        String[] path = userPath.split("\\.");
        if(path.length != 2){
            warn("Invalid path specified!");
            warn("Falling back to default: MongoBukkit.users");
            userDbPath = new String[]{"MongoBukkit", "users"};
        }

        setIsStrict(getConfig().getBoolean("strict-mode"));
        setDBNames((ArrayList<String>) getConfig().getStringList("databases"));

        databases = new ArrayList<>();

        new LoginListener();
    }

    public static MongoBukkit getPlugin(){
        return plugin;
    }

    public static ArrayList<MongoDatabase> getDatabases(){
        return databases;
    }

    public static MongoCollection<Document> getUserCollection(){
        return getCollection(userDbPath[0], userDbPath[1]);
    }

    public static void insertUser(final MongoCaller caller, Player player){
        final Document doc = new Document("_id", player.getUniqueId())
                .append("registerDate", System.currentTimeMillis() / 1000L)
                .append("username", player.getName());

        final UUID playerId = player.getUniqueId();

        Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable() {
            @Override
            public void run() {
                getUserCollection().insertOne(doc, getInsertUserCallback(caller, playerId));
            }
        });
    }

    public static void updatePlayer(final MongoCaller caller, final String context, final UUID playerId,
                                    final Document doc){
        updateDocument(plugin, caller, context, new Document("_id", playerId), doc, userDbPath[0], userDbPath[1]);
    }

    public static void queryPlayer(final MongoCaller caller, final String context, final UUID playerId){
        queryFirstDocument(plugin, caller, context, eq("_id", playerId), userDbPath[0], userDbPath[1]);
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
        userCache.clear();
        databases.clear();
        closeClient();
    }

    public static void log(String s){
        plugin.getLogger().info(s);
    }

    public static void warn(String s){
        plugin.getLogger().warning(s);
    }

    public void onInsertUser(UUID playerId){
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

    public void onInsert(String context, Document d){}

    public void onUpdate(String context, UpdateResult result){}

    public void onQuery(String context, Document d){}

    public void onQuery(String context, Void v){}


}
