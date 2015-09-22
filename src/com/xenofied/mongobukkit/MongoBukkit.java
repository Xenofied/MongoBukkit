package com.xenofied.mongobukkit;

import static com.mongodb.client.model.Filters.*;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.ArrayList;
import java.util.UUID;

/**
 * MongoDB backend storage for Bukkit plugins.
 * @author Alexander C.
 *
 */

public final class MongoBukkit extends JavaPlugin {
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

        MongoClientURI mongoAddress = new MongoClientURI(addressString);
        log("Connecting to: " + addressString);

        client = new MongoClient(mongoAddress);

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

    public static void insertUser(Player player){
        Document doc = new Document("_id", player.getUniqueId())
                .append("registerDate", System.currentTimeMillis() / 1000L)
                .append("username", player.getName());

        getUserCollection().insertOne(doc);
        log("Successfully added user " + player.getName());
    }


    public static UpdateResult updatePlayer(Player player, Document doc){
        return updatePlayer(player.getUniqueId(), doc);
    }

    public static UpdateResult updatePlayer(UUID playerId, Document doc){
        return getUserCollection().updateOne(new Document("_id", playerId), new Document("$set", doc)
                .append("$currentDate", new Document("lastModified", true)));
    }

    public static Document queryPlayer(Player player){
        return queryPlayer(player.getUniqueId());
    }

    public static Document queryPlayer(UUID playerId){
        return getUserCollection().find(eq("_id", playerId)).first();
    }

    public static boolean hasPlayer(Player player){
        return hasPlayer(player.getUniqueId());
    }

    public static boolean hasPlayer(UUID playerId){
        return getUserCollection().find(eq("_id", playerId)).first() != null;
    }

    @Override
    public void onDisable() {
        client.close();
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

}
