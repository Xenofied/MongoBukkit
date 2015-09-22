package com.xenofied.mongobukkit;

import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;

import java.io.File;
import java.io.IOException;

public class Config {
    private static File configFile;
    private static FileConfiguration config;

    public static void createConfig(){
        configFile = new File(MongoBukkit.getPlugin().getDataFolder(), "config.yml");
        config = YamlConfiguration.loadConfiguration(configFile);

        if(!(configFile.exists())) {
            MongoBukkit.log("Couldn't find config.yml! Creating one for you...");
            MongoBukkit.getPlugin().saveResource("config.yml", true);
            MongoBukkit.log("Successfully created config.yml!");
        }
    }

    public static File getConfigFile(){
        return configFile;
    }

    public static FileConfiguration getConfig(){
        return config;
    }

    public static FileConfiguration getFileConfig(File file){
        return YamlConfiguration.loadConfiguration(file);
    }

    public static void saveConfigFile(){
        saveFile(configFile, config);
    }

    public static void saveFile(File file, FileConfiguration config){
        try{
            config.save(file);
        } catch(IOException e){
            e.printStackTrace();
        }
    }

}