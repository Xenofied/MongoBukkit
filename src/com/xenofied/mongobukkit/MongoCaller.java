package com.xenofied.mongobukkit;

import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.UUID;


public interface MongoCaller {
    void onInsert(UUID playerId);
    void onHasUser(UUID playerId, boolean result);
    void onUpdate(String context, UpdateResult result);
    void onQuery(String context, Document d);
}
