package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;

public class SessionImpl implements Session{
    String id;
    long creationTime;
    long lastAccessedTime;
    int maxActiveInterval;
    Map<String, Object> attribute;

    SessionImpl(String id){
        this.id = id;
        creationTime = System.currentTimeMillis();
        lastAccessedTime = creationTime;
        maxActiveInterval = 300;
        attribute = new HashMap<>();
        Server.mSessions.put(id, this);
    }

    public boolean isValid(){
        return System.currentTimeMillis() - lastAccessedTime < maxActiveInterval * 1000;
    }

    // Returns the session ID (the value of the SessionID cookie) that this session is associated with
    public String id(){
        return id;
    }

    // The methods below return the time this session was created, and the time time this session was
    // last accessed. The return values should be in the same format as the return value of
    // System.currentTimeMillis().
    public long creationTime(){
        return creationTime;
    }
    public long lastAccessedTime(){
        return lastAccessedTime;
    }

    // Set the maximum time, in seconds, this session can be active without being accessed.
    public void maxActiveInterval(int seconds){
        maxActiveInterval = seconds;
    }

    // Invalidates the session. You do not need to delete the cookie on the client when this method
    // is called; it is sufficient if the session object is removed from the server.
    public void invalidate(){
        Server.mSessions.remove(id);
    }

    // The methods below look up the value for a given key, and associate a key with a new value,
    // respectively.
    public Object attribute(String name){
        return attribute.get(name);
    }
    public void attribute(String name, Object value){
        attribute.put(name, value);
    }
}
