package Mongo;

import com.mongodb.*;

public class MongoDB {
    private String db_name;
    private String collection_name;
    private DBCollection collection;

    public MongoDB(String db_name, String collection_name) {
        this.db_name = db_name;
        this.collection_name = collection_name;
        MongoClient m1=new MongoClient("localhost",27017);
        DB database=m1.getDB(this.db_name);
        this.collection = database.getCollection(this.collection_name);
    }

    public void add_record(BasicDBObject obj){
        WriteResult result = this.collection.insert(obj);
    }
}
