package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {


    /*
    * REFERENCE:: SQLite tutorial from https://www.tutorialspoint.com/android/android_content_providers.htm
    */
    private SQLiteDatabase db;
    static final String DATABASE_NAME = "GroupMessanger";
    static final String TABLE_NAME = "ContentProvider";
    static final int DATABASE_VERSION = 1;
    static final String CREATE_DB_TABLE =
            " CREATE TABLE " + TABLE_NAME +
                    " ( keys TEXT PRIMARY KEY, " +
                    " value TEXT NOT NULL);";

    private static class DatabaseHelper extends SQLiteOpenHelper {
        DatabaseHelper(Context context){
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(CREATE_DB_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL("DROP TABLE IF EXISTS " +  TABLE_NAME);
            onCreate(db);
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         * 
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */
        //check if the given key is already present in DB

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(TABLE_NAME);
        String[] select = {"value"};
        String selection = values.getAsString("key");
        String where = "keys = '"+selection+"' ";
        Log.d("Where11111", where);
        Cursor c = qb.query(db,	select,	where,
                null,null, null, null);
        String A = String.valueOf(values.get("key")) ;
        values.remove("key");
        values.put("keys", A);
        if(c.getCount()!=0)
        {
            Log.d("Insert:::", "Present");
            //where = "keys="+A;
            Log.d("Where", where);
            int count = db.update(TABLE_NAME,values,where,null);
            Log.d("Count::", String.valueOf(count));
        }
        else
        {
            Log.d("Insert::", "Not Present");

            long rowId = db.insert(TABLE_NAME,null,values);


        }
        A = String.valueOf(values.get("keys")) ;
        values.remove("keys");
        values.put("key", A);


        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.

        Context context = getContext();
        DatabaseHelper dbHelper = new DatabaseHelper(context);

        /**
         * Create a write able database which will trigger its
         * creation if it doesn't already exist.
         */

        db = dbHelper.getWritableDatabase();
        return (db == null)? false:true;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */

        Log.v("query", selection);
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(TABLE_NAME);
        String[] select = {"value","keys as key"};
        String where = "key = '"+selection+"' ";
        Cursor c = qb.query(db,	select,	where,
                selectionArgs,null, null, sortOrder);
        Log.v("where: ", where);
        Log.v("Value Retrieved:", String.valueOf(c.getColumnIndex("value")));

        /*
        *REFERENCE::  https://developer.android.com/reference/android/database/MatrixCursor
        */
        MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
        while (c.moveToNext())
        {
            Log.e("val:",c.getString(c.getColumnIndex("value")));
            mCur.newRow().add("key",selection)
                        .add("value",c.getString(c.getColumnIndex("value")));
        }
        return mCur;
    }
}
