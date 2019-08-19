package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.LinkedList;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    String portStr,myPort;
    Node node;
    static final int SERVER_PORT = 10000;
    LinkedList<Node> DHT = new LinkedList<Node>();

    private SQLiteDatabase db;
    static final String DATABASE_NAME = "SimpleDHT";
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
        // TODO Auto-generated method stub
        if(selection.equals("*"))
        {
            if(DHT.size()==1&&node.getMyPort().equals(node.getPredecessorPort())&&node.getMyPort().equals(node.getSuccessorPort()))
            {
                System.out.println("Only one node to delete all data");
                System.out.println("Delete keys for this node");
                int del = db.delete(TABLE_NAME,null,null);
                System.out.println("Del:"+ del);
                return del;
            }
            else{
                System.out.println("Deleting all data from all nodes:");
                SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
                qb.setTables(TABLE_NAME);
                String[] select = {"value","keys as key"};

                Node currNode = node;
                MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
                int count=1;
                while(true)
                {
                    count++;
                    System.out.println("Deleting value from Node:"+ currNode.getMyPort());
                    try{
                        Socket socket4= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(currNode.getMyPort()));
                        ObjectOutputStream outputStream3 = new ObjectOutputStream(socket4.getOutputStream());
                        ObjectInputStream inputStream3 = new ObjectInputStream(socket4.getInputStream());
                        Message keyMsg = new Message(node.myPort, selection,null,"DELETE ALL");
                        outputStream3.writeObject(keyMsg);
                        outputStream3.flush();
                        //TO DO
                        currNode = (Node)inputStream3.readObject();
                        String res = currNode.getPredecessorPort();
                        System.out.println("Deleted Message value:"+ currNode);
                        //String[] arrOfStr = res.split("\n");
                        //System.out.println("String length:"+ arrOfStr.length+ arrOfStr[0]);
                        //System.out.println("SSSS"+ arrOfStr[0].equals(""));
                        /*if(!arrOfStr[0].equals(""))
                        {
                            for (String a : arrOfStr)
                            {
                                System.out.println(a);
                                String[] sp = a.split(":");
                                mCur.newRow().add("key",sp[0])
                                        .add("value",sp[1]);

                            }
                        }*/
                        System.out.println(currNode.getMyPort()+"::::"+ node.myPort);
                        //if(currNode.getMyPort().equals(node.myPort))
                        if(count==6)
                            break;
                        else
                            continue;

                    }catch(Exception e)
                    {
                        e.printStackTrace();
                    }

                }
                System.out.println("All data deleted:"+ mCur.getCount());
                //return mCur;

            }
        }
        else if(selection.equals("@"))
        {
            System.out.println("Delete keys for this node");
            int del = db.delete(TABLE_NAME,null,null);
            System.out.println("Del:"+ del);
            return del;
        }
        else{
            System.out.println("Delete Key:"+ selection);

            //Check if only one node in DHT:
            if(DHT.size()==1&&node.getMyPort().equals(node.getPredecessorPort())&&node.getMyPort().equals(node.getSuccessorPort()))
            {
                System.out.println("Only one node to delete all data");
                System.out.println("Delete keys for this node");
                String whereClause = "keys=?";
                String[] whereArgs = new String[] { selection };
                int del=db.delete(TABLE_NAME, whereClause, whereArgs);
                //int del = db.delete(TABLE_NAME,selection,null);
                System.out.println("Del:"+ del);
                return del;
            }else{
                System.out.println("Search for given key");
                System.out.println("Key to search:"+ selection);
                String keyNode=findNodeForKey(selection,node);
                System.out.println("Get value from node:"+ keyNode);
                try{
                    Socket socket3= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(keyNode));
                    ObjectOutputStream outputStream3 = new ObjectOutputStream(socket3.getOutputStream());
                    ObjectInputStream inputStream3 = new ObjectInputStream(socket3.getInputStream());
                    Message keyMsg = new Message(node.myPort, selection,null,"DELETE VALUE");
                    System.out.println("Message sent for deletion:"+ keyMsg);
                    outputStream3.writeObject(keyMsg);
                    outputStream3.flush();
                    //String val = inputStream3.readUTF();
                    //MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
                    //mCur.newRow().add("key",selection).add("value",val);
                    //return mCur;

                }catch(Exception e)
                {
                    e.printStackTrace();
                }
            }

        }
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        //Check only one node in DHT..

        if(selection.equals("*"))
        {
            if(DHT.size()==1&&node.getMyPort().equals(node.getPredecessorPort())&&node.getMyPort().equals(node.getSuccessorPort()))
            {
                System.out.println("Only one node to query");
                SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
                qb.setTables(TABLE_NAME);
                String[] select = {"value","keys as key"};
                //String where = "key = '"+selection+"' ";
                Cursor c = qb.query(db,	select,	null,
                        selectionArgs,null, null, sortOrder);
                //Log.v("where: ", where);
                //Log.v("Value Retrieved:", String.valueOf(c.getColumnIndex("value")));
                MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
                while (c.moveToNext())
                {
                    Log.e("val:",c.getString(c.getColumnIndex("value")));
                    mCur.newRow().add("key",c.getString(c.getColumnIndex("key")))
                            .add("value",c.getString(c.getColumnIndex("value")));
                }
                return mCur;
            }
            else{
                System.out.println("Fetching all data from all nodes:");
                SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
                qb.setTables(TABLE_NAME);
                String[] select = {"value","keys as key"};

                Node currNode = node;
                MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
                int count=1;
                while(true)
                {
                    count++;
                    System.out.println("Getting value from Node:"+ currNode.getMyPort());
                    try{
                        Socket socket4= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(currNode.getMyPort()));
                        ObjectOutputStream outputStream3 = new ObjectOutputStream(socket4.getOutputStream());
                        ObjectInputStream inputStream3 = new ObjectInputStream(socket4.getInputStream());
                        Message keyMsg = new Message(node.myPort, selection,null,"GET ALL");
                        outputStream3.writeObject(keyMsg);
                        outputStream3.flush();
                        currNode = (Node)inputStream3.readObject();
                        String res = currNode.getPredecessorPort();
                        System.out.println("Received Message to append:"+ currNode.getPredecessorPort());
                        String[] arrOfStr = res.split("\n");
                        System.out.println("String length:"+ arrOfStr.length+ arrOfStr[0]);
                        System.out.println("SSSS"+ arrOfStr[0].equals(""));
                        if(!arrOfStr[0].equals(""))
                        {
                            for (String a : arrOfStr)
                            {
                                System.out.println(a);
                                String[] sp = a.split(":");
                                mCur.newRow().add("key",sp[0])
                                        .add("value",sp[1]);

                            }
                        }
                        System.out.println(currNode.getMyPort()+"::::"+ node.myPort);
                        //if(currNode.getMyPort().equals(node.myPort))
                        if(count==6)
                            break;
                        else
                            continue;

                    }catch(Exception e)
                    {
                        e.printStackTrace();
                    }

                }
                System.out.println("All data retrieved:"+ mCur.getCount());
                return mCur;

            }
        }
        else if(selection.equals("@"))
        {

                //if(DHT.size()==1&&node.getMyPort().equals(node.getPredecessorPort())&&node.getMyPort().equals(node.getSuccessorPort()))
                //{
                    System.out.println("Only one node to query");
                    SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
                    qb.setTables(TABLE_NAME);
                    String[] select = {"value","keys as key"};
                    //String where = "key = '"+selection+"' ";
                    Cursor c = qb.query(db,	select,	null,
                            selectionArgs,null, null, sortOrder);
                    //Log.v("where: ", where);
                    //Log.v("Value Retrieved:", String.valueOf(c.getColumnIndex("value")));
                    MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
                    while (c.moveToNext())
                    {
                        Log.e("val:",c.getString(c.getColumnIndex("value")));
                        mCur.newRow().add("key",c.getString(c.getColumnIndex("key")))
                                .add("value",c.getString(c.getColumnIndex("value")));
                    }
                    return mCur;
                //}

        }
        else{
            {
                if(DHT.size()==1&&node.getMyPort().equals(node.getPredecessorPort())&&node.getMyPort().equals(node.getSuccessorPort()))
                {
                    System.out.println("Only one node to query");
                    SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
                    qb.setTables(TABLE_NAME);
                    String[] select = {"value","keys as key"};
                    String where = "key = '"+selection+"' ";
                    Cursor c = qb.query(db,	select,	where,
                            selectionArgs,null, null, sortOrder);
                    //Log.v("where: ", where);
                    //Log.v("Value Retrieved:", String.valueOf(c.getColumnIndex("value")));
                    MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
                    while (c.moveToNext())
                    {
                        Log.e("val:",c.getString(c.getColumnIndex("value")));
                        mCur.newRow().add("key",selection)
                                .add("value",c.getString(c.getColumnIndex("value")));
                    }
                    return mCur;
                }
                else{
                    System.out.println("Search for given key");
                    System.out.println("Key to search:"+ selection);
                    String keyNode=findNodeForKey(selection,node);
                    System.out.println("Get value from node:"+ keyNode);
                    try{
                        Socket socket3= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(keyNode));
                        ObjectOutputStream outputStream3 = new ObjectOutputStream(socket3.getOutputStream());
                        ObjectInputStream inputStream3 = new ObjectInputStream(socket3.getInputStream());
                        Message keyMsg = new Message(node.myPort, selection,null,"GET VALUE");
                        outputStream3.writeObject(keyMsg);
                        outputStream3.flush();
                        String val = inputStream3.readUTF();
                        MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
                        mCur.newRow().add("key",selection).add("value",val);
                        return mCur;

                    }catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }




        return null;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        //Got the request to insert...



        //First check if the node is DHT contains only one value:
        if(DHT.size()==1&&node.getMyPort().equals(node.getPredecessorPort())&&node.getMyPort().equals(node.getSuccessorPort()))
        {
            System.out.println("Only one Node in DHT");
            //Enter all the key value in this node
            SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
            qb.setTables(TABLE_NAME);
            String[] select = {"value"};
            String selection = values.getAsString("key");
            String where = "keys = '"+selection+"' ";
            Cursor c = qb.query(db,	select,	where,
                    null,null, null, null);
            String A = String.valueOf(values.get("key")) ;
            values.remove("key");
            values.put("keys", A);
            if(c.getCount()!=0)
            {
                Log.d("Insert:::", "Present");
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
        else{
            System.out.println("All AVDs in DHT find the appropriate node to store key");
            String key=values.getAsString("key");
            String data = values.getAsString("value");
            String hashKey;
            System.out.println("Search for key:"+ key);
            try{
                hashKey=genHash(key);
                System.out.println("Hash:"+ hashKey);
            }catch(NoSuchAlgorithmException e)
            {
                e.printStackTrace();
            }

            String keyNode = findNodeForKey(key,node);
            System.out.println("key node:"+ keyNode);
            //Send message to this node to save the value

            try{
                Socket socket3= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(keyNode));
                ObjectOutputStream outputStream3 = new ObjectOutputStream(socket3.getOutputStream());
                ObjectInputStream inputStream3 = new ObjectInputStream(socket3.getInputStream());
                Message keyMsg = new Message(node.myPort, key,data,"SAVE KEY");
                outputStream3.writeObject(keyMsg);
                outputStream3.flush();
            }catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String findNodeForKey(String key, Node n) {

        //Send key to first node to find the approprite node to store value
        try{
            Socket socket9= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt("11108"));
            ObjectOutputStream outputStream = new ObjectOutputStream(socket9.getOutputStream());
            Message msg = new Message(myPort,key,null,"GET NODE");
            ObjectInputStream inputStream = new ObjectInputStream(socket9.getInputStream());
            System.out.println("Message to Send:"+ msg.toString());
            outputStream.writeObject(msg);
            outputStream.flush();
            String keyNode=inputStream.readUTF();
            return keyNode;
            //outputStream.close();
            //socket1.close();
        }catch(Exception e)
        {
            e.printStackTrace();

        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        //Creating all the nodes
        Context context = getContext();
        DatabaseHelper dbHelper = new DatabaseHelper(context);
        db = dbHelper.getWritableDatabase();
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        System.out.println("PORTT::"+ portStr);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        System.out.println("My Port:"+ myPort);
        try{
            System.out.println("My Port Hash:"+ genHash(portStr));
        }catch(NoSuchAlgorithmException e)
        {
            e.printStackTrace();
        }

        try {
            System.out.println("HIIII");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            System.out.println("Server Start");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch(IOException e)
        {
            e.printStackTrace();
            //Log.e(TAG,"IOException"+ e.getLocalizedMessage());
        }

        //Server has started....Create it as the first node and preceed...
        System.out.println("New Node Join..");
        node=new Node(myPort,myPort,myPort);
        Message message = new Message(myPort,myPort,myPort,"NODE JOIN");
        if(myPort.equals("11108"))
        {
            System.out.println("No need to send message create DHT here only");
            DHT.add(node);
            return (db == null)? false:true;
        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

        return false;
    }



    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];


            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            //REFERENCE::
            //https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
            //SERVER Code:::

            Log.v(TAG,"In ServerTask");
            Socket serversocket;
            BufferedReader clientMsg;
            Node nodeJoin;
            try{

                while(true)
                {
                    //Log.v(TAG, "In while");
                    serversocket = serverSocket.accept();
                    ObjectInputStream inStream1 = new ObjectInputStream(serversocket.getInputStream());
                    ObjectOutputStream outStream1 = new ObjectOutputStream(serversocket.getOutputStream());

                    Message message = (Message) inStream1.readObject();
                    System.out.println("Received Msg:"+message);
                    if(message.getMessage().equals("NODE JOIN"))
                    {
                        //Check the message: If Node Join then received by node 1 and have to check the hash value and assign successor and predessor:::
                        int p = Integer.parseInt(message.getMyPort())/2;
                        String hashPort = genHash(String.valueOf(p));

                        System.out.println("DHT size:"+ DHT.size());
                        //Check if it's first node joining::
                        if(DHT.size()==0)
                        {
                            System.out.println("First Node to join Hence Successor and predessor is null ");
                            nodeJoin = new Node(myPort,myPort,myPort);
                            DHT.add(nodeJoin);
                        }else
                        {
                            System.out.println("in else");
                            int i;
                            Node succNode, preNode,currNode=DHT.get(0);
                            for( i=0;i<DHT.size();i++)
                            {
                                currNode=DHT.get(i);

                                int currPort=Integer.parseInt(currNode.getMyPort())/2;
                                String currHash=genHash(String.valueOf(currPort));
                                if(hashPort.compareTo(currHash)<0)
                                {
                                    //the node we receive is shorter than the node in DHT, hence the given node comes before current node
                                    break;
                                }


                            }

                            System.out.println("Node Location:"+ i);


                            nodeJoin = new Node(message.getMyPort(),currNode.getMyPort(),currNode.getPredecessorPort());
                            DHT.add(i,nodeJoin);
                            Node nextNode=null;

                            //Successor

                            if(i==DHT.size()-1)
                            {
                                System.out.println("In IF..last node to insert...");
                                Node n = DHT.get(i);
                                n.setSuccessorPort(DHT.getFirst().getMyPort());
                                n.setPredecessorPort(DHT.get(i-1).getMyPort());
                                //nextNode = new Node(currNode.getMyPort(),DHT.get(0).getMyPort(),message.getMyPort());
                                nextNode=DHT.getFirst();
                                nextNode.setPredecessorPort(message.getMyPort());
                                //nextNode.setSuccessorPort(DHT.get(0).getMyPort());

                            }
                            else
                            {
                                System.out.println("Inserting in between");
                                nextNode=DHT.get(i+1);
                                nextNode.setPredecessorPort(message.getMyPort());
                                //nextNode
                            }
                            //nextNode=DHT
                            //nextNode = new Node(currNode.getMyPort(),currNode.getSuccessorPort(),message.getMyPort());

                            System.out.println("DHT TEST:::"+ DHT.toString());
                            //  if(i+1 == DHT.size())
                            // i=-1;
                            //DHT.remove(i+1);
                            //DHT.add(i+1,nextNode);

                            //Predecessor

                            Node prevNode;// = new Node(currNode.getPredecessorPort(),mes
                            if(i==0)
                            {
                                System.out.println("AA");
                                prevNode=DHT.getLast();
                            }
                            else {
                                System.out.println("BB");
                                prevNode = DHT.get(i - 1);
                            }
                            prevNode.setSuccessorPort(message.getMyPort());



                        }

                        System.out.println("Running DHT:"+ DHT.toString());

                        //Broadcast the new Setting to everyone
                        for(int j=0;j<DHT.size();j++)
                        {
                            Node n = DHT.get(j);
                            Message repMsg = new Message(n.getMyPort(),n.getSuccessorPort(),n.getPredecessorPort(),"JOINED");
                            if(n.getMyPort().equals("11108"))
                            {
                                //Preccesing done in node 1 hence not need to send to itself
                                node=n;
                            }
                            else
                            {
                                System.out.println("Sending to:"+ n.getMyPort());
                                Socket socket1=null;
                                try{
                                    socket1= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(n.getMyPort()));
                                    ObjectOutputStream outputStream = new ObjectOutputStream(socket1.getOutputStream());
                                    //ObjectInputStream inputStream = new ObjectInputStream(socket1.getInputStream());
                                    System.out.println("Message to Send:"+ repMsg.toString());
                                    outputStream.writeObject(repMsg);
                                    outputStream.flush();
                                    //outputStream.close();
                                    //socket1.close();
                                }catch(Exception e)
                                {
                                    e.printStackTrace();
                                    System.out.println("In Catch..OWN");
                                    node=n;

                                }
                                //socket.setSoTimeout(2000);

                            }

                        }

                        Message reply = new Message(nodeJoin.getMyPort(),nodeJoin.getSuccessorPort(),nodeJoin.getPredecessorPort(),"Node Joined Ack");
                        outStream1.writeObject(reply);
                        outStream1.flush();


                    }else if(message.getMessage().equals("JOINED"))
                    {
                        System.out.println("Update Node settings");
                        node.setMyPort(message.getMyPort());
                        node.setPredecessorPort(message.getPredecessorPort());
                        node.setSuccessorPort(message.getSuccessorPort());

                        System.out.println("Node New Status:"+ node.toString());
                    }
                    else if(message.getMessage().equals("KEY SEARCH"))
                    {
                        System.out.println("Node Search for key...Get next node");
                        outStream1.writeObject(node);
                        outStream1.flush();
                    }
                    else if(message.getMessage().equals("SAVE KEY"))
                    {

                        //Save the value to table
                        //In successorPort got Key and in prodessor Port got value
                        ContentValues values=new ContentValues();
                        String selection=message.getSuccessorPort();
                        String data = message.getPredecessorPort();
                        String selectionHash=null;
                        try{
                            selectionHash=genHash(selection);
                        }catch(NoSuchAlgorithmException e)
                        {
                            e.printStackTrace();
                        }
                        values.put("keys",selection);
                        values.put("value",data);
                        String where = "keys = '"+selection+"' ";
                        Log.d("Inserting where", where);
                        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
                        qb.setTables(TABLE_NAME);
                        String[] select = {"value"};

                        Cursor c = qb.query(db,	select,	where,
                                null,null, null, null);
                        //String A = String.valueOf(values.get("key")) ;
                        //values.remove("key");


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
                        String A = String.valueOf(values.get("keys")) ;
                        values.remove("keys");
                        values.put("key", A);


                        Log.v("insert", values.toString());


                    }
                    else if(message.getMessage().equals("GET NODE"))
                    {
                        System.out.println("key: "+ message.getSuccessorPort());
                        String hashKey=null;
                        String keyNode=null;
                        try{
                            hashKey=genHash(message.getSuccessorPort());
                        }catch(NoSuchAlgorithmException e)
                        {
                            e.printStackTrace();
                        }
                        //find for the correct node to store data
                        for (int i = 0; i < DHT.size(); i++) {
                            System.out.println(DHT.get(i));
                            Node n = DHT.get(i);
                            String myPostHash=null, succPortHash=null;
                            try{
                                myPostHash=genHash(String.valueOf(Integer.parseInt(n.getMyPort())/2));
                                succPortHash=genHash(String.valueOf(Integer.parseInt(n.getSuccessorPort())/2));
                            }catch (NoSuchAlgorithmException e)
                            {
                                e.printStackTrace();
                            }
                            int c1 = hashKey.compareTo(myPostHash);
                            int c2 = hashKey.compareTo(succPortHash);
                            if(c1>0 && c2<0)
                            {
                                System.out.println("Key belong to:"+ n.getSuccessorPort());
                                keyNode=n.getSuccessorPort();
                                break;
                            }
                            if(i==DHT.size()-1)
                            {
                                System.out.println("Found amongst all ..not found so value goes to 1st");
                                keyNode=DHT.get(0).getMyPort();
                            }

                        }
                        outStream1.writeUTF(keyNode);
                        outStream1.flush();
                    }
                    else if(message.getMessage().equals("GET VALUE"))
                    {

                        //Got key to saerch for corresponding value in CP
                        String selection=message.getSuccessorPort();
                        String selectionHash=null;
                        try{
                            selectionHash=genHash(selection);
                        }catch(NoSuchAlgorithmException e)
                        {
                            e.printStackTrace();
                        }
                        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
                        qb.setTables(TABLE_NAME);
                        String[] select = {"value","keys as key"};
                        String where = "key = '"+selection+"' ";
                        Cursor c = qb.query(db,	select,	where,
                                null,null, null, null);
                        //Log.v("where: ", where);
                        // Log.v("Value Retrieved:", String.valueOf(c.getColumnIndex("value")));

                        /*
                         *REFERENCE::  https://developer.android.com/reference/android/database/MatrixCursor
                         */
                        String returnValue=null;
                        MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
                        while (c.moveToNext())
                        {
                            //Log.e("val:",c.getString(c.getColumnIndex("value")));
                            returnValue=c.getString(c.getColumnIndex("value"));
                            //mCur.newRow().add("key",selection).add("value",c.getString(c.getColumnIndex("value")));
                        }
                        System.out.println("Value to return:"+ returnValue);
                        outStream1.writeUTF(returnValue);
                        outStream1.flush();
                        //return mCur;


                    }
                    else if(message.getMessage().equals("GET ALL"))
                    {
                        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
                        qb.setTables(TABLE_NAME);
                        String[] select = {"value","keys as key"};

                        Node currNode = node;
                        MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
                        String retCur = "";
                        //Get key value from this node

                        System.out.println("Getting values from node:"+ node.getMyPort());
                        Cursor c = qb.query(db,	select,	null,
                                null,null, null, null);
                        while (c.moveToNext())
                        {
                            retCur=retCur+c.getString(c.getColumnIndex("key"))+":"+c.getString(c.getColumnIndex("value"))+"\n";
                            mCur.newRow().add("key",c.getString(c.getColumnIndex("key")))
                                    .add("value",c.getString(c.getColumnIndex("value")));
                        }
                        Node returnNode = node;
                        returnNode.setMyPort(node.getSuccessorPort());
                        returnNode.setPredecessorPort(retCur);
                        System.out.println("Values Returned::"+ retCur);
                        System.out.println("Node:"+ node);
                        System.out.println("Return Node:"+ returnNode);
                        outStream1.writeObject(returnNode);
                        outStream1.flush();
                    }
                    else if(message.getMessage().equals("DELETE ALL"))
                    {
                        System.out.println("DELETING Value from node:"+ myPort);
                        int del = db.delete(TABLE_NAME,"1",null);
                        Node returnNode = node;
                        returnNode.setMyPort(node.getSuccessorPort());
                        returnNode.setPredecessorPort(String.valueOf(del));
                        System.out.println("Delete Value::"+ del);
                        System.out.println("Node:"+ node);
                        System.out.println("Return Node:"+ returnNode);
                        outStream1.writeObject(returnNode);
                        outStream1.flush();
                    }
                    else if(message.getMessage().equals("DELETE VALUE"))
                    {
                        System.out.println("Deleting from Node:"+ myPort);
                        String whereClause = "keys=?";
                        String[] whereArgs = new String[] { message.getSuccessorPort() };
                        int del=db.delete(TABLE_NAME, whereClause, whereArgs);
                        //int del = db.delete(TABLE_NAME,"keys='"+ message.getSuccessorPort()+"'",null);
                        System.out.println("Delete Status:"+ del);
                    }



                    //inStream1.close();
                    //outStream1.close();

                }
            }catch(Exception e)
            {
                ////Log.e(TAG,e.getLocalizedMessage());

                e.printStackTrace();
            }

            return null;
        }

    }



    //Client

    private class ClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {


            try {
                Message msgToSend = msgs[0];
                //Check Message...if it NODE JOin then send the message to first node i.e 11108
                if(msgToSend.getMessage().equals("NODE JOIN"))
                {
                    Socket socket= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("11108"));
                    //socket.setSoTimeout(2000);
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                    System.out.println("Message to Send:"+ msgToSend.toString());
                    outputStream.writeObject(msgToSend);
                    outputStream.flush();
                    Message replyMsg = (Message) inputStream.readObject();
                    node = new Node(myPort,replyMsg.getSuccessorPort(),replyMsg.getPredecessorPort());
                    System.out.println("My Node status:"+ node.toString());
                    //outputStream.close();
                    //inputStream.close();
                    //socket.close();

                }
            }catch(Exception e)
            {

                System.out.println("This is the only node present in DHT");
                node = new Node(myPort,myPort,myPort);
                DHT.add(node);
                e.printStackTrace();

            }

            return null;
        }
    }
}
