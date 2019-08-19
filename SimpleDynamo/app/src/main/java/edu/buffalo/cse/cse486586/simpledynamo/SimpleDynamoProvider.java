package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Formatter;
import java.util.LinkedList;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.renderscript.ScriptIntrinsicYuvToRGB;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	String portStr,myPort;
	static final int SERVER_PORT = 10000;
	static final LinkedList<Node> dynamoList = new LinkedList<Node>();


	String[] ports = {"11108","11112","11116","11120","11124"};

	String succ1=null;
	String succ2=null;
	String pro1=null;
	String pro2=null;
	private SQLiteDatabase db;
	static final String DATABASE_NAME = "SimpleDynamo";
	static final String TABLE_NAME = "ContentProvider";
	static final int DATABASE_VERSION = 1;
	static final String CREATE_DB_TABLE =
			" CREATE TABLE " + TABLE_NAME +
					" ( keys TEXT PRIMARY KEY, " +
					" value TEXT NOT NULL, version TEXT NOT NULL, port TEXT NOT NULL);";

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
			//System.out.println("Table deleted creating new");
			onCreate(db);
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if(selection.equals("*"))
		{}
		else if(selection.equals("@"))
		{}
		else
		{
			//Delete given record
			////System.out.println("Delete Key:"+ selection);
			String keyNode=findNodeForKey(selection);
			String keySuc1=null, keySuc2=null;
			for(int q=0;q<5;q++)
			{
				Node nq = dynamoList.get(q);
				if(nq.getMyPort().equals(keyNode))
					keySuc1=nq.getSuccessorPort();
			}
			for(int q=0;q<5;q++)
			{
				Node nq = dynamoList.get(q);
				if(nq.getMyPort().equals(keySuc1))
					keySuc2=nq.getSuccessorPort();
			}
			if(keyNode.equals(myPort))
			{
				//System.out.println("Delete in me");
				String whereClause = "keys=?";
				String[] whereArgs = new String[] { selection };
				int del=db.delete(TABLE_NAME, whereClause, whereArgs);
			}else{
				try{
					Socket socket5= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(keyNode));
					socket5.setSoTimeout(5000);
					ObjectOutputStream outputStream5 = new ObjectOutputStream(socket5.getOutputStream());
					Message keyMsg = new Message(myPort, selection,null,"DELETE VALUE IN COORDINATOR");
					outputStream5.writeObject(keyMsg);
					outputStream5.flush();

				}catch(Exception e)
				{
					e.printStackTrace();
				}}

			//Delete replica1:

			//System.out.println("Delete replica1:"+ keySuc1);
			if(keySuc1.equals(myPort))
			{
				//System.out.println("Delete in me");
				String whereClause = "keys=?";
				String[] whereArgs = new String[] { selection };
				int del=db.delete(TABLE_NAME, whereClause, whereArgs);
			}else{
				try{
					Socket socket6= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(keySuc1));
					socket6.setSoTimeout(5000);
					ObjectOutputStream outputStream6 = new ObjectOutputStream(socket6.getOutputStream());
					Message delAllKey = new Message(myPort,selection,null,"DELETE REPLICA");
					outputStream6.writeObject(delAllKey);
					outputStream6.flush();

				}catch (Exception e){
					e.printStackTrace();
				}
			}

			//delete replica2
			//System.out.println("Delete replica2:"+ keySuc2);
			if(keySuc2.equals(myPort))
			{
				//System.out.println("Delete in me");
				String whereClause = "keys=?";
				String[] whereArgs = new String[] { selection };
				int del=db.delete(TABLE_NAME, whereClause, whereArgs);
			}else{
				try{
					Socket socket7= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(keySuc2));
					socket7.setSoTimeout(5000);
					ObjectOutputStream outputStream7 = new ObjectOutputStream(socket7.getOutputStream());
					Message delAllKey = new Message(myPort,selection,null,"DELETE REPLICA");
					outputStream7.writeObject(delAllKey);
					outputStream7.flush();

				}catch (Exception e){
					e.printStackTrace();
				}
			}


		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	private synchronized String findNodeForKey(String key) {

		String hashKey=null;
		String keyNode=null;
		try{
			hashKey=genHash(key);
		}catch(NoSuchAlgorithmException e)
		{
			e.printStackTrace();
		}
		for (int i = 0; i < dynamoList.size(); i++) {
			////System.out.println(dynamoList.get(i));
			Node n = dynamoList.get(i);
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
				////System.out.println("Key belong to:"+ n.getSuccessorPort());
				keyNode=n.getSuccessorPort();
				break;
			}
			if(i==dynamoList.size()-1)
			{
				////System.out.println("Found amongst all ..not found so value goes to 1st");
				keyNode=dynamoList.get(0).getMyPort();
			}

		}

		return(keyNode);
	}


	@Override
	public Uri insert(Uri uri, ContentValues values) {


		String key=values.getAsString("key");
		String data = values.getAsString("value");
		//System.out.println("Insert Key:"+key);

		String keyNode = findNodeForKey(key);

		//System.out.println("Coordinator Node:"+ keyNode);
		String keySuc1=null, keySuc2=null;
		for(int q=0;q<5;q++)
		{
			Node nq = dynamoList.get(q);
			if(nq.getMyPort().equals(keyNode))
				keySuc1=nq.getSuccessorPort();
		}
		for(int q=0;q<5;q++)
		{
			Node nq = dynamoList.get(q);
			if(nq.getMyPort().equals(keySuc1))
				keySuc2=nq.getSuccessorPort();
		}

		//System.out.println("Sending keys to store:"+ keyNode+":"+keySuc1+":"+keySuc2);
		Message message = new Message(keyNode,keySuc1+":"+keySuc2,key+":"+data,"SEND DATA");
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

		return null;
	}

	@Override
	public boolean onCreate() {

		Collection<Node> collect = new ArrayList<Node>();
		collect.add(new Node("11124","11112","11120"));
		collect.add(new Node("11112","11108","11124"));
		collect.add(new Node("11108","11116","11112"));
		collect.add(new Node("11116","11120","11108"));
		collect.add(new Node("11120","11124","11116"));

		dynamoList.addAll(collect);


		boolean isRecovery = false;
		SharedPreferences check = this.getContext().getSharedPreferences("recovery", 0);
		if (check.getBoolean("birth", true)) {
			//System.out.println("First time");
			check.edit().putBoolean("birth", false).commit();
		} else {
			//System.out.println("rebirth");
			isRecovery = true;
		}

		//System.out.println(isRecovery);
		Context context = getContext();
		DatabaseHelper dbHelper = new DatabaseHelper(context);
		db = dbHelper.getWritableDatabase();
		dbHelper.onUpgrade(db,0,0);
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		//System.out.println("PORT:"+ portStr);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		//System.out.println("My Port:"+ myPort);

		//save succ1, succ2, pro1, pro2
		for(int i=0;i<5;i++)
		{
			//System.out.println(dynamoList.get(i));
			if(dynamoList.get(i).getMyPort().equals(myPort))
			{
				succ1=dynamoList.get(i).getSuccessorPort();
				pro1=dynamoList.get(i).getPredecessorPort();
			}

		}
		for(int i=0;i<5;i++)
		{
			if(dynamoList.get(i).getMyPort().equals(succ1))
				succ2=dynamoList.get(i).getSuccessorPort();
		}
		for(int i=0;i<5;i++) {
			if (dynamoList.get(i).getMyPort().equals(pro1))
				pro2 = dynamoList.get(i).getPredecessorPort();
		}

		//System.out.println("Main Ports:"+ succ1+":"+succ2+":"+pro1+":"+pro2);

		//Strt server
		try {
			////System.out.println("HIIII");
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			//System.out.println("Server Start");
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}catch(IOException e)
		{
			e.printStackTrace();
			////Log.e(TAG,"IOException"+ e.getLocalizedMessage());
		}

		if(isRecovery)
		{
			//System.out.println("Node recovering");
			Message message = new Message(null,null,null,"SYNC");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);


		}/*else{
			//System.out.println("Node first join start Client Task");
			Message message = new Message(null,null,null,"Join");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
		}*/
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {

		//System.out.println("Selection:"+ selection);
		if(selection.equals("@"))
		{
			MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
			//System.out.println("Get value for this node @");
			try{
				SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
				qb.setTables(TABLE_NAME);
				String[] select = {"value","keys as key","version","port"};
				Cursor c = qb.query(db,	select,	null,
						null,null, null, null);

				while (c.moveToNext())
				{
					mCur.newRow().add("key",c.getString(c.getColumnIndex("key")))
							.add("value",c.getString(c.getColumnIndex("value")));
				}
				//System.out.println("No. of data retrieved:"+ mCur.getCount());
				return mCur;
			}catch (Exception e){
				e.printStackTrace();}
		}else if(selection.equals("*"))
		{
			//* implement

			////System.out.println("Fetching all data from all nodes:");
			//I know all the node so need to send request to all nodes to send value
			MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
			//for(int sel = 0;sel<dynamoList.size();sel++)
			//int sel=0;
			String failNode=null;
			String failSuccNode=null;
			for(int sel=0;sel<5;sel++)
			{
				//System.out.println("Get data from:"+ dynamoList.get(sel));
				if(dynamoList.get(sel).equals(myPort))
				{
					//Get All my data
					try{
						SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
						qb.setTables(TABLE_NAME);
						String[] select = {"value","keys as key","version","port"};
						Cursor c = qb.query(db,	select,	null,
								null,null, null, null);

						while (c.moveToNext())
						{
							mCur.newRow().add("key",c.getString(c.getColumnIndex("key")))
									.add("value",c.getString(c.getColumnIndex("value")));
						}
						//System.out.println("No. of data retrieved:"+ mCur.getCount());
						//return mCur;
					}catch (Exception e){
						e.printStackTrace();}
				}else
				{
					//System.out.println("Ask for all data from:"+dynamoList.get(sel));
					try {
						////System.out.println("Sending req to "+ dynamoList.get(sel).getMyPort());
						Socket socket4= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(dynamoList.get(sel).getMyPort()));
						ObjectOutputStream outputStream4 = new ObjectOutputStream(socket4.getOutputStream());
						ObjectInputStream inputStream4 = new ObjectInputStream(socket4.getInputStream());
						Message keyMsg = new Message(myPort, selection,null,"GET ALL");
						outputStream4.writeObject(keyMsg);
						outputStream4.flush();
						String res = inputStream4.readUTF();
						//System.out.println("Data received from "+dynamoList.get(sel).getMyPort()+":"+res);
						if(res.equals("NO DATA"))
							continue;
						String[] arrOfStr = res.split("\n");
						if(!arrOfStr[0].equals(""))
						{
							for (String a : arrOfStr)
							{
								////System.out.println(a);
								String[] sp = a.split(":");
								mCur.newRow().add("key",sp[0])
										.add("value",sp[1]);

							}
						}

					}catch (Exception e)
					{
						////System.out.println(e.getMessage());
						//sel++;
						failNode=dynamoList.get(sel).getMyPort();
						failSuccNode=dynamoList.get(sel).getSuccessorPort();
						try{
							//Send data to get the data of failed node:
							{
								//System.out.println("Sending req to get failed node data"+ failSuccNode);
								Socket socket12= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(failSuccNode));
								ObjectOutputStream outputStream12 = new ObjectOutputStream(socket12.getOutputStream());
								ObjectInputStream inputStream12 = new ObjectInputStream(socket12.getInputStream());
								Message keyMsg = new Message(failNode, selection,null,"GET FAIL");
								outputStream12.writeObject(keyMsg);
								outputStream12.flush();
								String res = inputStream12.readUTF();
								//System.out.println("Data received from "+failSuccNode+":"+res);
								if(res.equals("NO DATA"))
									continue;
								String[] arrOfStr = res.split("\n");
								if(!arrOfStr[0].equals(""))
								{
									for (String a : arrOfStr)
									{
										////System.out.println(a);
										String[] sp = a.split(":");
										mCur.newRow().add("key",sp[0])
												.add("value",sp[1]);

									}
								}

							}

						}catch (Exception e1)
						{
							e1.printStackTrace();
						}
						continue;
						//e.printStackTrace();
					}
				}

			}
			////System.out.println("Sel:"+ sel);
			//System.out.println("Number of rows rreturned:"+ mCur.getCount());
			return mCur;

		}else
		{
			//System.out.println("Search for key:"+ selection);
			String keyNode=findNodeForKey(selection);
			MatrixCursor mCur = new MatrixCursor(new String[]{"key","value"});
			//System.out.println(keyNode+":"+myPort);
			if(keyNode.equals(myPort))
			{
				int flag=0;
				//System.out.println("My port no need to create new connection");
				//String selection=message.getSuccessorPort();
				SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
				qb.setTables(TABLE_NAME);
				String[] select = {"value","keys as key","version"};
				String where = "key = '"+selection+"' ";
				Cursor c = qb.query(db,	select,	where,
						null,null, null, null);
				////Log.v("where: ", where);
				// //Log.v("Value Retrieved:", String.valueOf(c.getColumnIndex("value")));

				/*
				 *REFERENCE::  https://developer.android.com/reference/android/database/MatrixCursor
				 */
				String val=null;
				int ver = 0;
				while (c.moveToNext())
				{
					////Log.e("val:",c.getString(c.getColumnIndex("value")));
					val=c.getString(c.getColumnIndex("value"));
					ver = Integer.parseInt(c.getString(c.getColumnIndex("version")));
					mCur.newRow().add("key",selection).add("value",c.getString(c.getColumnIndex("value")));
				}
				//System.out.println("val ret: "+ val);
				if(val.equals(null))
				{
					//System.out.println("Not found here so move on");
					flag=1;
				}else{
					mCur.newRow().add("key",selection).add("value",val);
					return mCur;
				}

				if(flag==1)
				{
					String keySuc=null;
					//System.out.println("Key Node is not running so get data from replicas");
					for( int i=0;i<5;i++)
					{
						if(dynamoList.get(i).myPort.equals(keyNode))
							keySuc=dynamoList.get(i).getSuccessorPort();

					}

					//System.out.println("Getting data from :"+ keySuc);
					try{
						Socket socket11= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(keySuc));
						socket11.setSoTimeout(5000);
						ObjectOutputStream outputStream11 = new ObjectOutputStream(socket11.getOutputStream());
						ObjectInputStream inputStream11 = new ObjectInputStream(socket11.getInputStream());
						Message keyMsg = new Message(myPort, selection,null,"GET VALUE");
						outputStream11.writeObject(keyMsg);
						outputStream11.flush();
						String val1 = inputStream11.readUTF();
						//System.out.println("Read Value:"+ val1);
						mCur.newRow().add("key",selection).add("value",val1);
						return mCur;

					}catch(Exception e)
					{
						e.printStackTrace();
					}

				}

				return mCur;
			}else
			{
				int flag=0;
				try{
					//System.out.println("In TRY1"+keyNode);
					Socket socket3= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(keyNode));
					socket3.setSoTimeout(5000);
					ObjectOutputStream outputStream3 = new ObjectOutputStream(socket3.getOutputStream());
					ObjectInputStream inputStream3 = new ObjectInputStream(socket3.getInputStream());
					Message keyMsg = new Message(myPort, selection,null,"GET VALUE");
					outputStream3.writeObject(keyMsg);
					outputStream3.flush();
					String val = inputStream3.readUTF();
					//System.out.println("Read Value:"+ val);
					if(val.equals(""))
					{
						//System.out.println("Not found here so move on");
						flag=1;
					}else{
						mCur.newRow().add("key",selection).add("value",val);
						return mCur;}

				}catch(Exception e)
				{
					flag=1;
					e.printStackTrace();
				}
				//System.out.println("Flag:"+flag);
				if(flag==1)
				{
					String keySuc=null;
					//System.out.println("Key Node is not running so get data from replicas");
					for( int i=0;i<5;i++)
					{
						if(dynamoList.get(i).myPort.equals(keyNode))
							keySuc=dynamoList.get(i).getSuccessorPort();

					}

					//System.out.println("Getting data from :"+ keySuc);
					try{
						Socket socket11= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(keySuc));
						socket11.setSoTimeout(5000);
						ObjectOutputStream outputStream11 = new ObjectOutputStream(socket11.getOutputStream());
						ObjectInputStream inputStream11 = new ObjectInputStream(socket11.getInputStream());
						Message keyMsg = new Message(myPort, selection,null,"GET VALUE");
						outputStream11.writeObject(keyMsg);
						outputStream11.flush();
						String val = inputStream11.readUTF();
						//System.out.println("Read Value:"+ val);
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
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private synchronized String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private class ClientTask extends AsyncTask<Message, Void, Void> {

		@Override
		protected synchronized Void doInBackground(Message... msgs) {


			try {
				Message message = msgs[0];

				if(message.getMessage().equals("SEND DATA"))
				{
					//Send data to coordinator
					String[] s = message.getPredecessorPort().split(":");
					String key = s[0];
					String data = s[1];
					//System.out.println("Send Data to C:"+ message.getMyPort());
					if(message.getMyPort().equals(myPort))
					{
						//Save in my CP
						String save = saveToMyCP(key,data,myPort);
						//System.out.println("Status of saving in my CP:"+ save);

					}
					else{
						try{
							Socket socket= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(message.getMyPort()));
							socket.setSoTimeout(2000);
							ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
							//ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
							Message keyMsg = new Message(message.getMyPort(), key,data,"SAVE KEY");
							////System.out.println("Message to Send:"+ keyMsg);
							outputStream.writeObject(keyMsg);
							outputStream.flush();}catch (Exception e)
						{
							e.printStackTrace();
						}
						//String re = inputStream.readUTF();
						////System.out.println("Res after save key"+ re);
					}

					//Send replica1:

					String[] succ=message.getSuccessorPort().split(":");

					//System.out.println("Replica1:"+ succ[0]);
					if(succ[0].equals(myPort))
					{
						//Save in my CP
						String save = saveToMyCP(key,data,message.getMyPort());
						//System.out.println("Status of saving in my CP:"+ save);

					}
					else{
						try{
							Socket socket1= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(succ[0]));
							//System.out.println("Soc status:"+ socket1.isConnected());
							socket1.setSoTimeout(5000);
							ObjectOutputStream outputStream1 = new ObjectOutputStream(socket1.getOutputStream());
							//ObjectInputStream inputStream1 = new ObjectInputStream(socket1.getInputStream());
							Message keyMsg = new Message(message.getMyPort(), key,data,"SAVE REPLICA");
							////System.out.println("Message to Send:"+ keyMsg);
							outputStream1.writeObject(keyMsg);
							outputStream1.flush();}catch (Exception e)
						{
							e.printStackTrace();
						}
						//String re = inputStream1.readUTF();
						////System.out.println("Resp from writing replica:"+ re);
						//outputStream1.close();
						//socket1.close();
					}

					//System.out.println("Replica2:"+ succ[1]);
					if(succ[1].equals(myPort))
					{
						//Save in my CP
						String save = saveToMyCP(key,data,message.getMyPort());
						////System.out.println("Status of saving in my CP:"+ save);

					}
					else{
						try{
							Socket socket2= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(succ[1]));
							//System.out.println("Soc status:"+ socket2.isConnected());
							socket2.setSoTimeout(2000);
							ObjectOutputStream outputStream2 = new ObjectOutputStream(socket2.getOutputStream());
							//ObjectInputStream inputStream2 = new ObjectInputStream(socket2.getInputStream());
							Message keyMsg = new Message(message.getMyPort(), key,data,"SAVE REPLICA");
							////System.out.println("Message to Send:"+ keyMsg);
							outputStream2.writeObject(keyMsg);
							outputStream2.flush();}catch (Exception e)
						{
							e.printStackTrace();
						}
						//String re = inputStream2.readUTF();
						////System.out.println("Response from saving repl 2:"+ re);
					}



				}else if(message.getMessage().equals("SYNC"))
				{
					//System.out.println("Syncing data");
					try{

						//Get data from both prodecessors
						// Pro1:
						Socket socket8= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(pro1));
						//socket8.setSoTimeout(5000);
						//System.out.println("GET pro1 data soc:"+ socket8.isConnected()+pro1);
						ObjectOutputStream outputStream8 = new ObjectOutputStream(socket8.getOutputStream());
						ObjectInputStream inputStream8 = new ObjectInputStream(socket8.getInputStream());
						Message getProKey = new Message(myPort,null,null,"GET PRO DATA");
						outputStream8.writeObject(getProKey);
						outputStream8.flush();
						//System.out.println("FLu::");
						String pro1Res = inputStream8.readUTF();
						//System.out.println("REspo received:"+ pro1Res);
						if(pro1Res.equals(""))
							System.out.println("No Data in PRO1");
						else {
							String writeResponsePro1 = saveToCP(pro1, pro1Res);
							//System.out.println("Pro1 write status:" + writeResponsePro1);
						}}catch(Exception e)
					{
						e.printStackTrace();
					}
					//socket8.close();
					try{
						//PRO2::
						//System.out.println("GET PRO2 data:"+pro2);
						Socket socket9= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(pro2));
						//socket9.setSoTimeout(5000);
						ObjectOutputStream outputStream9 = new ObjectOutputStream(socket9.getOutputStream());
						ObjectInputStream inputStream9 = new ObjectInputStream(socket9.getInputStream());
						Message getProKey = new Message(myPort,null,null,"GET PRO DATA");
						outputStream9.writeObject(getProKey);
						outputStream9.flush();
						//System.out.println("Flush 2");
						String pro2Res = inputStream9.readUTF();
						//System.out.println("PRO2 resp:"+ pro2Res);
						if(pro2Res.equals(""))
							System.out.println("No Data from pro2");
						else {
							String writeResponsePro2 = saveToCP(pro2, pro2Res);
							//System.out.println("Pro2 write status:" + writeResponsePro2);
						}}catch(Exception e){
						e.printStackTrace();
					}
					try{
						//SUC1:
						Socket socket10= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(succ1));
						//socket9.setSoTimeout(5000);
						ObjectOutputStream outputStream10 = new ObjectOutputStream(socket10.getOutputStream());
						ObjectInputStream inputStream10 = new ObjectInputStream(socket10.getInputStream());
						Message getSucKey = new Message(myPort,null,null,"GET SUC DATA");
						outputStream10.writeObject(getSucKey);
						outputStream10.flush();
						String suc1Res = inputStream10.readUTF();
						//System.out.println("Succ Resp:"+ suc1Res);
						if(suc1Res.equals(""))
							System.out.println("No Data from successor");
						else{
							String writeResponseSuc1 = saveToCP(myPort,suc1Res);
							//System.out.println("Suc1 write status:"+ writeResponseSuc1);
						}



					}catch(Exception e){
						e.printStackTrace();
					}

				}else{
					//System.out.println("Joined");
				}
			}catch(Exception e)
			{

				e.printStackTrace();
			}

			return null;
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

		@Override
		protected synchronized Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			//Log.v(TAG,"In ServerTask");
			Socket serversocket;
			try{
				while(true)
				{
					serversocket = serverSocket.accept();
					//Log.e(TAG,"Accepted soc");
					ObjectInputStream inStream1 = new ObjectInputStream(serversocket.getInputStream());
					//Log.e(TAG,"Got Input Stream");
					ObjectOutputStream outStream1 = new ObjectOutputStream(serversocket.getOutputStream());
					//Log.e(TAG,"Get Out Stream");
					//System.out.println(inStream1.read());
					Message message = (Message) inStream1.readObject();
					//System.out.println("Received Msg:"+message);
					//inStream1.close();
					if(message.getMessage().equals("SAVE KEY")){

						//This is the coordinator node

						//Save own
						ContentValues values=new ContentValues();
						String selection=message.getSuccessorPort();
						String data = message.getPredecessorPort();
						values.put("keys",selection);
						values.put("value",data);
						values.put("port",message.getMyPort());
						String where = "keys = '"+selection+"' ";
						//Log.d("Inserting where", where);
						SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
						qb.setTables(TABLE_NAME);
						String[] select = {"value","version"};

						Cursor c = qb.query(db,	select,	where,
								null,null, null, null);


						if(c.getCount()!=0)
						{
							//Log.d("Insert:::", "Present");

							//The given key is already present, update the version
							//Read the present version:
							while (c.moveToNext())
							{
								//Log.e("val:",c.getString(c.getColumnIndex("version")));
								String ver = c.getString(c.getColumnIndex("version"));
								String newVer = String.valueOf(Integer.parseInt(ver)+1);
								values.put("version",newVer);
							}
							//where = "keys="+A;
							//Log.d("Where", where);

							int count = db.update(TABLE_NAME,values,where,null);
							//Log.d("Count::", String.valueOf(count));
						}
						else
						{
							//Log.d("Insert::", "Not Present1");
							values.put("version","1");
							long rowId = db.insert(TABLE_NAME,null,values);
						}

						//outStream1.writeUTF("DONE");
						//outStream1.flush();


					}else if(message.getMessage().equals("SAVE REPLICA")){

						//This is the coordinator node

						//Save own
						ContentValues values=new ContentValues();
						String selection=message.getSuccessorPort();
						String data = message.getPredecessorPort();
						values.put("keys",selection);
						values.put("value",data);
						values.put("port",message.getMyPort());
						String where = "keys = '"+selection+"' ";
						//Log.d("Inserting where", where);
						SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
						qb.setTables(TABLE_NAME);
						String[] select = {"value","version"};

						Cursor c = qb.query(db,	select,	where,
								null,null, null, null);


						if(c.getCount()!=0)
						{
							//Log.d("Insert:::", "Present");

							//The given key is already present, update the version
							//Read the present version:
							while (c.moveToNext())
							{
								//Log.e("val:",c.getString(c.getColumnIndex("version")));
								String ver = c.getString(c.getColumnIndex("version"));
								String newVer = String.valueOf(Integer.parseInt(ver)+1);
								values.put("version",newVer);
							}
							//where = "keys="+A;
							//Log.d("Where", where);

							int count = db.update(TABLE_NAME,values,where,null);
							//Log.d("Count::", String.valueOf(count));
						}
						else
						{
							//Log.d("Insert::", "Not Present1");
							values.put("version","1");
							long rowId = db.insert(TABLE_NAME,null,values);

						}
						//outStream1.writeUTF("DONE");
						//outStream1.flush();


					}else if(message.getMessage().equals("GET VALUE")){
						//System.out.println("In Cordinator: Get value");
						String selection=message.getSuccessorPort();
						SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
						qb.setTables(TABLE_NAME);
						String[] select = {"value","keys as key","version"};
						String where = "key = '"+selection+"' ";
						Cursor c = qb.query(db,	select,	where,
								null,null, null, null);
						////Log.v("where: ", where);
						// //Log.v("Value Retrieved:", String.valueOf(c.getColumnIndex("value")));

						/*
						 *REFERENCE::  https://developer.android.com/reference/android/database/MatrixCursor
						 */
						String returnValue="";
						String val="";
						int ver = 0;
						while (c.moveToNext())
						{
							val=c.getString(c.getColumnIndex("value"));
							ver = Integer.parseInt(c.getString(c.getColumnIndex("version")));
						}
						returnValue=val;
						//System.out.println("Value return:"+ returnValue);

						outStream1.writeUTF(returnValue);
						outStream1.flush();
					}else if(message.getMessage().equals("GET ALL")){

						//Got message to send all my key...but i need to check if my version is recent or not
						SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
						qb.setTables(TABLE_NAME);
						String[] select = {"value","keys as key","version"};
						//Get all values that corresponds only to me
						String where = "port = '"+myPort+"' ";
						Cursor c = qb.query(db,	select,	where,
								null,null, null, null);
						String retVal="";
						while (c.moveToNext())
						{
							String k = c.getString(c.getColumnIndex("key"));
							String val = c.getString(c.getColumnIndex("value"));
							retVal=retVal+k+":"+val+"\n";

						}
						if(retVal==null)
							retVal="NO DATA";
						outStream1.writeUTF(retVal);
						outStream1.flush();
					}else if(message.getMessage().equals("GET FAIL")){

						//Got message to send all my key...but i need to check if my version is recent or not
						SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
						qb.setTables(TABLE_NAME);
						String[] select = {"value","keys as key","version"};
						//Get all values that corresponds only to me
						String where = "port = '"+message.getMyPort()+"' ";
						Cursor c = qb.query(db,	select,	where,
								null,null, null, null);
						String retVal="";
						while (c.moveToNext())
						{
							String k = c.getString(c.getColumnIndex("key"));
							String val = c.getString(c.getColumnIndex("value"));
							retVal=retVal+k+":"+val+"\n";

						}
						if(retVal==null)
							retVal="NO DATA";
						outStream1.writeUTF(retVal);
						outStream1.flush();
					}
					else if(message.getMessage().equals("DELETE VALUE IN COORDINATOR"))
					{
						//System.out.println("Deleting from Node:"+ myPort);
						String whereClause = "keys=?";
						String[] whereArgs = new String[] { message.getSuccessorPort() };
						int del=db.delete(TABLE_NAME, whereClause, whereArgs);
						//System.out.println("del status in co:"+ del);
					}else if(message.getMessage().equals("DELETE REPLICA"))
					{
						//System.out.println("DELETING REPLICA ");
						String whereClause = "keys=?";
						String[] whereArgs = new String[] { message.getSuccessorPort() };
						int del=db.delete(TABLE_NAME, whereClause, whereArgs);
						//System.out.println("del status rep:"+ del);
					}else if(message.getMessage().equals("GET PRO DATA"))
					{
						//System.out.println("GET PRO DATA");
						SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
						qb.setTables(TABLE_NAME);
						String[] select = {"value","keys as key","version"};
						String where = "port = '"+myPort+"' ";
						Cursor c = qb.query(db,	select,	where,
								null,null, null, null);
						//System.out.println("Number of data in table:"+ c.getCount());
						String returnValue="";
						while (c.moveToNext())
						{
							//System.out.println("Writing");
							returnValue=returnValue+c.getString(c.getColumnIndex("key"))+":"+c.getString(c.getColumnIndex("value"))+":"+c.getString(c.getColumnIndex("version"))+"\n";
						}
						//System.out.println("Value to return:"+ returnValue);
						outStream1.writeUTF(returnValue);
						outStream1.flush();

					}else if(message.getMessage().equals("GET SUC DATA"))
					{
						{
							//System.out.println("GET SUC DATA");
							SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
							qb.setTables(TABLE_NAME);
							String[] select = {"value","keys as key","version"};
							String where = "port = '"+message.getMyPort()+"' ";
							Cursor c = qb.query(db,	select,	where,
									null,null, null, null);
							//System.out.println("Number of data in table:"+ c.getCount());
							String returnValue="";
							while (c.moveToNext())
							{
								//System.out.println("Writing");
								returnValue=returnValue+c.getString(c.getColumnIndex("key"))+":"+c.getString(c.getColumnIndex("value"))+":"+c.getString(c.getColumnIndex("version"))+"\n";
							}
							//System.out.println("Value to return:"+ returnValue);
							outStream1.writeUTF(returnValue);
							outStream1.flush();

						}
					}


				}
			}catch (Exception e)
			{
				e.printStackTrace();
			}
			return  null;
		}

	}

	private synchronized String saveToCP(String Port, String Data) {

		ContentValues values=new ContentValues();
		String[] data=Data.split("\n");
		for(int i=0;i<data.length;i++)
		{
			String[] d = data[i].split(":");
			values.put("keys",d[0]);
			values.put("value",d[1]);
			//values.put("version",d[2]);
			values.put("port",Port);
			String where = "keys = '"+d[0]+"' ";
			SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
			qb.setTables(TABLE_NAME);
			String[] select = {"value","version"};
			Cursor c = qb.query(db,	select,	where,
					null,null, null, null);
			if(c.getCount()!=0)
			{
				//Log.d("Insert:::", "Present");

				//The given key is already present, update the version
				//Read the present version:
				while (c.moveToNext())
				{
					//Log.e("val:",c.getString(c.getColumnIndex("version")));
					String ver = c.getString(c.getColumnIndex("version"));
					String newVer = String.valueOf(Integer.parseInt(ver)+1);
					values.put("version",newVer);
				}
				//where = "keys="+A;
				//Log.d("Where", where);

				int count = db.update(TABLE_NAME,values,where,null);
				//Log.d("Count::", String.valueOf(count));
			}
			else
			{
				//Log.d("Insert::", "Not Present1");
				values.put("version",d[2]);
				long rowId = db.insert(TABLE_NAME,null,values);


			}
			String A = String.valueOf(values.get("keys")) ;
			//values.remove("keys");
			//values.put("key", A);

		}
		return "Done";
	}

	private synchronized String saveToMyCP(String key, String data, String myPort) {


		ContentValues values=new ContentValues();
		values.put("keys",key);
		values.put("value",data);
		values.put("port",myPort);
		String where = "keys = '"+key+"' ";
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(TABLE_NAME);
		String[] select = {"value","version"};
		Cursor c = qb.query(db,	select,	where,
				null,null, null, null);
		if(c.getCount()!=0)
		{
			//Log.d("Insert:::", "Present");

			//The given key is already present, update the version
			//Read the present version:
			while (c.moveToNext())
			{
				//Log.e("val:",c.getString(c.getColumnIndex("version")));
				String ver = c.getString(c.getColumnIndex("version"));
				String newVer = String.valueOf(Integer.parseInt(ver)+1);
				values.put("version",newVer);
			}
			//where = "keys="+A;
			//Log.d("Where", where);

			int count = db.update(TABLE_NAME,values,where,null);
			//Log.d("Count::", String.valueOf(count));
		}
		else
		{
			//Log.d("Insert::", "Not Present1");
			values.put("version",1);
			long rowId = db.insert(TABLE_NAME,null,values);


		}
		return "Done";

	}
}


