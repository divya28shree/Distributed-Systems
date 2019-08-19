package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import android.util.Log;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    private static int seqNo=0;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        // Create Server Socket

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch(IOException e)
        {
            Log.e(TAG,"IOException"+ e.getLocalizedMessage());
        }

        //REFERENCE:: https://developer.android.com/reference/android/widget/Button
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v){
                {

                    EditText editText = (EditText) findViewById(R.id.editText1);
                    String msg = editText.getText().toString() + "\n";
                    editText.setText("");

                    /*
                     * Note that the following AsyncTask uses AsyncTask.SERIAL_EXECUTOR, not
                     * AsyncTask.THREAD_POOL_EXECUTOR as the above ServerTask does. To understand
                     * the difference, please take a look at
                     * http://developer.android.com/reference/android/os/AsyncTask.html
                     */
                    Log.v(TAG, "Message to send:"+msg);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

                }
            }
        });



    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            //REFERENCE::
            //https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
            //SERVER Code:::

            Socket socket;
            BufferedReader clientMsg;
            try{

                while(true)
                {
                    Log.v(TAG, "In while");
                    socket = serverSocket.accept();
                    clientMsg =
                            new BufferedReader(
                                    new InputStreamReader(socket.getInputStream()));
                    String message = clientMsg.readLine();
                    Log.v(TAG, message+"Message Received:");
                    //REFERENCE:: https://developer.android.com/reference/android/os/AsyncTask
                    publishProgress(message);
                    socket.close();
                    clientMsg.close();

                    //Saving the data in content provider:::

                    /*
                    * REFERENCE: OnPTestClickListener.java
                    */
                   ContentValues val = new ContentValues();
                    val.put("key",seqNo);
                    seqNo++;
                    val.put("value",message);
                    getContentResolver().insert(mUri,val);
                }
            }catch(Exception e)
            {
                Log.e(TAG,e.getLocalizedMessage());
            }

            return null;
        }

        //REFERENCE:: OnPTestClickListener.java
        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append("\n"+strReceived + "\t\n");


            return;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String[] remotePort = {"11108","11112","11116","11120","11124"};

                for(int i =0;i<remotePort.length;i++)
                {
                    //The following code has been completed using the below resource::
                    //https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                    //Client Code:::
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort[i]));

                    String msgToSend = msgs[0];
                    try{
                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                        pw.println(msgToSend);
                        pw.close();
                    }catch(Exception e) {
                        e.printStackTrace();
                    }
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }
    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}
