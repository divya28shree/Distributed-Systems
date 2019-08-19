package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.Context;
import android.nfc.Tag;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.widget.TextView;
import android.content.ContentValues;
import android.net.Uri;
import android.os.AsyncTask;
import android.view.View;
import android.widget.EditText;


import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import android.util.Log;

//import org.apache.commons;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.HashMap;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.Arrays;

import android.telephony.TelephonyManager;

import android.provider.Settings.Secure;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static String[] remotePort = {"11108","11112","11116","11120","11124"};
    static String failedPort=null;
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    private int seqNo=0;
    private static double ownPriority=0.0;
    private int globalSeq=0;
    private static PriorityQueue<QueueElement> priorityQueue = new PriorityQueue();
    private String portStr = "";
    public final HashMap<String,String> PEs =new HashMap<String,String>();
    HashMap<String, List<Double>> messageMap = new HashMap<String, List<Double>>();
    //private static PriorityQueue<Proposals> proposalQueue = new PriorityQueue<Proposals>();
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();

    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        PEs.put("5554","1");
        PEs.put("5556","2");
        PEs.put("5558","3");
        PEs.put("5560","4");
        PEs.put("5562","5");

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        //Log.e(TAG,"Tel:"+ tel.getLine1Number());
        //Log.e(TAG,"portStr:"+ portStr);
        //Log.e(TAG,"MyPort:"+ myPort);
        ownPriority=0.0;
        ownPriority = ownPriority+Double.parseDouble(PEs.get(portStr))/10;

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


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch(IOException e)
        {
            //Log.e(TAG,"IOException"+ e.getLocalizedMessage());
        }


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
                    //Log.v(TAG, "Message to send:"+msg);

                    // set own priority:
                    //Log.e(TAG,"$$$$$$$$$$$$$$$$$$$$$$");
                    //Log.v(TAG,"Running on avd:"+ myPort+":"+portStr);
                    Message1 message = new Message1(msg,1, myPort,0);
                    //ownPriority = ownPriority+1;

                    /*Double max=0.0;
                    Iterator<QueueElement> value = priorityQueue.iterator();
                    while (value.hasNext()) {
                        QueueElement q = value.next();
                        if(max<q.getProposedPriority())
                            max=q.getProposedPriority();

                    }
                    if(((int)(max%10))>((int)(ownPriority%10)))
                    {
                        //Log.v(TAG,"changing priorities");
                        int dec = (int)((ownPriority*10) % 10);
                        int va = (int)((max+1)%10);
                        ownPriority=(Double.valueOf(va)*10+dec)/10;
                    }
                    maxProposalSeen=ownPriority;
                    Message1 message = new Message1(msg,1, myPort,0);
                    //Log.v(TAG,"Priority set by 0:"+ownPriority);
                    QueueElement qe = new QueueElement(msg,ownPriority,1);
                    priorityQueue.add(qe);

                    value = priorityQueue.iterator();
                    //Log.e(TAG,"Size of PQ:"+ priorityQueue.size());
                    while (value.hasNext()) {
                        QueueElement q = value.next();
                        //Log.d(TAG,"VALUE::::"+q.getMsg()+"P:"+q.getProposedPriority());
                    }

                    messageMap.put(msg,new ArrayList<Double>());
                    messageMap.get(msg).add(ownPriority);*/
                    //Ask for proposal from others

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                }
            }
        });

    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

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

            //Log.v(TAG,"In ServerTask");
            Socket socket;
            BufferedReader clientMsg;
            try{

                while(true)
                {
                    //Log.v(TAG, "In while");
                    socket = serverSocket.accept();
                    //clientMsg = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    //String message = clientMsg.readLine();
                    ObjectInputStream inStream1 = new ObjectInputStream(socket.getInputStream());
                    ObjectOutputStream outStream1 = new ObjectOutputStream(socket.getOutputStream());
                    Message1 message = (Message1) inStream1.readObject();


                    //Got the message now setting the proposal for priority

                    //Log.v(TAG,"Object message Received:"+ message.getMsg()+"Type: "+ message.getMsgType()+"\nSenderPort:"+ message.getSenderPort());

                    //Check type

                    if(message.getMsgType()==1)
                    {
                        //message has come for proposal
                        //check if the message is received by itself bcoz priority is already set for itself. Hence set priority if sender and receiver is not same.
                        //Log.e(TAG,"TEST"+ String.valueOf((Integer.parseInt(message.getSenderPort()) / 2))+":  "+ portStr);
                       /* if(String.valueOf((Integer.parseInt(message.getSenderPort()) / 2)).equals(portStr))
                        {
                            //Log.v(TAG,"Got Own message so not writing");
                            //break;

                        }
                        else
                        {*/
                            //Log.v(TAG,"Got message to propose priority");
                            ownPriority=ownPriority+1;
                            //Check if ther is any number in queue with value > ownPriority
                            QueueElement x;
                            Double max=0.0;
                            Iterator<QueueElement> value = priorityQueue.iterator();
                            while (value.hasNext()) {
                                QueueElement q = value.next();
                                if(max<q.getProposedPriority())
                                    max=q.getProposedPriority();

                            }
                            if(((int)(max%10))>((int)(ownPriority%10)))
                            {
                                //Log.v(TAG,"changing priorities");
                                int dec = (int)((ownPriority*10) % 10);
                                int v = (int)((max+1)%10);
                                ownPriority=(Double.valueOf(v)*10+dec)/10;
                            }

                            //Log.d(TAG,"Priority proposed by "+ portStr+" is:"+ownPriority);
                            QueueElement qe = new QueueElement(message.getMsg(),ownPriority,1,message.getSenderPort());
                            priorityQueue.add(qe);
                            //send proposed priority to sender
                            Message1 proposalMsg = new Message1(message.getMsg(),3,message.getSenderPort(),ownPriority);
                            outStream1.flush();
                            outStream1.writeObject(proposalMsg);
                            outStream1.flush();



                        //}
                    }
                    else if(message.getMsgType()==2)
                    {
                        //Log.v(TAG,"Received final Priority"+ message.getMsg()+"\nPri:"+ message.getProposal());
                        //The Received message is the message with final priority, make the message deliverable
                        //Remove the element form priority queue update and readd
                        //Print all element in priority queue:
                        QueueElement removeElement=null;
                        Iterator<QueueElement> value = priorityQueue.iterator();
                        while (value.hasNext()) {
                            QueueElement q = value.next();
                            //Log.d(TAG,"VALUE::::"+q.getMsg()+"P:"+q.getProposedPriority());
                            if(q.getMsg().equals(message.getMsg()))
                                removeElement=q;

                        }
                        //Log.v(TAG,removeElement.getMsg()+":"+removeElement.getProposedPriority());
                        //Log.e(TAG,"######################");
                        //Object[] arr = priorityQueue.toArray();
                        /*for(int i=0;i<arr.length;i++)
                        {
                            QueueElement q = (QueueElement)arr[i];
                            if(q.getMsg().equals(message.getMsg()))
                                removeElement=q;
                            //Log.v(TAG,removeElement.getMsg()+":"+removeElement.getProposedPriority());
                        }*/
                        ////Log.e(TAG,priorityQueue.toArray().toString());
                        QueueElement qe = new QueueElement(message.getMsg(),ownPriority,1,message.getSenderPort());
                        boolean r = priorityQueue.remove(removeElement);
                        //Log.d(TAG, "removed status:"+ r);
                        //qe.setProposedPriority(message.getProposal());
                        globalSeq++;
                        qe.setProposedPriority(globalSeq);
                        qe.setState(2);
                        //Log.v(TAG,"Proposal Set Val:"+ qe.getProposedPriority());
                        priorityQueue.add(qe);



                        //check if the head of the priority queue is set to deliverable

                        deliver(priorityQueue);

                        //Saving the data in content provider:::
                        /*Message1 ackMsg = new Message1(message.getMsg(),10,message.getSenderPort(),0);
                        outStream1.flush();
                        outStream1.writeObject(ackMsg);
                        outStream1.flush();*/


                    }

                    inStream1.close();
                    outStream1.close();

                }
            }catch(Exception e)
            {
                ////Log.e(TAG,e.getLocalizedMessage());
                e.printStackTrace();
            }

            return null;
        }

        private void deliver(PriorityQueue<QueueElement> priorityQueue) {

            //Log.e(TAG, "In deliver");
            //print all the element in PQ
            Iterator<QueueElement> value = priorityQueue.iterator();
            while (value.hasNext()) {
                QueueElement q = value.next();
                Log.d(TAG,"\nVALUE::::"+q.getMsg()+"P:"+q.getProposedPriority()+"  D Status:"+q.getState());

            }

            while (!priorityQueue.isEmpty()) {
                QueueElement qe = priorityQueue.peek();
                //Log.e(TAG,"Non empty queue");
                //Log.e(TAG,"Deliverable Status:"+qe.getState());
                if(failedPort==null)
                    System.out.println("null");
                else
                {
                    String pp = ""+(Integer.parseInt(failedPort)/2);
                    //System.out.println("Failed PE:"+ PEs.get(pp));
                }

                if(qe.getState()==2)
                {
                    //System.out.println("Failed Port"+ failedPort);

                    //System.out.println("Queue Length:"+ priorityQueue.size());
                    //System.out.println("Message to remove:"+ qe.getMsg());
                    //Log.e(TAG,"PN:"+ qe.getProposedPriority());
                   // Log.e(TAG,"CP: key:"+ seqNo+" VAlue:"+ qe.getMsg()+"source:"+ qe.getSourcePort());
                    priorityQueue.remove();
                    //System.out.println("Queue Length:"+ priorityQueue.size());
                    ContentValues val = new ContentValues();
                    //val.put("key",qe.getProposedPriority());
                    val.put("key",seqNo);
                    seqNo++;
                    val.put("value",qe.getMsg());
                    getContentResolver().insert(mUri,val);
                    publishProgress(qe.getMsg()+seqNo);

                } else{
                    //priorityQueue.add(qe);
                    if(failedPort==null)
                    {
                        //System.out.println("failed port NULL");
                    }
                    else
                    {
                        //System.out.println("failed port not NULL");
                        //System.out.println("Source Port:"+ qe.getSourcePort()+":"+ failedPort);

                        if(qe.getSourcePort().equals(failedPort))
                        {
                            System.out.print("hahahahaha");
                            priorityQueue.remove();
                            continue;
                        }

                    }
                    break;
                }

            }
        }


        //REFERENCE:: OnPTestClickListener.java


        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");


            return;
        }
    }



    private class ClientTask extends AsyncTask<Message1, Void, Void> {

        @Override
        protected Void doInBackground(Message1... msgs) {

            HashMap<String,List<Double>> messageHM=new HashMap<String,List<Double>>();
            ObjectInputStream inputStream=null;
            try {

                Socket socket=null;
                int failFlag=0;
               // Log.e(TAG,"port length"+ remotePort.length);
                for(int i =0;i<remotePort.length;i++) {
                    //The following code has been completed using the below resource::
                    //https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                    //Client Code:::
                    if (failedPort == null){
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort[i]));
                    socket.setSoTimeout(2000);
                    Message1 msgToSend = msgs[0];
                    //Log.e(TAG, "Message to Send  to : " + remotePort[i] + "::" + msgToSend.getMsg());
                    try {
                        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                        inputStream = new ObjectInputStream(socket.getInputStream());
                        outputStream.writeObject(msgToSend);
                        //pw.close();
                        outputStream.flush();
                        //outputStream.close();
                    } catch (Exception e) {
                        //System.out.println("In socket time out for port:" + remotePort[i]);
                        failedPort = remotePort[i];
                        failFlag=1;

                        //Send failed status to all alive processors:

                       // for(int j=0;j<remotePort.length;j++)


                        /*Log.e(TAG, "Failed Port:" + failedPort);
                        String[] temp = new String[4];
                        int x = 0;
                        for (int j = 0; j < remotePort.length; j++) {
                            if (remotePort[j] == failedPort)
                                continue;
                            else {
                                temp[x] = remotePort[j];
                                x++;
                            }

                        }
                        remotePort = temp;
                        ////System.out.println("Left Ports:"+ remotePort.toString());
                        i--;*/
                        //e.printStackTrace();
                    }


                    //Reading messages from all PEs
                        if(failFlag==0)
                        {
                            Message1 recMsg = (Message1) inputStream.readObject();

                            if (messageHM.containsKey(recMsg.getMsg())) {
                                messageHM.get(recMsg.getMsg()).add(recMsg.getProposal());
                            } else {
                                messageHM.put(recMsg.getMsg(), new ArrayList<Double>());
                                messageHM.get(recMsg.getMsg()).add(recMsg.getProposal());
                            }
                        }

                }
                else
                    {
                        if(failedPort.equals(remotePort[i]))
                            continue;
                        else
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remotePort[i]));
                            //socket.setSoTimeout(500);
                            Message1 msgToSend = msgs[0];
                            //Log.e(TAG, "Message to Send  to : " + remotePort[i] + "::" + msgToSend.getMsg());
                            try {
                                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                                inputStream = new ObjectInputStream(socket.getInputStream());
                                outputStream.writeObject(msgToSend);
                                //pw.close();
                                outputStream.flush();
                                //outputStream.close();
                            } catch (Exception e) {
                                //System.out.println("In socket time out for port 2:" + remotePort[i]);
                                failedPort = remotePort[i];

                                /*Log.e(TAG, "Failed Port:" + failedPort);
                                String[] temp = new String[4];
                                int x = 0;
                                for (int j = 0; j < remotePort.length; j++) {
                                    if (remotePort[j] == failedPort)
                                        continue;
                                    else {
                                        temp[x] = remotePort[j];
                                        x++;
                                    }

                                }
                                remotePort = temp;
                                ////System.out.println("Left Ports:"+ remotePort.toString());
                                i--;*/
                                e.printStackTrace();
                            }


                            //Reading messages from all PEs

                            Message1 recMsg = (Message1) inputStream.readObject();

                            if (messageHM.containsKey(recMsg.getMsg())) {
                                messageHM.get(recMsg.getMsg()).add(recMsg.getProposal());
                            } else {
                                messageHM.put(recMsg.getMsg(), new ArrayList<Double>());
                                messageHM.get(recMsg.getMsg()).add(recMsg.getProposal());
                            }
                        }
                    }
                }

                socket.close();

                //check for max
                //Log.v(TAG,"1");
                if(!messageHM.isEmpty())
                {
                    //System.out.println("1");
                    Iterator it = messageHM.entrySet().iterator();
                    while(it.hasNext())
                    {
                        Map.Entry pair = (Map.Entry)it.next();
                        //System.out.println(pair.getKey() + " = " + pair.getValue());
                    }
                    it = messageHM.entrySet().iterator();
                    while (it.hasNext()) {
                        //check if what all msg has all proposals
                        Map.Entry pair = (Map.Entry)it.next();
                        //System.out.println(pair.getKey() + " = " + pair.getValue());
                        //it.remove(); // avoids a ConcurrentModificationException
                        String message=pair.getKey().toString();
                        List<Double> msgList = messageHM.get(message);
                        Double maxPriority;
                        int alive=0;
                        if(failedPort==null)
                            alive=5;
                        else
                            alive=4;
                        if(msgList.size()==alive)
                        {
                            //Log.d(TAG,"Proposals");
                            maxPriority = Collections.max(msgList);
                            //Log.v(TAG,"Final Proposal"+maxPriority);
                        }
                        else
                            continue;

                        //System.out.println("Sending final Proposal");
                        Message1 finalProposalMsg = new Message1(message,2,"",maxPriority);

                        //System.out.println("port length"+ remotePort.length);
                        for(int i =0;i<remotePort.length;i++)
                        {
                            if(failedPort==null) {
                                //Log.e(TAG, "Send " + finalProposalMsg.getMsg() + " to " + remotePort[i]);
                                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(remotePort[i]));
                                try {
                                    ObjectOutputStream outputStream = new ObjectOutputStream(socket2.getOutputStream());
                                    inputStream = new ObjectInputStream(socket2.getInputStream());
                                    outputStream.writeObject(finalProposalMsg);
                                    //pw.close();
                                    outputStream.flush();
                                    //outputStream.close();
                                } catch (Exception e) {
                                    //System.out.println("POrt Not getting connected:"+ Integer.parseInt(remotePort[i]));
                                    failedPort=remotePort[i];
                                    e.printStackTrace();
                                }
                            }
                            else
                            {
                                if(failedPort.equals(remotePort[i]))
                                    continue;
                                else
                                {
                                   // Log.e(TAG, "Send " + finalProposalMsg.getMsg() + " to " + remotePort[i]);
                                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(remotePort[i]));
                                    try {
                                        ObjectOutputStream outputStream = new ObjectOutputStream(socket2.getOutputStream());
                                        inputStream = new ObjectInputStream(socket2.getInputStream());
                                        outputStream.writeObject(finalProposalMsg);
                                        //pw.close();
                                        outputStream.flush();
                                        //outputStream.close();
                                    } catch (Exception e) {
                                        failedPort=remotePort[i];
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }


                    }

                }



            } catch (UnknownHostException e) {
                //Log.e(TAG, "ClientTask UnknownHostException");
            } catch (SocketTimeoutException e) {
                //Log.e(TAG, "ClientTask socket IOException");
                //Log.e(TAG,e.getMessage());
                failedPort=portStr;
                String [] temp=null;
                int x=0;
                for(int i =0;i<remotePort.length;i++)
                {
                    if(remotePort[i]==failedPort)
                        continue;
                    else{
                        temp[x]=remotePort[i];
                        x++;
                    }

                }
                e.printStackTrace();
            }catch(IOException e)
            {
                e.printStackTrace();


            }catch(Exception e)
            {
                e.printStackTrace();
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
