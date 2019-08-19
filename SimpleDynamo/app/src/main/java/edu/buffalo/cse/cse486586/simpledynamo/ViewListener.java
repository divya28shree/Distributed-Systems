package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class ViewListener implements OnClickListener {

    private static final String TAG = OnTestClickListener.class.getName();
    private static final int TEST_CNT = 50;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;
    private final ContentValues[] mContentValues;

    public ViewListener(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        mContentValues = initTestValues();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private ContentValues[] initTestValues() {
        ContentValues[] cv = new ContentValues[TEST_CNT];
        for (int i = 0; i < TEST_CNT; i++) {
            cv[i] = new ContentValues();
            cv[i].put(KEY_FIELD, "key" + Integer.toString(i));
            cv[i].put(VALUE_FIELD, "val" + Integer.toString(i));
        }

        return cv;
    }

    @Override
    public void onClick(View v) {
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    private class Task extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {
            /*if (testInsert()) {
                publishProgress("Insert success\n");
            } else {
                publishProgress("Insert fail\n");
                return null;
            }*/

            /*if (testQuery()) {
                publishProgress("Query success\n");
            } else {
                publishProgress("Query fail\n");
            }*/

            String r = testQuery();
            publishProgress(r);


            return null;
        }

        protected void onProgressUpdate(String...strings) {
            mTextView.append(strings[0]);

            return;
        }

        private boolean testInsert() {
            try {
                for (int i = 0; i < TEST_CNT; i++) {
                    mContentResolver.insert(mUri, mContentValues[i]);
                }
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                return false;
            }

            return true;
        }

        private boolean deleteQuery()
        {
            try{
                String key="@";

                mContentResolver.delete(mUri,key,null);
            }catch(Exception e)
            {
                e.printStackTrace();
                return false;
            }
            return  true;
        }

        private String testQuery() {
            String r="";
            try {
                String key="@";
                Cursor resultCursor = mContentResolver.query(mUri, null,
                        key, null, null);
                System.out.println("Query Return:"+ resultCursor.getCount());
                if(resultCursor.getCount()==0)
                    r="No DATA";
                if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                }


                int c=0;
                while (resultCursor.moveToNext()) {
                    c++;
                    //Log.e("val:",c.getString(c.getColumnIndex("value")));
                    r = r + "Key:" + resultCursor.getString(resultCursor.getColumnIndex("key")) + "Value:" + resultCursor.getString(resultCursor.getColumnIndex("value")) + "\n";
                    //mCur.newRow().add("key",selection).add("value",c.getString(c.getColumnIndex("value")));

                }

                System.out.println("Number of keys stored overall::"+ c);
                System.out.println("Node values::"+ r);
                resultCursor.close();
            } catch (Exception e) {
                e.printStackTrace();
                //return false;
            }

            return r;
        }
    }
}
