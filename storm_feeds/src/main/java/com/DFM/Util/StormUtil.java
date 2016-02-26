package com.DFM.Util;

import backtype.storm.tuple.Tuple;
import com.DFM.Client.RedisClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;


public class StormUtil {
    public static byte[] serialize(Object obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        byte[] buffer = baos.toByteArray();
        oos.close();
        baos.close();
        return buffer;
    }

    public static Object deserialize(byte[] buffer) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object obj = ois.readObject();
        ois.close();
        bais.close();
        return obj;
    }

    public static void logError(Exception e, RedisClient redisClient) {
        String fullStackTrace = getException(e);
        logError(fullStackTrace, redisClient);
    }

    public static void logError(String msg, Exception e, RedisClient redisClient) {
        String fullStackTrace = getException(e);
        logError("", msg, fullStackTrace, redisClient);
    }

    public static void logError(Tuple tuple, Exception e, RedisClient redisClient) {
        logError(tuple, "", e, redisClient);
    }

    public static void logError(Tuple tuple, String msg, Exception e, RedisClient redisClient) {
        String tupleMsg = getTuple(tuple);
        String fullStackTrace = getException(e);
        logError(tupleMsg, msg, fullStackTrace, redisClient);
    }

    private static void logError(String tupleMsg, String friendlyMsg, String exceptionMsg, RedisClient redisClient) {
        String strTuple = (!tupleMsg.equals("")) ? "Tuple(" + tupleMsg + ");" : tupleMsg;
        String strMessage = (!friendlyMsg.equals("")) ? "Message(" + friendlyMsg + ");" : friendlyMsg;
        String strException = (!exceptionMsg.equals("")) ? "Exception(" + exceptionMsg + ");" : exceptionMsg;
        String strLogMessage = strTuple + " " + strMessage + " " + strException;
        logError("Error: " + strLogMessage.replace(");", ");\r\n"), redisClient);
    }

    public static void logError(String msg, RedisClient redisClient) {
        try {
            msg = (msg == null) ? "No error message to log." : msg;
            System.out.println(msg);
            String hostname = StringUtil.getHostname();
            String time = new Date().toString().replace(":", "-");
            redisClient.set("errors:" + hostname + ":" + time, msg);
        } catch (Exception ignored) {
        }
    }

    public static void log(String msg) {
        System.out.println(msg);
    }


    private static String getTuple(Tuple tuple) {
        return "source: " + tuple.getSourceComponent() + ", stream: " + tuple.getSourceStreamId() + ", id: " + tuple.getMessageId() + ", Task: " + tuple.getSourceTask();
    }

    private static String getException(Exception e) {
        return org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e);
    }
}
