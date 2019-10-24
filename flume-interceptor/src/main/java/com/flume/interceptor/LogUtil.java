package com.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

public class LogUtil {
    public static boolean validateStart(String log) {
        if(log == null){
            return false;
        }
        if(!log.trim().startsWith("{") || !log.trim().endsWith("}")) {
            return false;
        }
        return true;
    }

    public static boolean validateEvent(String log) {
        if(log == null){
            return false;
        }
        String[] arr = log.split("\\|");
        if(arr.length != 2){
            System.out.println(0);
            return false;
        }

        if(arr[0].length()!=13 || !NumberUtils.isDigits(arr[0])){
            System.out.println(1);
            return false;
        }
        if(!arr[1].trim().startsWith("{") || !arr[1].trim().endsWith("}")) {
            System.out.println(2);
            return false;
        }
        return true;
    }
}
