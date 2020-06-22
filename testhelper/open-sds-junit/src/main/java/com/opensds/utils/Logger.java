package com.opensds.utils;

public class Logger {

    /**
     * Print log in string.
     *
     * @param message Message
     */
    public static void logString(String message) {
        System.out.println(message);
    }


    /**
     * Print log in int.
     *
     * @param message Message
     */
    public static void logInt(Integer message) {
        System.out.println(message);
    }

    /**
     * Print log in double.
     *
     * @param message Message
     */
    public static void logDouble(Double message) {
        System.out.println(message);
    }

    /**
     * Print log in object.
     *
     * @param message Message
     */
    public static void logObject(Object message) {
        System.out.println(message);
    }
}
