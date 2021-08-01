package com.hlink.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * @Author: hyt
 */
public class DataGenUtil {
    /**
     * 功能描述: <br>
     * 〈〉
     *
     * @param
     * @return:
     * @since: 1.0.0
     * @Author:hytma
     * @Date: 2019/9/27 21:09
     */

    static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final int[] MOBILE_PREFIX = new int[]{133, 153, 177, 180,
            181, 189, 134, 135, 136, 137, 138, 139, 150, 151, 152, 157, 158, 159,
            178, 182, 183, 184, 187, 188, 130, 131, 132, 155, 156, 176, 185, 186,
            145, 147, 170};


    public static String getRandomIp() {
        // ip范围
        int[][] range = {{607649792, 608174079}, // 36.56.0.0-36.63.255.255
                {1038614528, 1039007743}, // 61.232.0.0-61.237.255.255
                {1783627776, 1784676351}, // 106.80.0.0-106.95.255.255
                {2035023872, 2035154943}, // 121.76.0.0-121.77.255.255
                {2078801920, 2079064063}, // 123.232.0.0-123.235.255.255
                {-1950089216, -1948778497}, // 139.196.0.0-139.215.255.255
                {-1425539072, -1425014785}, // 171.8.0.0-171.15.255.255
                {-1236271104, -1235419137}, // 182.80.0.0-182.92.255.255
                {-770113536, -768606209}, // 210.25.0.0-210.47.255.255
                {-569376768, -564133889}, // 222.16.0.0-222.95.255.255
        };

        Random rdint = new Random();
        int index = rdint.nextInt(10);
        String ip = num2ip(range[index][0] + new Random().nextInt(range[index][1] - range[index][0]));
        return ip;
    }

    /*
     * 将十进制转换成IP地址
     */
    public static String num2ip(int ip) {
        int[] b = new int[4];
        String x = "";
        b[0] = (int) ((ip >> 24) & 0xff);
        b[1] = (int) ((ip >> 16) & 0xff);
        b[2] = (int) ((ip >> 8) & 0xff);
        b[3] = (int) (ip & 0xff);
        x = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) + "." + Integer.toString(b[3]);

        return x;
    }

    /**
     * 功能描述: <br>
     * 〈生成固定长度的字符串〉
     *
     * @param length
     * @return:
     * @since: 1.0.0
     * @Author:hytma
     * @Date: 2019/9/27 21:13
     */

    public static String getString(int length) {
        return RandomStringUtils.randomAlphanumeric(length);
    }

    /**
     * 功能描述: <br>
     * 〈生成范围内的随机值〉
     *
     * @param min max
     * @return:
     * @since: 1.0.0
     * @Author:hytma
     * @Date: 2019/9/27 21:15
     */
    public static Integer getRandomNumber(Integer min, Integer max) {

        Random random = new Random();

        return random.nextInt(max) % (max - min + 1) + min;
    }


    public static Date getRandomDate(String beginDate, String endDate) {
        try {

            Date start = format.parse(beginDate);
            Date end = format.parse(endDate);

            if (start.getTime() >= end.getTime()) {
                return null;
            }
            long date = random(start.getTime(), end.getTime());

            return new Date(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Date();

    }


    public static String getRandomStringDate(String beginDate, String endDate) {
        try {

            Date start = format.parse(beginDate);
            Date end = format.parse(endDate);

            if (start.getTime() >= end.getTime()) {
                return null;
            }
            long date = random(start.getTime(), end.getTime());
            //      System.out.println(date);
            String dateString = formatter.format(new Date(date));
            return dateString;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "1000-01-01 00:00:00";

    }

    public static String getUUid() {
        UUID uuid = UUID.randomUUID();
        String uuidStr = uuid.toString();

        return uuidStr;
    }


    private static long random(long begin, long end) {
        long rtn = begin + (long) (Math.random() * (end - begin));
        if (rtn == begin || rtn == end) {
            return random(begin, end);
        }
        return rtn;
    }


    public static String getRandomMobile() {
        return "" + MOBILE_PREFIX[getRandomNumber(0, MOBILE_PREFIX.length)] + StringUtils.leftPad("" + getRandomNumber(0, 99999999 + 1), 8, "0");

    }


}
