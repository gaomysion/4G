package com.asiainfo.socket;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangjing5 on 2016/4/18.
 */
public class FlumeSource extends AbstractSource implements Configurable,PollableSource {
    private final static Logger logger = Logger.getLogger(FlumeSource.class);
    //public static LinkedBlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<Socket>(Integer.MAX_VALUE);
    public static LinkedBlockingQueue<byte[]> msgQueue = new LinkedBlockingQueue<byte[]>(Integer.MAX_VALUE);
    //private ExecutorService threadPool = Executors.newCachedThreadPool();
    private int analysisNum = 100;
    private String socketPort;
    private String socketIP;
    private static final String HEADERS_KEY = "key";
    private static final String MESSAGE_SEPARATOR = "\n";
    private static final String HEADERS_KEY_SEPARATOR = "\\|";


    @Override
    public void configure(Context context) {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(Constants.PROPERTIE_FILENAME);
        Properties props = new Properties();
        String port ;

        int recvworks;
        try {
            props.load(inputStream);
            analysisNum = Integer.parseInt(props.getProperty(Constants.ANALYSIS_THREAD_NUM));
            socketPort = props.getProperty(Constants.SOCKETSERVER_PORT);
            port = context.getString("port","5001");
            socketPort = port;
            logger.info("socketPort ="+socketPort);
            logger.info("load config ["+Constants.PROPERTIE_FILENAME+"]");

            recvworks = context.getInteger("maxworks", analysisNum);
            analysisNum = recvworks;

        } catch (IOException e) {
            logger.error("could not load config",e);
        }
    }

    @Override
    public void start() {
        String [] ports = socketPort.split(" ");

        for (String port : ports) {
            logger.info("port is " + port);
            new Thread(new SocketServer(port)).start();
        }
        logger.info("server socket,analysis task start...");
    }

    @Override
    public void stop () {
        // Disconnect from external client and do any additional cleanup
        super.stop();
    }

        @Override
    public Status process() {
            int off;
            int flag=0;
            int flage=0;
            long startTime=System.currentTimeMillis();
            try {
                byte[] msg = msgQueue.take();
                //off =1 ,跳过一位buffer[0]，这一位字节表示，XDR数据类型：1：合成XDR数据 2：单接口XDR数据
                off=1;
                //因为每次传输的数据并不是一条记录，所以需要对数据进行拆分解析
                String[] messages = getMessages(msg,off).split(MESSAGE_SEPARATOR);
                for (String signalling : messages){
                    flag++;
                    Map<String, String> headers = new HashMap();
                    //第6字段为imsi,作为key
                    String imsi = signalling.trim().split(HEADERS_KEY_SEPARATOR)[5];
                    try{
                        headers.put(HEADERS_KEY, imsi);
                    }catch (StringIndexOutOfBoundsException e){
                        logger.error("The message format is invalid. <" + signalling + ">");
                        continue;
                    }
                    //去除imsi为0的无效信令
                    if (!(imsi.equals("000000000000000")||imsi.equals("ffffffffffffffff") || imsi == null||imsi.equals(""))){
                        flage++;
                        Event e = EventBuilder.withBody(signalling.getBytes(), headers);
                        getChannelProcessor().processEvent(e);
                    }
                    //logger.info(" 第"+flag+"条记录是： "+signalling);
                }
                long endTime = System.currentTimeMillis(); //获取结束时间
                logger.info("接收记录数：总共有"+flag+"条记录，其中有效的有"+flage+"条，所耗时间为"+(endTime-startTime)+"ms");
                return Status.READY;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return Status.BACKOFF;
            }
        }
    //by jesusrui 转化2进制
    //by jesusrui 转化2进制
    private String getMessages(byte[] buffer, int off) {
        StringBuilder sb = new StringBuilder();
        //循环遍历，取出多条记录数据
        for (; off < buffer.length; ) {
            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Length
            off += 2;

            sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");//Local Province新增
            off += 2;
            sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");//Local City新增
            off += 2;
            sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");// Owner Province新增
            off += 2;
            sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");//Owner City新增
            off += 2;
            sb.append((buffer[off] & 0xff)).append("|");//Roaming Type新增
            off += 1;


            sb.append(buffer[off] + "|");//Interface
            off += 1;
            sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append("|");//XDR ID
            off += 16;
            sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//RAT
            off += 1;
            sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");//IMSI
            off += 8;
            sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");//IMEI
            off += 8;
            sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");//MSISDN
            off += 16;
            sb.append((buffer[off] & 0xff)).append("|");//Procedure Type
            off += 1;
            sb.append(ConvToByte.byteToLong(buffer, off)).append("|");//Procedure Start Time
            off += 8;
            sb.append(ConvToByte.byteToLong(buffer, off)).append("|");//Procedure End Time
            off += 8;

            sb.append(ConvToByte.bytes2Double(buffer, off)).append("|");//StartLocation-longitude**新增
            off += 8;
            sb.append(ConvToByte.bytes2Double(buffer, off)).append("|");//StartLocation-latitude**新增
            off += 8;
            sb.append((buffer[off] & 0xff)).append("|");//Location Source新增
            off += 1;
            sb.append((buffer[off] & 0xff)).append("|");// MsgFlag新增
            off += 1;


            sb.append((buffer[off] & 0xff)).append("|");//Procedure Status
            off += 1;
//            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Request Cause
//            off += 2;
//            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Failure Cause
//            off += 2;

            sb.append((buffer[off] & 0xff)).append("|");// Request Cause Group新增
            off += 1;
            sb.append((buffer[off] & 0xff)).append("|");// Request Cause Specific新增
            off += 1;
            sb.append((buffer[off] & 0xff)).append("|");// Failure Cause Group新增
            off += 1;
            sb.append((buffer[off] & 0xff)).append("|");// Failure Cause Specific新增
            off += 1;

            sb.append((buffer[off] & 0xff)).append("|");//Keyword 1
            off += 1;
            sb.append((buffer[off] & 0xff)).append("|");//Keyword 2
            off += 1;
            sb.append((buffer[off] & 0xff)).append("|");//Keyword 3
            off += 1;
            sb.append((buffer[off] & 0xff)).append("|");//Keyword 4
            off += 1;
            sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//MME UE S1AP ID
            off += 4;
            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Old MME Group ID
            off += 2;
            sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//Old MME Code
            off += 1;
            sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append("|");//Old M-TMSI
            off += 4;

//            Old GUTI type
            sb.append((buffer[off] & 0xff)).append("|");// Old GUTI type新增
            off += 1;

            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//MME Group ID
            off += 2;
            sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//MME Code
            off += 1;
            sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append("|");//M-TMSI
            off += 4;
            sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append("|");//TMSI
            off += 4;
            sb.append(ConvToByte.getIpv4(buffer, off)).append("|");//USER_IPv4
            off += 4;
            sb.append(ConvToByte.getIpv6(buffer, off)).append("|");//USER_IPv6
            off += 16;
            sb.append(ConvToByte.getIp(buffer, off)).append("|");//MME IP Add
            off += 16;
            sb.append(ConvToByte.getIp(buffer, off)).append("|");//eNB IP Add
            off += 16;
            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//MME Port
            off += 2;
            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//eNB Port
            off += 2;
            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//TAC
            off += 2;
            sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//Cell ID
            off += 4;
            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Other TAC
            off += 2;
            sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//Other ECI
            off += 4;
            sb.append(ConvToByte.getHexString(buffer, off, 32)).append("|");//APN
            off += 32;

            sb.append((buffer[off] & 0xff)).append("|");//VoiceDomain 新增字段
            off += 1;
            sb.append((buffer[off] & 0xff)).append("|");//Vopsopt 新增字段
            off += 1;

            int epsBearerNum = ConvToByte.byteToUnsignedByte(buffer, off);
            sb.append(epsBearerNum + "|");//EPS Bearer Number
            off += 1;
            for (int n = 0; n < epsBearerNum; n++) {
                sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//Bearer 1 ID
                off += 1;
                sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//Bearer 1 Type
                off += 1;
                sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//Bearer 1 QCI
                off += 1;
                sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//Bearer 1 Status
                off += 1;
//                sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Bearer 1 Request Cause
//                off += 2;
//                sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Bearer 1 Failure Cause
//                off += 2;
//
                sb.append((buffer[off] & 0xff)).append("|");//Bearer 1 Request Cause Group 新增字段
                off += 1;
                sb.append((buffer[off] & 0xff)).append("|");//Bearer 1 Request Cause Specific 新增字段
                off += 1;
                sb.append((buffer[off] & 0xff)).append("|");//Bearer 1 Failure Cause Group 新增字段
                off += 1;
                sb.append((buffer[off] & 0xff)).append("|");//Bearer 1 Failure Cause Specific 新增字段
                off += 1;

                sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//Bearer 1 eNB GTP-TEID
                off += 4;
                sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//Bearer 1 SGW GTP-TEID
                off += 4;

            }
            sb.deleteCharAt(sb.lastIndexOf("|"));
            //跳过换行符,并添加换行符，中兴的是 /n/r
            off += 1;
            sb.append("\n");
        }
        sb.deleteCharAt(sb.lastIndexOf("\n"));
        return sb.toString();
    }
//    private String getMessages(byte[] buffer,int off){
//        StringBuilder sb = new StringBuilder();
//        //循环遍历，取出多条记录数据
//        for(;off<buffer.length;) {
//       //for(int i=0;i<buffer.length;i++){
//            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Length
//            off += 2;
//            sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");//City
//            off += 2;
//            sb.append(buffer[off] + "|");//Interface
//            off += 1;
//            sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append("|");//XDR ID
//            off += 16;
//            sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//RAT
//            off += 1;
//            sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");//IMSI
//            off += 8;
//            sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");//IMEI
//            off += 8;
//            sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");//MSISDN
//            off += 16;
//            sb.append((buffer[off] & 0xff)).append("|");//Procedure Type
//            off += 1;
//            sb.append(ConvToByte.byteToLong(buffer, off)).append("|");//Procedure Start Time
//            off += 8;
//            sb.append(ConvToByte.byteToLong(buffer, off)).append("|");//Procedure End Time
//            off += 8;
//            sb.append((buffer[off] & 0xff)).append("|");//Procedure Status
//            off += 1;
//            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Request Cause
//            off += 2;
//            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Failure Cause
//            off += 2;
//            sb.append((buffer[off] & 0xff)).append("|");//Keyword 1
//            off += 1;
//            sb.append((buffer[off] & 0xff)).append("|");//Keyword 2
//            off += 1;
//            sb.append((buffer[off] & 0xff)).append("|");//Keyword 3
//            off += 1;
//            sb.append((buffer[off] & 0xff)).append("|");//Keyword 4
//            off += 1;
//            sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//MME UE S1AP ID
//            off += 4;
//            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Old MME Group ID
//            off += 2;
//            sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//Old MME Code
//            off += 1;
//            sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append("|");//Old M-TMSI
//            off += 4;
//            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//MME Group ID
//            off += 2;
//            sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//MME Code
//            off += 1;
//            sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append("|");//M-TMSI
//            off += 4;
//            sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append("|");//TMSI
//            off += 4;
//            sb.append(ConvToByte.getIpv4(buffer, off)).append("|");//USER_IPv4
//            off += 4;
//            sb.append(ConvToByte.getIpv6(buffer, off)).append("|");//USER_IPv6
//            off += 16;
//            sb.append(ConvToByte.getIp(buffer, off)).append("|");//MME IP Add
//            off += 16;
//            sb.append(ConvToByte.getIp(buffer, off)).append("|");//eNB IP Add
//            off += 16;
//            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//MME Port
//            off += 2;
//            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//eNB Port
//            off += 2;
//            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//TAC
//            off += 2;
//            sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//Cell ID
//            off += 4;
//            sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Other TAC
//            off += 2;
//            sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//Other ECI
//            off += 4;
//            sb.append(ConvToByte.getHexString(buffer, off, 32)).append("|");//APN
//            off += 32;
//
//            sb.append((buffer[off] & 0xff)).append("|");//VoiceDomain 新增字段
//            off +=1;
//            sb.append((buffer[off] & 0xff)).append("|");//Vopsopt 新增字段
//            off +=1;
//
//            int epsBearerNum = ConvToByte.byteToUnsignedByte(buffer, off);
//            sb.append(epsBearerNum + "|");//EPS Bearer Number
//            off += 1;
//            for (int n = 0; n < epsBearerNum; n++) {
//                sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//Bearer 1 ID
//                off += 1;
//                sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//Bearer 1 Type
//                off += 1;
//                sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//Bearer 1 QCI
//                off += 1;
//                sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");//Bearer 1 Status
//                off += 1;
//                sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Bearer 1 Request Cause
//                off += 2;
//                sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Bearer 1 Failure Cause
//                off += 2;
//                sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//Bearer 1 eNB GTP-TEID
//                off += 4;
//                sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//Bearer 1 SGW GTP-TEID
//                off += 4;
//
//            }
//            sb.deleteCharAt(sb.lastIndexOf("|"));
//            //跳过换行符,并添加换行符，中兴的是 /n/r
//            off +=1;
//            sb.append("\n");
//        }
//        sb.deleteCharAt(sb.lastIndexOf("\n"));
//        return sb.toString();
//    }
}