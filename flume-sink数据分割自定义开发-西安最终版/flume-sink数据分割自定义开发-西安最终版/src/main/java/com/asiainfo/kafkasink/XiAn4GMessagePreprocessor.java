/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package com.asiainfo.kafkasink;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an example of a <code>MessagePreprocessor</code> implementation.
 */
public class XiAn4GMessagePreprocessor implements MessagePreprocessor {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    /**
     * extract the hour of the time stamp as the key. So the data is partitioned
     * per hour.
     * @param event This is the Flume event that will be sent to Kafka
     * @param context The Flume runtime context.
     * @return Hour of the timestamp
     */
    @Override
    public String extractKey(Event event, Context context) {
        // get timestamp header if it's present.
        String key = event.getHeaders().get("key");
        return key;
    }

    /**
     * A custom property is read from the Flume config.
     * @param event This is the Flume event that will be sent to Kafka
     * @param context The Flume runtime context.
     * @return topic provided as a custom property
     */
    @Override
    public String extractTopic(Event event, Context context) {
        return context.getString("topic", "default-topic");

        //by chenrui 待测试
        //return context.getString("topic", "default-topic");
    }

//    /**
//     * Trying to prepend each message with the timestamp.
//     * @return modified message of the form: timestamp + ":" + original message body
//     */
//    @Override
//    public String transformMessage(String messageBody) {
//        StringBuilder sb=new StringBuilder();
//        if( StringUtils.isNotEmpty(messageBody)){
//            String[] msg = messageBody.split("\\|");
//            sb.append(msg[1]).append("|");//City :912
//            sb.append(TimeStamp2Date(msg[9])).append("|");//Procedure_Start_Time :1491011061129->yyyyMMddHHmmss
//            sb.append(TimeStamp2Date(msg[10])).append("|");//Procedure_End_Time :1491011061129->yyyyMMddHHmmss
//            sb.append(msg[7].replace("f","")).append("|");//MSISDN :15162262783fffffffffffffffffffff->15162262783
//            sb.append(msg[5]).append("|");//IMSI
//            sb.append(msg[6]).append("|");//IMEI
//            sb.append(msg[8]).append("|");//Procedure_Type
//            sb.append(msg[1]).append("|");//City
//            sb.append(msg[32]).append("|");//TAC
//            sb.append(msg[33]).append("|");//Cell_ID
//            sb.append(msg[34]).append("|");//Other_TAC
//            sb.append(msg[35]).append("|");//Other_ECI
//            sb.append("MME").append("|");//Interface
//            sb.append("|");//toPhoneNum
//            sb.append(msg[5]).append("|");//toimsi
//            sb.append("|");//toimei
//            sb.append("0").append("|");//calltime
//            sb.append(msg[11]);//Procedure_Status
//            return sb.toString();
//        }else
//        {
//            return messageBody;
//        }
//    }
    /**
     * Trying to prepend each message with the timestamp.
     * @return modified message of the form: timestamp + ":" + original message body
     */
    @Override
    public String transformMessage(String messageBody) {
        StringBuilder sb=new StringBuilder();
        if( StringUtils.isNotEmpty(messageBody)){
            String[] msg = messageBody.split("\\|");
            sb.append(msg[2]).append("|");//City :912
            sb.append(TimeStamp2Date(msg[13])).append("|");//Procedure_Start_Time :1491011061129->yyyyMMddHHmmss
            sb.append(TimeStamp2Date(msg[14])).append("|");//Procedure_End_Time :1491011061129->yyyyMMddHHmmss
            sb.append(msg[11].replace("f","")).append("|");//MSISDN :15162262783fffffffffffffffffffff->15162262783
            sb.append(msg[9]).append("|");//IMSI
            sb.append(msg[10]).append("|");//IMEI
            sb.append(msg[12]).append("|");//Procedure_Type
            sb.append(msg[2]).append("|");//City
            sb.append(msg[43]).append("|");//TAC
            sb.append(msg[44]).append("|");//Cell_ID
            sb.append(msg[45]).append("|");//Other_TAC
            sb.append(msg[46]).append("|");//Other_ECI
            sb.append("MME").append("|");//Interface
            sb.append("|");//toPhoneNum
            sb.append(msg[9]).append("|");//toimsi
            sb.append("|");//toimei
            sb.append("0").append("|");//calltime
            sb.append(msg[19]);//Procedure_Status
            return sb.toString();
        }else
        {
            return messageBody;
        }
    }
    // 将unix时间转成格式化时间
    public String TimeStamp2Date(String timestampString){
        Long timestamp = Long.parseLong(timestampString);
        String date = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date(timestamp));
        return date;
    }
}
