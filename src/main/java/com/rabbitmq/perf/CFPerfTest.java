package com.rabbitmq.perf;

import com.rabbitmq.tools.json.JSONReader;

import java.util.*;

/**
 * Created by pivotal on 18/10/2016.
 */
public class CFPerfTest {
    public static void main(String[] args) {

        String host = getHost();

        List<String> mainArgs = new ArrayList<String>();

        // pass host parameter
        mainArgs.add("-h");
        mainArgs.add(host);

        // pass the rest of the parameters to PerfTest
        append(mainArgs, "FLAG", "f");
        append(mainArgs, "CONSUMERS", "y");
        append(mainArgs, "PRODUCERS", "x");


        String[] arrayArgs = mainArgs.toArray(new String[mainArgs.size()]);

        System.err.println(Arrays.toString(arrayArgs));

        PerfTest.main(arrayArgs);


    }
    private static boolean has(String env) {
        return System.getenv(env) != null;
    }
    private static void append(List<String> args, String env, String flag) {
        if (has(env)) {
            args.add("-"+flag);
            args.add(System.getenv(env));
        }
    }
    private static String getHost() {
        String vcapServices = System.getenv("VCAP_SERVICES");
        JSONReader reader = new JSONReader();
        Map<String,Object> vcapMap = (Map<String, Object>) reader.read(vcapServices);
        List<Object> rmq = (List<Object>) vcapMap.get("p-rabbitmq");
        Map<String,Object> rmqSettings = (Map<String,Object>) rmq.get(0);
        Map<String,Object> creds = (Map<String,Object>) rmqSettings.get("credentials");
        Map<String,Object> protocols = (Map<String,Object>) creds.get("protocols");
        Map<String,Object> amqpProtocol = (Map<String,Object>) protocols.get("amqp");
        String amqpURI = (String)amqpProtocol.get("uri");

        return amqpURI;
    }
}
