package com.rabbitmq.perf;

import com.rabbitmq.tools.json.JSONReader;

import java.util.*;

/**
 * Created by pivotal on 18/10/2016.
 */
public class CFPerfTest {
    public static void main(String[] args) {

        String flags[] = { "flag", "consumers", "multiAckEvery", "autoack", "exchange", "routingKey",
                "qos", "consumerRate", "size", "type", "queue", "rate", "producers", };
        String host = getHost();

        List<String> mainArgs = new ArrayList<String>();

        // pass host parameter
        mainArgs.add("-h");
        mainArgs.add(host);

        mainArgs.add("--predeclared");

        // pass the rest of the parameters to PerfTest
        for (int i = 0; i < flags.length; i++) {
            append(mainArgs, flags[i]);
        }


        String[] arrayArgs = mainArgs.toArray(new String[mainArgs.size()]);

        System.out.println(Arrays.toString(arrayArgs));

        PerfTest.main(arrayArgs);


    }
    private static boolean has(String env) {
        return System.getenv(env) != null;
    }
    private static void append(List<String> args, String env) {
        if (has(env)) {
            args.add("--"+env);
            args.add(System.getenv(env));
        }
    }
    private static String getHost() {
        String vcapServices = System.getenv("VCAP_SERVICES");
        JSONReader reader = new JSONReader();
        Map<String,Object> services = (Map<String, Object>) reader.read(vcapServices);
        String amqpURI = null;
        for (Object vcapEntry : services.values()) {
            List<Object> serviceInstances = (List<Object>) vcapEntry;
            for (Object serviceInstanceObject : serviceInstances) {
                Map<String, Object> serviceInstance = (Map<String, Object>) serviceInstanceObject;
                if (isRabbitMqServer(serviceInstance)) {
                    Map<String, Object> creds = (Map<String, Object>) serviceInstance.get("credentials");
                    Map<String, Object> protocols = (Map<String, Object>) creds.get("protocols");
                    Map<String, Object> amqpProtocol = (Map<String, Object>) protocols.get("amqp");
                    amqpURI = (String) amqpProtocol.get("uri");
                }
            }
        }
        return amqpURI;
    }
    private static boolean isRabbitMqServer(Map<String, Object> serviceInstance) {
        List<Object> tags = (List<Object>) serviceInstance.get("tags");
        return tags.contains("rabbitmq");
    }

}
