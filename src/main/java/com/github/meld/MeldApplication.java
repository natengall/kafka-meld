package com.github.meld;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;

public class MeldApplication extends Application<Configuration> {

    public void run(Configuration configuration, Environment environment) throws Exception {

        environment.jersey().register(new IndexController());
        environment.jersey().register(new ConnectController());
        environment.jersey().register(new KafkaController());
    }

    public static void main(String[] args) throws Exception {
        new MeldApplication().run(args);
    }
}