package com.example.h2sync.config;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.sql.SQLException;

@Configuration
public class H2ServerConfig {
    private static final Logger log = LoggerFactory.getLogger(H2ServerConfig.class);

    @Value("${h2.tcp.port:9092}")
    private int tcpPort;

    @Value("${h2.tcp.allow-others:true}")
    private boolean allowOthers;

    @Value("${h2.tcp.web-console:false}")
    private boolean webConsole;

    private Server tcpServer;
    private Server webServer;

    @PostConstruct
    public void startServers() throws SQLException {
        tcpServer = Server.createTcpServer(
                "-tcp",
                allowOthers ? "-tcpAllowOthers" : "",
                "-tcpPort", String.valueOf(tcpPort))
            .start();
        log.info("H2 TCP server started at: {}", tcpServer.getURL());

        if (webConsole) {
            webServer = Server.createWebServer("-web", "-webAllowOthers").start();
            log.info("H2 web console started at: {}", webServer.getURL());
        }
    }

    @PreDestroy
    public void stopServers() {
        if (tcpServer != null) {
            tcpServer.stop();
            log.info("H2 TCP server stopped.");
        }
        if (webServer != null) {
            webServer.stop();
            log.info("H2 web server stopped.");
        }
    }
}
