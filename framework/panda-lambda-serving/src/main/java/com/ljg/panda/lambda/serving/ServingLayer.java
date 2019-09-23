package com.ljg.panda.lambda.serving;

import com.google.common.base.Preconditions;
import com.ljg.panda.common.io.IOUtils;
import com.ljg.panda.common.settings.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.catalina.*;
import org.apache.catalina.authenticator.DigestAuthenticator;
import org.apache.catalina.authenticator.jaspic.AuthConfigFactoryImpl;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.JreMemoryLeakPreventionListener;
import org.apache.catalina.core.ThreadLocalLeakPreventionListener;
import org.apache.catalina.startup.Tomcat;
import org.apache.coyote.http11.Http11Nio2Protocol;
import org.apache.coyote.http2.Http2Protocol;
import org.apache.tomcat.util.descriptor.web.ErrorPage;
import org.apache.tomcat.util.descriptor.web.LoginConfig;
import org.apache.tomcat.util.descriptor.web.SecurityCollection;
import org.apache.tomcat.util.descriptor.web.SecurityConstraint;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.message.config.AuthConfigFactory;
import javax.servlet.MultipartConfigElement;
import javax.servlet.http.HttpServletResponse;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Top-level implementation of the Serving Layer process.
 */
public class ServingLayer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ServingLayer.class);

    private static final int[] ERROR_PAGE_STATUSES = {
            HttpServletResponse.SC_BAD_REQUEST,
            HttpServletResponse.SC_UNAUTHORIZED,
            HttpServletResponse.SC_NOT_FOUND,
            HttpServletResponse.SC_METHOD_NOT_ALLOWED,
            HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            HttpServletResponse.SC_SERVICE_UNAVAILABLE,
    };

    private final Config config;
    private final String id;
    private final int port;
    private final int securePort;
    private final String userName;
    private final String password;
    private final Path keystoreFile;
    private final String keystorePassword;
    private final String keyAlias;
    private final String contextPathURIBase;
    private final String appResourcesPackages;

    private Tomcat tomcat;
    private Context context;
    private Path noSuchBaseDir;

    public ServingLayer(Config config) {
        Objects.requireNonNull(config);
        log.info("Configuration: \n{}", ConfigUtils.prettyPrint(config));
        this.config = config;
        this.id = ConfigUtils.getOptionalString(config, "panda.id");
        this.port = config.getInt("panda.serving.api.port");
        this.securePort = config.getInt("panda.serving.api.secure-port");
        this.userName = ConfigUtils.getOptionalString(config, "panda.serving.api.user-name");
        this.password = ConfigUtils.getOptionalString(config, "panda.serving.api.password");
        String keystoreFileString =
                ConfigUtils.getOptionalString(config, "panda.serving.api.keystore-file");
        this.keystoreFile = keystoreFileString == null ? null : Paths.get(keystoreFileString);
        this.keystorePassword =
                ConfigUtils.getOptionalString(config, "panda.serving.api.keystore-password");
        this.keyAlias = ConfigUtils.getOptionalString(config, "panda.serving.api.key-alias");
        String contextPathString = config.getString("panda.serving.api.context-path");
        if (contextPathString == null ||
                contextPathString.isEmpty() ||
                "/".equals(contextPathString)) {
            contextPathString = "";
        }
        this.contextPathURIBase = contextPathString;
        this.appResourcesPackages =
                config.getString("panda.serving.application-resources") + "," +
                        "com.ljg.panda.lambda.serving"; // Always append package for e.g. error page
    }

    public void start() throws IOException {
        if (id != null) {
            log.info("Starting Serving Layer {}", id);
        }
        Preconditions.checkState(tomcat == null);
        // Has to happen very early before Tomcat init:
        System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
        noSuchBaseDir = Files.createTempDirectory("Panda");
        noSuchBaseDir.toFile().deleteOnExit();

        Tomcat tomcat = new Tomcat();
        Connector connector = makeConnector();
        configureTomcat(tomcat, connector);
        configureEngine(tomcat.getEngine());
        configureServer(tomcat.getServer());
        configureHost(tomcat.getHost());
        makeContext(tomcat, noSuchBaseDir);

        try {
            tomcat.start();
        } catch (LifecycleException le) {
            throw new IOException(le);
        }
        this.tomcat = tomcat;
    }

    private Connector makeConnector() {
        Connector connector = new Connector(Http11Nio2Protocol.class.getName());

        if (keystoreFile == null) {

            // HTTP connector
            connector.setPort(port);
            connector.setSecure(false);
            connector.setScheme("http");

        } else {

            // HTTPS connector
            connector.setPort(securePort);
            connector.setSecure(true);
            connector.setScheme("https");
            connector.setAttribute("SSLEnabled", "true");
            SSLHostConfig sslHostConfig = new SSLHostConfig();
            SSLHostConfigCertificate cert =
                    new SSLHostConfigCertificate(sslHostConfig, SSLHostConfigCertificate.Type.RSA);
            cert.setCertificateKeystoreFile(keystoreFile.toAbsolutePath().toString());
            cert.setCertificateKeystorePassword(keystorePassword);
            cert.setCertificateKeyAlias(keyAlias);
            sslHostConfig.addCertificate(cert);
            connector.addSslHostConfig(sslHostConfig);
        }

        connector.addUpgradeProtocol(new Http2Protocol());

        // Keep quiet about the server type
        connector.setXpoweredBy(false);

        // Basic tuning params:
        connector.setAttribute("maxThreads", 400);
        connector.setAttribute("acceptCount", 50);
        //connector.setAttribute("connectionTimeout", 2000);
        connector.setAttribute("maxKeepAliveRequests", 100);

        // Avoid running out of ephemeral ports under heavy load?
        connector.setAttribute("socket.soReuseAddress", true);

        connector.setMaxPostSize(0);
        connector.setAttribute("disableUploadTimeout", false);

        // Allow long URLs
        connector.setAttribute("maxHttpHeaderSize", 65536);

        // Enable response compression
        connector.setAttribute("compression", "on");
        // Defaults are text/html,text/xml,text/plain,text/css
        connector.setAttribute("compressableMimeType", "text/html,text/xml,text/plain,text/css,text/csv,application/json");

        return connector;
    }

    private void configureTomcat(Tomcat tomcat, Connector connector) {
        tomcat.setBaseDir(noSuchBaseDir.toAbsolutePath().toString());
        tomcat.setConnector(connector);
    }

    private void configureEngine(Engine engine) {
        if (userName != null && password != null) {
            InMemoryRealm realm = new InMemoryRealm();
            realm.addUser(userName, password);
            engine.setRealm(realm);
        }
    }

    private static void configureServer(Server server) {
        server.addLifecycleListener(new JreMemoryLeakPreventionListener());
        server.addLifecycleListener(new ThreadLocalLeakPreventionListener());
    }

    private static void configureHost(Host host) {
        host.setAutoDeploy(false);
    }

    private void makeContext(Tomcat tomcat, Path noSuchBaseDir) throws IOException {
        Path contextPath = noSuchBaseDir.resolve("context");
        Files.createDirectories(contextPath);

        context = tomcat.addContext(contextPathURIBase, contextPath.toAbsolutePath().toString());

        context.setWebappVersion("3.1");
        context.setName("Panda");

        context.addWelcomeFile("index.html");
        addErrorPages(context);

        // PandaApplication only needs one config value, so just pass it
        context.addParameter(PandaApplication.class.getName() + ".packages", appResourcesPackages);
        // ModelManagerListener will need whole config
        String serializedConfig = ConfigUtils.serialize(config);
        context.addParameter(ConfigUtils.class.getName() + ".serialized", serializedConfig);

        Wrapper wrapper =
                Tomcat.addServlet(context, "Jersey", "org.glassfish.jersey.servlet.ServletContainer");
        wrapper.addInitParameter("javax.ws.rs.Application", PandaApplication.class.getName());
        //wrapper.addInitParameter(OryxApplication.class.getName() + ".packages", appResourcesPackage);
        wrapper.addMapping("/*");
        wrapper.setLoadOnStartup(1);
        wrapper.setMultipartConfigElement(new MultipartConfigElement(""));

        context.addApplicationListener(ModelManagerListener.class.getName());

        // Better way to configure JASPIC?
        AuthConfigFactory.setFactory(new AuthConfigFactoryImpl());

        boolean needHTTPS = keystoreFile != null;
        boolean needAuthentication = userName != null;

        if (needHTTPS || needAuthentication) {

            SecurityCollection securityCollection = new SecurityCollection();
            securityCollection.addPattern("/*");
            SecurityConstraint securityConstraint = new SecurityConstraint();
            securityConstraint.addCollection(securityCollection);

            if (needHTTPS) {
                securityConstraint.setUserConstraint("CONFIDENTIAL");
            }

            if (needAuthentication) {

                LoginConfig loginConfig = new LoginConfig();
                loginConfig.setAuthMethod("DIGEST");
                loginConfig.setRealmName(InMemoryRealm.NAME);
                context.setLoginConfig(loginConfig);

                securityConstraint.addAuthRole(InMemoryRealm.AUTH_ROLE);

                context.addSecurityRole(InMemoryRealm.AUTH_ROLE);
                DigestAuthenticator authenticator = new DigestAuthenticator();
                authenticator.setNonceValidity(10 * 1000L); // Shorten from 5 minutes to 10 seconds
                authenticator.setNonceCacheSize(20000); // Increase from 1000 to 20000
                context.getPipeline().addValve(authenticator);
            }

            context.addConstraint(securityConstraint);
        }

        context.setCookies(false);
    }

    private static void addErrorPages(Context context) {
        for (int errorCode : ERROR_PAGE_STATUSES) {
            ErrorPage errorPage = new ErrorPage();
            errorPage.setErrorCode(errorCode);
            errorPage.setLocation("/error");
            context.addErrorPage(errorPage);
        }
        ErrorPage errorPage = new ErrorPage();
        errorPage.setExceptionType(Throwable.class.getName());
        errorPage.setLocation("/error");
        context.addErrorPage(errorPage);
    }

    /**
     * Blocks and waits until the server shuts down.
     */
    public void await() {
        Server server;
        synchronized (this) {
            server = tomcat.getServer();
        }
        server.await(); // Can't do this with lock held
    }

    /**
     * @return Tomcat's internal context. Really only to be used for testing!
     */
    public Context getContext() {
        return context;
    }

    @Override
    public synchronized void close() throws IOException {
        if (tomcat != null) {
            try {
                tomcat.stop();
                tomcat.destroy();
            } catch (LifecycleException le) {
                log.warn("Unexpected error while stopping", le);
            } finally {
                tomcat = null;
            }
            IOUtils.deleteRecursively(noSuchBaseDir);
        }
    }
}
