package com.DFM.Client;

import com.DFM.Model.Helper.Publisher;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.xml.sax.InputSource;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;


public class WebClient {
    private String url;
    private Integer port;
    private Integer timeout;
    private String username;
    private String password;

    public WebClient() {
    }

    public WebClient(String url) {
        this.url = url;
        this.port = 80;
        this.timeout = 20000;
        this.username = "";
        this.password = "";
    }

    public WebClient(String url, Integer port, Integer timeout, String username, String password) {
        this.url = url;
        this.port = port;
        this.timeout = timeout;
        this.username = username;
        this.password = password;
    }

    public WebClient(Publisher publisher) {
        this.url = publisher.getUrl();
        this.port = 80;
        this.timeout = 20000;
        if (publisher.getUsername() != null) {
            this.username = publisher.getUsername();
            this.password = publisher.getPassword();
        }
    }

    public WebClient(String url, String username, String password) {
        this.url = url;
        this.port = 80;
        this.timeout = 20000;
        this.username = username;
        this.password = password;
    }

    public static Boolean isOK(Integer code) {
        return code >= 200 && code < 300;
    }

    public static Boolean isBad(Integer code) {
        return !isOK(code);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String get() throws IOException {
        String credentials = this.username + ":" + this.password;
        String credentials64 = Base64.encodeBase64URLSafeString(credentials.getBytes());
        while ((credentials64.length() % 4) > 0) {
            credentials64 += "=";
        }
        String authorizationString = "Basic " + credentials64;

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(this.url);
        request.setHeader("Authorization", authorizationString);

        HttpResponse response = client.execute(request);
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        String result = "";
        String line = "";
        while ((line = rd.readLine()) != null) {
            result += line;
        }

        return result;
    }

    public InputSource openStream() throws IOException {
        String credentials = this.username + ":" + this.password;
        String credentials64 = Base64.encodeBase64URLSafeString(credentials.getBytes());
        while ((credentials64.length() % 4) > 0) {
            credentials64 += "=";
        }
        String authorizationString = "Basic " + credentials64;

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(this.url);
        request.setHeader("Authorization", authorizationString);

        HttpResponse response = client.execute(request);
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        String result = "";
        String line = "";
        while ((line = rd.readLine()) != null) {
            result += line;
        }

        return new InputSource(new StringReader(result));
    }


    public BufferedImage GetImage() throws IOException {
        BufferedImage image = null;
        URL imageURL = new URL(this.url);
        image = ImageIO.read(imageURL);
        return image;
    }

}