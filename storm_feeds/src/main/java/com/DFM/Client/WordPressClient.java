package com.DFM.Client;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;


public class WordPressClient {
    private String host;
    private Integer port;
    private Integer timeout;
    private String username;
    private String password;

    public WordPressClient() {
    }

    public WordPressClient(String host, String username, String password) {
        this.host = host;
        this.username = username;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
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

    public HashMap<String, String> post(String endpoint, String json) throws IOException {
        String function = "post(String endpoint, String json)";
        return notImplemented(function);
    }

    public HashMap<String, String> post(String endpoint) throws IOException {
        String function = "post(String endpoint";
        return notImplemented(function);
    }

    public HashMap<String, String> delete(String endpoint) throws IOException {
        String function = "post(String endpoint";
        return notImplemented(function);
    }

    public HashMap<String, String> uploadImage(String endpoint, String imageSource, String imageType, String imageName) throws IOException, ParseException, java.text.ParseException {
        String function = "uploadImage(String endpoint, String imageSource, String imageType, String imageName)";
        return notImplemented(function);
    }

    public String get(String endpoint) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(endpoint);
        HttpResponse response = client.execute(request);
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        String result = "";
        String line = "";
        while ((line = rd.readLine()) != null) {
            result += line;
        }
        return result;
    }

    private HashMap<String, String> notImplemented(String function) {
        HashMap<String, String> mapResult = new HashMap<String, String>();
        mapResult.put("code", Integer.toString(900));
        mapResult.put("result", function + " Not implemented in WordPressClient");
        return mapResult;
    }
}
