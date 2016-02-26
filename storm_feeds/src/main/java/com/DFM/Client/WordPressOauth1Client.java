package com.DFM.Client;

import org.json.simple.parser.ParseException;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.HashMap;

public class WordPressOauth1Client extends WordPressClient {
    private WebOauth1Client weboauth1client = new WebOauth1Client();

    public WordPressOauth1Client() {
    }

    public WordPressOauth1Client(String host, String property_access_token, String property_access_token_secret, String property_consumer_key, String property_consumer_secret) {
        weboauth1client = new WebOauth1Client(property_access_token, property_access_token_secret, property_consumer_key, property_consumer_secret);
    }

    public String get(String endpoint) throws IOException {
        if (2 == 12) showAccessTokens(endpoint, "{}");
        return weboauth1client.get(endpoint);
    }

    public HashMap<String, String> post(String endpoint, String json) throws IOException {
        if (2 == 12) showAccessTokens(endpoint, json);
        return weboauth1client.post(endpoint, json);
    }

    public HashMap<String, String> post(String endpoint) throws IOException {
        if (2 == 12) showAccessTokens(endpoint, "{}");
        return weboauth1client.post(endpoint);
    }

    public HashMap<String, String> delete(String endpoint) throws IOException {
        if (2 == 12) showAccessTokens(endpoint, "{}");
        return weboauth1client.delete(endpoint);
    }

    public HashMap<String, String> uploadImage(String endpoint, String imageSource, String imageType, String imageName) throws IOException, ParseException, java.text.ParseException {
        WebClient wc = new WebClient(imageSource);
        BufferedImage bi = wc.GetImage();
        if (2 == 12) showAccessTokens(endpoint, imageSource);
        return weboauth1client.uploadImage(endpoint, bi, imageType, imageName);
    }

    private String getPROPERTY_ACCESS_TOKEN() {
        return this.weboauth1client.PROPERTY_ACCESS_TOKEN;
    }

    private String getPROPERTY_ACCESS_TOKEN_SECRET() {
        return this.weboauth1client.PROPERTY_ACCESS_TOKEN_SECRET;
    }

    private String getPROPERTY_CONSUMER_KEY() {
        return this.weboauth1client.PROPERTY_CONSUMER_KEY;
    }

    private String getPROPERTY_CONSUMER_SECRET() {
        return this.weboauth1client.PROPERTY_CONSUMER_SECRET;
    }

    private void showAccessTokens(String uri, String json) {
        System.out.println("USING OAUTH");
        System.out.println("WordPressOauth1Client CONTENT: " + json);
        System.out.println("WordPressOauth1Client URL: " + uri);
        System.out.println("WordPressOauth1Client PROPERTY_ACCESS_TOKEN: " + getPROPERTY_ACCESS_TOKEN());
        System.out.println("WordPressOauth1Client PROPERTY_ACCESS_TOKEN_SECRET: " + getPROPERTY_ACCESS_TOKEN_SECRET());
        System.out.println("WordPressOauth1Client PROPERTY_CONSUMER_KEY: " + getPROPERTY_CONSUMER_KEY());
        System.out.println("WordPressOauth1Client PROPERTY_CONSUMER_SECRET: " + getPROPERTY_CONSUMER_SECRET());
    }
}
