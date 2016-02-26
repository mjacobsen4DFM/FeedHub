package com.DFM.Model.WordPress;

import com.DFM.Util.XmlUtil;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

//@SuppressWarnings("restriction")
public class WordPressPost implements Serializable {
    private static final long serialVersionUID = -3525006222516929810L;

    private String id;

    private String date;

    private String date_gmt;

    ////String modified

    //String modified_gmt

    private String type;

    //link

    private String title;

    private String status;

    private String content;

    private String excerpt;

    private String author;

    //Integer featured_media

    private String comment_status;

    private String ping_status;

    private String sticky;

    private String post_format;

    private ArrayList<Integer> categories;

    private ArrayList<Integer> tags;


/*
    private String post_parent;

    private String password;

    private String menu_order;

    private String name;
*/

    //Variables which are NOT part of the WP-API JSON object, but needed for this interface
    private Images[] images;


    WordPressPost() {
    }

    public static WordPressPost fromXML(String xml) throws JAXBException, ParserConfigurationException, SAXException, IOException {
        JAXBContext jc = JAXBContext.newInstance(WordPressPost.class);
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        JAXBElement<WordPressPost> je1 = unmarshaller.unmarshal(XmlUtil.deserialize(xml), WordPressPost.class);
        return je1.getValue();
    }

    public String getPost_format() {
        return post_format;
    }

    public void setPost_format(String post_format) {
        this.post_format = post_format;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getComment_status() {
        return comment_status;
    }

    public void setComment_status(String comment_status) {
        this.comment_status = comment_status;
    }

/*
    public String getPost_parent() {
        return post_parent;
    }

    public void setPost_parent(String post_parent) {
        this.post_parent = post_parent;
    }
*/

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

/*
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
*/

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getSticky() {
        return sticky;
    }

    public void setSticky(String sticky) {
        this.sticky = sticky;
    }

    public String getTitle() {
        return title;
    }

    @XmlElement(name = "title")
    public void setTitle(String title) {
        this.title = title;
    }

/*
    public String getMenu_order() {
        return menu_order;
    }

    public void setMenu_order(String menu_order) {
        this.menu_order = menu_order;
    }

    public String[] getPost_meta() {
        return post_meta;
    }

    public void setPost_meta(String[] post_meta) {
        this.post_meta = post_meta;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
*/

    public String getID() {
        return id;
    }

    public void setID(String ID) {
        this.id = ID;
    }

    public String getContent() {
        return content;
    }


    @XmlElement(name = "content")
    public void setContent(String content) {
        this.content = content;
    }

    public String getDate_gmt() {
        return date_gmt;
    }

    public void setDate_gmt(String date_gmt) {
        this.date_gmt = date_gmt;
    }

    public String getPing_status() {
        return ping_status;
    }

    public void setPing_status(String ping_status) {
        this.ping_status = ping_status;
    }

    public String getExcerpt() {
        return excerpt;
    }

    @XmlElement(name = "excerpt")
    public void setExcerpt(String excerpt) {
        this.excerpt = excerpt;
    }

    public ArrayList<Integer> getCategories() {
        return categories;
    }

    public void setCategories(ArrayList<Integer> categories) {
        this.categories = categories;
    }

    public ArrayList<Integer> getTags() {
        return tags;
    }

    public void setTags(ArrayList<Integer> tags) {
        this.tags = tags;
    }

    public Images[] getImages() {
        return images;
    }

    @XmlElement(name = "images")
    public void setImages(Images[] images) {
        this.images = images;
    }
}
