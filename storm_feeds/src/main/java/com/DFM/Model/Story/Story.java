package com.DFM.Model.Story;

import com.DFM.Model.WordPress.Image;
import com.DFM.Util.XmlUtil;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.Serializable;


@XmlRootElement(name = "story")
public class Story implements Serializable {
    private static final long serialVersionUID = -3525006222516929810L;
    private String title;
    private String guid;
    private String md5;
    private String updateTest;
    private String viewURI;
    private StoryLinks storyLinks;
    private StorySubjects storySubjects;
    private Image[] images;

    public static Story fromXML(String xml) throws JAXBException, ParserConfigurationException, SAXException, IOException {
        JAXBContext jc = JAXBContext.newInstance(Story.class);
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        JAXBElement<Story> je1 = unmarshaller.unmarshal(XmlUtil.deserialize(xml), Story.class);
        return je1.getValue();
    }

    public String toXml() throws Exception {
        Document doc = XmlUtil.deserialize(this);
        return XmlUtil.XMLtoString(doc);
    }

    public String getTitle() {
        return title;
    }

    @XmlElement(name = "title")
    public void setTitle(String title) {
        this.title = title;
    }

    public String getGuid() {
        return guid;
    }

    @XmlElement(name = "guid")
    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getUpdateTest() {
        return updateTest;
    }

    @XmlElement(name = "updateTest")
    public void setUpdateTest(String updateTest) {
        this.updateTest = updateTest;
    }

    public String getMd5() {
        return md5;
    }

    @XmlElement(name = "md5")
    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public StoryLinks getStoryLinks() {
        return storyLinks;
    }

    @XmlElement(name = "storyLinks")
    public void setStoryLinks(StoryLinks storyLinks) {
        this.storyLinks = storyLinks;
    }

    public StorySubjects getStorySubjects() {
        return storySubjects;
    }

    @XmlElement(name = "subjects")
    public void setStorySubjects(StorySubjects storySubjects) {
        this.storySubjects = storySubjects;
    }

    public Image[] getImage() {
        return images;
    }


    @XmlElement(name = "images/image")
    public void setImage(Image[] image) {
        this.images = image;
    }

    public String getViewURI() {
        return viewURI;
    }

    @XmlElement(name = "viewURI")
    public void setViewURI(String viewURI) {
        this.viewURI = viewURI;
    }
}
